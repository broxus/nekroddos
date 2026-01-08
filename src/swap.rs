use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use ed25519_dalek::Keypair;
use everscale_rpc_client::RpcClient;
use futures_util::StreamExt;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, RateLimiter};
use ton_block::AccountStuff;

use crate::models::{
    EverWalletInfo, GenericDeploymentInfo, PayloadGeneratorsData, PayloadMeta, SendData,
};
use crate::util::TestEnv;
use crate::{app_cache, send, Args};

#[derive(Parser, Debug, Clone)]
pub struct SwapTestArgs {
    #[clap(short, long)]
    /// total swaps per wallet
    num_swaps: usize,

    #[clap(short, long)]
    rps: u32,

    #[clap(short, long, default_value = "5")]
    /// swap depth
    depth: u8,
}

pub async fn run(
    swap_args: SwapTestArgs,
    common_args: Args,
    keypair: &Keypair,
    client: RpcClient,
) -> Result<()> {
    if swap_args.depth < 2 {
        panic!("Depth should be at least 2");
    }

    let base_deployments_path = common_args.project_root.join("deployments");
    let network_deployments_path = if let Some(network_name) = &common_args.network {
        base_deployments_path.join(network_name)
    } else {
        base_deployments_path
    };

    if common_args.network.is_some() && !network_deployments_path.is_dir() {
        return Err(anyhow::anyhow!(
            "Specified network deployment directory not found: {:?}",
            network_deployments_path
        ));
    }

    let mut recipients = Vec::new();
    let mut pool_addresses = Vec::new();

    for file in walkdir::WalkDir::new(&network_deployments_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().map(|e| e == "json").unwrap_or(false))
    {
        let filename = file.file_name().to_string_lossy();
        if filename.contains("commonAccount") {
            let wallet_info: EverWalletInfo = serde_json::from_slice(&std::fs::read(file.path())?)?;
            recipients.push(wallet_info.address);
        }
        if filename.contains("DexPair") {
            let info: GenericDeploymentInfo = serde_json::from_slice(&std::fs::read(file.path())?)?;
            pool_addresses.push(info.address);
        }
    }

    log::info!(
        "Found {} wallets and {} pools",
        recipients.len(),
        pool_addresses.len()
    );
    recipients.sort();

    let app_cache = app_cache::AppCache::new(client.clone(), common_args.seed)
        .load_states(pool_addresses)
        .await
        .load_tokens_and_token_pairs()
        .await;

    log::info!("Loaded app cache");

    let start = std::time::Instant::now();
    let temp_client = client.clone();
    let filtered_recipients = futures_util::stream::iter(recipients)
        .map(move |addr| {
            let client = temp_client.clone();
            async move {
                let state_exists = client
                    .get_contract_state(&addr, None)
                    .await
                    .unwrap()
                    .is_some();
                (state_exists, addr)
            }
        })
        .buffered(100)
        .filter_map(|(state_exists, addr)| async move {
            if state_exists {
                Some(addr)
            } else {
                None
            }
        });

    let mut payloads = Vec::new();
    let mut filtered_recipients = std::pin::pin!(filtered_recipients);

    while let Some(recipient) = filtered_recipients.next().await {
        let payload_meta = app_cache.generate_payloads(recipient.clone(), swap_args.depth);
        let send_data = SendData::new(
            payload_meta,
            Keypair::from_bytes(&keypair.to_bytes())?,
            recipient,
        );
        payloads.push(send_data);
    }

    log::info!(
        "Generated {} payloads in {:?}",
        payloads.len(),
        start.elapsed()
    );

    let test_env = TestEnv::new(
        swap_args.num_swaps as u32,
        swap_args.rps,
        payloads.len(),
        client,
        common_args.seed,
        common_args.clone(),
    );

    for payload in payloads {
        let test_env = test_env.clone();
        tokio::spawn(async move { process_payload(payload, test_env).await });
    }
    log::info!("Spawned dudos tasks");

    test_env.spawn_progress_printer();
    test_env.barrier.wait().await;
    Ok(())
}

async fn process_payload(mut send_data: SendData, test_env: TestEnv) {
    let TestEnv {
        barrier,
        num_iterations: num_swaps,
        rate_limiter: rl,
        counter,
        client,
        ..
    } = test_env;

    let state = client
        .get_contract_state(&send_data.sender_addr, None)
        .await
        .unwrap()
        .unwrap();
    let account = Arc::new(state.account);

    let mut generator = load_generator(send_data.payload_generators.clone());
    let jitter = Jitter::new(Duration::from_millis(1), Duration::from_millis(50));
    for _ in 0..num_swaps {
        if let Err(e) = send_forward_and_backward(
            &client,
            &mut send_data,
            account.clone(),
            &rl,
            jitter,
            &mut generator,
            &test_env.args,
        )
        .await
        {
            log::info!("Failed to send: {:?}", e);
            continue;
        }
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    barrier.wait().await;
}

async fn send_forward_and_backward(
    client: &RpcClient,
    payload: &mut SendData,
    state: Arc<AccountStuff>,
    rl: &RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    jitter: Jitter,
    generator: &mut tokio::sync::mpsc::Receiver<(PayloadMeta, PayloadMeta)>,
    app_args: &Args,
) -> Result<()> {
    let (forward_meta, backward_meta) = generator.recv().await.unwrap();

    async fn send_transaction(
        client: RpcClient,
        payload: SendData,
        meta: PayloadMeta,
        state: Arc<AccountStuff>,
    ) -> Result<()> {
        let outcome = send::send(
            &client,
            &payload.signer,
            payload.sender_addr.clone(),
            meta.payload,
            meta.destination,
            3_000_000_000,
            &state,
        )
        .await?;
        outcome.broadcast_result?;
        Ok(())
    }

    async fn process_transaction(
        client: RpcClient,
        payload: SendData,
        meta: PayloadMeta,
        state: Arc<AccountStuff>,
        rl: &RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
        jitter: Jitter,
        no_wait: bool,
    ) -> Result<()> {
        rl.until_ready_with_jitter(jitter).await;
        let handle =
            tokio::spawn(async move { send_transaction(client, payload, meta, state).await });

        if !no_wait {
            handle.await??;
        }

        Ok(())
    }

    // Process forward transaction
    process_transaction(
        client.clone(),
        payload.clone(),
        forward_meta,
        state.clone(),
        rl,
        jitter,
        app_args.no_wait,
    )
    .await?;

    // Process backward transaction
    process_transaction(
        client.clone(),
        payload.clone(),
        backward_meta,
        state,
        rl,
        jitter,
        app_args.no_wait,
    )
    .await?;

    Ok(())
}

fn load_generator(
    mut generator: Arc<PayloadGeneratorsData>,
) -> tokio::sync::mpsc::Receiver<(PayloadMeta, PayloadMeta)> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        let generator = Arc::make_mut(&mut generator);
        loop {
            let forward = generator.forward.generate_payload_meta();
            let backward = generator.backward.generate_payload_meta();
            if tx.send((forward, backward)).await.is_err() {
                break;
            }
        }
    });
    rx
}
