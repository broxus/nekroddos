use crate::abi::{get_wallet, GetWalletFunctionInput, GetWalletFunctionOutput};
use crate::models::GenericDeploymentInfo;
use crate::util::TestEnv;
use crate::{send, Args};
use anyhow::{Context, Result};
use clap::Parser;
use ed25519_dalek::Keypair;
use everscale_rpc_client::RpcClient;
use governor::Jitter;
use nekoton_abi::{FunctionExt, PackAbiPlain, UnpackAbiPlain};
use nekoton_utils::SimpleClock;
use rand::prelude::{SliceRandom, StdRng};
use rand::{Rng, SeedableRng};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::UnboundedSender;
use ton_block::MsgAddressInt;
use ton_types::{BuilderData, UInt256};

#[derive(Parser, Debug, Clone)]
pub struct SendTestArgs {
    #[clap(short, long)]
    total_wallets: u32,
    #[clap(short, long)]
    rps: u32,
    #[clap(short, long)]
    num_iterations: u32,

    #[clap(long, default_value = "false")]
    only_stats: bool,

    #[clap(long)]
    log_file: Option<PathBuf>,
}
pub async fn run(
    swap_args: SendTestArgs,
    common_args: Args,
    key_pair: Arc<Keypair>,
    client: RpcClient,
) -> Result<()> {
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

    log::info!("Using deployments path: {:?}", network_deployments_path);

    let factory_abi = walkdir::WalkDir::new(&network_deployments_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().map(|e| e == "json").unwrap_or(false))
        .find(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.to_lowercase().contains("factory"))
                .unwrap_or(false)
        })
        .context("No factory abi")?;
    let factory: GenericDeploymentInfo =
        serde_json::from_slice(&std::fs::read(factory_abi.path())?)?;
    let mut recievers = get_wallets(
        client.clone(),
        &factory.address,
        swap_args.total_wallets,
        key_pair.public.to_bytes(),
    )
    .await
    .context("Failed to get wallets")?;
    recievers.sort();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    if let Some(log_file) = swap_args.log_file.clone() {
        tokio::spawn(async move {
            let file = tokio::fs::File::create(log_file).await.unwrap();
            let mut file = tokio::io::BufWriter::new(file);
            while let Some(addr) = rx.recv().await {
                file.write_all(addr.as_bytes()).await.unwrap();
                file.write_all(b"\n").await.unwrap();
            }
        });
    }

    spawn_ddos_jobs(&swap_args, client, recievers, common_args, key_pair, tx).await?;

    Ok(())
}

async fn spawn_ddos_jobs(
    args: &SendTestArgs,
    client: RpcClient,
    recievers: Vec<MsgAddressInt>,
    common_args: Args,
    key_pair: Arc<Keypair>,
    tx: UnboundedSender<String>,
) -> Result<()> {
    let test_env = TestEnv::new(
        args.num_iterations,
        args.rps,
        args.total_wallets as usize,
        client,
        common_args.seed,
        common_args.clone(),
    );

    if args.only_stats {
        test_env.set_counter(args.num_iterations as u64 * recievers.len() as u64);
        // print_stats(recievers, &test_env).await;
        return Ok(());
    }

    log::info!("Spawning ddos jobs for {} recievers", recievers.len());
    let receivers = Arc::new(recievers);

    let mut rng = StdRng::seed_from_u64(test_env.seed.unwrap_or_else(rand::random));
    for receiver in receivers.iter() {
        let seed = rng.gen();
        let rng = StdRng::seed_from_u64(seed); // Each job should have its own rng
        let env = test_env.clone();
        let receivers = receivers.clone();
        let key_pair = key_pair.clone();
        let tx = tx.clone();
        tokio::spawn(ddos_job(
            env,
            receiver.clone(),
            receivers,
            key_pair,
            rng,
            tx,
        ));
    }
    log::info!("All jobs spawned");

    let handle = test_env.spawn_progress_printer();
    test_env.barrier.wait().await;
    handle.abort();

    Ok(())
}

async fn ddos_job(
    test_env: TestEnv,
    from: MsgAddressInt,
    wallets: Arc<Vec<MsgAddressInt>>,
    signer: Arc<Keypair>,
    mut rng: StdRng,
    tx: UnboundedSender<String>,
) -> Result<()> {
    let jitter = Jitter::new(Duration::from_millis(1), Duration::from_millis(50));
    let state = test_env
        .client
        .get_contract_state(&from, None)
        .await?
        .unwrap_or_else(|| panic!("No state for {from}"))
        .account;

    for _ in 1..=test_env.num_iterations {
        let rand_dst = wallets.choose(&mut rng).unwrap().clone();

        test_env.rate_limiter.until_ready_with_jitter(jitter).await;
        let h = {
            let client = test_env.client.clone();
            let counter = test_env.counter.clone();
            let signer = signer.clone();
            let from = from.clone();
            let state = state.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                let addr_str = rand_dst.to_string();
                let outcome = send::send(
                    &client,
                    &signer,
                    from,
                    BuilderData::new(),
                    rand_dst,
                    1_000_000,
                    &state,
                )
                .await
                .unwrap();
                outcome.broadcast_result.unwrap();
                let _ = tx.send(addr_str);
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
        };
        if !test_env.args.no_wait {
            h.await?;
        }
    }

    test_env.barrier.wait().await;
    Ok(())
}

async fn get_wallets(
    client: RpcClient,
    factory: &MsgAddressInt,
    num_wallets: u32,
    pubkey: [u8; 32],
) -> Result<Vec<MsgAddressInt>> {
    let method = get_wallet();
    let state = client
        .get_contract_state(factory, None)
        .await?
        .context("No state")?;

    let mut recipients = Vec::new();
    for i in 0..num_wallets {
        let tokens = GetWalletFunctionInput {
            index: i as _,
            public_key: UInt256::from(pubkey),
        }
        .pack();
        let result = method.run_local(&SimpleClock, state.account.clone(), &tokens, &[])?;
        let tokens = result.tokens.context("No tokens")?;
        let addr: GetWalletFunctionOutput = tokens.unpack()?;
        recipients.push(addr.receiver);
    }

    Ok(recipients)
}
