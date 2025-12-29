use crate::abi::{get_wallet, GetWalletFunctionInput, GetWalletFunctionOutput};
use crate::models::GenericDeploymentInfo;
use crate::send::send;
use crate::Args;
use anyhow::{Context, Result};
use clap::Parser;
use ed25519_dalek::Keypair;
use everscale_rpc_client::RpcClient;
use futures_util::StreamExt;
use nekoton_abi::{FunctionExt, PackAbiPlain, UnpackAbiPlain};
use nekoton_utils::SimpleClock;
use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use ton_block::{AccountStuff, MsgAddressInt};
use ton_types::{BuilderData, UInt256};

mod rate_limiter;

#[derive(Parser, Debug, Clone)]
pub struct RandSendTestArgs {
    #[clap(short, long)]
    total_wallets: u32,
    #[clap(short, long)]
    rps: u32,

    #[clap(short, long)]
    num_seconds: u32,

    #[clap(short, long)]
    from_rps: u32,

    #[clap(short, long)]
    to_rps: u32,
    
    #[clap(long)]
    save_accounts: Option<std::path::PathBuf>,
}

pub async fn run(
    swap_args: RandSendTestArgs,
    common_args: Args,
    key_pair: Arc<Keypair>,
    client: RpcClient,
) -> Result<()> {
    let deployments_path = common_args.project_root.join("deployments");
    log::info!("Deployments path: {:?}", deployments_path);

    let factory_abi = walkdir::WalkDir::new(&deployments_path)
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

    if let Some(path) = &swap_args.save_accounts {
        save_accounts_to_file(&recievers, path)?;
        log::info!("Saved {} accounts to {:?}", recievers.len(), path);
    }

    spawn_ddos_jobs(&swap_args, client, recievers, common_args, key_pair).await?;

    Ok(())
}

async fn spawn_ddos_jobs(
    args: &RandSendTestArgs,
    client: RpcClient,
    receivers: Vec<MsgAddressInt>,
    common_args: Args,
    key_pair: Arc<Keypair>,
) -> Result<()> {
    let mut params =
        rate_limiter::LoadPattern::new(args.from_rps as f64, args.to_rps as f64, args.rps as f64)?;
    params.calibrate(args.num_seconds);
    let tps = params.generate_timeline(args.num_seconds);

    let mut rng = StdRng::seed_from_u64(common_args.seed.unwrap_or_default());
    let counter = Arc::new(AtomicU64::new(0));

    let states: HashMap<_, _> = {
        let client = client.clone();
        futures_util::stream::iter(receivers.iter())
            .map(move |addr| {
                let client = client.clone();
                async move {
                    let state = match client.get_contract_state(addr, None).await {
                        Ok(Some(state)) => Arc::new(state.account),
                        _ => panic!("Failed to get state for {addr}"),
                    };

                    (addr.clone(), state)
                }
            })
            .buffered(100)
            .collect()
            .await
    };

    spawn_progress_printer(counter.clone());
    const WINDOW_LEN: u64 = 1;
    let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_LEN));

    for tps in tps {
        let barrier = Arc::new(tokio::sync::Barrier::new(tps as usize + 1));
        let start = std::time::Instant::now();
        let tps = tps * WINDOW_LEN;
        for _ in 0..tps {
            let barrier = barrier.clone();
            let client = client.clone();
            let from = receivers.choose(&mut rng).unwrap().clone();
            let to = receivers.choose(&mut rng).unwrap().clone();
            let signer = key_pair.clone();
            let counter = counter.clone();
            let from_state = states.get(&from).unwrap().clone();

            tokio::spawn(async move {
                ddos_job(client, from, to, signer, counter, from_state)
                    .await
                    .unwrap();
                if !common_args.no_wait {
                    barrier.wait().await;
                }
            });
        }
        if !common_args.no_wait {
            barrier.wait().await;
        }

        let elapsed = start.elapsed();
        if elapsed > Duration::from_secs(1) {
            log::warn!("Missed deadline by {:?}", elapsed - Duration::from_secs(1));
        } else {
            log::info!(
                "Delivered {tps} transactions in {}sec",
                elapsed.as_secs_f64()
            );
        }
        interval.tick().await;
    }

    Ok(())
}

async fn ddos_job(
    rpc_client: RpcClient,
    from: MsgAddressInt,
    to: MsgAddressInt,
    signer: Arc<Keypair>,
    counter: Arc<AtomicU64>,
    from_state: Arc<AccountStuff>,
) -> Result<()> {
    send(
        &rpc_client,
        &signer,
        from,
        BuilderData::new(),
        to,
        100_000_000,
        &from_state,
    )
    .await?;
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

pub fn spawn_progress_printer(counter: Arc<AtomicU64>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            log::info!(
                "Sent: {} transactions in {} seconds",
                counter.load(std::sync::atomic::Ordering::Relaxed),
                start.elapsed().as_secs()
            );
        }
    })
}
fn save_accounts_to_file(accounts: &[MsgAddressInt], path: &Path) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    for account in accounts {
        writeln!(file, "{account}")?;
    }
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
