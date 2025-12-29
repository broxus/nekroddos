use crate::abi::dudos_factory;
use crate::build_payload::{get_dag_payload, get_stats};
use crate::models::GenericDeploymentInfo;
use crate::util::TestEnv;
use crate::Args;
use anyhow::{Context, Result};
use clap::Parser;
use everscale_rpc_client::RpcClient;
use futures_util::StreamExt;
use governor::Jitter;
use nekoton::transport::models::ExistingContract;
use nekoton_abi::{FunctionExt, UnpackAbiPlain};
use nekoton_utils::SimpleClock;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use ton_abi::{Token, TokenValue, Uint};
use ton_block::MsgAddressInt;

#[derive(Parser, Debug, Clone)]
pub struct DagTestArgs {
    #[clap(short, long)]
    total_wallets: u32,
    #[clap(short, long)]
    rps: u32,
    #[clap(short, long)]
    num_iterations: u32,

    #[clap(short, long)]
    payload_size: u32,

    #[clap(long, default_value = "false")]
    only_stats: bool,

    #[clap(long, default_value = "false")]
    rand_cell: bool,
}
pub async fn run(swap_args: DagTestArgs, common_args: Args, client: RpcClient) -> Result<()> {
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
    let recievers = get_wallets(client.clone(), &factory.address, swap_args.total_wallets)
        .await
        .context("Failed to get wallets")?;

    spawn_ddos_jobs(&swap_args, client, recievers, common_args).await?;

    Ok(())
}

async fn spawn_ddos_jobs(
    args: &DagTestArgs,
    client: RpcClient,
    recievers: Vec<MsgAddressInt>,
    common_args: Args,
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
        print_stats(recievers, &test_env).await;
        return Ok(());
    }

    log::info!("Spawning ddos jobs for {} recievers", recievers.len());

    for (receiver_idx, reciever) in recievers.clone().into_iter().enumerate() {
        let env = test_env.clone();
        tokio::spawn(ddos_job(env, reciever, args.payload_size, args.rand_cell, receiver_idx as u32));
    }
    log::info!("All jobs spawned");

    let handle = test_env.spawn_progress_printer();
    test_env.barrier.wait().await;
    handle.abort();

    print_stats(recievers, &test_env).await;

    Ok(())
}

async fn print_stats(recievers: Vec<MsgAddressInt>, test_env: &TestEnv) {
    let states: Vec<Result<ExistingContract>> = futures_util::stream::iter(recievers)
        .map(|reciever| {
            let client = test_env.client.clone();
            async move {
                let state = client
                    .get_contract_state(&reciever, None)
                    .await?
                    .context("No state")?;
                anyhow::Result::Ok(state)
            }
        })
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await;
    let stats = states
        .into_iter()
        .flat_map(|x| x.ok())
        .map(|s| get_stats(s.account))
        .fold(DagTestStats::new(&test_env.counter), |mut acc, x| {
            acc.success += x.success_count as u64;
            acc.failed += x.errors_count as u64;
            acc
        });

    for _ in 0..10 {
        log::info!(
            "Total: {}, Success: {}, Failed: {}, Percent failed: {:.2}%, Number non delivered: {}, Percent non delivered: {:.2}%",
            stats.total,
            stats.success,
            stats.failed,
            stats.failed as f64 / stats.total as f64 * 100.0,
            stats.total - stats.success - stats.failed,
            (stats.total - stats.success - stats.failed) as f64 / stats.total as f64 * 100.0
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn ddos_job(test_env: TestEnv, reciever: MsgAddressInt, payload_size: u32, rand_cell: bool, receiver_idx: u32) -> Result<()> {
    let jitter = Jitter::new(Duration::from_millis(1), Duration::from_millis(50));
    for i in 1..=test_env.num_iterations {
        let payload = get_dag_payload(i, payload_size, test_env.seed, reciever.clone(), rand_cell, receiver_idx);
        test_env.rate_limiter.until_ready_with_jitter(jitter).await;
        let h = {
            let client = test_env.client.clone();
            let counter = test_env.counter.clone();
            tokio::spawn(async move {
                if let Err(e) = client.broadcast_message(payload).await {
                    log::error!("Failed to send: {:?}", e);
                    return;
                }
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
) -> Result<Vec<MsgAddressInt>> {
    let abi = dudos_factory();
    let method = abi.function("get_receiver")?;
    let state = client
        .get_contract_state(factory, None)
        .await?
        .context("No factory state")?;

    #[derive(UnpackAbiPlain)]
    struct Output {
        #[abi(address)]
        reciever: MsgAddressInt,
    }

    let mut recipients = Vec::new();
    for i in 0..num_wallets {
        let tokens = vec![Token::new(
            "nonce",
            TokenValue::Uint(Uint::new(i as u128, 256)),
        )];
        let result = method.run_local(&SimpleClock, state.account.clone(), &tokens, &[])?;
        let tokens = result.tokens.context("No tokens")?;
        let addr: Output = tokens.unpack()?;
        recipients.push(addr.reciever);
    }

    Ok(recipients)
}

#[derive(Debug, Default)]
struct DagTestStats {
    total: u64,
    success: u64,
    failed: u64,
}

impl DagTestStats {
    fn new(total: &AtomicU64) -> Self {
        Self {
            total: total.load(std::sync::atomic::Ordering::Relaxed),
            ..Default::default()
        }
    }
}
