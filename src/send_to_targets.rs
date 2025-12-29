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
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use ton_block::MsgAddressInt;
use ton_types::{BuilderData, UInt256};

#[derive(Parser, Debug, Clone)]
pub struct SendToTargetsArgs {
    #[clap(short, long)]
    total_wallets: u32,
    #[clap(short, long)]
    rps: u32,
    #[clap(short, long)]
    num_iterations: u32,
    #[clap(short, long)]
    amount: u64,
    #[clap(short, long)]
    targets_file: PathBuf,
}

pub async fn run(
    args: SendToTargetsArgs,
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
    log::info!("Targets file: {:?}", args.targets_file);

    // Find factory ABI
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

    // Get sender wallets
    let sender_wallets = get_wallets(
        client.clone(),
        &factory.address,
        args.total_wallets,
        key_pair.public.to_bytes(),
    )
    .await
    .context("Failed to get sender wallets")?;
    log::info!("Deployed {} sender wallets", sender_wallets.len());

    // Read target addresses from file
    let target_addresses = read_targets(&args.targets_file).await?;
    if target_addresses.is_empty() {
        return Err(anyhow::anyhow!("No target addresses found in file"));
    }
    log::info!("Loaded {} target addresses", target_addresses.len());

    // Run the DDoS jobs
    spawn_ddos_jobs(&args, client, sender_wallets, target_addresses, common_args, key_pair).await?;

    Ok(())
}

async fn read_targets(path: &PathBuf) -> Result<Vec<MsgAddressInt>> {
    let file = File::open(path).await.context("Failed to open targets file")?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    let mut targets = Vec::new();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        match line.parse::<MsgAddressInt>() {
            Ok(addr) => targets.push(addr),
            Err(e) => log::warn!("Failed to parse address '{}': {}", line, e),
        }
    }
    
    Ok(targets)
}

async fn spawn_ddos_jobs(
    args: &SendToTargetsArgs,
    client: RpcClient,
    sender_wallets: Vec<MsgAddressInt>,
    target_addresses: Vec<MsgAddressInt>,
    common_args: Args,
    key_pair: Arc<Keypair>,
) -> Result<()> {
    let test_env = TestEnv::new(
        args.num_iterations,
        args.rps,
        sender_wallets.len(),
        client,
        common_args.seed,
        common_args.clone(),
    );

    log::info!("Spawning ddos jobs for {} sender wallets", sender_wallets.len());
    let target_addresses = Arc::new(target_addresses);

    let mut rng = StdRng::seed_from_u64(test_env.seed.unwrap_or_else(rand::random));
    for sender in &sender_wallets {
        let seed = rng.gen();
        let rng = StdRng::seed_from_u64(seed); // Each job should have its own rng
        let env = test_env.clone();
        let targets = target_addresses.clone();
        let key_pair = key_pair.clone();
        let amount = args.amount;
        tokio::spawn(ddos_job(
            env,
            sender.clone(),
            targets,
            key_pair,
            rng,
            amount,
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
    from_wallet: MsgAddressInt,
    target_addresses: Arc<Vec<MsgAddressInt>>,
    signer: Arc<Keypair>,
    mut rng: StdRng,
    amount: u64,
) -> Result<()> {
    let jitter = Jitter::new(Duration::from_millis(1), Duration::from_millis(50));
    let state = test_env
        .client
        .get_contract_state(&from_wallet, None)
        .await?
        .unwrap_or_else(|| panic!("No state for {from_wallet}"))
        .account;

    for _ in 1..=test_env.num_iterations {
        let random_target = target_addresses.choose(&mut rng).unwrap().clone();

        test_env.rate_limiter.until_ready_with_jitter(jitter).await;
        let h = {
            let client = test_env.client.clone();
            let counter = test_env.counter.clone();
            let signer = signer.clone();
            let from = from_wallet.clone();
            let state = state.clone();

            tokio::spawn(async move {
                send::send(
                    &client,
                    &signer,
                    from,
                    BuilderData::new(),
                    random_target,
                    amount,
                    &state,
                )
                .await
                .unwrap();
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

    let mut senders = Vec::new();
    for i in 0..num_wallets {
        let tokens = GetWalletFunctionInput {
            index: i as _,
            public_key: UInt256::from(pubkey),
        }
        .pack();
        let result = method.run_local(&SimpleClock, state.account.clone(), &tokens, &[])?;
        let tokens = result.tokens.context("No tokens")?;
        let addr: GetWalletFunctionOutput = tokens.unpack()?;
        senders.push(addr.receiver);
    }

    Ok(senders)
}
