use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::dag::DagTestArgs;
use crate::send_tokens::SendTestArgs;
use crate::swap::SwapTestArgs;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_rpc_client::{ClientOptions, ReliabilityParams, RpcClient};
use url::Url;

mod abi;
mod app_cache;
mod build_payload;
pub mod latency;
mod models;
mod send;
mod stream;

mod rand_send;

mod dag;
mod dos;
mod send_to_targets;
mod send_tokens;
mod swap;
#[cfg(test)]
mod test_chart_series;
mod util;

#[derive(Parser, Debug, Clone)]
pub(crate) struct Args {
    #[command(subcommand)]
    command: Commands,
    #[clap(short, long)]
    project_root: PathBuf,

    #[clap(short, long)]
    endpoints: Vec<Url>,

    /// seed for rng
    /// if you want to run multiple instances of the script with the same seed
    #[clap(short, long)]
    seed: Option<u64>,

    /// do not fait for the node answer on send message
    #[clap(short, long)]
    no_wait: bool,

    #[clap(long)]
    no_stream: bool,

    /// Which timediff makes the node dead
    #[clap(long = "dead-seconds", default_value = "120")]
    node_is_dead_seconds: u64,

    /// Select the network-specific deployment directory under <project_root>/deployments/
    #[clap(long)]
    network: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Swap(SwapTestArgs),
    Dag(DagTestArgs),
    Send(SendTestArgs),
    Latency(latency::LatencyTestArgs),
    RandSend(rand_send::RandSendTestArgs),
    SendToTargets(send_to_targets::SendToTargetsArgs),
    AccountsDos(dos::DosTestArgs),
}

pub async fn run_test() -> Result<()> {
    env_logger::init();
    let app_args = Args::parse();

    dotenvy::from_filename(app_args.project_root.join(".env"))
        .context("Failed to load .env file")?;

    let seed = dotenvy::var("BROXUS_PHRASE").context("SEED is not set")?;
    let keypair = nekoton::crypto::derive_from_phrase(
        &seed,
        nekoton::crypto::MnemonicType::Bip39(nekoton::crypto::Bip39MnemonicData::labs_old(0)),
    )
    .context("Failed to derive keypair")?;
    let keypair = Arc::new(keypair);
    let client = RpcClient::new(
        app_args.endpoints.clone(),
        ClientOptions {
            request_timeout: Duration::from_secs(60),
            choose_strategy: everscale_rpc_client::ChooseStrategy::RoundRobin,
            reliability_params: ReliabilityParams {
                mc_acceptable_time_diff_sec: app_args.node_is_dead_seconds,
                sc_acceptable_time_diff_sec: app_args.node_is_dead_seconds,
            },
            ..Default::default()
        },
    )
    .await?;

    match &app_args.command {
        Commands::Swap(args) => {
            swap::run(args.clone(), app_args, &keypair, client).await?;
        }
        Commands::Dag(args) => {
            dag::run(args.clone(), app_args, client).await?;
        }
        Commands::Send(args) => {
            send_tokens::run(args.clone(), app_args, keypair, client).await?;
        }
        Commands::Latency(args) => {
            latency::run(args.clone(), app_args, &keypair, client).await?;
        }
        Commands::RandSend(arg) => {
            rand_send::run(arg.clone(), app_args, keypair, client).await?;
        }
        Commands::SendToTargets(args) => {
            send_to_targets::run(args.clone(), app_args, keypair, client).await?;
        }
        Commands::AccountsDos(args) => {
            dos::run(args.clone(), app_args, client).await?;
        }
    }

    Ok(())
}
