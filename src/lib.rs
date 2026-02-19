use std::path::PathBuf;
use std::sync::Arc;

use crate::dag::DagTestArgs;
use crate::send_tokens::SendTestArgs;
use crate::swap::SwapTestArgs;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
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
mod execution;
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

    #[clap(long, value_enum, default_value_t = EndpointMode::Rr)]
    endpoint_mode: EndpointMode,

    // Runtime-internal worker shard metadata (not user-facing CLI params).
    #[clap(skip = 0usize)]
    worker_index: usize,
    #[clap(skip = 1usize)]
    workers_total: usize,
    #[clap(skip = None)]
    shared_output_tx: Option<std::sync::mpsc::Sender<String>>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum EndpointMode {
    Rr,
    Distinct,
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
    let args = Args::parse();

    dotenvy::from_filename(args.project_root.join(".env")).context("Failed to load .env file")?;

    let seed = dotenvy::var("BROXUS_PHRASE").context("SEED is not set")?;
    let keypair = nekoton::crypto::derive_from_phrase(
        &seed,
        nekoton::crypto::MnemonicType::Bip39(nekoton::crypto::Bip39MnemonicData::labs_old(0)),
    )
    .context("Failed to derive keypair")?;

    execution::Executor::run(args, Arc::new(keypair)).await
}
