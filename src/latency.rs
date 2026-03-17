pub mod combined_plot;
pub mod plotting;

use crate::abi::{get_wallet, GetWalletFunctionInput, GetWalletFunctionOutput};
use crate::models::GenericDeploymentInfo;
use crate::stream;
use crate::{send, Args};
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use ed25519_dalek::Keypair;
use everscale_rpc_client::RpcClient;
use nekoton_abi::{FunctionExt, PackAbiPlain, UnpackAbiPlain};
use nekoton_utils::SimpleClock;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;
use ton_block::MsgAddressInt;
use ton_types::UInt256;

#[derive(Parser, Debug, Clone)]
pub struct LatencyTestArgs {
    #[clap(short, long)]
    /// Number of transactions to send
    num_txs: usize,

    #[clap(short = 'w', long)]
    /// Number of sender wallets to use
    total_wallets: u32,

    #[clap(long, value_name = "MS")]
    /// Fixed spacing between wallet phases; omitted values are inferred from wallet count
    step_ms: Option<u64>,

    #[clap(long, value_name = "SECONDS", default_value = "5")]
    /// Scheduler cycle length in seconds
    window_secs: u64,

    #[clap(short, long, default_value = "1000000")]
    /// Amount to send in nanotons
    amount: u64,

    #[clap(short, long)]
    csv: Option<PathBuf>,

    #[clap(long, value_name = "PATH")]
    /// Path to log CSV with tx hashes and loss flag
    log_file: Option<PathBuf>,

    #[clap(long, value_name = "PATH")]
    /// Path to save interactive HTML plot (if specified, plot will be generated)
    plot: Option<PathBuf>,

    #[clap(long)]
    /// SLA threshold for marking violations
    sla_threshold: Option<u64>,

    #[clap(long)]
    /// Time window in minutes for time series plots (auto-calculated if not specified)
    time_window: Option<u64>,
}

pub(crate) async fn run(
    latency_args: LatencyTestArgs,
    common_args: Args,
    keypair: &Keypair,
    client: RpcClient,
) -> Result<()> {
    const COST_PER_TRANSACTION: u64 = 8_857_001;

    validate_total_wallets(latency_args.total_wallets)?;
    let window_ms = window_ms(latency_args.window_secs)?;
    if common_args.no_wait {
        anyhow::bail!("--no-wait is not supported by latency");
    }

    stream::init(
        client.clone(),
        common_args.endpoints.clone(),
        common_args.no_stream,
    )
    .await?;

    let deployments_path = deployments_path(&common_args)?;
    log::info!("Using deployments path: {:?}", deployments_path);

    if latency_args.total_wallets == 1 {
        run_single_wallet(
            latency_args,
            keypair,
            client,
            &deployments_path,
            COST_PER_TRANSACTION,
            window_ms,
        )
        .await
    } else {
        run_multi_wallet(
            latency_args,
            keypair,
            client,
            &deployments_path,
            COST_PER_TRANSACTION,
            window_ms,
        )
        .await
    }
}

async fn run_single_wallet(
    latency_args: LatencyTestArgs,
    keypair: &Keypair,
    client: RpcClient,
    deployments_path: &Path,
    cost_per_transaction: u64,
    window_ms: u64,
) -> Result<()> {
    let step_ms = latency_args.step_ms.unwrap_or(10);
    validate_legacy_step_ms(step_ms, window_ms)?;

    let sender = load_single_sender(deployments_path)?;
    log::info!("Sender address: {}", sender);

    let initial_balance = current_balance(&client, &sender).await?;
    let required_balance = cost_per_transaction * latency_args.num_txs as u64;
    let max_iterations = initial_balance / u128::from(cost_per_transaction);
    let requested_txs = std::cmp::min(latency_args.num_txs, max_iterations as usize);

    log::info!(
        "Starting latency test - sending {} transactions on {}ms phases within {}s windows",
        latency_args.num_txs,
        step_ms,
        latency_args.window_secs
    );
    log::info!(
        "Initial balance: {}, required balance: {}, max sendable transactions: {}",
        initial_balance,
        required_balance,
        max_iterations
    );

    let receiver = zero_address()?;
    let (events_tx, collector) = spawn_collector(&latency_args)?;
    let should_log = latency_args.log_file.is_some();

    for slot_index in 0..requested_txs {
        sleep_until_slot(step_ms, slot_index, window_ms).await?;
        let outcome = send_test_transaction(
            &client,
            keypair,
            &sender,
            &receiver,
            latency_args.amount,
            should_log,
        )
        .await;
        send_collector_event(&events_tx, outcome)?;
    }

    drop(events_tx);
    let results = collector
        .await
        .map_err(|error| anyhow::anyhow!("collector task panicked: {error}"))??;
    report_results(results, &latency_args)?;

    Ok(())
}

async fn run_multi_wallet(
    latency_args: LatencyTestArgs,
    keypair: &Keypair,
    client: RpcClient,
    deployments_path: &Path,
    cost_per_transaction: u64,
    window_ms: u64,
) -> Result<()> {
    let wallet_count = latency_args.total_wallets as usize;
    let per_wallet_txs = latency_args.num_txs.div_ceil(wallet_count);
    let amount = latency_args.amount;
    let phases_ms = fixed_phases_ms(latency_args.total_wallets, latency_args.step_ms, window_ms)?;
    let senders = load_sender_wallets(
        client.clone(),
        deployments_path,
        latency_args.total_wallets,
        keypair,
    )
    .await?;
    let keypair_bytes = keypair.to_bytes();

    log::info!(
        "Starting multi-wallet latency test - {} wallets, {} sends per wallet, {} total scheduled sends within {}s windows",
        wallet_count,
        per_wallet_txs,
        per_wallet_txs.saturating_mul(wallet_count),
        latency_args.window_secs
    );

    let receiver = zero_address()?;
    let (events_tx, collector) = spawn_collector(&latency_args)?;
    let should_log = latency_args.log_file.is_some();
    let mut handles = Vec::with_capacity(wallet_count);

    for ((sender, phase_ms), wallet_index) in senders.into_iter().zip(phases_ms).zip(0usize..) {
        let balance = current_balance(&client, &sender).await?;
        let max_iterations = balance / u128::from(cost_per_transaction);
        if max_iterations < per_wallet_txs as u128 {
            anyhow::bail!(
                "wallet {wallet_index} ({sender}) has balance for only {max_iterations} sends, need {per_wallet_txs}"
            );
        }

        log::info!(
            "Wallet {} phase={}ms balance={}",
            wallet_index + 1,
            phase_ms,
            balance
        );

        let client = client.clone();
        let sender_events = events_tx.clone();
        let sender_receiver = receiver.clone();
        let sender_keypair = Keypair::from_bytes(&keypair_bytes)
            .map_err(|error| anyhow::anyhow!("failed to clone keypair from bytes: {error}"))?;
        let handle = tokio::spawn(async move {
            for _ in 0..per_wallet_txs {
                sleep_until_fixed_phase(phase_ms, window_ms).await?;
                let outcome = send_test_transaction(
                    &client,
                    &sender_keypair,
                    &sender,
                    &sender_receiver,
                    amount,
                    should_log,
                )
                .await;
                send_collector_event(&sender_events, outcome)?;
            }
            Result::<()>::Ok(())
        });
        handles.push(handle);
    }

    drop(events_tx);
    for handle in handles {
        handle
            .await
            .map_err(|error| anyhow::anyhow!("latency worker panicked: {error}"))??;
    }

    let results = collector
        .await
        .map_err(|error| anyhow::anyhow!("collector task panicked: {error}"))??;
    report_results(results, &latency_args)?;

    Ok(())
}

fn deployments_path(common_args: &Args) -> Result<PathBuf> {
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

    Ok(network_deployments_path)
}

fn validate_total_wallets(total_wallets: u32) -> Result<()> {
    if total_wallets == 0 {
        anyhow::bail!("total-wallets must be > 0");
    }
    Ok(())
}

fn window_ms(window_secs: u64) -> Result<u64> {
    if window_secs == 0 {
        anyhow::bail!("window-secs must be > 0");
    }
    window_secs
        .checked_mul(1000)
        .context("window-secs is too large to convert to milliseconds")
}

fn validate_legacy_step_ms(step_ms: u64, window_ms: u64) -> Result<()> {
    if step_ms == 0 {
        anyhow::bail!("step-ms must be > 0");
    }
    if !window_ms.is_multiple_of(step_ms) {
        anyhow::bail!("step-ms must divide the configured window for slot scheduling");
    }
    Ok(())
}

fn validate_fixed_step_ms(total_wallets: u32, step_ms: u64, window_ms: u64) -> Result<()> {
    if step_ms == 0 {
        anyhow::bail!("step-ms must be > 0");
    }
    if u64::from(total_wallets).saturating_mul(step_ms) >= window_ms {
        anyhow::bail!("total-wallets * step-ms must be < the configured window");
    }
    Ok(())
}

fn fixed_phases_ms(total_wallets: u32, step_ms: Option<u64>, window_ms: u64) -> Result<Vec<u64>> {
    if let Some(step_ms) = step_ms {
        validate_fixed_step_ms(total_wallets, step_ms, window_ms)?;
        return Ok((1..=total_wallets)
            .map(|index| u64::from(index) * step_ms)
            .collect());
    }

    let phases: Vec<_> = (0..total_wallets)
        .map(|index| ((u64::from(index) + 1) * window_ms) / (u64::from(total_wallets) + 1))
        .collect();
    if phases
        .iter()
        .any(|phase_ms| *phase_ms == 0 || *phase_ms >= window_ms)
    {
        anyhow::bail!(
            "could not infer interior fixed phases for total-wallets={total_wallets} in the configured window"
        );
    }
    let unique = phases
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != phases.len() {
        anyhow::bail!(
            "could not infer interior fixed phases for total-wallets={total_wallets} in the configured window"
        );
    }
    Ok(phases)
}

fn load_single_sender(deployments_path: &Path) -> Result<MsgAddressInt> {
    let wallet = walkdir::WalkDir::new(deployments_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|extension| extension == "json")
                .unwrap_or(false)
        })
        .find(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_lowercase().contains("ever-wallet"))
                .unwrap_or(false)
        })
        .context("No factory abi")?;
    let deployment: GenericDeploymentInfo = serde_json::from_slice(&std::fs::read(wallet.path())?)?;
    Ok(deployment.address)
}

async fn load_sender_wallets(
    client: RpcClient,
    deployments_path: &Path,
    total_wallets: u32,
    keypair: &Keypair,
) -> Result<Vec<MsgAddressInt>> {
    let factory_abi = walkdir::WalkDir::new(deployments_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|extension| extension == "json")
                .unwrap_or(false)
        })
        .find(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_lowercase().contains("factory"))
                .unwrap_or(false)
        })
        .context("No factory abi")?;
    let factory: GenericDeploymentInfo =
        serde_json::from_slice(&std::fs::read(factory_abi.path())?)?;

    let method = get_wallet();
    let state = client
        .get_contract_state(&factory.address, None)
        .await?
        .context("No state")?;

    let mut wallets = Vec::with_capacity(total_wallets as usize);
    for index in 0..total_wallets {
        let tokens = GetWalletFunctionInput {
            index: index as _,
            public_key: UInt256::from(keypair.public.to_bytes()),
        }
        .pack();
        let result = method.run_local(&SimpleClock, state.account.clone(), &tokens, &[])?;
        let tokens = result.tokens.context("No tokens")?;
        let address: GetWalletFunctionOutput = tokens.unpack()?;
        wallets.push(address.receiver);
    }
    wallets.sort();
    Ok(wallets)
}

async fn current_balance(client: &RpcClient, sender: &MsgAddressInt) -> Result<u128> {
    Ok(client
        .get_contract_state(sender, None)
        .await?
        .context("No state")?
        .account
        .storage
        .balance
        .grams
        .as_u128())
}

fn zero_address() -> Result<MsgAddressInt> {
    MsgAddressInt::from_str("0:0000000000000000000000000000000000000000000000000000000000000000")
}

#[derive(Clone)]
struct LogData {
    hash: String,
    repr: String,
    ts: String,
}

impl LogData {
    fn write(&self, writer: &mut std::fs::File, lost: bool) -> Result<()> {
        writeln!(
            writer,
            "{hash},{repr},{ts},{lost}",
            hash = self.hash.as_str(),
            repr = self.repr.as_str(),
            ts = self.ts.as_str(),
            lost = lost,
        )?;
        writer.flush()?;
        Ok(())
    }
}

struct LogRecord {
    data: LogData,
    lost: bool,
}

enum TxCompletion {
    Confirmed {
        started_at: SystemTime,
        latency: Duration,
        log_record: Option<LogRecord>,
    },
    Failed {
        error: String,
        timed_out: bool,
        log_record: Option<LogRecord>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitForUpdateError {
    Closed,
    TimedOut,
    Reconcile,
}

struct CollectedResults {
    latencies: Vec<Duration>,
    timestamped_latencies: Vec<plotting::TimestampedLatency>,
    sent_count: usize,
    confirmed_count: usize,
    failed_count: usize,
    timed_out_count: usize,
}

fn spawn_collector(
    latency_args: &LatencyTestArgs,
) -> Result<(
    mpsc::UnboundedSender<TxCompletion>,
    tokio::task::JoinHandle<Result<CollectedResults>>,
)> {
    let mut csv_writer = if let Some(csv_path) = &latency_args.csv {
        let mut writer = std::fs::File::create(csv_path)?;
        writeln!(writer, "latency_ns")?;
        Some(writer)
    } else {
        None
    };

    let mut log_writer = if let Some(log_path) = &latency_args.log_file {
        let mut writer = std::fs::File::create(log_path)?;
        writeln!(writer, "blake3_hash,repr_hash,timestamp,lost")?;
        Some(writer)
    } else {
        None
    };

    let (tx, mut rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        let mut results = CollectedResults {
            latencies: Vec::new(),
            timestamped_latencies: Vec::new(),
            sent_count: 0,
            confirmed_count: 0,
            failed_count: 0,
            timed_out_count: 0,
        };

        while let Some(outcome) = rx.recv().await {
            results.sent_count += 1;
            match outcome {
                TxCompletion::Confirmed {
                    started_at,
                    latency,
                    log_record,
                } => {
                    results.latencies.push(latency);
                    results
                        .timestamped_latencies
                        .push(plotting::TimestampedLatency {
                            timestamp: started_at,
                            latency,
                        });
                    results.confirmed_count += 1;
                    log::debug!("Transaction confirmed in {:?}", latency);

                    if let Some(writer) = csv_writer.as_mut() {
                        writeln!(writer, "{}", latency.as_nanos())?;
                        writer.flush()?;
                    }
                    write_log_record(&mut log_writer, log_record)?;
                }
                TxCompletion::Failed {
                    error,
                    timed_out,
                    log_record,
                } => {
                    if timed_out {
                        results.timed_out_count += 1;
                    } else {
                        results.failed_count += 1;
                    }
                    log::error!("Transaction failed: {error}");
                    write_log_record(&mut log_writer, log_record)?;
                }
            }
        }

        Result::<CollectedResults>::Ok(results)
    });

    Ok((tx, handle))
}

fn send_collector_event(
    tx: &mpsc::UnboundedSender<TxCompletion>,
    outcome: TxCompletion,
) -> Result<()> {
    tx.send(outcome)
        .map_err(|_| anyhow::anyhow!("collector dropped before receiving latency event"))
}

fn write_log_record(
    log_writer: &mut Option<std::fs::File>,
    log_record: Option<LogRecord>,
) -> Result<()> {
    if let (Some(writer), Some(log_record)) = (log_writer.as_mut(), log_record) {
        log_record.data.write(writer, log_record.lost)?;
    }
    Ok(())
}

fn format_timeout_error(
    address: &str,
    prev_lt: u64,
    waited: Duration,
    balance: std::result::Result<u128, String>,
) -> String {
    let waited_ms = waited.as_millis();
    match balance {
        Ok(balance) => format!(
            "timeout waiting for stream update: address={address} prev_lt={prev_lt} waited_ms={waited_ms} balance={balance}"
        ),
        Err(error) => format!(
            "timeout waiting for stream update: address={address} prev_lt={prev_lt} waited_ms={waited_ms} balance_query_error={error}"
        ),
    }
}

fn report_results(mut results: CollectedResults, latency_args: &LatencyTestArgs) -> Result<()> {
    if !results.latencies.is_empty() {
        results.latencies.sort();
        let total: Duration = results.latencies.iter().sum();
        let avg = total / results.latencies.len() as u32;
        let p50 = results.latencies[results.latencies.len() / 2];
        let p95 = results.latencies[(results.latencies.len() as f64 * 0.95) as usize];
        let p99 = results.latencies[(results.latencies.len() as f64 * 0.99) as usize];
        let min = results.latencies[0];
        let max = results.latencies[results.latencies.len() - 1];

        log::info!("Latency test results:");
        log::info!("Sent transactions: {}", results.sent_count);
        log::info!("Confirmed transactions: {}", results.confirmed_count);
        log::info!("Failed transactions: {}", results.failed_count);
        log::info!("Timed out transactions: {}", results.timed_out_count);
        log::info!("Average latency: {:?}", avg);
        log::info!("P50 latency: {:?}", p50);
        log::info!("P95 latency: {:?}", p95);
        log::info!("P99 latency: {:?}", p99);

        if let Some(plot_path) = &latency_args.plot {
            results
                .timestamped_latencies
                .sort_by_key(|item| item.timestamp);
            let stats = plotting::LatencyStats {
                avg,
                p50,
                p95,
                p99,
                min,
                max,
            };

            plotting::generate_combined_plots(
                &results.latencies,
                &results.timestamped_latencies,
                plot_path.clone(),
                &stats,
                latency_args.time_window,
                latency_args.sla_threshold.map(|value| value as f64),
            )?;

            log::info!("Plot saved to: {:?}", plot_path);
        }
    } else {
        log::info!("Latency test results:");
        log::info!("Sent transactions: {}", results.sent_count);
        log::info!("Confirmed transactions: {}", results.confirmed_count);
        log::info!("Failed transactions: {}", results.failed_count);
        log::info!("Timed out transactions: {}", results.timed_out_count);
    }

    Ok(())
}

fn slot_phase_ms(step_ms: u64, slot_index: usize, window_ms: u64) -> u64 {
    let slots_per_window = window_ms / step_ms;
    let slot_index = u64::try_from(slot_index).unwrap();
    (slot_index % slots_per_window) * step_ms
}

fn next_slot_time(
    after: SystemTime,
    step_ms: u64,
    slot_index: usize,
    window_ms: u64,
) -> Result<SystemTime> {
    let phase_ms = slot_phase_ms(step_ms, slot_index, window_ms);
    next_phase_time(after, phase_ms, window_ms, false)
}

fn next_fixed_phase_time(after: SystemTime, phase_ms: u64, window_ms: u64) -> Result<SystemTime> {
    next_phase_time(after, phase_ms, window_ms, true)
}

fn next_phase_time(
    after: SystemTime,
    phase_ms: u64,
    window_ms: u64,
    strict_future: bool,
) -> Result<SystemTime> {
    let since_epoch = after.duration_since(SystemTime::UNIX_EPOCH)?;
    let window_ns = u128::from(window_ms) * 1_000_000;
    let phase_ns = u128::from(phase_ms) * 1_000_000;
    let current_ns = since_epoch.as_nanos();
    let window_start_ns = current_ns - (current_ns % window_ns);
    let mut target_ns = window_start_ns + phase_ns;

    if (strict_future && target_ns <= current_ns) || (!strict_future && target_ns < current_ns) {
        target_ns += window_ns;
    }

    Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(u64::try_from(target_ns).unwrap()))
}

async fn sleep_until_slot(step_ms: u64, slot_index: usize, window_ms: u64) -> Result<()> {
    sleep_until(next_slot_time(
        SystemTime::now(),
        step_ms,
        slot_index,
        window_ms,
    )?)
    .await
}

async fn sleep_until_fixed_phase(phase_ms: u64, window_ms: u64) -> Result<()> {
    sleep_until(next_fixed_phase_time(
        SystemTime::now(),
        phase_ms,
        window_ms,
    )?)
    .await
}

async fn sleep_until(target: SystemTime) -> Result<()> {
    let now = SystemTime::now();
    let wait = target.duration_since(now).unwrap_or(Duration::ZERO);
    tokio::time::sleep(wait).await;
    Ok(())
}

async fn wait_for_wallet_update<F, Fut>(
    mut reconcile: F,
    updates: &mut tokio::sync::watch::Receiver<Option<stream::StreamUpdate>>,
    gaps: &mut tokio::sync::watch::Receiver<u64>,
    prev_lt: u64,
    timeout: Duration,
) -> std::result::Result<(), WaitForUpdateError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    tokio::time::timeout(timeout, async {
        loop {
            if updates
                .borrow_and_update()
                .as_ref()
                .is_some_and(|update| update.max_lt > prev_lt)
            {
                return Ok(());
            }

            tokio::select! {
                changed = updates.changed() => {
                    changed.map_err(|_| WaitForUpdateError::Closed)?;
                }
                changed = gaps.changed() => {
                    changed.map_err(|_| WaitForUpdateError::Closed)?;
                    let _ = gaps.borrow_and_update();
                    if reconcile()
                        .await
                        .map_err(|_| WaitForUpdateError::Reconcile)?
                    {
                        return Ok(());
                    }
                }
            }
        }
    })
    .await
    .map_err(|_| WaitForUpdateError::TimedOut)?
}

async fn reconcile_wallet_update(
    client: &RpcClient,
    sender: &MsgAddressInt,
    prev_lt: u64,
) -> Result<bool> {
    let state = client
        .get_contract_state(sender, None)
        .await?
        .context("sender state not found during reconciliation")?;
    Ok(state.account.storage.last_trans_lt > prev_lt)
}

async fn send_test_transaction(
    client: &RpcClient,
    keypair: &Keypair,
    sender: &MsgAddressInt,
    receiver: &MsgAddressInt,
    amount: u64,
    should_log: bool,
) -> TxCompletion {
    let started_at = SystemTime::now();
    let start = Instant::now();
    let payload = ton_types::BuilderData::new();
    let state = match client.get_contract_state(sender, None).await {
        Ok(Some(state)) => state,
        Ok(None) => {
            return TxCompletion::Failed {
                error: "sender state not found".to_owned(),
                timed_out: false,
                log_record: None,
            };
        }
        Err(error) => {
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };
    let balance = state.account.storage.balance.grams.as_u128();
    log::info!("Sender balance: {}", balance);
    let prev_lt = state.account.storage.last_trans_lt;
    let sender = sender.clone();
    let address = sender.to_string();
    let stream = match stream::global() {
        Ok(stream) => stream,
        Err(error) => {
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };
    let mut updates = match stream.subscribe_addr(&address).await {
        Ok(updates) => updates,
        Err(error) => {
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };
    let _ = updates.borrow_and_update();
    let mut gaps = stream.subscribe_gaps();
    let _ = gaps.borrow_and_update();

    let outcome = send::send(
        client,
        keypair,
        sender.clone(),
        payload,
        receiver.clone(),
        amount,
        &state.account,
    )
    .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            drop(updates);
            drop(gaps);
            stream.release_addr(&address);
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };

    let log_data = match build_log_data(should_log, &outcome.message) {
        Ok(log_data) => log_data,
        Err(error) => {
            drop(updates);
            drop(gaps);
            stream.release_addr(&address);
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };

    if let Err(error) = outcome.broadcast_result {
        drop(updates);
        drop(gaps);
        stream.release_addr(&address);
        return TxCompletion::Failed {
            error: error.to_string(),
            timed_out: false,
            log_record: log_data.map(|data| LogRecord { data, lost: true }),
        };
    }

    let outcome = match wait_for_wallet_update(
        || reconcile_wallet_update(client, &sender, prev_lt),
        &mut updates,
        &mut gaps,
        prev_lt,
        Duration::from_secs(60),
    )
    .await
    {
        Ok(()) => TxCompletion::Confirmed {
            started_at,
            latency: start.elapsed(),
            log_record: log_data.map(|data| LogRecord { data, lost: false }),
        },
        Err(WaitForUpdateError::Closed) => TxCompletion::Failed {
            error: "stream updates channel closed".to_owned(),
            timed_out: false,
            log_record: log_data.map(|data| LogRecord { data, lost: true }),
        },
        Err(WaitForUpdateError::Reconcile) => TxCompletion::Failed {
            error: "stream gap reconciliation failed".to_owned(),
            timed_out: false,
            log_record: log_data.map(|data| LogRecord { data, lost: true }),
        },
        Err(WaitForUpdateError::TimedOut) => {
            match reconcile_wallet_update(client, &sender, prev_lt).await {
                Ok(true) => TxCompletion::Confirmed {
                    started_at,
                    latency: start.elapsed(),
                    log_record: log_data.map(|data| LogRecord { data, lost: false }),
                },
                Ok(false) => {
                    // Re-query the sender only on timeout so the failure log shows whether
                    // the wallet was still funded when the stream confirmation never arrived.
                    let error = format_timeout_error(
                        &address,
                        prev_lt,
                        start.elapsed(),
                        current_balance(client, &sender)
                            .await
                            .map_err(|error| error.to_string()),
                    );
                    TxCompletion::Failed {
                        error,
                        timed_out: true,
                        log_record: log_data.map(|data| LogRecord { data, lost: true }),
                    }
                }
                Err(error) => TxCompletion::Failed {
                    error: format!("failed to reconcile wallet update after timeout: {error}"),
                    timed_out: false,
                    log_record: log_data.map(|data| LogRecord { data, lost: true }),
                },
            }
        }
    };
    drop(updates);
    drop(gaps);
    stream.release_addr(&address);
    outcome
}

fn build_log_data(should_log: bool, message: &ton_block::Message) -> Result<Option<LogData>> {
    if !should_log {
        return Ok(None);
    }

    let ts = Utc::now().to_rfc3339();
    let cell = ton_block::Serializable::write_to_new_cell(message)
        .and_then(ton_types::BuilderData::into_cell)?;
    let boc = ton_types::serialize_toc(&cell)?;
    let hash = blake3::hash(&boc).to_hex().to_string();
    let repr = hex::encode(cell.repr_hash().inner());
    Ok(Some(LogData { hash, repr, ts }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, SecondsFormat};
    use std::fs::File;
    use std::path::Path;
    use std::process::Command;
    use std::time::UNIX_EPOCH;

    struct WalletSchedule {
        wallet_index: usize,
        phase_ms: u64,
        sends: Vec<SystemTime>,
    }

    fn build_wallet_schedule(
        now: SystemTime,
        total_wallets: u32,
        sends_per_wallet: usize,
        window_ms: u64,
    ) -> Result<Vec<WalletSchedule>> {
        let phases_ms = fixed_phases_ms(total_wallets, None, window_ms)?;

        phases_ms
            .into_iter()
            .enumerate()
            .map(|(wallet_index, phase_ms)| {
                let first_send = next_fixed_phase_time(now, phase_ms, window_ms)?;
                let sends = (0..sends_per_wallet)
                    .map(|send_index| {
                        let offset = Duration::from_millis(window_ms * send_index as u64);
                        first_send + offset
                    })
                    .collect();

                Ok(WalletSchedule {
                    wallet_index,
                    phase_ms,
                    sends,
                })
            })
            .collect()
    }

    fn format_utc(ts: SystemTime) -> String {
        DateTime::<Utc>::from(ts).to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    fn write_wallet_schedule_csv(path: &Path, rows: &[WalletSchedule]) -> Result<()> {
        let mut writer = File::create(path)?;
        write!(writer, "wallet_index,phase_ms")?;
        for send_index in 1..=rows.first().map(|row| row.sends.len()).unwrap_or(0) {
            write!(writer, ",send_{send_index:02}_utc")?;
        }
        writeln!(writer)?;

        for row in rows {
            write!(writer, "{},{}", row.wallet_index, row.phase_ms)?;
            for send_at in &row.sends {
                write!(writer, ",{}", format_utc(*send_at))?;
            }
            writeln!(writer)?;
        }

        Ok(())
    }

    fn schedule_output_dir() -> Result<PathBuf> {
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let path = std::env::temp_dir().join(format!(
            "nekroddos-latency-schedule-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    fn render_wallet_schedule_animation_html(csv_path: &Path, html_path: &Path) -> Result<()> {
        const SCRIPT: &str = r###"
import csv
import datetime as dt
import json
import sys

csv_path = sys.argv[1]
html_path = sys.argv[2]

rows = []
first_ms = None
last_ms = None

with open(csv_path, newline="") as handle:
    reader = csv.DictReader(handle)
    for row in reader:
        wallet = int(row["wallet_index"])
        send_times = []
        for key, value in row.items():
            if key.startswith("send_") and value:
                parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                send_ms = int(parsed.timestamp() * 1000)
                send_times.append({"iso": value, "ms": send_ms})
                first_ms = send_ms if first_ms is None else min(first_ms, send_ms)
                last_ms = send_ms if last_ms is None else max(last_ms, send_ms)
        rows.append({
            "wallet_index": wallet,
            "phase_ms": int(row["phase_ms"]),
            "sends": send_times,
        })

if first_ms is None or last_ms is None:
    raise SystemExit("no schedule rows found in csv")

payload = {
    "rows": rows,
    "first_ms": first_ms,
    "last_ms": last_ms,
}

html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Wallet schedule animation</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f4ec;
      --ink: #17212b;
      --muted: #7e8a96;
      --dot: #7da3c2;
      --active: #d94841;
      --grid: #d7d2c7;
      --accent: #0f766e;
    }}
    body {{
      margin: 0;
      background: linear-gradient(180deg, #f9f6ef 0%, var(--bg) 100%);
      color: var(--ink);
      font: 14px/1.4 "Iosevka Term", "SFMono-Regular", monospace;
    }}
    .shell {{
      max-width: 1680px;
      margin: 0 auto;
      padding: 24px;
    }}
    .stack {{
      display: grid;
      gap: 14px;
    }}
    .topline {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      margin-bottom: 12px;
    }}
    .headline {{
      font-size: 20px;
      font-weight: 700;
    }}
    .controls {{
      display: flex;
      align-items: center;
      gap: 12px;
      color: var(--muted);
    }}
    button {{
      border: 1px solid var(--accent);
      background: white;
      color: var(--accent);
      padding: 6px 12px;
      border-radius: 999px;
      cursor: pointer;
      font: inherit;
    }}
    svg {{
      width: 100%;
      height: auto;
      display: block;
      border: 1px solid #d9d3c7;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 18px 40px rgba(23, 33, 43, 0.08);
    }}
    .tick {{
      stroke: var(--grid);
      stroke-width: 1;
    }}
    .tick-label {{
      fill: var(--muted);
      font-size: 11px;
      text-anchor: middle;
    }}
    .axis-label {{
      fill: var(--muted);
      font-size: 12px;
    }}
    .wallet-label {{
      fill: var(--muted);
      font-size: 9px;
      text-anchor: end;
      dominant-baseline: middle;
    }}
    .cursor {{
      stroke: var(--accent);
      stroke-width: 2;
      stroke-dasharray: 6 4;
    }}
    .dot {{
      fill: var(--dot);
      transition: fill 80ms linear;
    }}
    .dot.sent {{
      fill: var(--active);
    }}
    .message-panel {{
      border: 1px solid #d9d3c7;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 18px 40px rgba(23, 33, 43, 0.08);
      padding: 14px 16px;
    }}
    .message-title {{
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .message-subtitle {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 8px;
    }}
    .event-timeline {{
      position: relative;
      min-height: 78px;
      overflow: hidden;
      border-top: 1px solid var(--grid);
      padding-top: 12px;
    }}
    .event-track {{
      position: absolute;
      left: 0;
      right: 0;
      top: 34px;
      height: 2px;
      background: linear-gradient(90deg, #e7e1d6 0%, #d7d2c7 100%);
    }}
    .event-dot {{
      position: absolute;
      top: 18px;
      width: 44px;
      height: 28px;
      margin-left: -22px;
      border-radius: 999px;
      display: grid;
      place-items: center;
      background: var(--active);
      color: white;
      font-size: 11px;
      font-weight: 700;
      box-shadow: 0 10px 22px rgba(217, 72, 65, 0.28);
      transform: scale(0.7);
      opacity: 0;
      animation: pop-in 220ms ease-out forwards;
    }}
    @keyframes pop-in {{
      0% {{
        transform: scale(0.35) translateY(12px);
        opacity: 0;
      }}
      75% {{
        transform: scale(1.08) translateY(-2px);
        opacity: 1;
      }}
      100% {{
        transform: scale(1) translateY(0);
        opacity: 1;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="stack">
      <div class="topline">
        <div>
          <div class="headline">Wallet send schedule animation</div>
          <div>1x playback, account 0 on top, dots stay red after send time.</div>
        </div>
        <div class="controls">
          <button id="replay" type="button">Replay</button>
          <div id="status">Current: --:--:--</div>
        </div>
      </div>
      <svg id="chart" role="img" aria-label="Animated wallet send schedule"></svg>
      <div class="message-panel">
        <div class="message-title">Last sent</div>
        <div id="event-timeline" class="event-timeline" aria-live="polite">
          <div class="event-track"></div>
        </div>
        <div id="event-caption" class="message-subtitle">Waiting for first send...</div>
      </div>
    </div>
  </div>
  <script>
    const payload = {json.dumps(payload)};
    const rows = payload.rows;
    const firstMs = payload.first_ms;
    const lastMs = payload.last_ms;
    const spanMs = Math.max(lastMs - firstMs, 1);
    const margin = {{ top: 36, right: 24, bottom: 46, left: 72 }};
    const rowHeight = 10;
    const chartWidth = 1560;
    const chartHeight = margin.top + margin.bottom + rows.length * rowHeight;
    const svg = document.getElementById("chart");
    const status = document.getElementById("status");
    const eventTimeline = document.getElementById("event-timeline");
    const eventCaption = document.getElementById("event-caption");
    svg.setAttribute("viewBox", `0 0 ${{chartWidth}} ${{chartHeight}}`);

    const ns = "http://www.w3.org/2000/svg";
    const plotWidth = chartWidth - margin.left - margin.right;
    const plotHeight = chartHeight - margin.top - margin.bottom;
    const dots = [];
    const events = [];
    const eventTrack = eventTimeline.firstElementChild;
    let lastSentDot = null;

    function xFor(ms) {{
      return margin.left + ((ms - firstMs) / spanMs) * plotWidth;
    }}

    function yFor(walletIndex) {{
      return margin.top + walletIndex * rowHeight + rowHeight / 2;
    }}

    function fmt(ms) {{
      return new Date(ms).toISOString().slice(11, 19);
    }}

    function el(name, attrs = {{}}, text = null) {{
      const node = document.createElementNS(ns, name);
      for (const [key, value] of Object.entries(attrs)) {{
        node.setAttribute(key, value);
      }}
      if (text !== null) {{
        node.textContent = text;
      }}
      return node;
    }}

    svg.appendChild(el("rect", {{ x: 0, y: 0, width: chartWidth, height: chartHeight, fill: "transparent" }}));

    for (let step = 0; step <= 6; step += 1) {{
      const ms = firstMs + (spanMs * step) / 6;
      const x = xFor(ms);
      svg.appendChild(el("line", {{ x1: x, y1: margin.top, x2: x, y2: chartHeight - margin.bottom, class: "tick" }}));
      svg.appendChild(el("text", {{ x, y: chartHeight - 14, class: "tick-label" }}, fmt(ms)));
    }}

    svg.appendChild(el("text", {{ x: chartWidth / 2, y: chartHeight - 4, class: "axis-label", "text-anchor": "middle" }}, "Scheduled UTC time"));

    for (const row of rows) {{
      const y = yFor(row.wallet_index);
      if (row.wallet_index % 25 === 0 || row.wallet_index === rows.length - 1) {{
        svg.appendChild(el("text", {{ x: margin.left - 10, y, class: "wallet-label" }}, `${{row.wallet_index}}`));
      }}
      svg.appendChild(el("line", {{ x1: margin.left, y1: y, x2: chartWidth - margin.right, y2: y, class: "tick", "stroke-opacity": "0.18" }}));
      for (const send of row.sends) {{
        const dot = el("circle", {{
          cx: xFor(send.ms),
          cy: y,
          r: 2.3,
          class: "dot",
          "data-send-ms": send.ms,
        }});
        svg.appendChild(dot);
        dots.push(dot);
        events.push({{ sendMs: send.ms, dot, walletIndex: row.wallet_index }});
      }}
    }}

    svg.appendChild(el("text", {{
      x: 16,
      y: margin.top + plotHeight / 2,
      class: "axis-label",
      transform: `rotate(-90 16 ${{margin.top + plotHeight / 2}})`,
      "text-anchor": "middle",
    }}, "Wallet index"));

    const cursor = el("line", {{
      x1: margin.left,
      y1: margin.top,
      x2: margin.left,
      y2: chartHeight - margin.bottom,
      class: "cursor",
    }});
    svg.appendChild(cursor);

    events.sort((a, b) => a.sendMs - b.sendMs);
    let startAt = null;
    let nextEvent = 0;

    function pushTimelineDot(walletIndex, sendMs) {{
      if (lastSentDot) {{
        lastSentDot.remove();
      }}
      const dot = document.createElement("div");
      dot.className = "event-dot";
      dot.textContent = `[${{walletIndex}}]`;
      const left = ((sendMs - firstMs) / spanMs) * 100;
      dot.style.left = `${{left}}%`;
      dot.title = `account ${{walletIndex}} at ${{fmt(sendMs)}}`;
      eventTimeline.appendChild(dot);
      lastSentDot = dot;
      eventCaption.textContent = `Last sent: [${{walletIndex}}] at ${{fmt(sendMs)}}`;
    }}

    function reset() {{
      for (const dot of dots) {{
        dot.classList.remove("sent");
      }}
      eventTimeline.replaceChildren(eventTrack.cloneNode(true));
      lastSentDot = null;
      nextEvent = 0;
      startAt = null;
      cursor.setAttribute("x1", margin.left);
      cursor.setAttribute("x2", margin.left);
      status.textContent = `Current: ${{fmt(firstMs)}}`;
      eventCaption.textContent = "Waiting for first send...";
      requestAnimationFrame(step);
    }}

    function step(frameAt) {{
      if (startAt === null) {{
        startAt = frameAt;
      }}
      const elapsed = frameAt - startAt;
      const currentMs = Math.min(firstMs + elapsed, lastMs);
      const x = xFor(currentMs);
      cursor.setAttribute("x1", x);
      cursor.setAttribute("x2", x);
      status.textContent = `Current: ${{fmt(currentMs)}}`;

      while (nextEvent < events.length && events[nextEvent].sendMs <= currentMs) {{
        const event = events[nextEvent];
        event.dot.classList.add("sent");
        pushTimelineDot(event.walletIndex, event.sendMs);
        nextEvent += 1;
      }}

      if (currentMs < lastMs) {{
        requestAnimationFrame(step);
      }}
    }}

    document.getElementById("replay").addEventListener("click", reset);
    status.textContent = `Current: ${{fmt(firstMs)}}`;
    requestAnimationFrame(step);
  </script>
</body>
</html>
"""

with open(html_path, "w", encoding="utf-8") as handle:
    handle.write(html)
"###;

        let output = Command::new("python")
            .arg("-c")
            .arg(SCRIPT)
            .arg(csv_path)
            .arg(html_path)
            .output()?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!(
            "python html render failed: status={} stdout={} stderr={}",
            output.status,
            stdout.trim(),
            stderr.trim()
        );
    }

    #[test]
    fn window_ms_rejects_zero() {
        let error = window_ms(0).unwrap_err();
        assert_eq!(error.to_string(), "window-secs must be > 0");
    }

    #[test]
    fn slot_phase_wraps_each_window() {
        assert_eq!(slot_phase_ms(10, 0, 5_000), 0);
        assert_eq!(slot_phase_ms(10, 1, 5_000), 10);
        assert_eq!(slot_phase_ms(10, 2, 5_000), 20);
        assert_eq!(slot_phase_ms(10, 499, 5_000), 4_990);
        assert_eq!(slot_phase_ms(10, 500, 5_000), 0);
    }

    #[test]
    fn validate_legacy_step_ms_rejects_non_divisor() {
        let error = validate_legacy_step_ms(7, 5_000).unwrap_err();
        assert_eq!(
            error.to_string(),
            "step-ms must divide the configured window for slot scheduling"
        );
    }

    #[test]
    fn validate_fixed_step_ms_rejects_dense_spacing() {
        let error = validate_fixed_step_ms(51, 100, 5_000).unwrap_err();
        assert_eq!(
            error.to_string(),
            "total-wallets * step-ms must be < the configured window"
        );
    }

    #[test]
    fn inferred_phases_evenly_spread_wallets() {
        assert_eq!(
            fixed_phases_ms(3, None, 5_000).unwrap(),
            vec![1_250, 2_500, 3_750]
        );
    }

    #[test]
    fn inferred_phases_reject_non_interior_ms() {
        let error = fixed_phases_ms(5_000, None, 5_000).unwrap_err();
        assert_eq!(
            error.to_string(),
            "could not infer interior fixed phases for total-wallets=5000 in the configured window"
        );
    }

    #[test]
    fn inferred_phases_reject_duplicate_ms() {
        let error = fixed_phases_ms(5_001, None, 5_000).unwrap_err();
        assert_eq!(
            error.to_string(),
            "could not infer interior fixed phases for total-wallets=5001 in the configured window"
        );
    }

    #[test]
    fn explicit_phases_use_step_ms() {
        assert_eq!(
            fixed_phases_ms(3, Some(100), 5_000).unwrap(),
            vec![100, 200, 300]
        );
    }

    #[test]
    fn next_slot_time_rolls_to_same_phase_next_window() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_043);
        let next = next_slot_time(after, 10, 1, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(10_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_uses_same_window_when_phase_is_future() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_007);
        let next = next_slot_time(after, 10, 1, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(5_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_uses_exact_boundary() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_010);
        let next = next_slot_time(after, 10, 1, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(5_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_wraps_slot_phase_after_full_window() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(13_400);
        let next = next_slot_time(after, 10, 500, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(15_000);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_uses_current_window_when_future() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_200);
        let next = next_fixed_phase_time(after, 300, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(5_300);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_skips_exact_boundary_to_next_window() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_300);
        let next = next_fixed_phase_time(after, 300, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(10_300);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_skips_missed_window() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(5_301);
        let next = next_fixed_phase_time(after, 300, 5_000).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(10_300);
        assert_eq!(next, expected);
    }

    #[test]
    fn timeout_error_includes_balance() {
        let error = format_timeout_error("0:abc", 42, Duration::from_millis(60_000), Ok(123_456));
        assert_eq!(
            error,
            "timeout waiting for stream update: address=0:abc prev_lt=42 waited_ms=60000 balance=123456"
        );
    }

    #[test]
    fn timeout_error_includes_balance_query_error() {
        let error = format_timeout_error(
            "0:def",
            77,
            Duration::from_millis(60_500),
            Err("rpc exploded".to_owned()),
        );
        assert_eq!(
            error,
            "timeout waiting for stream update: address=0:def prev_lt=77 waited_ms=60500 balance_query_error=rpc exploded"
        );
    }

    #[test]
    #[ignore = "writes temp artifacts and requires python"]
    fn generate_multi_wallet_schedule_csv_and_animation_html() {
        let now = SystemTime::now();
        let rows = build_wallet_schedule(now, 250, 60, 5_000).unwrap();

        assert_eq!(rows.len(), 250);
        assert_eq!(rows.first().map(|row| row.wallet_index), Some(0));
        assert_eq!(rows.last().map(|row| row.wallet_index), Some(249));
        let unique = rows
            .iter()
            .map(|row| row.phase_ms)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), rows.len());

        for row in &rows {
            assert_eq!(row.sends.len(), 60);

            let first = next_fixed_phase_time(now, row.phase_ms, 5_000).unwrap();
            assert_eq!(row.sends[0], first);

            for pair in row.sends.windows(2) {
                assert_eq!(pair[1].duration_since(pair[0]).unwrap(), Duration::from_secs(5));
            }
        }

        let out_dir = schedule_output_dir().unwrap();
        let csv_path = out_dir.join("wallet_schedule.csv");
        let html_path = out_dir.join("wallet_schedule.html");

        write_wallet_schedule_csv(&csv_path, &rows).unwrap();
        render_wallet_schedule_animation_html(&csv_path, &html_path).unwrap();

        assert!(csv_path.is_file(), "missing csv artifact at {:?}", csv_path);
        assert!(html_path.is_file(), "missing html artifact at {:?}", html_path);

        let html = std::fs::read_to_string(&html_path).unwrap();
        assert!(html.contains("requestAnimationFrame"));
        assert!(html.contains("dots stay red after send time"));
        assert!(html.contains("Replay"));

        println!("wallet schedule csv: {}", csv_path.display());
        println!("wallet schedule animation: {}", html_path.display());
    }

    #[tokio::test]
    async fn wait_for_wallet_update_accepts_newer_lt() {
        let (tx, mut rx) = tokio::sync::watch::channel(None);
        let (_gap_tx, mut gaps) = tokio::sync::watch::channel(0);

        tokio::spawn(async move {
            tx.send_replace(Some(stream::StreamUpdate {
                address: "0:abc".to_owned(),
                max_lt: 10,
            }));
        });

        assert_eq!(
            wait_for_wallet_update(
                || async { Ok(false) },
                &mut rx,
                &mut gaps,
                9,
                Duration::from_secs(1),
            )
            .await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn wait_for_wallet_update_ignores_same_lt() {
        let (tx, mut rx) = tokio::sync::watch::channel(None);
        let (_gap_tx, mut gaps) = tokio::sync::watch::channel(0);
        tx.send_replace(Some(stream::StreamUpdate {
            address: "0:abc".to_owned(),
            max_lt: 9,
        }));

        assert_eq!(
            wait_for_wallet_update(
                || async { Ok(false) },
                &mut rx,
                &mut gaps,
                9,
                Duration::from_millis(20),
            )
            .await,
            Err(WaitForUpdateError::TimedOut)
        );
    }

    #[tokio::test]
    async fn wait_for_wallet_update_times_out_once_per_attempt() {
        let (_tx, mut rx) = tokio::sync::watch::channel(None);
        let (_gap_tx, mut gaps) = tokio::sync::watch::channel(0);

        assert_eq!(
            wait_for_wallet_update(
                || async { Ok(false) },
                &mut rx,
                &mut gaps,
                9,
                Duration::from_millis(20),
            )
            .await,
            Err(WaitForUpdateError::TimedOut)
        );
    }

    #[tokio::test]
    async fn wait_for_wallet_update_reconciles_after_gap() {
        let (_tx, mut rx) = tokio::sync::watch::channel(None);
        let (gap_tx, mut gaps) = tokio::sync::watch::channel(0);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            gap_tx.send_replace(1);
        });

        assert_eq!(
            wait_for_wallet_update(
                || async { Ok(true) },
                &mut rx,
                &mut gaps,
                9,
                Duration::from_secs(1),
            )
            .await,
            Ok(())
        );
    }
}
