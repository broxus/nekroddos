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
use tokio::sync::{broadcast, mpsc};
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
        )
        .await
    } else {
        run_multi_wallet(
            latency_args,
            keypair,
            client,
            &deployments_path,
            COST_PER_TRANSACTION,
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
) -> Result<()> {
    let step_ms = latency_args.step_ms.unwrap_or(10);
    validate_legacy_step_ms(step_ms)?;

    let sender = load_single_sender(deployments_path)?;
    log::info!("Sender address: {}", sender);

    let initial_balance = current_balance(&client, &sender).await?;
    let required_balance = cost_per_transaction * latency_args.num_txs as u64;
    let max_iterations = initial_balance / u128::from(cost_per_transaction);
    let requested_txs = std::cmp::min(latency_args.num_txs, max_iterations as usize);

    log::info!(
        "Starting latency test - sending {} transactions on {}ms per-second phases",
        latency_args.num_txs,
        step_ms
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
        sleep_until_slot(step_ms, slot_index).await?;
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
) -> Result<()> {
    let wallet_count = latency_args.total_wallets as usize;
    let per_wallet_txs = latency_args.num_txs.div_ceil(wallet_count);
    let amount = latency_args.amount;
    let phases_ms = fixed_phases_ms(latency_args.total_wallets, latency_args.step_ms)?;
    let senders = load_sender_wallets(
        client.clone(),
        deployments_path,
        latency_args.total_wallets,
        keypair,
    )
    .await?;
    let keypair_bytes = keypair.to_bytes();

    log::info!(
        "Starting multi-wallet latency test - {} wallets, {} sends per wallet, {} total scheduled sends",
        wallet_count,
        per_wallet_txs,
        per_wallet_txs.saturating_mul(wallet_count)
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
                sleep_until_fixed_phase(phase_ms).await?;
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

fn validate_legacy_step_ms(step_ms: u64) -> Result<()> {
    if step_ms == 0 {
        anyhow::bail!("step-ms must be > 0");
    }
    if 1000 % step_ms != 0 {
        anyhow::bail!("step-ms must divide 1000 for per-second slot scheduling");
    }
    Ok(())
}

fn validate_fixed_step_ms(total_wallets: u32, step_ms: u64) -> Result<()> {
    if step_ms == 0 {
        anyhow::bail!("step-ms must be > 0");
    }
    if u64::from(total_wallets).saturating_mul(step_ms) >= 1000 {
        anyhow::bail!("total-wallets * step-ms must be < 1000");
    }
    Ok(())
}

fn fixed_phases_ms(total_wallets: u32, step_ms: Option<u64>) -> Result<Vec<u64>> {
    if let Some(step_ms) = step_ms {
        validate_fixed_step_ms(total_wallets, step_ms)?;
        return Ok((1..=total_wallets)
            .map(|index| u64::from(index) * step_ms)
            .collect());
    }

    let phases: Vec<_> = (0..total_wallets)
        .map(|index| ((u64::from(index) + 1) * 1000) / (u64::from(total_wallets) + 1))
        .collect();
    if phases
        .iter()
        .any(|phase_ms| *phase_ms == 0 || *phase_ms >= 1000)
    {
        anyhow::bail!("could not infer interior fixed phases for total-wallets={total_wallets}");
    }
    let unique = phases
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != phases.len() {
        anyhow::bail!("could not infer interior fixed phases for total-wallets={total_wallets}");
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
        .map_err(Into::into)
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

fn slot_phase_ms(step_ms: u64, slot_index: usize) -> u64 {
    let slots_per_second = 1000 / step_ms;
    let slot_index = u64::try_from(slot_index).unwrap();
    (slot_index % slots_per_second) * step_ms
}

fn next_slot_time(after: SystemTime, step_ms: u64, slot_index: usize) -> Result<SystemTime> {
    let phase_ms = slot_phase_ms(step_ms, slot_index);
    next_phase_time(after, phase_ms, false)
}

fn next_fixed_phase_time(after: SystemTime, phase_ms: u64) -> Result<SystemTime> {
    next_phase_time(after, phase_ms, true)
}

fn next_phase_time(after: SystemTime, phase_ms: u64, strict_future: bool) -> Result<SystemTime> {
    let since_epoch = after.duration_since(SystemTime::UNIX_EPOCH)?;
    let second_ns = 1_000_000_000u128;
    let phase_ns = u128::from(phase_ms) * 1_000_000;
    let current_ns = since_epoch.as_nanos();
    let second_start_ns = current_ns - (current_ns % second_ns);
    let mut target_ns = second_start_ns + phase_ns;

    if (strict_future && target_ns <= current_ns) || (!strict_future && target_ns < current_ns) {
        target_ns += second_ns;
    }

    Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(u64::try_from(target_ns).unwrap()))
}

async fn sleep_until_slot(step_ms: u64, slot_index: usize) -> Result<()> {
    sleep_until(next_slot_time(SystemTime::now(), step_ms, slot_index)?).await
}

async fn sleep_until_fixed_phase(phase_ms: u64) -> Result<()> {
    sleep_until(next_fixed_phase_time(SystemTime::now(), phase_ms)?).await
}

async fn sleep_until(target: SystemTime) -> Result<()> {
    let now = SystemTime::now();
    let wait = target.duration_since(now).unwrap_or(Duration::ZERO);
    tokio::time::sleep(wait).await;
    Ok(())
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
            return TxCompletion::Failed {
                error: error.to_string(),
                timed_out: false,
                log_record: None,
            };
        }
    };

    if let Err(error) = outcome.broadcast_result {
        return TxCompletion::Failed {
            error: error.to_string(),
            timed_out: false,
            log_record: log_data.map(|data| LogRecord { data, lost: true }),
        };
    }

    loop {
        match tokio::time::timeout(Duration::from_secs(60), updates.recv()).await {
            Ok(Ok(update)) => {
                let _ = (update.gen_utime, update.dropped);
                if update.address == address && update.max_lt > prev_lt {
                    return TxCompletion::Confirmed {
                        started_at,
                        latency: start.elapsed(),
                        log_record: log_data.map(|data| LogRecord { data, lost: false }),
                    };
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return TxCompletion::Failed {
                    error: "stream updates channel closed".to_owned(),
                    timed_out: false,
                    log_record: log_data.map(|data| LogRecord { data, lost: true }),
                };
            }
            Err(_) => {
                return TxCompletion::Failed {
                    error: "timeout waiting for stream update".to_owned(),
                    timed_out: true,
                    log_record: log_data.map(|data| LogRecord { data, lost: true }),
                };
            }
        }
    }
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

    #[test]
    fn slot_phase_wraps_each_second() {
        assert_eq!(slot_phase_ms(10, 0), 0);
        assert_eq!(slot_phase_ms(10, 1), 10);
        assert_eq!(slot_phase_ms(10, 2), 20);
        assert_eq!(slot_phase_ms(10, 99), 990);
        assert_eq!(slot_phase_ms(10, 100), 0);
    }

    #[test]
    fn validate_legacy_step_ms_rejects_non_divisor() {
        let error = validate_legacy_step_ms(7).unwrap_err();
        assert_eq!(
            error.to_string(),
            "step-ms must divide 1000 for per-second slot scheduling"
        );
    }

    #[test]
    fn validate_fixed_step_ms_rejects_dense_spacing() {
        let error = validate_fixed_step_ms(10, 100).unwrap_err();
        assert_eq!(error.to_string(), "total-wallets * step-ms must be < 1000");
    }

    #[test]
    fn inferred_phases_evenly_spread_wallets() {
        assert_eq!(
            fixed_phases_ms(10, None).unwrap(),
            vec![90, 181, 272, 363, 454, 545, 636, 727, 818, 909]
        );
    }

    #[test]
    fn inferred_phases_reject_non_interior_ms() {
        let error = fixed_phases_ms(1000, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "could not infer interior fixed phases for total-wallets=1000"
        );
    }

    #[test]
    fn inferred_phases_reject_duplicate_ms() {
        let error = fixed_phases_ms(1001, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "could not infer interior fixed phases for total-wallets=1001"
        );
    }

    #[test]
    fn explicit_phases_use_step_ms() {
        assert_eq!(fixed_phases_ms(3, Some(100)).unwrap(), vec![100, 200, 300]);
    }

    #[test]
    fn next_slot_time_rolls_to_same_phase_next_second() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_043);
        let next = next_slot_time(after, 10, 1).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(2_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_uses_same_second_when_phase_is_future() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_007);
        let next = next_slot_time(after, 10, 1).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(1_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_uses_exact_boundary() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_010);
        let next = next_slot_time(after, 10, 1).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(1_010);
        assert_eq!(next, expected);
    }

    #[test]
    fn next_slot_time_wraps_slot_phase_after_full_second() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(3_400);
        let next = next_slot_time(after, 10, 100).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(4_000);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_uses_current_second_when_future() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_200);
        let next = next_fixed_phase_time(after, 300).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(1_300);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_skips_exact_boundary_to_next_second() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_300);
        let next = next_fixed_phase_time(after, 300).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(2_300);
        assert_eq!(next, expected);
    }

    #[test]
    fn fixed_phase_skips_missed_second() {
        let after = SystemTime::UNIX_EPOCH + Duration::from_millis(1_301);
        let next = next_fixed_phase_time(after, 300).unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(2_300);
        assert_eq!(next, expected);
    }
}
