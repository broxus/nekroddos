pub mod combined_plot;
pub mod plotting;

use crate::models::GenericDeploymentInfo;
use crate::stream;
use crate::{send, Args};
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use ed25519_dalek::Keypair;
use everscale_rpc_client::RpcClient;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;

#[derive(Parser, Debug, Clone)]
pub struct LatencyTestArgs {
    #[clap(short, long)]
    /// Number of transactions to send
    num_txs: usize,

    #[clap(long, value_name = "MS", default_value = "10")]
    /// Slot spacing in milliseconds within each second; must divide 1000
    step_ms: u64,

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

    validate_step_ms(latency_args.step_ms)?;
    if common_args.no_wait {
        anyhow::bail!("--no-wait is not supported by latency");
    }

    stream::init(
        client.clone(),
        common_args.endpoints.clone(),
        common_args.no_stream,
    )
    .await?;

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

    let wallet = walkdir::WalkDir::new(&network_deployments_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().map(|e| e == "json").unwrap_or(false))
        .find(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.to_lowercase().contains("ever-wallet"))
                .unwrap_or(false)
        })
        .context("No factory abi")?;
    let deployment: GenericDeploymentInfo = serde_json::from_slice(&std::fs::read(wallet.path())?)?;
    let sender = deployment.address;

    log::info!("Sender address: {}", sender);

    let initial_balance = client
        .get_contract_state(&sender, None)
        .await?
        .unwrap()
        .account
        .storage
        .balance
        .grams
        .as_u128();

    let required_balance = COST_PER_TRANSACTION * latency_args.num_txs as u64;
    let max_iterations = initial_balance / COST_PER_TRANSACTION as u128;
    log::info!(
        "Starting latency test - sending {} transactions on {}ms per-second phases",
        latency_args.num_txs,
        latency_args.step_ms
    );
    log::info!(
        "Initial balance: {}, required balance: {}, max sendable transactions: {}",
        initial_balance,
        required_balance,
        max_iterations
    );

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

    let requested_txs = std::cmp::min(latency_args.num_txs, max_iterations as usize);
    let mut latencies = Vec::with_capacity(requested_txs);
    let mut timestamped_latencies = Vec::with_capacity(requested_txs);
    let mut sent_count = 0usize;
    let mut confirmed_count = 0usize;
    let mut failed_count = 0usize;
    let mut timed_out_count = 0usize;
    let mut slot_index = 0usize;

    let receiver = ton_block::MsgAddressInt::from_str(
        "0:0000000000000000000000000000000000000000000000000000000000000000",
    )?;
    while sent_count < requested_txs {
        sleep_until_slot(latency_args.step_ms, slot_index).await?;

        let outcome = send_test_transaction(
            &client,
            keypair,
            &sender,
            &receiver,
            latency_args.amount,
            &mut log_writer,
        )
        .await;
        sent_count += 1;

        handle_tx_completion(
            outcome,
            &mut latencies,
            &mut timestamped_latencies,
            &mut csv_writer,
            &mut confirmed_count,
            &mut failed_count,
            &mut timed_out_count,
        )?;

        if sent_count < requested_txs {
            slot_index += 1;
        }
    }

    if !latencies.is_empty() {
        latencies.sort();
        let total: Duration = latencies.iter().sum();
        let avg = total / latencies.len() as u32;
        let p50 = latencies[latencies.len() / 2];
        let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
        let min = latencies[0];
        let max = latencies[latencies.len() - 1];

        log::info!("Latency test results:");
        log::info!("Sent transactions: {}", sent_count);
        log::info!("Confirmed transactions: {}", confirmed_count);
        log::info!("Failed transactions: {}", failed_count);
        log::info!("Timed out transactions: {}", timed_out_count);
        log::info!("Average latency: {:?}", avg);
        log::info!("P50 latency: {:?}", p50);
        log::info!("P95 latency: {:?}", p95);
        log::info!("P99 latency: {:?}", p99);

        if let Some(plot_path) = latency_args.plot {
            let stats = plotting::LatencyStats {
                avg,
                p50,
                p95,
                p99,
                min,
                max,
            };

            plotting::generate_combined_plots(
                &latencies,
                &timestamped_latencies,
                plot_path.clone(),
                &stats,
                latency_args.time_window,
                latency_args.sla_threshold.map(|t| t as f64),
            )?;

            log::info!("Plot saved to: {:?}", plot_path);
        }
    } else {
        log::info!("Latency test results:");
        log::info!("Sent transactions: {}", sent_count);
        log::info!("Confirmed transactions: {}", confirmed_count);
        log::info!("Failed transactions: {}", failed_count);
        log::info!("Timed out transactions: {}", timed_out_count);
    }

    Ok(())
}

fn validate_step_ms(step_ms: u64) -> Result<()> {
    if step_ms == 0 {
        anyhow::bail!("step-ms must be > 0");
    }
    if 1000 % step_ms != 0 {
        anyhow::bail!("step-ms must divide 1000 for per-second slot scheduling");
    }
    Ok(())
}

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

enum TxCompletion {
    Confirmed {
        started_at: SystemTime,
        latency: Duration,
    },
    Failed {
        error: anyhow::Error,
        timed_out: bool,
    },
}

fn handle_tx_completion(
    outcome: TxCompletion,
    latencies: &mut Vec<Duration>,
    timestamped_latencies: &mut Vec<plotting::TimestampedLatency>,
    csv_writer: &mut Option<std::fs::File>,
    confirmed_count: &mut usize,
    failed_count: &mut usize,
    timed_out_count: &mut usize,
) -> Result<()> {
    match outcome {
        TxCompletion::Confirmed {
            started_at,
            latency,
        } => {
            latencies.push(latency);
            timestamped_latencies.push(plotting::TimestampedLatency {
                timestamp: started_at,
                latency,
            });
            *confirmed_count += 1;
            log::debug!("Transaction confirmed in {:?}", latency);

            if let Some(writer) = csv_writer {
                writeln!(writer, "{}", latency.as_nanos())?;
                writer.flush()?;
            }
        }
        TxCompletion::Failed { error, timed_out } => {
            if timed_out {
                *timed_out_count += 1;
            } else {
                *failed_count += 1;
            }
            log::error!("Transaction failed: {error}");
        }
    }
    Ok(())
}

fn slot_phase_ms(step_ms: u64, slot_index: usize) -> u64 {
    let slots_per_second = 1000 / step_ms;
    let slot_index = u64::try_from(slot_index).unwrap();
    (slot_index % slots_per_second) * step_ms
}

fn next_slot_time(after: SystemTime, step_ms: u64, slot_index: usize) -> Result<SystemTime> {
    let since_epoch = after.duration_since(SystemTime::UNIX_EPOCH)?;
    let second_ns = 1_000_000_000u128;
    let phase_ns = u128::from(slot_phase_ms(step_ms, slot_index)) * 1_000_000;
    let current_ns = since_epoch.as_nanos();
    let second_start_ns = current_ns - (current_ns % second_ns);
    let mut target_ns = second_start_ns + phase_ns;

    if target_ns < current_ns {
        target_ns += second_ns;
    }

    Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(u64::try_from(target_ns).unwrap()))
}

async fn sleep_until_slot(step_ms: u64, slot_index: usize) -> Result<()> {
    let now = SystemTime::now();
    let wait = next_slot_time(now, step_ms, slot_index)?
        .duration_since(now)
        .unwrap_or(Duration::ZERO);
    tokio::time::sleep(wait).await;
    Ok(())
}

async fn send_test_transaction(
    client: &RpcClient,
    keypair: &Keypair,
    sender: &ton_block::MsgAddressInt,
    receiver: &ton_block::MsgAddressInt,
    amount: u64,
    log_writer: &mut Option<std::fs::File>,
) -> TxCompletion {
    let started_at = SystemTime::now();
    let start = Instant::now();
    let payload = ton_types::BuilderData::new();
    let state = match client.get_contract_state(sender, None).await {
        Ok(Some(state)) => state,
        Ok(None) => {
            return TxCompletion::Failed {
                error: anyhow::anyhow!("sender state not found"),
                timed_out: false,
            };
        }
        Err(error) => {
            return TxCompletion::Failed {
                error,
                timed_out: false,
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
                error,
                timed_out: false,
            };
        }
    };
    let mut updates = match stream.subscribe_addr(&address).await {
        Ok(updates) => updates,
        Err(error) => {
            return TxCompletion::Failed {
                error,
                timed_out: false,
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
                error,
                timed_out: false,
            };
        }
    };

    let log_data = match build_log_data(log_writer.is_some(), &outcome.message) {
        Ok(log_data) => log_data,
        Err(error) => {
            return TxCompletion::Failed {
                error,
                timed_out: false,
            };
        }
    };

    if let Err(err) = outcome.broadcast_result {
        if let Err(error) = write_tx_log(&log_data, log_writer, true) {
            return TxCompletion::Failed {
                error,
                timed_out: false,
            };
        }
        return TxCompletion::Failed {
            error: err,
            timed_out: false,
        };
    }

    loop {
        match tokio::time::timeout(Duration::from_secs(60), updates.recv()).await {
            Ok(Ok(update)) => {
                let _ = (update.gen_utime, update.dropped);
                if update.address == address && update.max_lt > prev_lt {
                    if let Err(error) = write_tx_log(&log_data, log_writer, false) {
                        return TxCompletion::Failed {
                            error,
                            timed_out: false,
                        };
                    }
                    return TxCompletion::Confirmed {
                        started_at,
                        latency: start.elapsed(),
                    };
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                if let Err(error) = write_tx_log(&log_data, log_writer, true) {
                    return TxCompletion::Failed {
                        error,
                        timed_out: false,
                    };
                }
                return TxCompletion::Failed {
                    error: anyhow::anyhow!("stream updates channel closed"),
                    timed_out: false,
                };
            }
            Err(_) => {
                if let Err(error) = write_tx_log(&log_data, log_writer, true) {
                    return TxCompletion::Failed {
                        error,
                        timed_out: false,
                    };
                }
                return TxCompletion::Failed {
                    error: anyhow::anyhow!("timeout waiting for stream update"),
                    timed_out: true,
                };
            }
        }
    }
}

fn build_log_data(
    should_log: bool,
    message: &ton_block::Message,
) -> Result<Option<LogData>> {
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

fn write_tx_log(
    log_data: &Option<LogData>,
    log_writer: &mut Option<std::fs::File>,
    lost: bool,
) -> Result<()> {
    if let (Some(log_data), Some(writer)) = (log_data.as_ref(), log_writer.as_mut()) {
        log_data.write(writer, lost)?;
    }
    Ok(())
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
    fn validate_step_ms_rejects_non_divisor() {
        let error = validate_step_ms(7).unwrap_err();
        assert_eq!(
            error.to_string(),
            "step-ms must divide 1000 for per-second slot scheduling"
        );
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
}
