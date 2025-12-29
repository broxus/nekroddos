use crate::Args;
use anyhow::{Context, Result};
use clap::Parser;
use everscale_rpc_client::RpcClient;
use governor::{Jitter, RateLimiter};
use histogram::{AtomicHistogram, Histogram};
use rand::prelude::*;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use ton_block::MsgAddressInt;

#[derive(Parser, Debug, Clone)]
pub struct DosTestArgs {
    #[clap(long)]
    /// Code hash as hex string
    code_hash: String,

    #[clap(short, long, default_value = "10")]
    /// Requests per second for random account fetches
    rps: u32,

    #[clap(short, long, default_value = "60")]
    /// Test duration in seconds
    duration: u64,

    #[clap(long, default_value = "100")]
    /// Maximum concurrent requests
    max_concurrent: usize,
}

pub(crate) async fn run(
    dos_args: DosTestArgs,
    _common_args: Args,
    client: RpcClient,
) -> Result<()> {
    let code_hash_bytes = hex::decode(&dos_args.code_hash).context("Failed to decode code hash")?;
    let code_hash: [u8; 32] = code_hash_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Code hash must be exactly 32 bytes (64 hex chars)"))?;

    log::info!("Fetching accounts with code hash: {}", dos_args.code_hash);

    let mut all_accounts = Vec::new();
    let mut continuation: Option<MsgAddressInt> = None;

    loop {
        let batch = client
            .get_accounts_by_code_hash(code_hash, continuation.as_ref(), 100)
            .await
            .context("Failed to fetch accounts by code hash")?;

        if batch.is_empty() {
            break;
        }

        let batch_size = batch.len();
        continuation = batch.last().cloned();
        all_accounts.extend(batch);

        log::info!(
            "Fetched {} accounts (total: {})",
            batch_size,
            all_accounts.len()
        );

        if batch_size < 100 {
            break;
        }
    }

    if all_accounts.is_empty() {
        return Err(anyhow::anyhow!(
            "No accounts found with the given code hash"
        ));
    }

    log::info!("Total accounts found: {}", all_accounts.len());
    let all_accounts = Arc::new(all_accounts);

    let rate_limiter = Arc::new(RateLimiter::direct(
        governor::Quota::per_second(std::num::NonZeroU32::new(dos_args.rps).unwrap())
            .allow_burst(std::num::NonZeroU32::new(dos_args.rps.max(10)).unwrap()),
    ));
    let jitter = Jitter::new(Duration::ZERO, Duration::from_millis(100));

    const MAX_VALUE_POWER: u8 = 34;
    const GROUPING_POWER: u8 = 10;
    let histogram = Arc::new(
        AtomicHistogram::new(GROUPING_POWER, MAX_VALUE_POWER)
            .context("Failed to create histogram")?,
    );

    let success = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let semaphore = Arc::new(tokio::sync::Semaphore::new(dos_args.max_concurrent));

    let (task_tx, mut task_rx) = mpsc::channel::<()>(dos_args.max_concurrent * 2);

    let monitor_histogram = histogram.clone();
    let monitor_success = success.clone();
    let monitor_failed = failed.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let success_count = monitor_success.load(std::sync::atomic::Ordering::Relaxed);
            let failed_count = monitor_failed.load(std::sync::atomic::Ordering::Relaxed);
            let total = success_count + failed_count;

            if total > 0 {
                log::info!(
                    "Progress: {} requests (success: {}, failed: {})",
                    total,
                    success_count,
                    failed_count
                );
                let snapshot = monitor_histogram.load();
                print_histogram(&snapshot);
            }
        }
    });

    let test_duration = Duration::from_secs(dos_args.duration);
    let test_start = Instant::now();
    let mut rng = rand::thread_rng();
    let mut spawned_count = 0u64;

    log::info!(
        "Starting DOS test for {} seconds with {} RPS",
        dos_args.duration,
        dos_args.rps
    );

    while test_start.elapsed() < test_duration {
        rate_limiter.until_ready_with_jitter(jitter).await;

        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let address_idx = rng.gen_range(0..all_accounts.len());
        let address = all_accounts[address_idx].clone();

        let client = client.clone();
        let histogram = histogram.clone();
        let success = success.clone();
        let failed = failed.clone();
        let task_tx = task_tx.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let _task_guard = task_tx;

            let start = Instant::now();
            let result = client.get_contract_state(&address, None).await;
            let elapsed = start.elapsed().as_nanos() as u64;

            match result {
                Ok(_) => {
                    success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    log::debug!("Request failed: {}", e);
                }
            }

            let _ = histogram.increment(elapsed);
        });

        spawned_count += 1;
    }

    log::info!("Spawned {} tasks, waiting for completion...", spawned_count);

    drop(task_tx);

    while task_rx.recv().await.is_some() {}

    monitor_handle.abort();

    let total_success = success.load(std::sync::atomic::Ordering::Relaxed);
    let total_failed = failed.load(std::sync::atomic::Ordering::Relaxed);
    let total_requests = total_success + total_failed;

    log::info!("\n=== Final Statistics ===");
    log::info!("Total requests spawned: {}", spawned_count);
    log::info!("Total requests completed: {}", total_requests);

    if total_requests > 0 {
        log::info!(
            "Successful: {} ({:.2}%)",
            total_success,
            (total_success as f64 / total_requests as f64) * 100.0
        );
        log::info!(
            "Failed: {} ({:.2}%)",
            total_failed,
            (total_failed as f64 / total_requests as f64) * 100.0
        );
    } else {
        log::info!("No requests completed");
    }

    let test_duration_secs = test_start.elapsed().as_secs_f64();
    log::info!("Test duration: {:.1}s", test_duration_secs);

    if test_duration_secs > 0.0 {
        log::info!(
            "Actual RPS: {:.2}",
            total_requests as f64 / test_duration_secs
        );
    }

    log::info!("\n=== Response Time Distribution ===");
    let final_snapshot = histogram.load();
    print_histogram(&final_snapshot);

    Ok(())
}

fn print_histogram(histogram: &Histogram) -> Option<()> {
    fn print_percentile(percentile: f64, hist: &Histogram, mut io: impl Write) -> Option<()> {
        match hist.percentile(percentile) {
            Ok(Some(bucket)) => {
                let low = Duration::from_nanos(bucket.start());
                let low = humantime::format_duration(low);
                let high = Duration::from_nanos(bucket.end());
                let high = humantime::format_duration(high);

                writeln!(
                    io,
                    "Percentile {}%  from {} to {}  => {} count",
                    percentile,
                    low,
                    high,
                    bucket.count()
                )
                .ok()?;
            }
            Ok(None) => {}
            Err(_) => return None,
        }

        Some(())
    }

    let mut io = std::io::stdout().lock();

    print_percentile(0.1, histogram, &mut io)?;
    print_percentile(10.0, histogram, &mut io)?;
    print_percentile(25.0, histogram, &mut io)?;
    print_percentile(50.0, histogram, &mut io)?;
    print_percentile(75.0, histogram, &mut io)?;
    print_percentile(90.0, histogram, &mut io)?;
    print_percentile(95.0, histogram, &mut io)?;
    print_percentile(99.0, histogram, &mut io)?;
    print_percentile(99.9, histogram, &mut io)?;
    print_percentile(99.99, histogram, &mut io)?;
    print_percentile(99.999, histogram, &mut io)?;

    // Time buckets for distribution visualization
    let interesting_times_in_ns = [
        0,
        250_000,        // 250µs
        500_000,        // 500µs
        1_000_000,      // 1ms
        2_500_000,      // 2.5ms
        5_000_000,      // 5ms
        10_000_000,     // 10ms
        25_000_000,     // 25ms
        100_000_000,    // 100ms
        500_000_000,    // 500ms
        1_000_000_000,  // 1s
        10_000_000_000, // 10s
        25_000_000_000, // 25s
    ];

    let count_sum = histogram.into_iter().map(|x| x.count()).sum::<u64>();

    let max_duration_len: usize = interesting_times_in_ns
        .iter()
        .map(|x| Duration::from_nanos(*x))
        .map(|x| humantime::format_duration(x).to_string().len())
        .max()
        .unwrap();
    let max_count_len: usize = 9;

    writeln!(&mut io, "\nLatency distribution:").ok()?;
    for pair in interesting_times_in_ns.windows(2) {
        let low = pair[0];
        let high = pair[1];

        let count_between = histogram
            .into_iter()
            .filter(|x| x.start() > low && x.start() < high)
            .map(|x| x.count())
            .sum::<u64>();
        let between_percent = if count_sum > 0 {
            count_between * 100 / count_sum
        } else {
            0
        };
        let num_stars = between_percent / 2; // 2% per star
        let pattern = "*".repeat(num_stars as usize);
        let high_duration = humantime::format_duration(Duration::from_nanos(high)).to_string();
        let high_duration = format!("{:width$}", high_duration, width = max_duration_len);

        let count_between_str = format!("{:width$}", count_between, width = max_count_len);

        writeln!(
            &mut io,
            "{} [{}] {}",
            high_duration, count_between_str, pattern
        )
        .ok()?;
    }

    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use histogram::Histogram;

    #[test]
    fn test_print_histogram() {
        const MAX_VALUE_POWER: u8 = 34;
        const GROUPING_POWER: u8 = 10;
        let mut histogram = Histogram::new(GROUPING_POWER, MAX_VALUE_POWER).unwrap();

        for _ in 0..600_077 {
            histogram.increment(2_000_000).unwrap();
        }

        for _ in 0..9_138_246 {
            histogram.increment(4_000_000).unwrap();
        }
        for _ in 0..9_138_246 {
            histogram.increment(8_000_000).unwrap();
        }

        for _ in 0..9_377_994 {
            histogram.increment(16_000_000).unwrap();
        }
        for _ in 0..940_592 {
            histogram.increment(33_000_000).unwrap();
        }

        for _ in 0..3_824_248 {
            histogram.increment(67_000_000).unwrap();
        }
        for _ in 0..1_477_687 {
            histogram.increment(134_000_000).unwrap();
        }

        for _ in 0..16_828 {
            histogram.increment(536_000_000).unwrap();
        }
        for _ in 0..4_421 {
            histogram.increment(2_147_000_000).unwrap();
        }
        for _ in 0..1_230 {
            histogram.increment(4_294_000_000).unwrap();
        }

        let result = print_histogram(&histogram);
        assert!(result.is_some());
    }

    #[test]
    fn test_print_histogram_empty() {
        const MAX_VALUE_POWER: u8 = 34;
        const GROUPING_POWER: u8 = 10;
        let histogram = Histogram::new(GROUPING_POWER, MAX_VALUE_POWER).unwrap();

        let result = print_histogram(&histogram);
        assert!(result.is_some());
    }

    #[test]
    fn test_print_histogram_single_bucket() {
        const MAX_VALUE_POWER: u8 = 34;
        const GROUPING_POWER: u8 = 10;
        let mut histogram = Histogram::new(GROUPING_POWER, MAX_VALUE_POWER).unwrap();

        for _ in 0..1000 {
            histogram.increment(10_000_000).unwrap();
        }

        let result = print_histogram(&histogram);
        assert!(result.is_some());
    }

    #[test]
    fn test_percentile_calculations() {
        const MAX_VALUE_POWER: u8 = 34;
        const GROUPING_POWER: u8 = 10;
        let mut histogram = Histogram::new(GROUPING_POWER, MAX_VALUE_POWER).unwrap();

        for i in 1..=100 {
            for _ in 0..10 {
                histogram.increment(i * 1_000_000).unwrap();
            }
        }

        let p50 = histogram.percentile(50.0).unwrap();
        assert!(p50.is_some());

        let p99 = histogram.percentile(99.0).unwrap();
        assert!(p99.is_some());

        if let Some(bucket) = p50 {
            assert!(bucket.start() >= 45_000_000 && bucket.end() <= 55_000_000);
        }
    }
}
