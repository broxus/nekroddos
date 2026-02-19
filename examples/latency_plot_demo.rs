use anyhow::Result;
use rand::Rng;
use rand_distr::{Distribution, Normal};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod plotting {
    pub use nekroddos::latency::plotting::*;
}

fn main() -> Result<()> {
    env_logger::init();

    let mut rng = rand::thread_rng();

    // Create bimodal distribution: 70% around 500ms, 30% around 3500ms
    let normal_fast = Normal::new(500.0, 100.0).unwrap();
    let normal_slow = Normal::new(3500.0, 200.0).unwrap();

    let mut latencies = Vec::new();
    for _ in 0..1000 {
        let latency: f64 = if rng.gen_bool(0.7) {
            // Fast mode: centered around 500ms
            normal_fast.sample(&mut rng)
        } else {
            // Slow mode: centered around 3500ms
            normal_slow.sample(&mut rng)
        };

        let latency = latency.clamp(100.0, 5000.0);
        latencies.push(Duration::from_millis(latency as u64));
    }

    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min = latencies[0];
    let max = latencies[latencies.len() - 1];

    println!("Generated {} mock latency measurements", latencies.len());
    println!("Average: {avg:?}");
    println!("P50: {p50:?}");
    println!("P95: {p95:?}");
    println!("P99: {p99:?}");
    println!("Min: {min:?}");
    println!("Max: {max:?}");

    let stats = plotting::LatencyStats {
        avg,
        p50,
        p95,
        p99,
        min,
        max,
    };

    let base_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 3600;

    let timestamped_latencies: Vec<plotting::TimestampedLatency> = latencies
        .iter()
        .enumerate()
        .map(|(i, &latency)| {
            let timestamp = UNIX_EPOCH + Duration::from_secs(base_time + (i as u64 * 3));
            plotting::TimestampedLatency { timestamp, latency }
        })
        .collect();

    let combined_path = PathBuf::from("demo_combined_plots.html");
    plotting::generate_combined_plots(
        &latencies,
        &timestamped_latencies,
        combined_path.clone(),
        &stats,
        None, // Auto-calculate optimal window based on data
        Some(1000.0),
    )?;
    println!("\nCombined plot saved to: {combined_path:?}");

    Ok(())
}
