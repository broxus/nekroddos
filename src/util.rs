use crate::Args;
use everscale_rpc_client::RpcClient;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;
use std::num::NonZeroU32;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TestEnv {
    pub barrier: Arc<Barrier>,
    pub num_iterations: u32,
    pub rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    pub counter: Arc<AtomicU64>,
    pub client: RpcClient,
    pub seed: Option<u64>,
    pub args: Args,
}

impl TestEnv {
    pub fn new(
        num_iterations: u32,
        rps: u32,
        num_wallets: usize,
        client: RpcClient,
        seed: Option<u64>,
        args: Args,
    ) -> Self {
        let barrier = Barrier::new(num_wallets + 1);
        let barrier = Arc::new(barrier);

        let quota = governor::Quota::per_minute(NonZeroU32::new(rps * 60).unwrap())
            // Low-rate runs still need a non-zero burst budget for governor.
            .allow_burst(burst_size(rps));

        let rate_limiter = Arc::new(governor::RateLimiter::direct(quota));
        let counter = Arc::new(AtomicU64::new(0));
        TestEnv {
            barrier,
            num_iterations,
            rate_limiter,
            counter,
            client,
            seed,
            args,
        }
    }

    pub fn spawn_progress_printer(&self) -> JoinHandle<()> {
        let counter = self.counter.clone();
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
}

fn burst_size(rps: u32) -> NonZeroU32 {
    NonZeroU32::new((rps / 10).max(1)).unwrap()
}

pub fn belongs_to_worker(index: usize, worker_index: usize, workers_total: usize) -> bool {
    if workers_total <= 1 {
        return true;
    }
    index % workers_total == worker_index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_size_is_clamped_for_low_rps() {
        assert_eq!(burst_size(1).get(), 1);
        assert_eq!(burst_size(5).get(), 1);
        assert_eq!(burst_size(9).get(), 1);
    }

    #[test]
    fn burst_size_matches_existing_formula_above_threshold() {
        assert_eq!(burst_size(10).get(), 1);
        assert_eq!(burst_size(20).get(), 2);
        assert_eq!(burst_size(55).get(), 5);
    }
}
