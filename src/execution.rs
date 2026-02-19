use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_rpc_client::{ClientOptions, ReliabilityParams, RpcClient};
use indexmap::IndexSet;
use url::Url;

use crate::latency;
use crate::rand_send;
use crate::{dag, dos, send_to_targets, send_tokens, swap, Args, Commands, EndpointMode};

pub(crate) struct Executor {
    args: Args,
    keypair: Arc<ed25519_dalek::Keypair>,
}

impl Executor {
    pub(crate) async fn run(args: Args, keypair: Arc<ed25519_dalek::Keypair>) -> Result<()> {
        let mut executor = Self { args, keypair };
        match executor.args.endpoint_mode {
            EndpointMode::Rr => executor.run_rr().await,
            EndpointMode::Distinct => executor.run_distinct().await,
        }
    }

    fn make_client_options(node_is_dead_seconds: u64) -> ClientOptions {
        ClientOptions {
            request_timeout: Duration::from_secs(60),
            choose_strategy: everscale_rpc_client::ChooseStrategy::RoundRobin,
            reliability_params: ReliabilityParams {
                mc_acceptable_time_diff_sec: node_is_dead_seconds,
                sc_acceptable_time_diff_sec: node_is_dead_seconds,
            },
            ..Default::default()
        }
    }

    async fn run_rr(&self) -> Result<()> {
        let client = RpcClient::new(
            self.args.endpoints.clone(),
            Self::make_client_options(self.args.node_is_dead_seconds),
        )
        .await?;
        self.run_command(self.args.clone(), client).await
    }

    async fn run_command(&self, args: Args, client: RpcClient) -> Result<()> {
        match &args.command {
            Commands::Swap(command_args) => {
                swap::run(command_args.clone(), args, self.keypair.as_ref(), client).await?;
            }
            Commands::Dag(command_args) => {
                dag::run(command_args.clone(), args, client).await?;
            }
            Commands::Send(command_args) => {
                send_tokens::run(command_args.clone(), args, self.keypair.clone(), client).await?;
            }
            Commands::Latency(command_args) => {
                latency::run(command_args.clone(), args, self.keypair.as_ref(), client).await?;
            }
            Commands::RandSend(command_args) => {
                rand_send::run(command_args.clone(), args, self.keypair.clone(), client).await?;
            }
            Commands::SendToTargets(command_args) => {
                send_to_targets::run(command_args.clone(), args, self.keypair.clone(), client)
                    .await?;
            }
            Commands::AccountsDos(command_args) => {
                dos::run(command_args.clone(), args, client).await?;
            }
        }
        Ok(())
    }

    async fn run_distinct(&mut self) -> Result<()> {
        if !matches!(
            self.args.command,
            Commands::Swap(_)
                | Commands::Dag(_)
                | Commands::Send(_)
                | Commands::RandSend(_)
                | Commands::SendToTargets(_)
        ) {
            log::warn!(
                "endpoint-mode=distinct is currently only implemented for sender commands, falling back to rr"
            );
            return self.run_rr().await;
        }

        let endpoints = self.distinct_endpoints()?;
        let workers_total = endpoints.len();
        let per_worker_rps = self.per_worker_rps(workers_total)?;

        log::info!(
            "endpoint-mode=distinct endpoints={} per_worker_rps={} no_wait={}",
            workers_total,
            per_worker_rps,
            self.args.no_wait
        );

        let path = match &self.args.command {
            Commands::Send(args) => args.log_file.clone(),
            Commands::RandSend(args) => args.save_accounts.clone(),
            _ => None,
        };
        let mut output_writer = OutputWriter { path, handle: None };
        output_writer.prepare(&mut self.args)?;

        let mut handles = Vec::with_capacity(workers_total);
        for (worker_index, endpoint) in endpoints.into_iter().enumerate() {
            let worker_args = self.args_for_worker(
                self.args.clone(),
                endpoint,
                worker_index,
                workers_total,
                per_worker_rps,
            );
            let keypair = self.keypair.clone();
            let handle = std::thread::Builder::new()
                .name(format!("endpoint-worker-{worker_index}"))
                .spawn(move || -> Result<()> {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    rt.block_on(async move {
                        let client = RpcClient::new(
                            worker_args.endpoints.clone(),
                            Self::make_client_options(worker_args.node_is_dead_seconds),
                        )
                        .await?;
                        let executor = Executor {
                            args: worker_args.clone(),
                            keypair,
                        };
                        executor.run_command(worker_args, client).await
                    })
                })?;
            handles.push(handle);
        }

        for handle in handles {
            let outcome = handle
                .join()
                .map_err(|_| anyhow::anyhow!("endpoint worker thread panicked"))?;
            outcome?;
        }

        self.args.shared_output_tx = None;
        output_writer.finish()?;
        Ok(())
    }

    fn distinct_endpoints(&self) -> Result<Vec<Url>> {
        let mut unique = IndexSet::new();
        for endpoint in &self.args.endpoints {
            unique.insert(endpoint.clone());
        }
        let endpoints: Vec<Url> = unique.into_iter().collect();
        if endpoints.is_empty() {
            anyhow::bail!("No endpoints provided");
        }
        Ok(endpoints)
    }

    fn per_worker_rps(&self, workers_total: usize) -> Result<u32> {
        if workers_total == 0 {
            anyhow::bail!("workers_total must be > 0");
        }
        let workers_total = workers_total as u32;
        let ceil_div = |value: u32| value.div_ceil(workers_total).max(1);

        let rps = match &self.args.command {
            Commands::Swap(args) => ceil_div(args.rps),
            Commands::Dag(args) => ceil_div(args.rps),
            Commands::Send(args) => ceil_div(args.rps),
            Commands::RandSend(args) => ceil_div(args.rps),
            Commands::SendToTargets(args) => ceil_div(args.rps),
            Commands::Latency(_) | Commands::AccountsDos(_) => {
                anyhow::bail!("per_worker_rps is not defined for this command")
            }
        };
        Ok(rps)
    }

    fn args_for_worker(
        &self,
        mut args: Args,
        endpoint: Url,
        worker_index: usize,
        workers_total: usize,
        per_worker_rps: u32,
    ) -> Args {
        args.endpoints = vec![endpoint];
        args.worker_index = worker_index;
        args.workers_total = workers_total;
        match &mut args.command {
            Commands::Swap(command_args) => {
                command_args.rps = per_worker_rps;
            }
            Commands::Dag(command_args) => {
                command_args.rps = per_worker_rps;
            }
            Commands::Send(command_args) => {
                command_args.rps = per_worker_rps;
            }
            Commands::RandSend(command_args) => {
                command_args.rps = per_worker_rps;
                command_args.from_rps = command_args.from_rps.div_ceil(workers_total as u32).max(1);
                command_args.to_rps = command_args.to_rps.div_ceil(workers_total as u32).max(1);
            }
            Commands::SendToTargets(command_args) => {
                command_args.rps = per_worker_rps;
            }
            Commands::Latency(_) | Commands::AccountsDos(_) => {}
        }
        args
    }
}

struct OutputWriter {
    path: Option<PathBuf>,
    handle: Option<std::thread::JoinHandle<Result<()>>>,
}

impl OutputWriter {
    fn prepare(&mut self, args: &mut Args) -> Result<()> {
        let Some(path) = self.path.clone() else {
            return Ok(());
        };

        let (tx, rx) = std::sync::mpsc::channel::<String>();
        args.shared_output_tx = Some(tx);

        let handle = std::thread::Builder::new()
            .name("distinct-output-writer".to_owned())
            .spawn(move || -> Result<()> {
                use std::io::Write;
                let file = std::fs::File::create(path)?;
                let mut writer = std::io::BufWriter::new(file);
                while let Ok(line) = rx.recv() {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
                writer.flush()?;
                Ok(())
            })?;

        self.handle = Some(handle);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let outcome = handle
                .join()
                .map_err(|_| anyhow::anyhow!("output writer thread panicked"))?;
            outcome?;
        }
        Ok(())
    }
}
