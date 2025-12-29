use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use futures_util::StreamExt;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sse_stream::SseStream;
use tokio::sync::{broadcast, Notify};
use url::Url;

use everscale_rpc_client::{RpcClient, RpcRequest, RpcResponse};
use nekoton_abi::GenTimings;
use ton_block::MsgAddressInt;

#[derive(Debug, Clone)]
pub(crate) struct StreamUpdate {
    pub address: String,
    pub max_lt: u64,
    pub gen_utime: u32,
    pub dropped: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdatePayload {
    address: String,
    max_lt: u64,
    gen_utime: u32,
    #[serde(default)]
    dropped: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubRequest<'a> {
    uuid: &'a str,
    addrs: &'a [String],
}

#[derive(Serialize)]
struct JsonRpcRequest<'a, T> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: &'a T,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

pub(crate) struct StreamHandle {
    updates_tx: broadcast::Sender<StreamUpdate>,
    uuid: RwLock<String>,
    uuid_notify: Notify,
    subscribed: DashSet<String>,
    poll: Option<Arc<PollState>>,
    streaming: bool,
    http: reqwest::Client,
    rpc: Url,
    stream: Url,
}

struct PollState {
    client: RpcClient,
    entries: DashMap<String, PollEntry>,
}

#[derive(Clone)]
struct PollEntry {
    account: MsgAddressInt,
    last_lt: u64,
}

static STREAM: OnceCell<Arc<StreamHandle>> = OnceCell::new();

pub(crate) async fn supports_streaming(client: &RpcClient) -> Result<bool> {
    let RpcClient::Jrpc(_) = client else {
        return Ok(false);
    };

    let params = ();
    let request = RpcRequest::create_jrpc_request("getCapabilities", &params);
    let response = match client.request(&request).await {
        Ok(response) => response,
        Err(err) => {
            log::warn!("getCapabilities failed: {err}");
            return Ok(false);
        }
    };

    let capabilities: Vec<String> = match response {
        RpcResponse::JRPC(answer) => answer.into_inner(),
        RpcResponse::PROTO(_) => return Ok(false),
    };

    let has_streaming = capabilities.iter().any(|cap| {
        cap == "streaming"
            || cap == "subSubscribe"
            || cap == "subUnsubscribe"
            || cap == "subUnsubscribeAll"
            || cap == "subStatus"
            || cap == "subListSubscriptions"
    });

    Ok(has_streaming)
}

pub(crate) async fn init(client: RpcClient, endpoints: Vec<Url>) -> Result<Arc<StreamHandle>> {
    let streaming = supports_streaming(&client).await?;
    if !streaming {
        log::info!("Streaming is not supported, using polling");
    }

    let poll = if streaming {
        None
    } else {
        Some(Arc::new(PollState {
            client: client.clone(),
            entries: DashMap::new(),
        }))
    };

    let handle = STREAM.get_or_try_init(|| StreamHandle::new(endpoints, poll, streaming))?;
    Ok(handle.clone())
}

pub(crate) fn global() -> Result<Arc<StreamHandle>> {
    STREAM.get().cloned().context("stream is not initialized")
}

impl StreamHandle {
    fn new(
        endpoints: Vec<Url>,
        poll: Option<Arc<PollState>>,
        streaming: bool,
    ) -> Result<Arc<Self>> {
        let rpc = endpoints
            .into_iter()
            .next()
            .context("no endpoints provided")?;
        let stream = rpc.join("stream").context("failed to build stream url")?;
        let http = reqwest::Client::builder().build()?;
        let (updates_tx, _) = broadcast::channel(1024);
        let handle = Arc::new(Self {
            updates_tx,
            uuid: RwLock::new(String::new()),
            uuid_notify: Notify::new(),
            subscribed: DashSet::new(),
            poll,
            streaming,
            http,
            rpc,
            stream,
        });

        if handle.streaming {
            let task = handle.clone();
            tokio::spawn(async move {
                task.run().await;
            });
        } else if let Some(poll) = handle.poll.as_ref() {
            let task = handle.clone();
            let poll = poll.clone();
            tokio::spawn(async move {
                task.run_poll(poll).await;
            });
        }
        Ok(handle)
    }

    pub async fn subscribe_addr(&self, addr: &str) -> Result<broadcast::Receiver<StreamUpdate>> {
        if self.streaming {
            self.ensure_subscribed(addr).await?;
        } else if let Some(poll) = self.poll.as_ref() {
            let address = addr.to_owned();
            if !poll.entries.contains_key(&address) {
                let account = address.parse()?;
                poll.entries.insert(
                    address,
                    PollEntry {
                        account,
                        last_lt: 0,
                    },
                );
            }
        }
        Ok(self.updates_tx.subscribe())
    }

    //todo add unsubscribe, not needed with a single addr

    async fn ensure_subscribed(&self, addr: &str) -> Result<()> {
        let uuid = self.wait_for_uuid().await?;
        let addr = addr.to_owned();
        if !self.subscribed.insert(addr.clone()) {
            return Ok(());
        }
        if let Err(err) = self
            .send_subscribe(&uuid, std::slice::from_ref(&addr))
            .await
        {
            self.subscribed.remove(&addr);
            return Err(err);
        }
        Ok(())
    }

    async fn wait_for_uuid(&self) -> Result<String> {
        loop {
            let uuid = self.uuid.read().clone();
            if !uuid.is_empty() {
                return Ok(uuid);
            }
            self.uuid_notify.notified().await;
        }
    }

    async fn resubscribe_all(&self, uuid: &str) -> Result<()> {
        let addrs: Vec<String> = self
            .subscribed
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        if addrs.is_empty() {
            return Ok(());
        }
        self.send_subscribe(uuid, &addrs).await
    }

    async fn send_subscribe(&self, uuid: &str, addrs: &[String]) -> Result<()> {
        if addrs.is_empty() {
            return Ok(());
        }

        let params = SubRequest { uuid, addrs };
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "subSubscribe",
            params: &params,
        };

        let response = self
            .http
            .post(self.rpc.clone())
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        let body: JsonRpcResponse = response.json().await?;

        if !status.is_success() {
            return Err(anyhow::anyhow!("subSubscribe status {status}"));
        }

        if let Some(error) = body.error {
            return Err(anyhow::anyhow!(
                "subSubscribe error {}: {}",
                error.code,
                error.message
            ));
        }

        Ok(())
    }

    async fn run(self: Arc<Self>) {
        loop {
            if let Err(err) = self.run_once().await {
                log::warn!("stream error: {err}");
                self.uuid.write().clear();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // todo: spawn tasks per poll, currently it's a single addr and does not matter
    async fn run_poll(self: Arc<Self>, poll: Arc<PollState>) {
        loop {
            let mut snapshot = Vec::new();
            for entry in poll.entries.iter() {
                let item = entry.value();
                snapshot.push((entry.key().clone(), item.account.clone(), item.last_lt));
            }

            for (address, account, last_lt) in snapshot {
                let last_lt = (last_lt > 0).then_some(last_lt);
                let result = poll.client.get_contract_state(&account, last_lt).await;

                match result {
                    Ok(Some(state)) => {
                        let max_lt = state.account.storage.last_trans_lt;
                        if max_lt == last_lt.unwrap_or(0) {
                            continue;
                        }
                        poll.entries.insert(
                            address.clone(),
                            PollEntry {
                                account,
                                last_lt: max_lt,
                            },
                        );

                        let gen_utime = match state.timings {
                            GenTimings::Known { gen_utime, .. } => gen_utime,
                            GenTimings::Unknown => 0,
                        };

                        let update = StreamUpdate {
                            address,
                            max_lt,
                            gen_utime,
                            dropped: None,
                        };
                        let _ = self.updates_tx.send(update);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        log::warn!("Polling error for {address}: {err}");
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn run_once(&self) -> Result<()> {
        let response = self
            .http
            .get(self.stream.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("stream status {}", response.status()));
        }

        let mut sse_stream = SseStream::from_byte_stream(response.bytes_stream());
        while let Some(sse) = sse_stream.next().await {
            let sse = sse?;
            let Some(name) = sse.event else {
                continue;
            };
            let Some(data) = sse.data else {
                continue;
            };
            self.handle_event(&name, &data).await?;
        }

        Err(anyhow::anyhow!("stream ended"))
    }

    async fn handle_event(&self, name: &str, data: &str) -> Result<()> {
        match name {
            "uuid" => {
                *self.uuid.write() = data.to_owned();
                self.uuid_notify.notify_waiters();
                self.resubscribe_all(data).await?;
            }
            "update" => {
                let payload: UpdatePayload = serde_json::from_str(data)?;
                let update = StreamUpdate {
                    address: payload.address,
                    max_lt: payload.max_lt,
                    gen_utime: payload.gen_utime,
                    dropped: payload.dropped,
                };
                let _ = self.updates_tx.send(update);
            }
            _ => {}
        }
        Ok(())
    }
}
