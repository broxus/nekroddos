use everscale_rpc_client::RpcClient;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use indexmap::IndexSet;
use nekoton::utils::SimpleClock;
use nekoton_abi::{FunctionExt, UnpackAbiPlain};
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{random, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use ton_block::{AccountStuff, MsgAddressInt};

use crate::abi::dex_pair;
use crate::build_payload::build_double_side_payloads_data;
use crate::models::{GetTokenRoots, PayloadGeneratorsData, PayloadInput, StepInput};

fn build_answer_id_camel() -> ton_abi::Token {
    ton_abi::Token::new(
        "answerId",
        ton_abi::TokenValue::Uint(ton_abi::Uint::new(1337, 32)),
    )
}

#[derive(Clone)]
pub struct AppCache {
    pub pool_states: HashMap<MsgAddressInt, AccountStuff>,
    pub tokens_states: HashMap<MsgAddressInt, AccountStuff>,
    pub token_pairs: HashMap<(MsgAddressInt, MsgAddressInt), MsgAddressInt>,
    pub tokens: Vec<MsgAddressInt>,
    pub tx: RpcClient,
    token_index: Arc<AtomicUsize>,
    rng: Arc<Mutex<StdRng>>,
}

impl AppCache {
    pub fn new(tx: RpcClient, seed: Option<u64>) -> Self {
        let seed = seed.unwrap_or_else(random);
        log::info!("Using seed {seed}");

        Self {
            pool_states: HashMap::new(),
            tokens_states: Default::default(),
            token_pairs: HashMap::new(),
            tokens: Vec::new(),
            tx,
            token_index: Arc::new(AtomicUsize::new(0)),
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        }
    }

    pub async fn load_states(mut self, pool_addresses: Vec<MsgAddressInt>) -> Self {
        let start = std::time::Instant::now();
        let tx = &self.tx;
        let futures = pool_addresses.into_iter().map(|address| async move {
            tx.get_contract_state(&address, None)
                .await
                .ok()
                .flatten()
                .map(|account| (address, account.account))
        });

        self.pool_states = FuturesUnordered::from_iter(futures)
            .filter_map(|x| async move { x })
            .collect()
            .await;

        log::info!(
            "Loaded {} states in {:?}",
            self.pool_states.len(),
            start.elapsed()
        );

        self
    }

    pub async fn load_tokens_and_token_pairs(mut self) -> Self {
        let start = std::time::Instant::now();
        let mut token_pairs = HashMap::new();
        let mut tokens = HashSet::new();

        let answer_id = vec![build_answer_id_camel()];

        for (address, account) in self.pool_states.iter() {
            if let Ok(token_pair) = dex_pair().function("getTokenRoots").unwrap().run_local(
                &SimpleClock,
                account.clone(),
                &answer_id,
                &[],
            ) {
                let token_roots: GetTokenRoots = token_pair.tokens.unwrap().unpack().unwrap();
                tokens.insert(token_roots.left.clone());
                tokens.insert(token_roots.right.clone());
                token_pairs.insert((token_roots.left, token_roots.right), address.clone());
            }
        }

        self.token_pairs = token_pairs;
        self.tokens = tokens.into_iter().collect();
        self.tokens.sort();

        let tx = &self.tx;
        let futures = self.tokens.iter().map(|address| async move {
            tx.get_contract_state(address, None)
                .await
                .ok()
                .flatten()
                .map(|account| (address.clone(), account.account))
        });
        self.tokens_states = FuturesUnordered::from_iter(futures)
            .filter_map(|x| async move { x })
            .collect()
            .await;

        log::info!(
            "Loaded {} tokens and {} token pairs in {:?}",
            self.tokens.len(),
            self.token_pairs.len(),
            start.elapsed()
        );

        self
    }

    pub fn generate_payloads(
        &self,
        recipient: MsgAddressInt,
        steps_len: u8,
    ) -> PayloadGeneratorsData {
        let index = self
            .token_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.tokens.len();
        let route = self.generate_route(steps_len, index);
        build_double_side_payloads_data(
            PayloadInput {
                steps: route,
                recipient,
            },
            self,
        )
    }

    fn generate_route(&self, steps_len: u8, starting_with: usize) -> Vec<StepInput> {
        let mut res = vec![];
        let mut exists_tokens = HashSet::new();

        let mut from_token = self.tokens.get(starting_with).cloned().unwrap();
        exists_tokens.insert(from_token.clone());

        for _ in 0..steps_len {
            let mut temp_tokens: IndexSet<MsgAddressInt> =
                IndexSet::from_iter(self.tokens.iter().cloned());
            loop {
                let mut rng = self.rng.lock().unwrap();
                let to_token = temp_tokens.iter().choose(&mut *rng).cloned().unwrap();
                drop(rng);

                if exists_tokens.contains(&to_token) {
                    temp_tokens.shift_remove(&to_token);
                    continue;
                }

                let (left_token, right_token, pool_address) = if let Some(pool_address) = self
                    .token_pairs
                    .get(&(from_token.clone(), to_token.clone()))
                {
                    (from_token.clone(), to_token.clone(), pool_address.clone())
                } else if let Some(pool_address) = self
                    .token_pairs
                    .get(&(to_token.clone(), from_token.clone()))
                {
                    (to_token.clone(), from_token.clone(), pool_address.clone())
                } else {
                    temp_tokens.shift_remove(&to_token);
                    continue;
                };

                exists_tokens.insert(to_token.clone());

                res.push(StepInput {
                    pool_address,
                    currency_addresses: vec![left_token.clone(), right_token.clone()],
                    from_currency_address: from_token.clone(),
                    to_currency_address: to_token.clone(),
                });

                from_token = to_token;

                break;
            }
        }

        res
    }
}
