use chrono::Utc;
use ed25519_dalek::Keypair;
use nekoton_abi::{BuildTokenValue, FunctionExt, PackAbi, PackAbiPlain, UnpackAbiPlain};
use nekoton_utils::{serde_address, SimpleClock};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ton_abi::{Function, Token, TokenValue, Uint};
use ton_block::{AccountStuff, MsgAddressInt};
use ton_types::{BuilderData, Cell};

#[derive(PackAbiPlain, UnpackAbiPlain, Debug, Clone)]
pub struct Transfer {
    #[abi]
    pub amount: u128,
    #[abi]
    pub recipient: MsgAddressInt,
    #[abi(name = "deployWalletValue")]
    pub deploy_wallet_value: u128,
    #[abi(name = "remainingGasTo")]
    pub remaining_gas_to: MsgAddressInt,
    #[abi]
    pub notify: bool,
    #[abi]
    pub payload: Cell,
}

pub struct PayloadInput {
    pub steps: Vec<StepInput>,
    pub recipient: MsgAddressInt,
}

#[derive(Clone)]
pub struct StepInput {
    pub pool_address: MsgAddressInt,
    pub currency_addresses: Vec<MsgAddressInt>,
    pub from_currency_address: MsgAddressInt,
    pub to_currency_address: MsgAddressInt,
}

#[derive(UnpackAbiPlain, Debug, Clone)]
pub struct GetTokenRoots {
    #[abi(address)]
    pub left: MsgAddressInt,
    #[abi(address)]
    pub right: MsgAddressInt,
    #[abi(address)]
    pub lp: MsgAddressInt,
}

#[derive(Debug, Clone, PackAbiPlain)]
pub struct DexPairV9BuildCrossPairExchangePayloadV2 {
    #[abi(name = "_id", uint64)]
    pub id: u64,
    #[abi(name = "_deployWalletGrams", uint128)]
    pub deploy_wallet_grams: u128,
    #[abi(name = "_expectedAmount", uint128)]
    pub expected_amount: u128,
    #[abi(name = "_outcoming", address)]
    pub outcoming: ton_block::MsgAddressInt,
    #[abi(name = "_nextStepIndices", array)]
    pub next_step_indices: Vec<u32>,
    #[abi(name = "_steps", array)]
    pub steps: Vec<DexPairV9Steps>,
    #[abi(name = "_recipient", address)]
    pub recipient: ton_block::MsgAddressInt,
    #[abi(name = "_referrer", address)]
    pub referrer: ton_block::MsgAddressInt,
    #[abi(name = "_successPayload")]
    pub success_payload: Option<ton_types::Cell>,
    #[abi(name = "_cancelPayload")]
    pub cancel_payload: Option<ton_types::Cell>,
}

#[derive(Debug, Clone, PackAbi, nekoton_abi::KnownParamType)]
pub struct DexPairV9Steps {
    #[abi(uint128)]
    pub amount: u128,
    #[abi(array)]
    pub roots: Vec<ton_block::MsgAddressInt>,
    #[abi(address)]
    pub outcoming: ton_block::MsgAddressInt,
    #[abi(uint128)]
    pub numerator: u128,
    #[abi(name = "nextStepIndices", array)]
    pub next_step_indices: Vec<u32>,
}

#[derive(Clone)]
pub struct PayloadGeneratorsData {
    pub forward: PayloadGenerator,
    pub backward: PayloadGenerator,
}

pub struct PayloadMeta {
    pub payload: BuilderData,
    pub destination: MsgAddressInt,
}

#[derive(Clone)]
pub struct SendData {
    pub payload_generators: Arc<PayloadGeneratorsData>,
    pub signer: Arc<Keypair>,
    pub sender_addr: MsgAddressInt,
}

impl SendData {
    pub fn new(
        payload_generators: PayloadGeneratorsData,
        signer: Keypair,
        sender_addr: MsgAddressInt,
    ) -> Self {
        Self {
            payload_generators: Arc::new(payload_generators),
            signer: Arc::new(signer),
            sender_addr,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct EverWalletInfo {
    #[serde(with = "serde_address")]
    pub address: MsgAddressInt,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GenericDeploymentInfo {
    #[serde(with = "serde_address")]
    pub address: MsgAddressInt,
}

#[derive(Clone)]
pub struct PayloadGenerator {
    pub first_pool_state: AccountStuff,
    pub swap_fun: Function,
    pub transfer_fun: Function,
    pub destination: MsgAddressInt,
    pub tokens: PayloadTokens,
}

#[derive(Clone)]
pub struct PayloadTokens {
    pub swap: Vec<Token>,
    pub transfer: Vec<Token>,
}

impl PayloadGenerator {
    pub fn generate_payload_meta(&mut self) -> PayloadMeta {
        let id_token = self.tokens.swap.get_mut(0).unwrap();
        id_token.value =
            TokenValue::Uint(Uint::new(Utc::now().timestamp_subsec_nanos() as u128, 64));

        let payload = &self
            .swap_fun
            .run_local(
                &SimpleClock,
                self.first_pool_state.clone(),
                &self.tokens.swap,
            )
            .map_err(|x| {
                log::error!("run_local error {:#?}", x);
                x
            })
            .ok()
            .and_then(|x| {
                if x.tokens.is_none() {
                    log::error!("run_local tokens none, result_code: {}", x.result_code);
                }
                x.tokens
            })
            .and_then(|x| x.into_iter().next())
            .and_then(|x| match x.value {
                TokenValue::Cell(x) => Some(x),
                _ => None,
            })
            .unwrap();

        let payload_token = self.tokens.transfer.get_mut(5).unwrap();
        payload_token.value = payload.token_value();

        PayloadMeta {
            payload: self
                .transfer_fun
                .encode_internal_input(&self.tokens.transfer)
                .unwrap(),
            destination: self.destination.clone(),
        }
    }
}
