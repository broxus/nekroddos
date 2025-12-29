use crate::abi::{dex_pair, receiver, token_root, token_wallet};
use crate::app_cache::AppCache;
use crate::models::{
    DexPairV9BuildCrossPairExchangePayloadV2, DexPairV9Steps, PayloadGenerator,
    PayloadGeneratorsData, PayloadInput, PayloadTokens, StepInput, Transfer,
};
use nekoton_abi::num_bigint::BigUint;
use nekoton_abi::{
    BuildTokenValue, FunctionExt, KnownParamTypePlain, PackAbiPlain, UnpackAbiPlain,
};
use nekoton_utils::SimpleClock;
use rand::{RngCore, SeedableRng};
use std::collections::HashMap;
use std::sync::OnceLock;
use ton_abi::contract::ABI_VERSION_2_3;
use ton_abi::{Token, TokenValue, Uint};
use ton_block::{AccountStuff, MsgAddressInt};
use ton_types::Cell;

pub fn build_double_side_payloads_data(
    mut input: PayloadInput,
    app_cache: &AppCache,
) -> PayloadGeneratorsData {
    let forward_route = build_route_data(input.recipient.clone(), input.steps.clone(), app_cache);

    input.steps.reverse();
    input.steps.iter_mut().for_each(|x| {
        std::mem::swap(&mut x.from_currency_address, &mut x.to_currency_address);
    });

    let backward_route = build_route_data(input.recipient, input.steps, app_cache);
    PayloadGeneratorsData {
        forward: forward_route,
        backward: backward_route,
    }
}

fn build_route_data(
    recipient: MsgAddressInt,
    mut steps: Vec<StepInput>,
    app_cache: &AppCache,
) -> PayloadGenerator {
    let first_pool = steps.remove(0);
    let chain_len = steps.len();

    let steps = steps
        .clone()
        .into_iter()
        .enumerate()
        .map(|(index, x)| DexPairV9Steps {
            amount: 0,
            roots: x.currency_addresses,
            outcoming: x.to_currency_address,
            numerator: 1,
            next_step_indices: if index == chain_len - 1 {
                vec![]
            } else {
                vec![index as u32 + 1]
            },
        })
        .collect();

    let swap_tokens = DexPairV9BuildCrossPairExchangePayloadV2 {
        id: 0,
        deploy_wallet_grams: 0,
        expected_amount: 0,
        outcoming: first_pool.to_currency_address,
        next_step_indices: vec![0],
        steps,
        recipient: recipient.clone(),
        referrer: MsgAddressInt::default(),
        success_payload: None,
        cancel_payload: None,
    }
    .pack();

    let transfer_tokens = Transfer {
        amount: 100_000, // todo! conf
        recipient: first_pool.pool_address.clone(),
        deploy_wallet_value: 0,
        remaining_gas_to: recipient.clone(),
        notify: true,
        payload: Default::default(),
    }
    .pack();

    let state = app_cache
        .tokens_states
        .get(&first_pool.from_currency_address)
        .cloned()
        .unwrap();
    let destination = get_wallet_of(state, &first_pool.from_currency_address, recipient);

    PayloadGenerator {
        first_pool_state: app_cache
            .pool_states
            .get(&first_pool.pool_address)
            .cloned()
            .unwrap(),
        swap_fun: dex_pair()
            .function("buildCrossPairExchangePayloadV2")
            .cloned()
            .unwrap(),
        transfer_fun: token_wallet().function("transfer").cloned().unwrap(),
        destination,
        tokens: PayloadTokens {
            swap: swap_tokens,
            transfer: transfer_tokens,
        },
    }
}

fn get_wallet_of(
    state: AccountStuff,
    from_token_root: &MsgAddressInt,
    recipient: MsgAddressInt,
) -> MsgAddressInt {
    let res = token_root()
        .function("walletOf")
        .unwrap()
        .run_local(
            &SimpleClock,
            state,
            &[
                Token::new(
                    "answerId",
                    TokenValue::Uint(Uint {
                        number: BigUint::from(0_u32),
                        size: 32,
                    }),
                ),
                Token::new("walletOwner", recipient.clone().token_value()),
            ],
            &[],
        )
        .unwrap()
        .tokens
        .and_then(|x| x.into_iter().next())
        .unwrap();

    let wallet_token = match res.value {
        TokenValue::Address(x) => x,
        _ => {
            panic!("walletOf return not address, recipient: {recipient}, token_root: {from_token_root}")
        }
    };

    wallet_token.clone().to_msg_addr_int().unwrap()
}

#[derive(Debug, Clone, PackAbiPlain, KnownParamTypePlain)]
pub struct FillFunctionInput {
    #[abi(uint32)]
    pub number: u32,
    #[abi(cell)]
    pub cell: Cell,
}

pub fn get_dag_payload(
    index: u32,
    data_size: u32,
    seed: Option<u64>,
    dst: MsgAddressInt,
    rand_cell: bool,
    receiver_idx: u32,
) -> ton_block::Message {
    static PAYLOAD: OnceLock<Cell> = OnceLock::new();
    let data = if rand_cell {
        // Generate unique data for each message
        let base_seed = seed.unwrap_or_else(rand::random);
        // Combine base seed with both index and receiver_idx for uniqueness
        let unique_seed = base_seed
            .wrapping_add(index as u64)
            .wrapping_add((receiver_idx as u64) << 32);
        
        let mut rng = rand::rngs::StdRng::seed_from_u64(unique_seed);
        let mut data = vec![0_u8; data_size as usize];
        rng.fill_bytes(&mut data);

        let data = data
            .token_value()
            .pack_into_chain(&ABI_VERSION_2_3)
            .unwrap();
        data.into_cell().unwrap()
    } else {
        // Use static payload for all messages
        PAYLOAD.get_or_init(|| {
            let seed = seed.unwrap_or_else(rand::random);
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut data = vec![0_u8; data_size as usize];
            rng.fill_bytes(&mut data);

            let data = data
                .token_value()
                .pack_into_chain(&ABI_VERSION_2_3)
                .unwrap();
            data.into_cell().unwrap()
        }).clone()
    };

    let abi = receiver();
    let method = abi.function("fill").unwrap();
    let tokens = FillFunctionInput {
        number: index,
        cell: data.clone(),
    }
    .pack();

    let body = method
        .encode_input(&HashMap::new(), &tokens, false, None, None)
        .unwrap();

    ton_block::Message::with_ext_in_header_and_body(
        ton_block::ExternalInboundMessageHeader {
            dst,
            ..Default::default()
        },
        ton_types::SliceData::load_builder(body).unwrap(),
    )
}

#[derive(Debug, Clone, UnpackAbiPlain, KnownParamTypePlain, Default)]
pub struct StatsFunctionOutput {
    #[abi(uint32)]
    pub errors_count: u32,
    #[abi(uint32)]
    pub success_count: u32,
}

pub fn get_stats(state: AccountStuff) -> StatsFunctionOutput {
    let abi = receiver();
    let method = abi.function("stats").unwrap();
    let res = method.run_local(&SimpleClock, state, &[], &[]).unwrap();
    res.tokens.unwrap().unpack().unwrap()
}
