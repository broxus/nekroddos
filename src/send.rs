use ed25519_dalek::Keypair;
use nekoton::core::ton_wallet::TransferAction;
use nekoton::models::Expiration;
use nekoton_utils::SimpleClock;
use once_cell::sync::OnceCell;
use ton_abi::sign_with_signature_id;
use ton_block::{AccountStuff, GlobalCapabilities, MsgAddressInt};
use ton_types::{BuilderData, SliceData};

pub struct SendOutcome {
    pub broadcast_result: anyhow::Result<()>,
}

pub async fn resolve_sign_id(
    client: &everscale_rpc_client::RpcClient,
) -> anyhow::Result<Option<i32>> {
    static SIGN_ID: OnceCell<Option<i32>> = OnceCell::new();
    if let Some(sign_id) = SIGN_ID.get() {
        return Ok(*sign_id);
    }

    let config = client.get_blockchain_config().await?;
    let sign_id = if config.has_capability(GlobalCapabilities::CapSignatureWithId) {
        Some(config.global_id())
    } else {
        None
    };
    let _ = SIGN_ID.set(sign_id);
    Ok(*SIGN_ID.get().unwrap())
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_signed_message(
    sign_id: Option<i32>,
    signer: &Keypair,
    from: MsgAddressInt,
    payload: BuilderData,
    destination: MsgAddressInt,
    amount: u64,
    state: &AccountStuff,
) -> anyhow::Result<ton_block::Message> {
    let gift = nekoton::core::ton_wallet::Gift {
        flags: 3,
        bounce: false,
        destination,
        amount: amount.into(),
        body: Some(SliceData::load_builder(payload)?),
        state_init: None,
    };

    let now = nekoton_utils::now_sec_u64() as u32 + 60;

    let message = nekoton::core::ton_wallet::ever_wallet::prepare_transfer(
        &SimpleClock,
        &signer.public,
        state,
        from.clone(),
        vec![gift],
        Expiration::Timestamp(now),
    )?;
    let message = match message {
        TransferAction::DeployFirst => panic!("DeployFirst not supported"),
        TransferAction::Sign(message) => message,
    };

    let signature = sign_with_signature_id(signer, message.hash(), sign_id);
    Ok(message.sign(&signature.to_bytes()).unwrap().message)
}

pub async fn broadcast_prepared_message(
    client: &everscale_rpc_client::RpcClient,
    message: ton_block::Message,
) -> anyhow::Result<()> {
    client.broadcast_message(message).await.map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
pub async fn send(
    client: &everscale_rpc_client::RpcClient,
    signer: &Keypair,
    from: MsgAddressInt,
    payload: BuilderData,
    destination: MsgAddressInt,
    amount: u64,
    state: &AccountStuff,
) -> anyhow::Result<SendOutcome> {
    let sign_id = resolve_sign_id(client).await?;
    let signed_message =
        prepare_signed_message(sign_id, signer, from, payload, destination, amount, state)?;
    let broadcast_result = broadcast_prepared_message(client, signed_message).await;

    Ok(SendOutcome { broadcast_result })
}
