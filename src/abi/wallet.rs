use nekoton_abi::{KnownParamTypePlain, PackAbi, PackAbiPlain, UnpackAbiPlain};
use ton_block::MsgAddressInt;
use ton_types::UInt256;

use super::wallet_factory;

#[derive(Debug, Clone, PackAbiPlain, UnpackAbiPlain, KnownParamTypePlain)]
pub struct GetWalletFunctionInput {
    #[abi(name = "_index", uint64)]
    pub index: u64,
    #[abi(name = "_publicKey", uint256)]
    pub public_key: UInt256,
}

#[derive(Debug, Clone, PackAbi, UnpackAbiPlain, KnownParamTypePlain)]
pub struct GetWalletFunctionOutput {
    #[abi(address)]
    pub receiver: MsgAddressInt,
}

pub fn get_wallet() -> &'static ton_abi::Function {
    wallet_factory().function("get_wallet").unwrap()
}
