use once_cell::sync::OnceCell;
use std::io::Cursor;
use ton_abi::Contract;
macro_rules! declare_abi {
    ($($contract:ident => $source:literal),*$(,)?) => {$(
    #[allow(non_snake_case)]
        pub fn $contract() -> &'static Contract {
            static ABI: OnceCell<Contract> = OnceCell::new();
            ABI.load(include_str!($source))
        }
    )*};
}

trait OnceCellExt {
    fn load(&self, data: &str) -> &Contract;
}

impl OnceCellExt for OnceCell<Contract> {
    fn load(&self, data: &str) -> &Contract {
        self.get_or_init(|| Contract::load(Cursor::new(data)).expect("Trust me"))
    }
}

declare_abi! {
    dex_pair => "DexPair.abi.json",
    token_wallet => "TokenWallet.abi.json",
    token_root => "TokenRoot.abi.json",
    receiver => "Receiver.abi.json",
    dudos_factory => "dudos-factory.json",
    wallet_factory => "WalletFactory.json"
}

mod wallet;
pub use wallet::*;
