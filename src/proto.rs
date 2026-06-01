// ── Bebop protobuf types (generated, re-exported) ──────────────────
include!(concat!(env!("OUT_DIR"), "/bebop.rs"));

/// All chains Bebop supports via their Price API.
pub const BEBOP_CHAINS: &[BebopChain] = &[
    BebopChain {
        name: "ethereum",
        chain_id: 1,
        network: "ethereum",
    },
    BebopChain {
        name: "polygon",
        chain_id: 137,
        network: "polygon",
    },
    BebopChain {
        name: "arbitrum",
        chain_id: 42161,
        network: "arbitrum",
    },
    BebopChain {
        name: "base",
        chain_id: 8453,
        network: "base",
    },
    BebopChain {
        name: "bsc",
        chain_id: 56,
        network: "bsc",
    },
];

pub struct BebopChain {
    pub name: &'static str,
    pub chain_id: u64,
    pub network: &'static str,
}
