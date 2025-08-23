#![doc = include_str!("../README.md")]
#![allow(clippy::comparison_chain)]
#![allow(clippy::len_without_is_empty)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod congee;
mod congee_inner;
mod congee_raw;
mod congee_set;
mod error;
mod lock;
mod nodes;
mod range_scan;
mod stats;
mod utils;
pub mod congee_compact_v2;
use congee_inner::CongeeInner;

#[cfg(test)]
mod tests;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam_epoch::{Guard, pin};
}

pub use congee::Congee;
pub use congee_raw::CongeeRaw;
pub use congee_set::CongeeSet;
pub use congee_compact_v2::{CongeeCompactV2, CompactV2Stats};
pub use utils::{Allocator, DefaultAllocator, MemoryStatsAllocator};
