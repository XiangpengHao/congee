#![doc = include_str!("../README.md")]
#![allow(clippy::comparison_chain)]
#![allow(clippy::len_without_is_empty)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_variables)]

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
use congee_inner::CongeeInner;
pub mod congee_flat_generated;
pub mod congee_flat;
pub mod congee_flat_struct_generated;
pub mod congee_flat_struct;
pub mod congee_compact;

#[cfg(test)]
mod tests;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam_epoch::{Guard, pin};
}

pub use congee::Congee;
pub use congee_raw::CongeeRaw;
pub use congee_set::CongeeSet;
pub use congee_flat::CongeeFlat;
pub use congee_flat_struct::CongeeFlatStruct;
pub use congee_compact::CongeeCompact;
pub use stats::NodeStats;
pub use utils::{Allocator, DefaultAllocator, MemoryStatsAllocator};
