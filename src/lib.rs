#![doc = include_str!("../README.md")]
#![allow(clippy::comparison_chain)]
#![allow(clippy::len_without_is_empty)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod congee;
mod congee_arc;
mod congee_set;
mod error;
mod lock;
mod nodes;
mod range_scan;
mod stats;
mod congee_raw;
mod utils;
use congee_raw::RawCongee;

#[cfg(test)]
mod tests;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam_epoch::{Guard, pin};
}

pub use congee::{Congee, U64Congee};
pub use congee_arc::CongeeArc;
pub use congee_set::CongeeSet;
pub use utils::{Allocator, DefaultAllocator, MemoryStatsAllocator};
