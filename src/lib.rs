#![feature(core_intrinsics)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::collapsible_if)]

mod base_node;
mod child_ptr;
mod key;
mod lock;
mod node_16;
mod node_256;
mod node_4;
mod node_48;
pub mod tree;
mod utils;

mod range_scan;

pub use crossbeam_epoch as Epoch;
pub use key::{GeneralKey, Key, UsizeKey};
pub use tree::Tree;
