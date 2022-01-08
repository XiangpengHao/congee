#![feature(core_intrinsics)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::enum_variant_names)]

mod base_node;
mod key;
mod node_16;
mod node_256;
mod node_4;
mod node_48;
pub mod tree;
mod utils;
mod lock;

mod range_scan;

pub use crossbeam_epoch as Epoch;
pub use key::Key;
pub use tree::Tree;
