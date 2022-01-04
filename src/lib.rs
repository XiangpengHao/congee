#![feature(core_intrinsics)]

mod key;
mod base_node;
mod node_16;
mod node_256;
mod node_4;
mod node_48;
pub mod tree;
mod utils;

pub(crate) type Tid = usize;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
