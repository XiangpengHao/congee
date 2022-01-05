#![feature(core_intrinsics)]

mod base_node;
mod key;
mod node_16;
mod node_256;
mod node_4;
mod node_48;
pub mod tree;
mod utils;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
