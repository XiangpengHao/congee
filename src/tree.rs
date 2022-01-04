use crate::{node::BaseNode, node_256::Node256};

struct Tree {
    root: *mut BaseNode,
}

impl Tree {
    fn new() -> Self {
        Tree {
            root: Node256::new(),
        }
    }
    fn look_up(&self, key: usize) {}
    fn look_up_range(&self, start: usize, end: usize) {}
    fn insert(&self, key: usize) {}
}
