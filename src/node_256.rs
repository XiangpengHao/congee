use crate::node::BaseNode;

#[repr(C)]
struct Node256 {
    base: BaseNode,

    children: [*mut BaseNode; 256],
}

impl Node256 {
    fn is_full(&self) -> bool {
        self.base.count == 16
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 37
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        self.children[key as usize] = node;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: *mut BaseNode) {
        self.children[key as usize] = val;
    }

    fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        return Some(self.children[key as usize]);
    }
}
