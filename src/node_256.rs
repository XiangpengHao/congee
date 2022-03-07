use crate::{
    base_node::{BaseNode, Node, NodeType},
    child_ptr::NodePtr,
};

#[repr(C)]
pub(crate) struct Node256 {
    base: BaseNode,

    children: [NodePtr; 256],
}

impl Node for Node256 {
    fn get_type() -> NodeType {
        NodeType::N256
    }

    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, NodePtr)> {
        let mut children = Vec::with_capacity(256);

        for (i, c) in self
            .children
            .iter()
            .take(end as usize + 1)
            .skip(start as usize)
            .enumerate()
        {
            if !c.is_null() {
                children.push((i as u8 + start, *c));
            }
        }

        children
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for (i, c) in self.children.iter().enumerate() {
            if !c.is_null() {
                dst.insert(i as u8, *c);
            }
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseNode {
        &mut self.base
    }

    fn is_full(&self) -> bool {
        false
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 37
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        self.children[key as usize] = node;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) {
        self.children[key as usize] = val;
    }

    fn remove(&mut self, k: u8) {
        self.children[k as usize] = NodePtr::from_null();
        self.base.count -= 1;
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        let child = &self.children[key as usize];
        if child.is_null() {
            None
        } else {
            Some(*child)
        }
    }
}
