use crate::{
    base_node::{BaseNode, Node, NodeType},
    child_ptr::NodePtr,
};

pub(crate) const EMPTY_MARKER: u8 = 48;

#[repr(C)]
pub(crate) struct Node48 {
    base: BaseNode,

    pub(crate) child_idx: [u8; 256],
    children: [NodePtr; 48],
}

pub(crate) struct Node48Iter<'a> {
    start: u16,
    end: u16,
    node: &'a Node48,
}

impl<'a> Iterator for Node48Iter<'a> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.start > self.end {
                return None;
            }

            let key = self.start as usize;
            self.start += 1;

            let child_loc = self.node.child_idx[key];
            if child_loc != EMPTY_MARKER {
                return Some((key as u8, self.node.children[child_loc as usize]));
            }
        }
    }
}

impl Node for Node48 {
    type NodeIter<'a> = Node48Iter<'a>;

    fn get_type() -> NodeType {
        NodeType::N48
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        self.children[self.child_idx[k as usize] as usize] = NodePtr::from_null();
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.base.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children_iter(&self, start: u8, end: u8) -> Node48Iter {
        Node48Iter {
            start: start as u16,
            end: end as u16,
            node: self,
        }
    }

    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, NodePtr)> {
        let mut children = Vec::with_capacity(24);

        children.clear();

        for i in start..=end {
            if self.child_idx[i as usize] != EMPTY_MARKER {
                children.push((i, self.children[self.child_idx[i as usize] as usize]));
            }
        }

        children
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for (i, c) in self.child_idx.iter().enumerate() {
            if *c != EMPTY_MARKER {
                dst.insert(i as u8, self.children[*c as usize]);
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
        self.base.count == 48
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 12
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        let mut pos = self.base.count as usize;

        if !self.children[pos].is_null() {
            pos = 0;
            while !self.children[pos].is_null() {
                pos += 1;
            }
        }
        debug_assert!(pos < 48);

        self.children[pos] = node;
        self.child_idx[key as usize] = pos as u8;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) {
        self.children[self.child_idx[key as usize] as usize] = val;
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            None
        } else {
            Some(self.children[self.child_idx[key as usize] as usize])
        }
    }
}
