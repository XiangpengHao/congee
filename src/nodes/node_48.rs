use crate::cast_ptr;

use super::{
    NodePtr,
    base_node::{BaseNode, Node, NodeIter, NodeType},
};

pub(crate) const EMPTY_MARKER: u8 = 48;

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node48 {
    base: BaseNode,

    pub(crate) child_idx: [u8; 256],
    next_empty: u8,
    children: [NodePtr; 48],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node48>() == 672);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node48>() == 8);

impl Node48 {
    pub(crate) fn init_empty(&mut self) {
        for v in self.child_idx.iter_mut() {
            *v = EMPTY_MARKER;
        }
        self.next_empty = 0;
        for (i, child) in self.children.iter_mut().enumerate() {
            *child = NodePtr::from_payload(i + 1);
        }
    }
}

pub(crate) struct Node48Iter<'a> {
    start: u16,
    end: u16,
    node: &'a Node48,
}

impl Iterator for Node48Iter<'_> {
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
    fn get_type() -> NodeType {
        NodeType::N48
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        let pos = self.child_idx[k as usize];
        self.children[pos as usize] = NodePtr::from_payload(self.next_empty as usize);
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.next_empty = pos;
        self.base.meta.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter {
        NodeIter::N48(Node48Iter {
            start: start as u16,
            end: end as u16,
            node: self,
        })
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

    fn is_full(&self) -> bool {
        self.base.meta.count == 48
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        let pos = self.next_empty as usize;
        let next_empty = cast_ptr!(self.children[pos] => {
            Payload(payload) => payload,
            SubNode(_sub_node) => {
                unreachable!()
            }
        });
        self.next_empty = next_empty as u8;

        debug_assert!(pos < 48);

        self.children[pos] = node;
        self.child_idx[key as usize] = pos as u8;
        self.base.meta.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        let old = self.children[self.child_idx[key as usize] as usize];
        self.children[self.child_idx[key as usize] as usize] = val;
        old
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        let pos = unsafe { self.child_idx.get_unchecked(key as usize) };
        if *pos == EMPTY_MARKER {
            None
        } else {
            let child = unsafe { self.children.get_unchecked(*pos as usize) };
            Some(*child)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node48 {
        let mut node = Node48 {
            base: BaseNode::new(NodeType::N48, &[]),
            child_idx: [EMPTY_MARKER; 256],
            next_empty: 0,
            children: [NodePtr::from_payload(0); 48],
        };
        node.init_empty();
        node
    }

    #[test]
    fn test_node_operations() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        assert_eq!(Node48::get_type(), NodeType::N48);
        assert!(!node.is_full());
        assert_eq!(node.base().meta.count, 0);

        node.insert(10, ptr1);
        node.insert(100, ptr2);
        node.insert(200, ptr3);

        assert_eq!(node.base().meta.count, 3);

        assert_ne!(node.child_idx[10], EMPTY_MARKER);
        assert_ne!(node.child_idx[100], EMPTY_MARKER);
        assert_ne!(node.child_idx[200], EMPTY_MARKER);
        assert_eq!(node.child_idx[50], EMPTY_MARKER); // Unused key

        assert!(matches!(node.get_child(10), Some(_)));
        assert!(matches!(node.get_child(100), Some(_)));
        assert!(matches!(node.get_child(200), Some(_)));
        assert!(node.get_child(50).is_none());

        let new_ptr = NodePtr::from_payload(0x5000);
        let _old_ptr = node.change(10, new_ptr);
        assert!(matches!(node.get_child(10), Some(_)));
        assert_eq!(node.base().meta.count, 3); // Count unchanged

        node.remove(100);
        assert_eq!(node.base().meta.count, 2);
        assert!(node.get_child(100).is_none());
        assert_eq!(node.child_idx[100], EMPTY_MARKER);
    }

    #[test]
    fn test_capacity_and_indirect_indexing() {
        let mut node = create_test_node();

        for i in 0..48 {
            let key = (i * 5) as u8; // Spread keys across the 256 range
            node.insert(key, NodePtr::from_payload((i + 1) * 0x1000));
            assert_eq!(node.base().meta.count, (i + 1) as u16);
        }

        assert!(node.is_full());
        assert_eq!(node.base().meta.count, 48);

        for i in 0..48 {
            let key = (i * 5) as u8;
            assert!(node.get_child(key).is_some());
            assert_ne!(node.child_idx[key as usize], EMPTY_MARKER);
        }

        assert!(node.get_child(1).is_none());
        assert!(node.get_child(2).is_none());
        assert_eq!(node.child_idx[1], EMPTY_MARKER);
        assert_eq!(node.child_idx[2], EMPTY_MARKER);
    }

    #[test]
    fn test_iterators_and_copy() {
        let mut src_node = create_test_node();
        let mut dst_node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        src_node.insert(50, ptr1);
        src_node.insert(150, ptr2);
        src_node.insert(250, ptr3);

        let iter = src_node.get_children(0, 255);
        if let NodeIter::N48(mut n48_iter) = iter {
            let first = n48_iter.next();
            assert!(matches!(first, Some((50, _))));
            let second = n48_iter.next();
            assert!(matches!(second, Some((150, _))));
            let third = n48_iter.next();
            assert!(matches!(third, Some((250, _))));
            assert!(n48_iter.next().is_none());
        } else {
            panic!("Expected N48 iterator");
        }

        let iter = src_node.get_children(100, 200);
        if let NodeIter::N48(mut n48_iter) = iter {
            let first = n48_iter.next();
            assert!(matches!(first, Some((150, _))));
            assert!(n48_iter.next().is_none());
        }

        src_node.copy_to(&mut dst_node);
        assert_eq!(dst_node.base().meta.count, 3);
        assert!(dst_node.get_child(50).is_some());
        assert!(dst_node.get_child(150).is_some());
        assert!(dst_node.get_child(250).is_some());
    }

    #[test]
    fn test_edge_cases_and_empty_marker() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);

        for i in 0..256 {
            assert_eq!(node.child_idx[i], EMPTY_MARKER);
        }

        node.insert(10, ptr1);

        let empty_node = create_test_node();
        let iter = empty_node.get_children(0, 255);
        if let NodeIter::N48(mut n48_iter) = iter {
            assert!(n48_iter.next().is_none());
        }

        let mut cycle_node = create_test_node();
        cycle_node.insert(42, NodePtr::from_payload(0x1000));
        cycle_node.insert(84, NodePtr::from_payload(0x2000));
        assert_eq!(cycle_node.base().meta.count, 2);

        cycle_node.remove(42);
        assert_eq!(cycle_node.base().meta.count, 1);
        assert_eq!(cycle_node.child_idx[42], EMPTY_MARKER);

        cycle_node.insert(99, NodePtr::from_payload(0x3000));
        assert_eq!(cycle_node.base().meta.count, 2);
        assert!(cycle_node.get_child(99).is_some());
        assert!(cycle_node.get_child(84).is_some());
    }
}
