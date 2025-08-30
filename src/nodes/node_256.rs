use super::{
    base_node::{BaseNode, Node, NodeIter, NodeType},
    node_ptr::NodePtr,
};

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node256 {
    base: BaseNode,
    key_mask: [u8; 32],
    children: [NodePtr; 256],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node256>() == 2096);
#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node256>() == 8);

impl Node256 {
    #[inline]
    fn set_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] |= 1 << bit;
    }

    #[inline]
    fn unset_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] &= !(1 << bit);
    }

    #[inline]
    fn get_mask(&self, key: usize) -> bool {
        let idx = key / 8;
        let bit = key % 8;
        let key_mask = unsafe { self.key_mask.get_unchecked(idx) };
        *key_mask & (1 << bit) != 0
    }
}

pub(crate) struct Node256Iter<'a> {
    start: u8,
    end: u8,
    idx: u16,
    node: &'a Node256,
}

impl Iterator for Node256Iter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur = self.idx + self.start as u16;

            if cur > self.end as u16 {
                return None;
            }

            self.idx += 1;

            if self.node.get_mask(cur as usize) {
                return Some((cur as u8, self.node.children[cur as usize]));
            } else {
                continue;
            }
        }
    }
}

impl Node for Node256 {
    fn get_type() -> NodeType {
        NodeType::N256
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_> {
        NodeIter::N256(Node256Iter {
            start,
            end,
            idx: 0,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for (i, c) in self.children.iter().enumerate() {
            if self.get_mask(i) {
                dst.insert(i as u8, *c);
            }
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        false
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        self.children[key as usize] = node;
        self.set_mask(key as usize);
        self.base.meta.inc_count();
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        let old = self.children[key as usize];
        self.children[key as usize] = val;
        old
    }

    fn remove(&mut self, k: u8) {
        self.unset_mask(k as usize);
        self.base.meta.dec_count();
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.get_mask(key as usize) {
            let child = unsafe { self.children.get_unchecked(key as usize) };
            Some(*child)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node256 {
        Node256 {
            base: BaseNode::new(NodeType::N256, &[]),
            key_mask: [0; 32],
            children: [NodePtr::from_payload(0); 256],
        }
    }

    #[test]
    fn test_node_operations() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        assert_eq!(Node256::get_type(), NodeType::N256);
        assert!(!node.is_full()); // Node256 never reports as full
        assert_eq!(node.base().meta.count(), 0);

        node.insert(0, ptr1);
        node.insert(127, ptr2);
        node.insert(255, ptr3);

        assert_eq!(node.base().meta.count(), 3);

        assert!(node.get_mask(0));
        assert!(node.get_mask(127));
        assert!(node.get_mask(255));
        assert!(!node.get_mask(1));
        assert!(!node.get_mask(128));

        assert!(node.get_child(0).is_some());
        assert!(node.get_child(127).is_some());
        assert!(node.get_child(255).is_some());
        assert!(node.get_child(1).is_none());
        assert!(node.get_child(128).is_none());

        let new_ptr = NodePtr::from_payload(0x5000);
        let _old_ptr = node.change(127, new_ptr);
        assert!(node.get_child(127).is_some());
        assert_eq!(node.base().meta.count(), 3); // Count unchanged

        node.remove(127);
        assert_eq!(node.base().meta.count(), 2);
        assert!(node.get_child(127).is_none());
        assert!(!node.get_mask(127)); // Bit mask should be unset
    }

    #[test]
    fn test_large_capacity_and_never_full() {
        let mut node = create_test_node();

        for i in (0..256).step_by(16) {
            node.insert(i as u8, NodePtr::from_payload((i + 1) * 0x1000));
            assert!(!node.is_full()); // Should never be full
        }

        assert_eq!(node.base().meta.count(), 16);

        for i in (0..256).step_by(16) {
            assert!(node.get_child(i as u8).is_some());
            assert!(node.get_mask(i));
        }

        for i in (1..256).step_by(16) {
            assert!(node.get_child(i as u8).is_none());
            assert!(!node.get_mask(i));
        }
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
        if let NodeIter::N256(mut n256_iter) = iter {
            let first = n256_iter.next();
            assert!(matches!(first, Some((50, _))));
            let second = n256_iter.next();
            assert!(matches!(second, Some((150, _))));
            let third = n256_iter.next();
            assert!(matches!(third, Some((250, _))));
            assert!(n256_iter.next().is_none());
        } else {
            panic!("Expected N256 iterator");
        }

        let iter = src_node.get_children(100, 200);
        if let NodeIter::N256(mut n256_iter) = iter {
            let first = n256_iter.next();
            assert!(matches!(first, Some((150, _))));
            assert!(n256_iter.next().is_none());
        }

        src_node.copy_to(&mut dst_node);
        assert_eq!(dst_node.base().meta.count(), 3);
        assert!(dst_node.get_child(50).is_some());
        assert!(dst_node.get_child(150).is_some());
        assert!(dst_node.get_child(250).is_some());
        assert!(dst_node.get_mask(50));
        assert!(dst_node.get_mask(150));
        assert!(dst_node.get_mask(250));
    }

    #[test]
    fn test_edge_cases_and_boundaries() {
        let mut node = create_test_node();

        let empty_node = create_test_node();
        let iter = empty_node.get_children(0, 255);
        if let NodeIter::N256(mut n256_iter) = iter {
            assert!(n256_iter.next().is_none());
        }

        node.insert(0, NodePtr::from_payload(0x1000));
        node.insert(255, NodePtr::from_payload(0x2000));

        assert!(node.get_child(0).is_some());
        assert!(node.get_child(255).is_some());
        assert!(node.get_mask(0));
        assert!(node.get_mask(255));

        let boundary_keys = [7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 191, 192, 247, 248];
        for &key in &boundary_keys {
            node.insert(key, NodePtr::from_payload(key as usize * 0x1000));
            assert!(node.get_mask(key as usize));
            assert!(node.get_child(key).is_some());
        }

        node.remove(0);
        node.remove(255);
        assert!(!node.get_mask(0));
        assert!(!node.get_mask(255));
        assert!(node.get_child(0).is_none());
        assert!(node.get_child(255).is_none());
    }
}
