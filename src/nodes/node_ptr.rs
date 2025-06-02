use std::{fmt::Debug, ptr::NonNull};

use crate::{
    Allocator,
    nodes::{BaseNode, Node},
    utils::KeyTracker,
};

pub(crate) struct ChildIsPayload<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl ChildIsPayload<'_> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

pub(crate) struct ChildIsSubNode<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl ChildIsSubNode<'_> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

pub(crate) enum PtrType {
    Payload(usize),
    SubNode(NonNull<BaseNode>),
}

#[derive(Clone, Copy)]
pub(crate) union NodePtr {
    payload: usize,
    sub_node: NonNull<BaseNode>,
}

impl Debug for NodePtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write!(
                f,
                "payload: {:?} or sub_node: {:?}",
                self.payload, self.sub_node
            )
        }
    }
}

impl NodePtr {
    #[inline]
    pub(crate) fn from_node(ptr: &BaseNode) -> Self {
        Self {
            sub_node: NonNull::from(ptr),
        }
    }

    fn from_node_new(ptr: NonNull<BaseNode>) -> Self {
        Self { sub_node: ptr }
    }

    pub(crate) fn from_payload(payload: usize) -> Self {
        Self { payload }
    }

    pub(crate) unsafe fn as_payload_unchecked(&self) -> usize {
        unsafe { self.payload }
    }

    pub(crate) fn downcast<const K_LEN: usize>(&self, current_level: usize) -> PtrType {
        if current_level == (K_LEN - 1) {
            PtrType::Payload(unsafe { self.as_payload_unchecked() })
        } else {
            PtrType::SubNode(unsafe { self.sub_node })
        }
    }

    pub(crate) fn downcast_key_tracker<const K_LEN: usize>(
        &self,
        key_tracker: &KeyTracker<K_LEN>,
    ) -> PtrType {
        self.downcast::<K_LEN>(key_tracker.len() - 1)
    }
}

/// This is a wrapper around a node that is allocated on the heap.
/// User need to explicitly call into_note_ptr to convert it to a NodePtr,
/// otherwise the node will be deallocated when the AllocatedNode is dropped.
///
/// This prevents the memory leak if the node is allocated but not used (e.g., due to concurrent insertions)
pub(crate) struct AllocatedNode<'a, N: Node, A: Allocator> {
    ptr: NonNull<N>,
    allocator: &'a A,
}

impl<'a, N: Node, A: Allocator> AllocatedNode<'a, N, A> {
    pub(crate) fn new(ptr: NonNull<N>, allocator: &'a A) -> Self {
        Self { ptr, allocator }
    }

    pub(crate) fn as_mut(&mut self) -> &mut N {
        unsafe { self.ptr.as_mut() }
    }

    pub(crate) fn into_note_ptr(self) -> NodePtr {
        let ptr = self.ptr;
        std::mem::forget(self);
        unsafe { NodePtr::from_node_new(std::mem::transmute::<NonNull<N>, NonNull<BaseNode>>(ptr)) }
    }

    pub(crate) fn into_non_null(self) -> NonNull<N> {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }
}

impl<'a, N: Node, A: Allocator> Drop for AllocatedNode<'a, N, A> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_mut());
            let ptr = std::ptr::NonNull::new(self.ptr.as_ptr() as *mut u8).unwrap();
            let layout = N::get_type().node_layout();
            self.allocator.deallocate(ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::nodes::{BaseNode, Node, Node4};
    use crate::{Allocator, Congee, DefaultAllocator, MemoryStatsAllocator};

    #[test]
    fn test_deallocator_called_on_drop() {
        let stats_allocator = MemoryStatsAllocator::new(DefaultAllocator {});
        let tree: Congee<usize, usize, _> = Congee::new(stats_allocator);

        let allocated_before = tree.allocated_memory();
        let deallocated_before = tree.deallocated_memory();

        // Create an AllocatedNode which should trigger allocation
        let allocated_node = {
            BaseNode::make_node::<Node4, _>(&[], tree.allocator()).expect("Failed to allocate node")
        };

        let allocated_after_creation = tree.allocated_memory();
        let deallocated_after_creation = tree.deallocated_memory();

        // Verify that memory was allocated but not yet deallocated
        assert!(
            allocated_after_creation > allocated_before,
            "Memory should have been allocated"
        );
        assert_eq!(
            deallocated_after_creation, deallocated_before,
            "Memory should not have been deallocated yet"
        );

        // Drop the AllocatedNode explicitly to trigger the deallocator
        drop(allocated_node);

        let allocated_after_drop = tree.allocated_memory();
        let deallocated_after_drop = tree.deallocated_memory();

        // Verify that the deallocator was called
        assert_eq!(
            allocated_after_drop, allocated_after_creation,
            "Allocated memory should not change after drop"
        );
        assert!(
            deallocated_after_drop > deallocated_after_creation,
            "Memory should have been deallocated"
        );

        // The amount deallocated should equal the amount allocated for this node
        let node_size = allocated_after_creation - allocated_before;
        let deallocated_size = deallocated_after_drop - deallocated_before;
        assert_eq!(
            node_size, deallocated_size,
            "Deallocated size should match allocated size"
        );
    }

    #[test]
    fn test_into_node_ptr_prevents_deallocation() {
        let stats_allocator = MemoryStatsAllocator::new(DefaultAllocator {});
        let tree: Congee<usize, usize, _> = Congee::new(stats_allocator);

        let deallocated_before = tree.deallocated_memory();

        let allocated_node = BaseNode::make_node::<Node4, _>(&[], tree.allocator())
            .expect("Failed to allocate node");

        let node_ptr = allocated_node.into_note_ptr();

        let deallocated_after = tree.deallocated_memory();

        // Verify that no deallocation occurred because into_note_ptr() calls std::mem::forget()
        assert_eq!(
            deallocated_after, deallocated_before,
            "No deallocation should occur when converting to NodePtr because std::mem::forget is called"
        );

        unsafe {
            tree.allocator().deallocate(
                std::ptr::NonNull::new(node_ptr.sub_node.as_ptr() as *mut u8).unwrap(),
                Node4::get_type().node_layout(),
            );
        }
    }
}
