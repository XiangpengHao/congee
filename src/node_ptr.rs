use std::ptr::NonNull;

use crate::{
    base_node::{BaseNode, Node},
    node_256::Node256,
};

#[derive(Clone, Copy)]
pub(crate) union NodePtr {
    tid: usize,
    sub_node: *const BaseNode,
}

impl NodePtr {
    #[inline]
    pub(crate) fn from_node(ptr: &BaseNode) -> Self {
        Self { sub_node: ptr }
    }

    pub(crate) fn from_root(ptr: *const Node256) -> Self {
        Self {
            sub_node: ptr as *const BaseNode,
        }
    }

    fn from_node_new(ptr: NonNull<BaseNode>) -> Self {
        Self {
            sub_node: ptr.as_ptr(),
        }
    }

    #[inline]
    pub(crate) fn from_tid(tid: usize) -> Self {
        Self { tid }
    }

    #[inline]
    pub(crate) fn as_tid(&self) -> usize {
        unsafe { self.tid }
    }

    pub(crate) fn as_ptr_safe<const MAX_LEVEL: usize>(
        &self,
        current_level: usize,
    ) -> *const BaseNode {
        debug_assert!(current_level < MAX_LEVEL);
        unsafe { self.sub_node }
    }
}

pub(crate) struct AllocatedNode<N: Node> {
    ptr: NonNull<N>,
}

impl<N: Node> AllocatedNode<N> {
    pub(crate) fn new(ptr: NonNull<N>) -> Self {
        Self { ptr }
    }

    pub(crate) fn as_mut(&mut self) -> &mut N {
        unsafe { self.ptr.as_mut() }
    }

    pub(crate) fn into_note_ptr(self) -> NodePtr {
        let ptr = self.ptr;
        std::mem::forget(self);
        unsafe { NodePtr::from_node_new(std::mem::transmute::<NonNull<N>, NonNull<BaseNode>>(ptr)) }
    }
}

impl<N: Node> Drop for AllocatedNode<N> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_mut());
        }
    }
}
