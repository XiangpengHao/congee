use crate::base_node::BaseNode;

#[derive(Clone, Copy)]
pub(crate) union NodePtr {
    tid: usize,
    sub_node: *const BaseNode,
}

impl NodePtr {
    #[inline]
    pub(crate) fn from_node(ptr: *const BaseNode) -> Self {
        Self { sub_node: ptr }
    }

    #[inline]
    pub(crate) fn from_tid(tid: usize) -> Self {
        Self { tid }
    }

    #[inline]
    pub(crate) fn as_tid(&self) -> usize {
        unsafe { self.tid }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const BaseNode {
        unsafe { self.sub_node }
    }
}
