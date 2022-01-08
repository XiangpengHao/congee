use core::panic;
use std::{ops::Deref, sync::atomic::Ordering};

use crate::base_node::BaseNode;

pub(crate) struct ReadGuard<'a> {
    version: usize,
    node: &'a BaseNode,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(v: usize, node: &'a BaseNode) -> Self {
        Self { version: v, node }
    }

    pub(crate) fn check_version(&self) -> Result<usize, usize> {
        let v = self.node.type_version_lock_obsolete.load(Ordering::Acquire);
        if v == self.version {
            Ok(v)
        } else {
            Err(v)
        }
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        self.node
    }
}
