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

    pub(crate) fn unlock(guard: ReadGuard) -> Result<usize, usize> {
        let v = guard.type_version_lock_obsolete.load(Ordering::Acquire);
        if v == guard.version {
            Ok(v)
        } else {
            Err(v)
        }
    }

    pub(crate) fn check_version(guard: &ReadGuard, version: usize) -> Result<usize, usize> {
        let v = guard.type_version_lock_obsolete.load(Ordering::Acquire);
        if v == version {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = BaseNode;
    fn deref(&self) -> &Self::Target {
        self.node
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        panic!("Optimistic read guard must be dropped manually");
    }
}
