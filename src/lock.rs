#![allow(dead_code)]

use std::sync::atomic::Ordering;

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

    #[allow(clippy::cast_ref_to_mut)]
    pub(crate) fn upgrade_to_write_lock(self) -> Result<WriteGuard<'a>, (Self, usize)> {
        let new_version = self.version + 0b10;
        match self.node.type_version_lock_obsolete.compare_exchange_weak(
            self.version,
            new_version,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(WriteGuard {
                version: new_version,
                node: unsafe { &mut *(self.node as *const BaseNode as *mut BaseNode) }, // TODO: this is the most dangerous thing in the world
            }),
            Err(v) => Err((self, v)),
        }
    }
}

pub(crate) struct WriteGuard<'a> {
    version: usize,
    node: &'a mut BaseNode,
}

impl<'a> WriteGuard<'a> {
    pub(crate) fn check_version(&self) -> Result<usize, usize> {
        let v = self.node.type_version_lock_obsolete.load(Ordering::Acquire);
        if v == self.version {
            Ok(v)
        } else {
            Err(v)
        }
    }

    pub(crate) fn unlock(&self) {
        self.node
            .type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        self.node
    }

    pub(crate) fn as_mut(&mut self) -> &mut BaseNode {
        self.node
    }
}
