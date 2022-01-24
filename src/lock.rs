#![allow(dead_code)]

use std::sync::atomic::Ordering;

use crate::base_node::{BaseNode, Node};

pub(crate) struct ConcreteReadGuard<'a, T: Node> {
    version: usize,
    node: &'a T,
}

impl<'a, T: Node> ConcreteReadGuard<'a, T> {
    pub(crate) fn as_ref(&self) -> &T {
        self.node
    }

    pub(crate) fn upgrade_to_write_lock(self) -> Result<ConcreteWriteGuard<'a, T>, (Self, usize)> {
        let new_version = self.version + 0b10;
        match self
            .node
            .base()
            .type_version_lock_obsolete
            .compare_exchange_weak(
                self.version,
                new_version,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
            Ok(_) => Ok(ConcreteWriteGuard {
                version: new_version,
                node: unsafe { &mut *(self.node as *const T as *mut T) }, // TODO: this is the most dangerous thing in the world
            }),
            Err(v) => Err((self, v)),
        }
    }
}

pub(crate) struct ConcreteWriteGuard<'a, T: Node> {
    version: usize,
    node: &'a mut T,
}

impl<'a, T: Node> ConcreteWriteGuard<'a, T> {
    pub(crate) fn as_ref(&self) -> &T {
        self.node
    }

    pub(crate) fn as_mut(&self) -> &mut T {
        self.node
    }

    pub(crate) fn unlock_obsolete(&self) {
        self.node
            .base()
            .type_version_lock_obsolete
            .fetch_add(0b11, Ordering::Release);
    }
}

impl<'a, T: Node> Drop for ConcreteWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.node
            .base()
            .type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }
}

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

    pub(crate) fn unlock(self) -> Result<usize, usize> {
        self.check_version()
    }

    #[must_use]
    pub(crate) fn to_concrete<T: Node>(self) -> ConcreteReadGuard<'a, T> {
        ConcreteReadGuard {
            version: self.version,
            node: unsafe { &*(self.node as *const BaseNode as *const T) },
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
