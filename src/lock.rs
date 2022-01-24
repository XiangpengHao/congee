#![allow(dead_code)]

use std::{cell::UnsafeCell, sync::atomic::Ordering};

use crate::base_node::{BaseNode, Node};

pub(crate) struct ConcreteReadGuard<'a, T: Node> {
    version: usize,
    node: &'a UnsafeCell<T>,
}

impl<'a, T: Node> ConcreteReadGuard<'a, T> {
    pub(crate) fn as_ref(&self) -> &T {
        unsafe { &*self.node.get() }
    }

    pub(crate) fn upgrade_to_write_lock(self) -> Result<ConcreteWriteGuard<'a, T>, (Self, usize)> {
        let new_version = self.version + 0b10;
        match self
            .as_ref()
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
                node: unsafe { &mut *self.node.get() },
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

    pub(crate) fn as_mut(&mut self) -> &mut T {
        self.node
    }

    pub(crate) fn mark_obsolete(&self) {
        self.node
            .base()
            .type_version_lock_obsolete
            .fetch_add(0b01, Ordering::Release);
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
    node: &'a UnsafeCell<BaseNode>,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(v: usize, node: &'a BaseNode) -> Self {
        Self {
            version: v,
            node: unsafe { std::mem::transmute::<&BaseNode, &UnsafeCell<BaseNode>>(node) }, // todo: the caller should pass UnsafeCell<BaseNode> instead
        }
    }

    pub(crate) fn check_version(&self) -> Result<usize, usize> {
        let v = self
            .as_ref()
            .type_version_lock_obsolete
            .load(Ordering::Acquire);
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
    pub(crate) fn into_concrete<T: Node>(self) -> ConcreteReadGuard<'a, T> {
        ConcreteReadGuard {
            version: self.version,
            node: unsafe {
                std::mem::transmute::<&UnsafeCell<BaseNode>, &UnsafeCell<T>>(self.node)
            },
        }
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        unsafe { &*self.node.get() }
    }

    pub(crate) fn upgrade_to_write_lock(self) -> Result<WriteGuard<'a>, (Self, usize)> {
        let new_version = self.version + 0b10;
        match self
            .as_ref()
            .type_version_lock_obsolete
            .compare_exchange_weak(
                self.version,
                new_version,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
            Ok(_) => Ok(WriteGuard {
                version: new_version,
                node: unsafe { &mut *self.node.get() },
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

    pub(crate) fn as_ref(&self) -> &BaseNode {
        self.node
    }

    pub(crate) fn as_mut(&mut self) -> &mut BaseNode {
        self.node
    }

    pub(crate) fn mark_obsolete(&mut self) {
        self.node
            .type_version_lock_obsolete
            .fetch_add(0b01, Ordering::Release);
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.node
            .type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }
}
