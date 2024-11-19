use std::{marker::PhantomData, sync::atomic::Ordering};

use crate::{
    base_node::{BaseNode, Node},
    error::ArtError,
};

pub(crate) struct TypedReadGuard<'a, T: Node> {
    version: usize,
    node: *const T,
    _pt_node: PhantomData<&'a T>,
}

impl<'a, T: Node> TypedReadGuard<'a, T> {
    pub(crate) fn as_ref(&self) -> &T {
        unsafe { &*self.node }
    }

    pub(crate) fn upgrade(self) -> Result<ConcreteWriteGuard<'a, T>, (Self, ArtError)> {
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
                // SAFETY: this is seems to be unsound, but we (1) acquired write lock, (2) has the right memory ordering.
                node: unsafe { &mut *(self.node as *const T as *mut T) },
            }),
            Err(_v) => Err((self, ArtError::VersionNotMatch)),
        }
    }
}

pub(crate) struct ConcreteWriteGuard<'a, T: Node> {
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
    node: *const BaseNode,
    _pt_node: PhantomData<&'a BaseNode>,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(v: usize, node: *const BaseNode) -> Self {
        Self {
            version: v,
            node,
            _pt_node: PhantomData,
        }
    }

    pub(crate) fn check_version(&self) -> Result<usize, ArtError> {
        let v = self
            .as_ref()
            .type_version_lock_obsolete
            .load(Ordering::Acquire);

        if v == self.version {
            Ok(v)
        } else {
            Err(ArtError::VersionNotMatch)
        }
    }

    pub(crate) fn unlock(self) -> Result<usize, ArtError> {
        self.check_version()
    }

    #[must_use]
    pub(crate) fn into_concrete<T: Node>(self) -> TypedReadGuard<'a, T> {
        assert_eq!(self.as_ref().get_type(), T::get_type());

        TypedReadGuard {
            version: self.version,
            node: unsafe { &*(self.node as *const BaseNode as *const T) },
            _pt_node: PhantomData,
        }
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        unsafe { &*self.node }
    }

    pub(crate) fn upgrade(self) -> Result<WriteGuard<'a>, (Self, ArtError)> {
        #[cfg(test)]
        {
            if crate::utils::fail_point(ArtError::VersionNotMatch).is_err() {
                return Err((self, ArtError::VersionNotMatch));
            };
        }

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
                node: unsafe { &mut *(self.node as *mut BaseNode) },
            }),
            Err(_v) => Err((self, ArtError::VersionNotMatch)),
        }
    }
}

pub(crate) struct WriteGuard<'a> {
    node: &'a mut BaseNode,
}

impl<'a> WriteGuard<'a> {
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
