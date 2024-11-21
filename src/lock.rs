use std::{marker::PhantomData, ptr::NonNull, sync::atomic::Ordering};

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

    pub(crate) fn upgrade(self) -> Result<TypedWriteGuard<'a, T>, (Self, ArtError)> {
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
            Ok(_) => Ok(TypedWriteGuard {
                node: unsafe { &mut *(self.node as *mut T) },
            }),
            Err(_v) => Err((self, ArtError::VersionNotMatch)),
        }
    }
}

pub(crate) struct TypedWriteGuard<'a, T: Node> {
    node: &'a mut T,
}

impl<T: Node> TypedWriteGuard<'_, T> {
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

impl<T: Node> Drop for TypedWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.node
            .base()
            .type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }
}

pub(crate) struct ReadGuard<'a> {
    version: usize,
    node: NonNull<BaseNode>,
    _pt_node: PhantomData<&'a BaseNode>,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(v: usize, node: NonNull<BaseNode>) -> Self {
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
    pub(crate) fn into_typed<T: Node>(self) -> TypedReadGuard<'a, T> {
        assert_eq!(self.as_ref().get_type(), T::get_type());

        TypedReadGuard {
            version: self.version,
            node: unsafe { &*(self.node.as_ptr() as *const T) },
            _pt_node: PhantomData,
        }
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        unsafe { &*self.node.as_ptr() }
    }

    pub(crate) fn upgrade(self) -> Result<WriteGuard<'a>, (Self, ArtError)> {
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
                node: unsafe { &mut *(self.node.as_ptr()) },
            }),
            Err(_v) => Err((self, ArtError::VersionNotMatch)),
        }
    }
}

pub(crate) struct WriteGuard<'a> {
    node: &'a mut BaseNode,
}

impl WriteGuard<'_> {
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

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.node
            .type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }
}
