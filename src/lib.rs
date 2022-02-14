#![feature(core_intrinsics)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::collapsible_if)]

mod base_node;
mod child_ptr;
mod key;
mod lock;
mod node_16;
mod node_256;
mod node_4;
mod node_48;
mod tree;
mod utils;

mod range_scan;

#[cfg(test)]
mod tests;

pub use crossbeam_epoch as epoch;
pub use key::Key as RawKey;
use key::UsizeKey;
pub use tree::Tree as RawArt;

pub struct Art {
    inner: RawArt<UsizeKey>,
}

impl Art {
    #[inline]
    pub fn get(&self, key: &usize, guard: &epoch::Guard) -> Option<usize> {
        let key = UsizeKey::key_from(*key);
        self.inner.get(&key, guard)
    }

    #[inline]
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    #[inline]
    pub fn new() -> Self {
        Art {
            inner: RawArt::new(),
        }
    }

    #[inline]
    pub fn remove(&self, k: &usize, guard: &epoch::Guard) {
        let key = UsizeKey::key_from(*k);
        self.inner.remove(&key, guard)
    }

    #[inline]
    pub fn insert(&self, k: usize, v: usize, guard: &epoch::Guard) {
        let key = UsizeKey::key_from(k);
        self.inner.insert(key, v, guard)
    }

    #[inline]
    pub fn range(
        &self,
        start: &usize,
        end: &usize,
        result: &mut [usize],
        guard: &epoch::Guard,
    ) -> Option<usize> {
        let start = UsizeKey::key_from(*start);
        let end = UsizeKey::key_from(*end);
        self.inner.range(&start, &end, result, guard)
    }
}
