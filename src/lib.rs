//! [![congee](https://github.com/XiangpengHao/congee/actions/workflows/ci.yml/badge.svg)](https://github.com/XiangpengHao/congee/actions/workflows/ci.yml)
//! [![Crates.io](https://img.shields.io/crates/v/congee.svg)](
//! https://crates.io/crates/congee)
//! [![dependency status](https://deps.rs/repo/github/xiangpenghao/congee/status.svg)](https://deps.rs/crate/congee)
//! [![codecov](https://codecov.io/gh/XiangpengHao/congee/branch/main/graph/badge.svg?token=x0PSjQrqyR)](https://codecov.io/gh/XiangpengHao/congee)
//! [![Documentation](https://docs.rs/congee/badge.svg)](https://docs.rs/congee)
//! A Rust implementation of ART-OLC [concurrent adaptive radix tree](https://db.in.tum.de/~leis/papers/artsync.pdf).
//! It implements the optimistic lock coupling with proper SIMD support.
//!
//! The code is based on its [C++ implementation](https://github.com/flode/ARTSynchronized), with many bug fixes.
//!
//! It only supports (and is optimized for) 8 byte key;
//! due to this specialization, this ART has great performance -- basic operations are ~40% faster than [flurry](https://github.com/jonhoo/flurry) hash table, range scan is an order of magnitude faster.
//!
//! The code is extensively tested with [{address|leak} sanitizer](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html) as well as [libfuzzer](https://llvm.org/docs/LibFuzzer.html).
//!
//! ### Why this library?
//! - Fast performance, faster than most hash tables.
//! - Concurrent, super scalable, it reaches 150Mop/s on 32 cores.
//! - Super low memory consumption. Hash tables often have exponential bucket size growth, which often lead to low load factors. ART is more space efficient.
//!
//!
//! ### Why not this library?
//! - Not for arbitrary key size. This library only supports 8 byte key.
//! - The value must be a valid, user-space, 64 bit pointer, aka non-null and zeros on 48-63 bits.
//! - Not for sparse keys. ART is optimized for dense keys, if your keys are sparse, you should consider a hashtable.
//!
//! ### Example:
//! ```
//! use congee::Art;
//! let art = Art::new();
//! let guard = art.pin(); // enter an epoch
//!
//! art.insert(0, 42, &guard); // insert a value
//! let val = art.get(&0, &guard).unwrap(); // read the value
//! assert_eq!(val, 42);
//!
//! let mut scan_buffer = vec![0; 8];
//! let scan_result = art.range(&0, &10, &mut scan_buffer, &guard); // scan values
//! assert_eq!(scan_result.unwrap(), 1);
//! ```

#![feature(core_intrinsics)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::len_without_is_empty)]

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

#[cfg(feature = "stats")]
mod stats;

#[cfg(test)]
mod tests;

pub use key::RawKey;
use key::UsizeKey;
pub use tree::RawTree as RawArt;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam_epoch::{pin, Guard};
}

pub struct Art {
    inner: RawArt<UsizeKey>,
}

impl Default for Art {
    fn default() -> Self {
        Self::new()
    }
}

impl Art {
    /// Returns a copy of the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// assert_eq!(tree.get(&1, &guard).unwrap(), 42);
    /// ```
    #[inline]
    pub fn get(&self, key: &usize, guard: &epoch::Guard) -> Option<usize> {
        let key = UsizeKey::key_from(*key);
        self.inner.get(&key, guard)
    }

    /// Enters an epoch.
    /// Note: this can be expensive, try to reuse it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// let guard = tree.pin();
    /// ```
    #[inline]
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    /// Create an empty `ART` tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Art {
            inner: RawArt::new(),
        }
    }

    /// Removes key-value pair from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// tree.remove(&1, &guard);
    /// assert!(tree.get(&1, &guard).is_none());
    /// ```
    #[inline]
    pub fn remove(&self, k: &usize, guard: &epoch::Guard) {
        let key = UsizeKey::key_from(*k);
        self.inner.remove(&key, guard)
    }

    /// Insert a key-value pair to the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// assert_eq!(tree.get(&1, &guard).unwrap(), 42);
    /// ```
    #[inline]
    pub fn insert(&self, k: usize, v: usize, guard: &epoch::Guard) {
        let key = UsizeKey::key_from(k);
        self.inner.insert(key, v, guard)
    }

    /// Scan the tree with the range of [start, end], write the result to the
    /// `result` buffer.
    /// It scans the length of `result` or the number of the keys within the range, whichever is smaller;
    /// returns the number of the keys scanned.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Art;
    /// let tree = Art::new();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    ///
    /// let low_key = 1;
    /// let high_key = 2;
    /// let mut result = [0; 2];
    /// let scanned = tree.range(&low_key, &high_key, &mut result, &guard);
    /// assert_eq!(scanned.unwrap(), 1);
    /// assert_eq!(result, [42, 0]);
    /// ```
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
