const STACK_KEY_LEN: usize = 56;

/// A trait for Art-specific keys, don't use it unless you know what you are doing.
pub trait RawKey: Eq + PartialEq + Default + PartialOrd + Ord {
    fn len(&self) -> usize;

    fn as_bytes(&self) -> &[u8];

    fn key_from(tid: usize) -> Self;
}

#[derive(Clone)]
pub(crate) struct TestingKey {
    len: usize,
    stack_keys: [u8; STACK_KEY_LEN],
}

impl RawKey for TestingKey {
    fn len(&self) -> usize {
        self.len
    }

    fn as_bytes(&self) -> &[u8] {
        self.stack_keys[..self.len].as_ref()
    }

    fn key_from(tid: usize) -> TestingKey {
        let mut stack_keys = [0; STACK_KEY_LEN];

        let swapped = tid.swap_bytes();

        for (i, v) in swapped.to_le_bytes().iter().enumerate() {
            stack_keys[i] = *v;
        }

        TestingKey {
            len: std::mem::size_of::<usize>(),
            stack_keys,
        }
    }
}
impl Ord for TestingKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for i in 0..std::cmp::min(self.len(), other.len()) {
            if self.as_bytes()[i] > other.as_bytes()[i] {
                return std::cmp::Ordering::Greater;
            } else if self.as_bytes()[i] < other.as_bytes()[i] {
                return std::cmp::Ordering::Less;
            }
        }
        if self.len() == other.len() {
            std::cmp::Ordering::Equal
        } else {
            std::cmp::Ordering::Less
        }
    }
}

impl PartialOrd for TestingKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TestingKey {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        for (i, v) in self.as_bytes().iter().enumerate() {
            if other.as_bytes()[i] != *v {
                return false;
            }
        }
        true
    }
}

impl Eq for TestingKey {}

impl Default for TestingKey {
    fn default() -> Self {
        Self::new()
    }
}

impl TestingKey {
    fn new() -> Self {
        TestingKey {
            len: 0,
            stack_keys: [0; STACK_KEY_LEN],
        }
    }
}

#[derive(Default, PartialEq, Eq)]
#[repr(C)]
pub struct UsizeKey {
    val: usize,
}

impl Ord for UsizeKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = self.val.swap_bytes();
        let b = other.val.swap_bytes();
        a.cmp(&b)
    }
}

impl PartialOrd for UsizeKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RawKey for UsizeKey {
    #[inline]
    fn len(&self) -> usize {
        8
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(&self.val as *const usize as *const u8, 8) }
    }

    fn key_from(tid: usize) -> Self {
        Self {
            val: tid.swap_bytes(),
        }
    }
}
