const STACK_KEY_LEN: usize = 56;

pub trait Key: Eq + PartialEq + Default + PartialOrd + Ord {
    fn len(&self) -> usize;

    fn as_bytes(&self) -> &[u8];

    fn key_from(tid: usize) -> Self;
}

pub struct GeneralKey {
    len: usize,
    stack_keys: [u8; STACK_KEY_LEN],
}

impl Key for GeneralKey {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn key_from(tid: usize) -> GeneralKey {
        let mut stack_keys = [0; STACK_KEY_LEN];

        let swapped = std::intrinsics::bswap(tid);

        for (i, v) in swapped.to_le_bytes().iter().enumerate() {
            stack_keys[i] = *v;
        }

        GeneralKey {
            len: std::mem::size_of::<usize>(),
            stack_keys,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.stack_keys[..self.len as usize].as_ref()
    }
}
impl Ord for GeneralKey {
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

impl PartialOrd for GeneralKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for GeneralKey {
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

impl Eq for GeneralKey {}

impl Default for GeneralKey {
    fn default() -> Self {
        Self::new()
    }
}

impl GeneralKey {
    fn new() -> Self {
        GeneralKey {
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
        let a = std::intrinsics::bswap(self.val);
        let b = std::intrinsics::bswap(other.val);
        a.cmp(&b)
    }
}

impl PartialOrd for UsizeKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Key for UsizeKey {
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
            val: std::intrinsics::bswap(tid),
        }
    }
}
