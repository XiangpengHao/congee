const STACK_KEY_LEN: usize = 56;

pub trait Key: Eq + PartialEq + Default + PartialOrd + Ord {
    fn len(&self) -> usize;

    fn as_bytes(&self) -> &[u8];

    fn key_from(tid: usize) -> Self;
}

pub struct GeneralKey {
    len: usize,
    stack_keys: [u8; STACK_KEY_LEN],
    data: *mut u8,
}

impl Key for GeneralKey {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn key_from(tid: usize) -> GeneralKey {
        // Why bother?
        // We need to ensure the stack_keys are aligned to 8 bytes,
        // if we just use [u8; STACK_KEY_LEN], we will run into alignment issues
        const STACK_LEN: usize = STACK_KEY_LEN / std::mem::size_of::<usize>();
        let mut stack_keys: [usize; STACK_LEN] = [0; STACK_LEN];

        let swapped = std::intrinsics::bswap(tid);
        unsafe {
            let start = &mut *(stack_keys.as_mut_ptr() as *mut usize);
            *start = swapped;
        }
        GeneralKey {
            len: std::mem::size_of::<usize>(),
            data: std::ptr::null_mut(),
            stack_keys: unsafe {
                std::mem::transmute::<[usize; STACK_LEN], [u8; STACK_KEY_LEN]>(stack_keys)
            },
        }
    }

    fn as_bytes(&self) -> &[u8] {
        if self.len as usize > STACK_KEY_LEN {
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        } else {
            unsafe { std::slice::from_raw_parts(self.stack_keys.as_ptr(), STACK_KEY_LEN) }
        }
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
            data: std::ptr::null_mut(),
        }
    }
}

#[derive(Default, PartialEq, Eq)]
#[repr(C)]
pub struct UsizeKey {
    val: usize,
}

impl UsizeKey {
    pub fn new(val: usize) -> Self {
        UsizeKey::key_from(val)
    }
}

impl Ord for UsizeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = std::intrinsics::bswap(self.val);
        let b = std::intrinsics::bswap(other.val);
        a.cmp(&b)
    }
}

impl PartialOrd for UsizeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Key for UsizeKey {
    fn len(&self) -> usize {
        8
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(&self.val as *const usize as *const u8, 8) }
    }

    fn key_from(tid: usize) -> Self {
        Self {
            val: std::intrinsics::bswap(tid),
        }
    }
}
