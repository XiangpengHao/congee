use std::alloc;

const STACK_KEY_LEN: usize = 52;

pub trait Key: Eq + PartialEq + Default + PartialOrd + Ord {
    fn len(&self) -> usize;

    fn as_bytes(&self) -> &[u8];

    fn key_from(tid: usize) -> Self;
}

pub struct GeneralKey {
    len: u32,
    stack_keys: [u8; STACK_KEY_LEN],
    data: *mut u8,
}

impl Key for GeneralKey {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn key_from(tid: usize) -> GeneralKey {
        let mut key = GeneralKey::new();
        let swapped = std::intrinsics::bswap(tid);
        key.set_len(std::mem::size_of::<usize>() as u32);
        unsafe {
            let start = &mut *(key.as_bytes().as_ptr() as *mut usize);
            *start = swapped;
        }
        key
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

    fn set_len(&mut self, new_len: u32) {
        if new_len == self.len {
            return;
        }
        if self.len as usize > STACK_KEY_LEN {
            unsafe {
                let layout = alloc::Layout::from_size_align_unchecked(
                    self.len as usize,
                    std::mem::align_of::<u8>(),
                );
                std::alloc::dealloc(self.data, layout);
            }
        }

        self.len = new_len;

        if self.len as usize > STACK_KEY_LEN {
            unsafe {
                let layout = alloc::Layout::from_size_align_unchecked(
                    self.len as usize,
                    std::mem::align_of::<u8>(),
                );
                let mem = std::alloc::alloc(layout);
                self.data = mem;
            }
        }
    }
}

#[derive(Default, PartialEq, Eq)]
#[repr(C)]
pub struct UsizeKey {
    val: usize,
}

impl Ord for UsizeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for i in 0..8 {
            if self.as_bytes()[i] > other.as_bytes()[i] {
                return std::cmp::Ordering::Greater;
            } else if self.as_bytes()[i] < other.as_bytes()[i] {
                return std::cmp::Ordering::Less;
            }
        }
        std::cmp::Ordering::Equal
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
