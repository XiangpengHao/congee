use std::{alloc, ops::Deref};

const STACK_KEY_LEN: usize = 52;

pub trait Key: Eq + PartialEq + Default + Deref<Target = [u8]> {
    fn new() -> Self {
        Self::default()
    }

    fn len(&self) -> usize;

    fn load_full_key(key: &mut Self, tid: usize);
}

pub struct GeneralKey {
    len: u32,
    stack_keys: [u8; STACK_KEY_LEN],
    data: *mut u8,
}

impl Key for GeneralKey {
    fn new() -> Self {
        GeneralKey {
            len: 0,
            stack_keys: [0; STACK_KEY_LEN],
            data: std::ptr::null_mut(),
        }
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn load_full_key(key: &mut GeneralKey, tid: usize) {
        let swapped = std::intrinsics::bswap(tid);
        key.set_len(std::mem::size_of::<usize>() as u32);
        unsafe {
            let start = &mut *(key.as_ptr() as *mut usize);
            *start = swapped;
        }
    }
}

impl PartialEq for GeneralKey {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        for (i, v) in self.iter().enumerate() {
            if other[i] != *v {
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

impl From<usize> for GeneralKey {
    fn from(val: usize) -> Self {
        let mut key = GeneralKey::new();
        load_key(val, &mut key);
        key
    }
}

impl Deref for GeneralKey {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        if self.len as usize > STACK_KEY_LEN {
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        } else {
            unsafe { std::slice::from_raw_parts(self.stack_keys.as_ptr(), STACK_KEY_LEN) }
        }
    }
}

pub fn load_key(tid: usize, key: &mut GeneralKey) {
    let swapped = std::intrinsics::bswap(tid);
    key.set_len(std::mem::size_of::<usize>() as u32);
    unsafe {
        let start = &mut *(key.as_ptr() as *mut usize);
        *start = swapped;
    }
}
