use std::{alloc, ops::Deref};

const STACK_KEY_LEN: usize = 52;
pub struct Key {
    len: u32,
    stack_keys: [u8; STACK_KEY_LEN],
    data: *mut u8,
}

impl Key {
    pub fn new() -> Self {
        Key {
            len: 0,
            stack_keys: [0; STACK_KEY_LEN],
            data: std::ptr::null_mut(),
        }
    }

    pub fn get_key_len(&self) -> u32 {
        self.len
    }

    fn set_key_len(&mut self, new_len: u32) {
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

impl Deref for Key {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        if self.len as usize > STACK_KEY_LEN {
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        } else {
            unsafe { std::slice::from_raw_parts(self.stack_keys.as_ptr(), STACK_KEY_LEN) }
        }
    }
}

pub fn load_key(tid: usize, key: &mut Key) {
    let swapped = std::intrinsics::bswap(tid);
    unsafe {
        let start = &mut *(key.data as *mut usize);
        *start = swapped;
    }
}
