use std::arch::x86_64::_mm_cmplt_epi8;

use crate::base_node::BaseNode;

#[repr(C)]
pub(crate) struct Node16 {
    base: BaseNode,

    keys: [u8; 16],
    children: [*mut BaseNode; 16],
}

impl Node16 {
    fn is_full(&self) -> bool {
        self.base.count == 16
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 3
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        let key_flipped = Self::flip_sign(key);
        use std::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8};
        let pos = unsafe {
            let cmp = _mm_cmplt_epi8(
                _mm_set1_epi8(key_flipped as i8),
                _mm_loadu_si128(&self.keys as *const [u8; 16] as *const __m128i),
            );
            let bit_field = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - self.base.count));
            let pos = if bit_field > 0 {
                Self::ctz(bit_field as u16)
            } else {
                self.base.count as u16
            };
            pos as usize
        };

        unsafe {
            std::ptr::copy(
                self.keys.as_ptr().add(pos),
                self.keys.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos,
            );

            std::ptr::copy(
                self.children.as_ptr().add(pos),
                self.children.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos,
            );
        }
        self.keys[pos] = key;
        self.children[pos] = node;
        self.base.count += 1;
    }

    pub(crate) fn change(&mut self, key: u8, val: *mut BaseNode) {
        for (i, k) in self.keys.iter().enumerate() {
            if *k == key {
                self.children[i] = val;
            }
        }
    }

    fn get_child_pos(&self, key: u8) -> Option<usize> {
        use std::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
        };
        unsafe {
            let cmp = _mm_cmpeq_epi8(
                _mm_set1_epi8(Self::flip_sign(key) as i8),
                _mm_loadu_si128(&self.keys as *const [u8; 16] as *const __m128i),
            );
            let bit_field = _mm_movemask_epi8(cmp) & ((1 << self.base.count) - 1);
            if bit_field > 0 {
                return Some(Self::ctz(bit_field as u16) as usize);
            } else {
                return None;
            }
        }
    }

    pub(crate) fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        let pos = self.get_child_pos(key)?;
        return Some(self.children[pos]);
    }

    pub(crate) fn get_any_child(&self) -> *const BaseNode {
        for c in self.children.iter() {
            if BaseNode::is_leaf(*c) {
                return *c;
            }
        }
        return self.children[0];
    }

    fn flip_sign(val: u8) -> u8 {
        val ^ 128
    }

    fn ctz(val: u16) -> u16 {
        std::intrinsics::cttz(val)
    }
}
