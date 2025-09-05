use std::arch::x86_64::*;

/// Count the number of 1 bits from position 0 up to (but not including) the given position
/// in a 256-bit array using SIMD instructions
///
/// # Arguments
/// * `bits` - 256-bit array (32 bytes) representing the bitmap
/// * `pos` - Position to count up to (0-255, exclusive)
///
/// # Returns
/// Number of 1 bits from position 0 to pos-1
///
/// # Safety
/// This function uses x86_64 SIMD instructions and requires SSE4.2 and POPCNT support.
#[cfg(target_arch = "x86_64")]
#[inline]
pub fn count_ones_up_to_simd(bits: &[u8; 32], pos: u8) -> usize {
    if pos == 0 {
        return 0;
    }

    let full_u64_chunks = (pos as usize) / 64;
    let remaining_bits = (pos as usize) % 64;
    
    let mut count = 0usize;
    
    // Process complete 64-bit chunks using hardware popcnt
    for i in 0..full_u64_chunks {
        let chunk = unsafe {
            std::ptr::read_unaligned(bits.as_ptr().add(i * 8) as *const u64)
        };
        count += unsafe { _popcnt64(chunk as i64) as usize };
    }
    
    // Handle remaining bits in the partial chunk
    if remaining_bits > 0 && full_u64_chunks < 4 {
        let chunk = unsafe {
            std::ptr::read_unaligned(bits.as_ptr().add(full_u64_chunks * 8) as *const u64)
        };
        
        // Create mask to only count bits up to the position
        let mask = if remaining_bits >= 64 {
            u64::MAX
        } else {
            (1u64 << remaining_bits) - 1
        };
        
        count += unsafe { _popcnt64((chunk & mask) as i64) as usize };
    }
    
    count
}

/// Fallback implementation for non-x86_64 platforms or when SIMD is not available
#[cfg(not(target_arch = "x86_64"))]
pub fn count_ones_up_to_simd(bits: &[u8; 32], pos: u8) -> usize {
    count_ones_up_to_fallback(bits, pos)
}

/// Fallback bit counting implementation using standard library methods
pub fn count_ones_up_to_fallback(bits: &[u8; 32], pos: u8) -> usize {
    if pos == 0 {
        return 0;
    }

    let full_bytes = (pos as usize) / 8;
    let remaining_bits = (pos as usize) % 8;
    
    let mut count = 0usize;
    
    // Count complete bytes
    for i in 0..full_bytes {
        count += bits[i].count_ones() as usize;
    }
    
    // Handle remaining bits in partial byte
    if remaining_bits > 0 && full_bytes < 32 {
        let mask = (1u8 << remaining_bits) - 1;
        count += (bits[full_bytes] & mask).count_ones() as usize;
    }
    
    count
}

/// Set a bit at the given position in a 256-bit array
#[inline]
pub fn set_bit(bits: &mut [u8; 32], pos: u8) {
    let byte_idx = pos as usize / 8;
    let bit_idx = pos as usize % 8;
    if byte_idx < 32 {
        bits[byte_idx] |= 1u8 << bit_idx;
    }
}

/// Check if a bit is set at the given position in a 256-bit array
#[inline]
pub fn is_bit_set(bits: &[u8; 32], pos: u8) -> bool {
    let byte_idx = pos as usize / 8;
    let bit_idx = pos as usize % 8;
    if byte_idx < 32 {
        bits[byte_idx] & (1u8 << bit_idx) != 0
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_ones_up_to_empty() {
        let bits = [0u8; 32];
        assert_eq!(count_ones_up_to_simd(&bits, 0), 0);
        assert_eq!(count_ones_up_to_simd(&bits, 128), 0);
        assert_eq!(count_ones_up_to_simd(&bits, 255), 0);
    }

    #[test]
    fn test_count_ones_up_to_single_bit() {
        let mut bits = [0u8; 32];
        set_bit(&mut bits, 10);
        
        assert_eq!(count_ones_up_to_simd(&bits, 10), 0);
        assert_eq!(count_ones_up_to_simd(&bits, 11), 1);
        assert_eq!(count_ones_up_to_simd(&bits, 100), 1);
    }

    #[test]
    fn test_count_ones_up_to_multiple_bits() {
        let mut bits = [0u8; 32];
        set_bit(&mut bits, 5);
        set_bit(&mut bits, 10);
        set_bit(&mut bits, 65); // Cross 64-bit boundary
        set_bit(&mut bits, 200);
        
        assert_eq!(count_ones_up_to_simd(&bits, 5), 0);
        assert_eq!(count_ones_up_to_simd(&bits, 6), 1);
        assert_eq!(count_ones_up_to_simd(&bits, 11), 2);
        assert_eq!(count_ones_up_to_simd(&bits, 66), 3);
        assert_eq!(count_ones_up_to_simd(&bits, 201), 4);
        assert_eq!(count_ones_up_to_simd(&bits, 255), 4);
    }

    #[test]
    fn test_count_ones_up_to_boundary_conditions() {
        let mut bits = [0u8; 32];
        
        // Set bits at various boundaries
        set_bit(&mut bits, 7);   // End of first byte
        set_bit(&mut bits, 8);   // Start of second byte
        set_bit(&mut bits, 63);  // End of first 64-bit chunk
        set_bit(&mut bits, 64);  // Start of second 64-bit chunk
        set_bit(&mut bits, 127); // End of second 64-bit chunk
        set_bit(&mut bits, 128); // Start of third 64-bit chunk
        set_bit(&mut bits, 255); // Last bit
        
        assert_eq!(count_ones_up_to_simd(&bits, 8), 1);
        assert_eq!(count_ones_up_to_simd(&bits, 9), 2);
        assert_eq!(count_ones_up_to_simd(&bits, 64), 3);
        assert_eq!(count_ones_up_to_simd(&bits, 65), 4);
        assert_eq!(count_ones_up_to_simd(&bits, 128), 5);
        assert_eq!(count_ones_up_to_simd(&bits, 129), 6);
        assert_eq!(count_ones_up_to_simd(&bits, 255), 6);
    }

    #[test]
    fn test_simd_vs_fallback_consistency() {
        let mut bits = [0u8; 32];
        
        // Set some random bits
        for pos in [1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 128, 129, 200, 254] {
            set_bit(&mut bits, pos);
        }
        
        // Test that SIMD and fallback give same results
        for pos in 0..=255 {
            let simd_result = count_ones_up_to_simd(&bits, pos);
            let fallback_result = count_ones_up_to_fallback(&bits, pos);
            assert_eq!(simd_result, fallback_result, 
                      "Mismatch at position {}: SIMD={}, fallback={}", 
                      pos, simd_result, fallback_result);
        }
    }

    #[test]
    fn test_bit_operations() {
        let mut bits = [0u8; 32];
        
        assert!(!is_bit_set(&bits, 10));
        set_bit(&mut bits, 10);
        assert!(is_bit_set(&bits, 10));
        
        // Test boundary conditions
        set_bit(&mut bits, 0);
        set_bit(&mut bits, 255);
        assert!(is_bit_set(&bits, 0));
        assert!(is_bit_set(&bits, 255));
    }
}