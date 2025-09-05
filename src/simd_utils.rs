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

/// Compute precomputed popcount values for 64-bit boundaries in a 256-bit array
pub fn compute_precomputed_popcounts(bits: &[u8; 32]) -> [u32; 4] {
    let mut counts = [0u32; 4];
    
    // Count bits 0..64 (first 8 bytes)
    counts[0] = bits[0..8].iter().map(|b| b.count_ones()).sum::<u32>();
    
    // Count bits 0..128 (first 16 bytes)  
    counts[1] = bits[0..16].iter().map(|b| b.count_ones()).sum::<u32>();
    
    // Count bits 0..192 (first 24 bytes)
    counts[2] = bits[0..24].iter().map(|b| b.count_ones()).sum::<u32>();
    
    // Count bits 0..256 (all 32 bytes)
    counts[3] = bits.iter().map(|b| b.count_ones()).sum::<u32>();
    
    counts
}

/// Count ones up to position using precomputed values for O(1) lookup
pub fn count_ones_up_to_precomputed(
    precomputed: &[u32; 4], 
    bits: &[u8; 32], 
    pos: u8
) -> usize {
    if pos == 0 {
        return 0;
    }
    
    let pos = pos as usize;
    
    match pos {
        1..=64 => {
            // Calculate popcount for bits 0..pos within first 64 bits
            let chunk_bytes = &bits[0..8];
            count_bits_in_range(chunk_bytes, 0, pos)
        },
        65..=128 => {
            // Precomputed[64] + popcount(64..pos)
            let base_count = precomputed[0] as usize;
            let remaining_pos = pos - 64;
            let chunk_bytes = &bits[8..16];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        },
        129..=192 => {
            // Precomputed[128] + popcount(128..pos)
            let base_count = precomputed[1] as usize;
            let remaining_pos = pos - 128;
            let chunk_bytes = &bits[16..24];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        },
        193..=256 => {
            // Precomputed[192] + popcount(192..pos)
            let base_count = precomputed[2] as usize;
            let remaining_pos = pos - 192;
            let chunk_bytes = &bits[24..32];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        },
        _ => 0, // pos > 256, shouldn't happen
    }
}

/// Count bits in a range within a byte slice (helper function)
fn count_bits_in_range(bytes: &[u8], start_bit: usize, end_bit: usize) -> usize {
    if start_bit >= end_bit {
        return 0;
    }
    
    let mut count = 0;
    
    for (byte_idx, &byte) in bytes.iter().enumerate() {
        let byte_start_bit = byte_idx * 8;
        let byte_end_bit = byte_start_bit + 8;
        
        if byte_start_bit >= end_bit {
            break;
        }
        
        if byte_end_bit <= start_bit {
            continue;
        }
        
        // Calculate which bits within this byte we need to count
        let count_start = start_bit.saturating_sub(byte_start_bit);
        let count_end = (end_bit - byte_start_bit).min(8);
        
        if count_start == 0 && count_end == 8 {
            // Count all bits in this byte
            count += byte.count_ones() as usize;
        } else {
            // Create mask for the range of bits we want
            let mask_bits = count_end - count_start;
            let mask = if mask_bits >= 8 {
                0xFF
            } else {
                (1u8 << mask_bits) - 1
            };
            let shifted_mask = mask << count_start;
            count += (byte & shifted_mask).count_ones() as usize;
        }
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

    #[test]
    fn test_precomputed_popcounts() {
        let mut bits = [0u8; 32];
        
        // Set some bits across different 64-bit boundaries
        set_bit(&mut bits, 5);   // First 64 bits
        set_bit(&mut bits, 10);  // First 64 bits
        set_bit(&mut bits, 65);  // Second 64 bits
        set_bit(&mut bits, 100); // Second 64 bits
        set_bit(&mut bits, 130); // Third 64 bits
        set_bit(&mut bits, 200); // Fourth 64 bits
        set_bit(&mut bits, 250); // Fourth 64 bits
        
        let precomputed = compute_precomputed_popcounts(&bits);
        
        // Check precomputed values
        assert_eq!(precomputed[0], 2); // bits 0..64: positions 5, 10
        assert_eq!(precomputed[1], 4); // bits 0..128: positions 5, 10, 65, 100
        assert_eq!(precomputed[2], 5); // bits 0..192: + position 130
        assert_eq!(precomputed[3], 7); // bits 0..256: + positions 200, 250
    }

    #[test]
    fn test_count_ones_up_to_precomputed() {
        let mut bits = [0u8; 32];
        
        // Set bits at: 5, 10, 65, 100, 130, 200, 250
        set_bit(&mut bits, 5);
        set_bit(&mut bits, 10);
        set_bit(&mut bits, 65);
        set_bit(&mut bits, 100);
        set_bit(&mut bits, 130);
        set_bit(&mut bits, 200);
        set_bit(&mut bits, 250);
        
        let precomputed = compute_precomputed_popcounts(&bits);
        
        // Test various positions
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 0), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 5), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 6), 1);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 11), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 64), 2); // End of first 64-bit chunk
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 66), 3);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 128), 4); // End of second 64-bit chunk
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 131), 5);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 192), 5); // End of third 64-bit chunk
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 201), 6);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 251), 7);
    }

    #[test]
    fn test_precomputed_vs_simd_consistency() {
        let mut bits = [0u8; 32];
        
        // Set some random bits
        for pos in [1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 128, 129, 200, 254] {
            set_bit(&mut bits, pos);
        }
        
        let precomputed = compute_precomputed_popcounts(&bits);
        
        // Test that precomputed and SIMD give same results
        for pos in 0..=255 {
            let precomputed_result = count_ones_up_to_precomputed(&precomputed, &bits, pos);
            let simd_result = count_ones_up_to_simd(&bits, pos);
            assert_eq!(precomputed_result, simd_result, 
                      "Mismatch at position {}: precomputed={}, SIMD={}", 
                      pos, precomputed_result, simd_result);
        }
    }

    #[test]
    fn test_precomputed_boundary_conditions() {
        let mut bits = [0u8; 32];
        
        // Set bits exactly at 64-bit boundaries
        set_bit(&mut bits, 63);  // End of first 64-bit chunk
        set_bit(&mut bits, 64);  // Start of second 64-bit chunk
        set_bit(&mut bits, 127); // End of second 64-bit chunk
        set_bit(&mut bits, 128); // Start of third 64-bit chunk
        set_bit(&mut bits, 191); // End of third 64-bit chunk
        set_bit(&mut bits, 192); // Start of fourth 64-bit chunk
        
        let precomputed = compute_precomputed_popcounts(&bits);
        
        // Test boundary positions
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 63), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 64), 1);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 65), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 127), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 128), 3);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 129), 4);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 191), 4);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 192), 5);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 193), 6);
    }
}