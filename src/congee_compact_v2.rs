pub struct NodeType(pub u8);

#[allow(non_upper_case_globals)]
impl NodeType {
    pub const N4_INTERNAL: u8 = 0;
    pub const N16_INTERNAL: u8 = 1;
    pub const N48_INTERNAL: u8 = 2;
    pub const N256_INTERNAL: u8 = 3;
    pub const N4_LEAF: u8 = 4;
    pub const N16_LEAF: u8 = 5;
    pub const N48_LEAF: u8 = 6;
    pub const N256_LEAF: u8 = 7;
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct NodeHeader {
    node_type: u8,
    prefix_len: u8,
    children_len: u16,
}

#[derive(Default, Debug, Clone)]
pub struct CompactV2Stats {
    pub total_data_size: usize,
    pub total_nodes: usize,
    pub header_bytes: usize,
    pub prefix_bytes: usize,
    pub children_bytes: usize,
    pub total_children: usize,
    pub kv_pairs: usize,
    
    // Node type counts
    pub n4_internal_count: usize,
    pub n16_internal_count: usize,
    pub n48_internal_count: usize,
    pub n256_internal_count: usize,
    pub n4_leaf_count: usize,
    pub n16_leaf_count: usize,
    pub n48_leaf_count: usize,
    pub n256_leaf_count: usize,
}

impl CompactV2Stats {
    pub fn total_internal_nodes(&self) -> usize {
        self.n4_internal_count + self.n16_internal_count + 
        self.n48_internal_count + self.n256_internal_count
    }
    
    pub fn total_leaf_nodes(&self) -> usize {
        self.n4_leaf_count + self.n16_leaf_count + 
        self.n48_leaf_count + self.n256_leaf_count
    }
    
    pub fn bytes_per_key(&self) -> f64 {
        if self.kv_pairs == 0 {
            0.0
        } else {
            self.total_data_size as f64 / self.kv_pairs as f64
        }
    }
    
    pub fn memory_efficiency_vs_original(&self) -> f64 {
        // Rough estimate based on typical ART node sizes
        // For accurate comparison, use memory_efficiency_vs_congee_set() with actual stats
        let estimated_original = (self.n4_internal_count + self.n4_leaf_count) * 56 +
                                 (self.n16_internal_count + self.n16_leaf_count) * 160 +
                                 (self.n48_internal_count + self.n48_leaf_count) * 664 +
                                 (self.n256_internal_count + self.n256_leaf_count) * 2096;
        
        if estimated_original == 0 {
            1.0
        } else {
            estimated_original as f64 / self.total_data_size as f64
        }
    }
    
    pub fn memory_efficiency_vs_congee_set(&self, original_memory_bytes: usize) -> f64 {
        if original_memory_bytes == 0 {
            1.0
        } else {
            original_memory_bytes as f64 / self.total_data_size as f64
        }
    }
}

impl std::fmt::Display for CompactV2Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "╭─────────────────────────────────────────────────────────────────╮")?;
        writeln!(f, "│                    CongeeCompactV2 Statistics                   │")?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│ Total Data Size:     {:>8} bytes                            │", self.total_data_size)?;
        writeln!(f, "│ Total Nodes:         {:>8}                                   │", self.total_nodes)?;
        writeln!(f, "│ KV Pairs:            {:>8}                                   │", self.kv_pairs)?;
        writeln!(f, "│ Bytes per Key:       {:>8.2}                                 │", self.bytes_per_key())?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│                        Memory Breakdown                        │")?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│ Headers:             {:>8} bytes ({:>5.1}%)                  │", 
                 self.header_bytes, 
                 self.header_bytes as f64 / self.total_data_size as f64 * 100.0)?;
        writeln!(f, "│ Prefixes:            {:>8} bytes ({:>5.1}%)                  │", 
                 self.prefix_bytes, 
                 self.prefix_bytes as f64 / self.total_data_size as f64 * 100.0)?;
        writeln!(f, "│ Children:            {:>8} bytes ({:>5.1}%)                  │", 
                 self.children_bytes, 
                 self.children_bytes as f64 / self.total_data_size as f64 * 100.0)?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│                        Node Type Counts                        │")?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│ Internal Nodes:      {:>8} (N4:{} N16:{} N48:{} N256:{})       │", 
                 self.total_internal_nodes(),
                 self.n4_internal_count,
                 self.n16_internal_count,
                 self.n48_internal_count,
                 self.n256_internal_count)?;
        writeln!(f, "│ Leaf Nodes:          {:>8} (N4:{} N16:{} N48:{} N256:{})       │", 
                 self.total_leaf_nodes(),
                 self.n4_leaf_count,
                 self.n16_leaf_count,
                 self.n48_leaf_count,
                 self.n256_leaf_count)?;
        writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│ Memory Efficiency:   {:>6.1}x vs original format              │", self.memory_efficiency_vs_original())?;
        writeln!(f, "╰─────────────────────────────────────────────────────────────────╯")?;
        Ok(())
    }
}

pub struct CongeeCompactV2<'a> {
    data: &'a [u8],
    node_offsets: Vec<usize>, // Precomputed node boundaries for fast access
}

impl<'a> CongeeCompactV2<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let mut node_offsets = Vec::new();
        let mut offset = 0;
        
        // Precompute all node boundaries
        while offset < data.len() {
            node_offsets.push(offset);
            
            if offset + 4 > data.len() {
                break;
            }
            
            // Read node header
            let header = unsafe { *(data.as_ptr().add(offset) as *const NodeHeader) }; // Copy to avoid packed field access
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            let node_type = header.node_type;
            
            // Calculate node size based on type
            let children_size = match node_type {
                NodeType::N4_LEAF | NodeType::N16_LEAF | 
                NodeType::N48_LEAF | NodeType::N256_LEAF => {
                    children_len // Only keys, 1 byte each
                }
                _ => {
                    children_len * 5 // key + node_index, 5 bytes each
                }
            };
            
            offset += 4 + prefix_len + children_size; // header + prefix + children
        }
        
        Self {
            data,
            node_offsets,
        }
    }

    #[inline]
    fn get_node_header(&self, node_index: usize) -> &NodeHeader {
        if node_index >= self.node_offsets.len() {
            panic!("Node index {} out of bounds", node_index);
        }
        
        let offset = self.node_offsets[node_index];
        unsafe { &*(self.data.as_ptr().add(offset) as *const NodeHeader) }
    }

    #[inline]
    fn get_node_prefix(&self, node_index: usize) -> &[u8] {
        let offset = self.node_offsets[node_index];
        let header = *self.get_node_header(node_index); // Copy to avoid packed field access
        let prefix_start = offset + 4; // After header
        let prefix_len = header.prefix_len as usize;
        &self.data[prefix_start..prefix_start + prefix_len]
    }

    #[inline]
    fn get_children_start_offset(&self, node_index: usize) -> usize {
        let offset = self.node_offsets[node_index];
        let header = *self.get_node_header(node_index); // Copy to avoid packed field access
        offset + 4 + header.prefix_len as usize // header + prefix
    }

    #[inline(always)]
    fn get_child_at(&self, node_index: usize, child_index: usize) -> (u8, Option<u32>) {
        let header = *self.get_node_header(node_index); // Copy to avoid packed field access
        let children_start = self.get_children_start_offset(node_index);
        
        let children_len = header.children_len as usize;
        if child_index >= children_len {
            panic!("Child index {} out of bounds for node with {} children", 
                   child_index, children_len);
        }
        
        match header.node_type {
            NodeType::N4_LEAF | NodeType::N16_LEAF | 
            NodeType::N48_LEAF | NodeType::N256_LEAF => {
                // Leaf nodes: only store keys
                let key = self.data[children_start + child_index];
                (key, None)
            }
            _ => {
                // Internal nodes: store key + node_index
                let child_offset = children_start + child_index * 5;
                let key = self.data[child_offset];
                let node_index = u32::from_le_bytes([
                    self.data[child_offset + 1],
                    self.data[child_offset + 2],
                    self.data[child_offset + 3],
                    self.data[child_offset + 4],
                ]);
                (key, Some(node_index))
            }
        }
    }

    #[inline]
    fn binary_search_child(&self, node_index: usize, target_key: u8) -> Option<(u8, Option<u32>)> {
        let header = *self.get_node_header(node_index); // Copy to avoid packed field access
        let children_len = header.children_len as usize;
        
        if children_len == 0 {
            return None;
        }
        
        let mut low = 0;
        let mut high = children_len;
        
        while low < high {
            let mid = low + (high - low) / 2;
            let (mid_key, mid_node_index) = self.get_child_at(node_index, mid);
            
            match mid_key.cmp(&target_key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Some((mid_key, mid_node_index)),
            }
        }
        
        None
    }


    pub fn contains(&self, key: &[u8]) -> bool {
        let mut current_node_index = 0;
        let mut key_pos = 0;

        loop {
            if current_node_index >= self.node_offsets.len() {
                return false;
            }
            
            let header = *self.get_node_header(current_node_index);
            let prefix = self.get_node_prefix(current_node_index);
            
            // Check prefix match
            if !prefix.is_empty() {
                if key_pos + prefix.len() > key.len() {
                    return false;
                }
                if &key[key_pos..key_pos + prefix.len()] != prefix {
                    return false;
                }
                key_pos += prefix.len();
            }

            // If we've consumed the entire key, check if this is a valid termination point
            if key_pos >= key.len() {
                match header.node_type {
                    NodeType::N4_LEAF | NodeType::N16_LEAF | 
                    NodeType::N48_LEAF | NodeType::N256_LEAF => {
                        // Leaf nodes: any child means value exists
                        return header.children_len > 0;
                    }
                    _ => {
                        // Internal nodes: check if any child has node_index = 0
                        for child_idx in 0..header.children_len {
                            let (_, node_index_opt) = self.get_child_at(current_node_index, child_idx as usize);
                            if let Some(node_index) = node_index_opt {
                                if node_index == 0 {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                }
            }

            let next_key_byte = key[key_pos];
            
            // Search for the next key byte in children
            if let Some((_, node_index_opt)) = self.binary_search_child(current_node_index, next_key_byte) {
                match node_index_opt {
                    Some(next_node_index) => {
                        if next_node_index == 0 {
                            // Found stored value at this position
                            return key_pos + 1 == key.len();
                        } else {
                            current_node_index = next_node_index as usize;
                            key_pos += 1;
                        }
                    }
                    None => {
                        // At leaf node, check if we've consumed entire key
                        return key_pos + 1 == key.len();
                    }
                }
            } else {
                return false;
            }
        }
    }

    pub fn debug_print(&self) {
        println!("\n=== CongeeCompactV2 Debug Structure ===");
        println!("Total nodes: {}", self.node_offsets.len());
        println!("Total data size: {} bytes", self.data.len());
        
        for (i, &offset) in self.node_offsets.iter().enumerate() {
            let header = *self.get_node_header(i); // Copy to avoid packed field access
            let prefix = self.get_node_prefix(i);
            
            let children_len = header.children_len;
            println!("Node[{}] @ offset {}: type={}, prefix={:?}, children={}",
                     i, offset, header.node_type, prefix, children_len);
            
            // Print children
            for child_idx in 0..children_len {
                let (key, node_index_opt) = self.get_child_at(i, child_idx as usize);
                match node_index_opt {
                    Some(node_index) => println!("  Child[{}]: key=0x{:02x} -> node {}", 
                                                  child_idx, key, node_index),
                    None => println!("  Child[{}]: key=0x{:02x} -> VALUE", child_idx, key),
                }
            }
        }
        
        println!("=== End Debug Structure ===\n");
    }

    pub fn node_count(&self) -> usize {
        self.node_offsets.len()
    }

    pub fn stats(&self) -> CompactV2Stats {
        let mut stats = CompactV2Stats::default();
        stats.total_data_size = self.data.len();
        stats.total_nodes = self.node_offsets.len();
        
        for i in 0..self.node_offsets.len() {
            let header = *self.get_node_header(i);
            let prefix = self.get_node_prefix(i);
            let children_len = header.children_len as usize;
            
            // Update header bytes
            stats.header_bytes += 4; // NodeHeader is 4 bytes
            
            // Update prefix bytes
            stats.prefix_bytes += prefix.len();
            
            // Update children bytes and counts by node type
            match header.node_type {
                NodeType::N4_LEAF => {
                    stats.n4_leaf_count += 1;
                    stats.children_bytes += children_len; // 1 byte per child
                    stats.total_children += children_len;
                }
                NodeType::N16_LEAF => {
                    stats.n16_leaf_count += 1;
                    stats.children_bytes += children_len; // 1 byte per child
                    stats.total_children += children_len;
                }
                NodeType::N48_LEAF => {
                    stats.n48_leaf_count += 1;
                    stats.children_bytes += children_len; // 1 byte per child
                    stats.total_children += children_len;
                }
                NodeType::N256_LEAF => {
                    stats.n256_leaf_count += 1;
                    stats.children_bytes += children_len; // 1 byte per child
                    stats.total_children += children_len;
                }
                NodeType::N4_INTERNAL => {
                    stats.n4_internal_count += 1;
                    stats.children_bytes += children_len * 5; // 5 bytes per child
                    stats.total_children += children_len;
                }
                NodeType::N16_INTERNAL => {
                    stats.n16_internal_count += 1;
                    stats.children_bytes += children_len * 5; // 5 bytes per child
                    stats.total_children += children_len;
                }
                NodeType::N48_INTERNAL => {
                    stats.n48_internal_count += 1;
                    stats.children_bytes += children_len * 5; // 5 bytes per child
                    stats.total_children += children_len;
                }
                NodeType::N256_INTERNAL => {
                    stats.n256_internal_count += 1;
                    stats.children_bytes += children_len * 5; // 5 bytes per child
                    stats.total_children += children_len;
                }
                _ => {
                    // Unknown node type
                }
            }
            
            // Count leaf children as values
            if matches!(header.node_type, 
                NodeType::N4_LEAF | NodeType::N16_LEAF | 
                NodeType::N48_LEAF | NodeType::N256_LEAF) {
                stats.kv_pairs += children_len;
            }
        }
        
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CongeeSet;

    #[test]
    fn test_sequential_keys() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert sequential keys
        for i in 1..=100 {
            tree.insert(i, &guard).unwrap();
        }
        
        // Serialize to compact v2 format
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all keys exist
        for i in 1usize..=100 {
            let key_bytes = i.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key {} should exist", i);
        }
        
        // Test non-existent keys
        for i in [0usize, 101, 1000] {
            let key_bytes = i.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key {} should not exist", i);
        }
        
        println!("Sequential keys test passed with {} nodes", compact.node_count());
    }

    #[test]
    fn test_sparse_keys() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert sparse keys to create interesting tree structure
        let keys = [1, 100, 1000, 10000, 50000, 100000];
        
        for &key in &keys {
            tree.insert(key, &guard).unwrap();
        }
        
        // Serialize to compact v2 format
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all inserted keys exist
        for &key in &keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key {} should exist", key);
        }
        
        // Test some non-existent keys
        for key in [0usize, 50, 500, 5000, 25000, 75000, 200000] {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key {} should not exist", key);
        }
        
        println!("Sparse keys test passed with {} keys, {} nodes", 
                 keys.len(), compact.node_count());
    }

    #[test]
    fn test_empty_tree() {
        let tree = CongeeSet::<usize>::default();
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test that no keys exist in empty tree
        for i in 1usize..=10 {
            let key_bytes = i.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Empty tree should not contain key {}", i);
        }
        
        assert_eq!(compact.node_count(), 0, "Empty tree should have 0 nodes");
    }

    #[test]
    fn test_single_key() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        tree.insert(42, &guard).unwrap();
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test the single key exists
        let key_bytes = 42usize.to_be_bytes();
        assert!(compact.contains(&key_bytes), "Key 42 should exist");
        
        // Test other keys don't exist
        for i in [1usize, 41, 43, 100] {
            let key_bytes = i.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key {} should not exist", i);
        }
        
        println!("Single key test passed with {} nodes", compact.node_count());
    }

    #[test]
    fn test_prefix_keys() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert keys that will create nodes with common prefixes
        let keys = [
            0x1234567800000001u64,
            0x1234567800000002u64,
            0x1234567800000003u64,
            0x1234567800000010u64,
            0x1234567800000020u64,
        ];
        
        for &key in &keys {
            tree.insert(key as usize, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all keys exist
        for &key in &keys {
            let key_bytes = (key as usize).to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key 0x{:016x} should exist", key);
        }
        
        // Test similar but non-existent keys
        let non_keys = [
            0x1234567800000000u64,
            0x1234567800000004u64,
            0x1234567800000011u64,
        ];
        
        for &key in &non_keys {
            let key_bytes = (key as usize).to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key 0x{:016x} should not exist", key);
        }
        
        println!("Prefix keys test passed with {} nodes", compact.node_count());
    }
}