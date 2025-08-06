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
    
    // Access frequency tracking
    pub n4_accesses: usize,
    pub n16_accesses: usize,
    pub n48_accesses: usize,
    pub n256_accesses: usize,
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
    
    pub fn total_accesses(&self) -> usize {
        self.n4_accesses + self.n16_accesses + self.n48_accesses + self.n256_accesses
    }
    
    pub fn access_ratios(&self) -> (f64, f64, f64, f64) {
        let n4_total = self.n4_internal_count + self.n4_leaf_count;
        let n16_total = self.n16_internal_count + self.n16_leaf_count;
        let n48_total = self.n48_internal_count + self.n48_leaf_count;
        let n256_total = self.n256_internal_count + self.n256_leaf_count;
        
        let n4_ratio = if n4_total > 0 { self.n4_accesses as f64 / n4_total as f64 } else { 0.0 };
        let n16_ratio = if n16_total > 0 { self.n16_accesses as f64 / n16_total as f64 } else { 0.0 };
        let n48_ratio = if n48_total > 0 { self.n48_accesses as f64 / n48_total as f64 } else { 0.0 };
        let n256_ratio = if n256_total > 0 { self.n256_accesses as f64 / n256_total as f64 } else { 0.0 };
        
        (n4_ratio, n16_ratio, n48_ratio, n256_ratio)
    }
    
    pub fn access_distribution(&self) -> (f64, f64, f64, f64) {
        let total = self.total_accesses() as f64;
        if total == 0.0 {
            (0.0, 0.0, 0.0, 0.0)
        } else {
            (
                self.n4_accesses as f64 / total * 100.0,
                self.n16_accesses as f64 / total * 100.0,
                self.n48_accesses as f64 / total * 100.0,
                self.n256_accesses as f64 / total * 100.0,
            )
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
        
        // Add access frequency statistics if any accesses were recorded
        if self.total_accesses() > 0 {
            writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
            writeln!(f, "│                       Access Frequency Analysis                │")?;
            writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
            writeln!(f, "│ Total Accesses:      {:>8}                                 │", self.total_accesses())?;
            writeln!(f, "│ N4 Accesses:         {:>8} ({:>5.1}%)                       │", 
                     self.n4_accesses, 
                     self.access_distribution().0)?;
            writeln!(f, "│ N16 Accesses:        {:>8} ({:>5.1}%)                       │", 
                     self.n16_accesses, 
                     self.access_distribution().1)?;
            writeln!(f, "│ N48 Accesses:        {:>8} ({:>5.1}%)                       │", 
                     self.n48_accesses, 
                     self.access_distribution().2)?;
            writeln!(f, "│ N256 Accesses:       {:>8} ({:>5.1}%)                       │", 
                     self.n256_accesses, 
                     self.access_distribution().3)?;
            writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
            writeln!(f, "│                    Accesses per Node Ratios                    │")?;
            writeln!(f, "├─────────────────────────────────────────────────────────────────┤")?;
            let (n4_ratio, n16_ratio, n48_ratio, n256_ratio) = self.access_ratios();
            writeln!(f, "│ N4 Ratio:            {:>8.2} accesses/node                   │", n4_ratio)?;
            writeln!(f, "│ N16 Ratio:           {:>8.2} accesses/node                   │", n16_ratio)?;
            writeln!(f, "│ N48 Ratio:           {:>8.2} accesses/node                   │", n48_ratio)?;
            writeln!(f, "│ N256 Ratio:          {:>8.2} accesses/node                   │", n256_ratio)?;
        }
        
        writeln!(f, "╰─────────────────────────────────────────────────────────────────╯")?;
        Ok(())
    }
}

pub struct CongeeCompactV2<'a> {
    data: &'a [u8],
    node_offsets: Vec<usize>, // Precomputed node boundaries for fast access
    access_stats: std::sync::Arc<std::sync::Mutex<AccessStats>>,
}

#[derive(Default, Debug, Clone)]
pub struct AccessStats {
    pub n4_accesses: usize,
    pub n16_accesses: usize,
    pub n48_accesses: usize,
    pub n256_accesses: usize,
    
    // Detailed breakdown by internal/leaf
    pub n4_internal_accesses: usize,
    pub n4_leaf_accesses: usize,
    pub n16_internal_accesses: usize,
    pub n16_leaf_accesses: usize,
    pub n48_internal_accesses: usize,
    pub n48_leaf_accesses: usize,
    pub n256_internal_accesses: usize,
    pub n256_leaf_accesses: usize,
}

impl<'a> CongeeCompactV2<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let mut node_offsets = Vec::new();
        let mut offset = 0;
        
        // Precompute all node boundaries
        while offset < data.len() {
            if offset + 4 > data.len() {
                break;
            }
            node_offsets.push(offset);
            
            
            // Read node header
            let header = unsafe { *(data.as_ptr().add(offset) as *const NodeHeader) }; // Copy to avoid packed field access
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            let node_type = header.node_type;
            
            // Calculate node size based on type
            let children_size = match node_type {
                NodeType::N48_INTERNAL => {
                    256 + children_len * 4 // 256-byte key array + child indices (4 bytes each)
                }
                NodeType::N48_LEAF => {
                    256 // 256-byte presence array only
                }
                NodeType::N256_INTERNAL => {
                    256 * 4 // 256 x 4-byte direct node indices = 1024 bytes
                }
                NodeType::N256_LEAF => {
                    256 // 256-byte direct presence indicators
                }
                NodeType::N4_LEAF | NodeType::N16_LEAF => {
                    children_len // Only keys, 1 byte each
                }
                _ => {
                    children_len * 5 // key + node_index, 5 bytes each
                }
            };
            
            offset += 4 + prefix_len + children_size; // header + prefix + children
        }
        
        // println!("node_offsets: {:?}", node_offsets);
        Self {
            data,
            node_offsets,
            access_stats: std::sync::Arc::new(std::sync::Mutex::new(AccessStats::default())),
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
    fn linear_search_node16(&self, children_start: usize, children_len: usize, target_key: u8, node_type: u8) -> Option<usize> {
        for i in 0..children_len {
            let child_key = match node_type {
                NodeType::N16_LEAF => self.data[children_start + i],
                _ => self.data[children_start + i * 5],
            };
            if child_key == target_key {
                return Some(i);
            }
        }
        None
    }

    #[cfg(target_arch = "x86_64")]
    #[inline]
    fn simd_search_node16(&self, children_start: usize, children_len: usize, target_key: u8, node_type: u8) -> Option<usize> {
        unsafe {
            use std::arch::x86_64::{_mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8};
            
            let target_vec = _mm_set1_epi8(target_key as i8);
            
            match node_type {
                NodeType::N16_LEAF => {
                    // For leaf nodes, keys are contiguous
                    if children_len <= 16 {
                        let mut key_bytes = [0u8; 16];
                        for i in 0..children_len {
                            key_bytes[i] = self.data[children_start + i];
                        }
                        let key_vec = _mm_loadu_si128(key_bytes.as_ptr() as *const _);
                        let cmp = _mm_cmpeq_epi8(key_vec, target_vec);
                        let mask = _mm_movemask_epi8(cmp) as u16;
                        
                        if mask != 0 {
                            let pos = mask.trailing_zeros() as usize;
                            if pos < children_len {
                                return Some(pos);
                            }
                        }
                    }
                }
                _ => {
                    // For internal nodes, extract keys from 5-byte structures
                    let mut key_bytes = [0u8; 16];
                    for i in 0..std::cmp::min(children_len, 16) {
                        key_bytes[i] = self.data[children_start + i * 5];
                    }
                    let key_vec = _mm_loadu_si128(key_bytes.as_ptr() as *const _);
                    let cmp = _mm_cmpeq_epi8(key_vec, target_vec);
                    let mask = _mm_movemask_epi8(cmp) as u16;
                    
                    if mask != 0 {
                        let pos = mask.trailing_zeros() as usize;
                        if pos < children_len {
                            return Some(pos);
                        }
                    }
                }
            }
            
            None
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
            
            let node_offset = self.node_offsets[current_node_index];
            let header = unsafe { *(self.data.as_ptr().add(node_offset) as *const NodeHeader) };
            let node_type = header.node_type;
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            
            // INLINED: Direct prefix access and check
            if prefix_len > 0 {
                let prefix_start = node_offset + 4; // After header
                if key_pos + prefix_len > key.len() {
                    return false;
                }
                let key_slice = &key[key_pos..key_pos + prefix_len];
                let prefix_slice = &self.data[prefix_start..prefix_start + prefix_len];
                if !key_slice.eq(prefix_slice) {
                    return false;
                }
                key_pos += prefix_len;
            }

            // If we've consumed the entire key, check if this is a valid termination point
            // if key_pos >= key.len() {
            //     // INLINED: Direct children access for termination check
            //     let children_start = node_offset + 4 + prefix_len;
                
            //     match node_type {
            //         NodeType::N4_LEAF | NodeType::N16_LEAF | 
            //         NodeType::N48_LEAF | NodeType::N256_LEAF => {
            //             // Leaf nodes: any child means value exists
            //             return children_len > 0;
            //         }
            //         _ => {
            //             // Internal nodes: check if any child has node_index = 0
            //             for child_idx in 0..children_len {
            //                 let child_offset = children_start + child_idx * 5;
            //                 let node_index = u32::from_le_bytes([
            //                     self.data[child_offset + 1],
            //                     self.data[child_offset + 2],
            //                     self.data[child_offset + 3],
            //                     self.data[child_offset + 4],
            //                 ]);
            //                 if node_index == 0 {
            //                     return true;
            //                 }
            //             }
            //             return false;
            //         }
            //     }
            // }

            let next_key_byte = key[key_pos];
            
            // Optimized search based on node type
            let children_start = node_offset + 4 + prefix_len;
            let mut found_child = None;
            
            match node_type {
                NodeType::N4_INTERNAL | NodeType::N4_LEAF => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n4_accesses += 1;
                        match node_type {
                            NodeType::N4_INTERNAL => stats.n4_internal_accesses += 1,
                            NodeType::N4_LEAF => stats.n4_leaf_accesses += 1,
                            _ => {}
                        }
                    }
                    
                    // Linear search for Node4 - eliminates branch mispredictions
                    for i in 0..children_len {
                        let child_key = match node_type {
                            NodeType::N4_LEAF => self.data[children_start + i],
                            _ => self.data[children_start + i * 5],
                        };
                        if child_key == next_key_byte {
                            found_child = Some(i);
                            break;
                        }
                    }
                }
                NodeType::N16_INTERNAL | NodeType::N16_LEAF => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n16_accesses += 1;
                        match node_type {
                            NodeType::N16_INTERNAL => stats.n16_internal_accesses += 1,
                            NodeType::N16_LEAF => stats.n16_leaf_accesses += 1,
                            _ => {}
                        }
                    }
                    
                    // SIMD search for Node16
                    #[cfg(target_arch = "x86_64")]
                    {
                        if is_x86_feature_detected!("sse2") {
                            found_child = self.simd_search_node16(children_start, children_len, next_key_byte, node_type);
                        } else {
                            found_child = self.linear_search_node16(children_start, children_len, next_key_byte, node_type);
                        }
                    }
                    #[cfg(not(target_arch = "x86_64"))]
                    {
                        found_child = self.linear_search_node16(children_start, children_len, next_key_byte, node_type);
                    }
                }
                NodeType::N48_INTERNAL => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n48_accesses += 1;
                        stats.n48_internal_accesses += 1;
                    }
                    
                    // O(1) direct lookup: key_array[key] gives 1-based index into child_indices
                    let key_array_index = next_key_byte as usize;
                    let child_array_index = self.data[children_start + key_array_index];
                    
                    if child_array_index != 0 {
                        found_child = Some(child_array_index as usize); // 1-based, will be handled below
                    }
                }
                NodeType::N48_LEAF => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n48_accesses += 1;
                        stats.n48_leaf_accesses += 1;
                    }
                    
                    // O(1) direct lookup: presence_array[key] == 1 means present
                    let presence = self.data[children_start + next_key_byte as usize];
                    if presence == 1 {
                        // For leaf nodes, we found the value
                        return key_pos + 1 == key.len();
                    }
                }
                NodeType::N256_INTERNAL => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n256_accesses += 1;
                        stats.n256_internal_accesses += 1;
                    }
                    
                    // O(1) direct lookup: direct_array[key] gives node index
                    let direct_index_offset = children_start + next_key_byte as usize * 4;
                    let node_index = u32::from_le_bytes([
                        self.data[direct_index_offset],
                        self.data[direct_index_offset + 1],
                        self.data[direct_index_offset + 2],
                        self.data[direct_index_offset + 3],
                    ]);
                    if node_index != 0 {
                        found_child = Some(next_key_byte as usize); // Use key as dummy index
                    }
                }
                NodeType::N256_LEAF => {
                    // Track access
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n256_accesses += 1;
                        stats.n256_leaf_accesses += 1;
                    }
                    
                    // O(1) direct lookup: presence_array[key] == 1 means present
                    let presence = self.data[children_start + next_key_byte as usize];
                    if presence == 1 {
                        // For leaf nodes, we found the value
                        return key_pos + 1 == key.len();
                    }
                }
                _ => {
                    // Unknown node type - should not happen
                }
            }
            
            match found_child {
                Some(child_idx) => {
                    match node_type {
                        NodeType::N4_LEAF | NodeType::N16_LEAF | 
                        NodeType::N48_LEAF | NodeType::N256_LEAF => {
                            // At leaf node, check if we've consumed entire key
                            return key_pos + 1 == key.len();
                        }
                        NodeType::N48_INTERNAL => {
                            // For N48_INTERNAL: child_idx is 1-based index into child_indices array
                            let child_indices_start = children_start + 256; // After key array
                            let child_index_offset = child_indices_start + (child_idx - 1) * 4; // Convert to 0-based
                            let next_node_index = u32::from_le_bytes([
                                self.data[child_index_offset],
                                self.data[child_index_offset + 1],
                                self.data[child_index_offset + 2],
                                self.data[child_index_offset + 3],
                            ]) as usize;
                            
                            if next_node_index == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_index = next_node_index;
                                key_pos += 1;
                            }
                        }
                        NodeType::N256_INTERNAL => {
                            // For N256_INTERNAL: we already read the node_index directly
                            let direct_index_offset = children_start + next_key_byte as usize * 4;
                            let next_node_index = u32::from_le_bytes([
                                self.data[direct_index_offset],
                                self.data[direct_index_offset + 1],
                                self.data[direct_index_offset + 2],
                                self.data[direct_index_offset + 3],
                            ]) as usize;
                            
                            if next_node_index == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_index = next_node_index;
                                key_pos += 1;
                            }
                        }
                        _ => {
                            // N4/N16 internal nodes: get the node index (original format)
                            let child_offset = children_start + child_idx * 5;
                            let next_node_index = u32::from_le_bytes([
                                self.data[child_offset + 1],
                                self.data[child_offset + 2],
                                self.data[child_offset + 3],
                                self.data[child_offset + 4],
                            ]) as usize;
                            
                            if next_node_index == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_index = next_node_index;
                                key_pos += 1;
                            }
                        }
                    }
                }
                None => return false,
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

    pub fn debug_raw_bytes(&self) {
        println!("Raw bytes ({} total):", self.data.len());
        for (i, byte) in self.data.iter().enumerate() {
            if i % 16 == 0 {
                print!("\n{:04x}: ", i);
            }
            else if i % 4 == 0 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }
        println!();
    }

    pub fn node_count(&self) -> usize {
        self.node_offsets.len()
    }
    
    pub fn get_access_stats(&self) -> AccessStats {
        self.access_stats.lock().map(|stats| stats.clone()).unwrap_or_else(|_| AccessStats::default())
    }
    
    pub fn reset_access_stats(&self) {
        if let Ok(mut stats) = self.access_stats.lock() {
            *stats = AccessStats::default();
        }
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
        
        // Include access statistics
        let access_stats = self.get_access_stats();
        stats.n4_accesses = access_stats.n4_accesses;
        stats.n16_accesses = access_stats.n16_accesses;
        stats.n48_accesses = access_stats.n48_accesses;
        stats.n256_accesses = access_stats.n256_accesses;
        
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
        println!("{}", tree.stats());
        println!("{}", compact.stats());
        compact.debug_print();
        compact.debug_raw_bytes();
        
        
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
    fn test_access_tracking() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert some keys to create a tree structure
        for i in 1..=50 {
            tree.insert(i, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Reset access stats before testing
        compact.reset_access_stats();
        
        // Perform some lookups
        for i in 1usize..=10 {
            let key_bytes = i.to_be_bytes();
            compact.contains(&key_bytes);
        }
        
        let access_stats = compact.get_access_stats();
        let total_accesses = access_stats.n4_accesses + access_stats.n16_accesses + 
                             access_stats.n48_accesses + access_stats.n256_accesses;
        
        println!("Access tracking test:");
        println!("N4 accesses: {}", access_stats.n4_accesses);
        println!("N16 accesses: {}", access_stats.n16_accesses);
        println!("N48 accesses: {}", access_stats.n48_accesses);
        println!("N256 accesses: {}", access_stats.n256_accesses);
        println!("Total accesses: {}", total_accesses);
        
        // Show full stats with access frequency
        let stats = compact.stats();
        println!("{}", stats);
        
        assert!(total_accesses > 0, "Should have recorded some accesses");
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