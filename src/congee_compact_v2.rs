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
    

    pub n4_internal_count: usize,
    pub n16_internal_count: usize,
    pub n48_internal_count: usize,
    pub n256_internal_count: usize,
    pub n4_leaf_count: usize,
    pub n16_leaf_count: usize,
    pub n48_leaf_count: usize,
    pub n256_leaf_count: usize,
    
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
    
    pub fn total_nodes(&self) -> usize {
        self.total_internal_nodes() + self.total_leaf_nodes()
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
    // Note: node_offsets removed - offsets now stored directly in children data
    #[cfg(feature = "access-stats")]
    access_stats: std::sync::Arc<std::sync::Mutex<AccessStats>>,
}

#[cfg(feature = "access-stats")]
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
        // No need to build offset table - offsets are stored directly in children data
        Self {
            data,
            #[cfg(feature = "access-stats")]
            access_stats: std::sync::Arc::new(std::sync::Mutex::new(AccessStats::default())),
        }
    }

    #[inline]
    fn get_node_header(&self, offset: usize) -> &NodeHeader {
        if offset + 4 > self.data.len() {
            panic!("Node offset {} out of bounds", offset);
        }
        
        unsafe { &*(self.data.as_ptr().add(offset) as *const NodeHeader) }
    }

    #[inline]
    fn get_node_prefix(&self, offset: usize) -> &[u8] {
        let header = *self.get_node_header(offset); // Copy to avoid packed field access
        let prefix_start = offset + 4; // After header
        let prefix_len = header.prefix_len as usize;
        &self.data[prefix_start..prefix_start + prefix_len]
    }

    #[inline]
    fn get_children_start_offset(&self, offset: usize) -> usize {
        let header = *self.get_node_header(offset); // Copy to avoid packed field access
        offset + 4 + header.prefix_len as usize // header + prefix
    }

    #[inline(always)]
    fn get_child_at(&self, offset: usize, child_index: usize) -> (u8, Option<u32>) {
        let header = *self.get_node_header(offset); // Copy to avoid packed field access
        let children_start = self.get_children_start_offset(offset);
        
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
                // Internal nodes: [keys][offsets] layout
                let key = self.data[children_start + child_index];
                let offset_start = children_start + children_len;
                let offset_index = offset_start + child_index * 4;
                let node_index = u32::from_le_bytes([
                    self.data[offset_index],
                    self.data[offset_index + 1],
                    self.data[offset_index + 2],
                    self.data[offset_index + 3],
                ]);
                (key, Some(node_index))
            }
        }
    }

    #[inline]
    fn linear_search_node16(&self, children_start: usize, children_len: usize, target_key: u8, node_type: u8) -> Option<usize> {

        match node_type {
            NodeType::N16_LEAF => {
                for i in 0..children_len {
                    if self.data[children_start + i] == target_key {
                        return Some(i);
                    }
                }
            }
            _ => {
                // Internal nodes: keys are stored contiguously
                for i in 0..children_len {
                    let child_key = self.data[children_start + i];
                    if child_key == target_key {
                        return Some(i);
                    }
                }
            }
        }
        None
    }

        // for i in 0..children_len {
        //     let child_key = match node_type {
        //         NodeType::N16_LEAF => self.data[children_start + i],
        //         _ => self.data[children_start + i * 5],
        //     };
        //     if child_key == target_key {
        //         return Some(i);
        //     }
        // }
        // None
    // }

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
                    // For internal nodes, keys are now stored contiguously
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
        let mut current_node_offset = 0; // Start at root node (offset 0)
        let mut key_pos = 0;

        loop {
            if current_node_offset >= self.data.len() {
                return false;
            }
            
            let header = unsafe { *(self.data.as_ptr().add(current_node_offset) as *const NodeHeader) };
            let node_type = header.node_type;
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            
            if prefix_len > 0 {
                let prefix_start = current_node_offset + 4; 
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

            let next_key_byte = key[key_pos];
            
            let children_start = current_node_offset + 4 + prefix_len;
            let mut found_child = None;
            
            match node_type {
                NodeType::N4_INTERNAL | NodeType::N4_LEAF => {
            
                    #[cfg(feature = "access-stats")]
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n4_accesses += 1;
                        match node_type {
                            NodeType::N4_INTERNAL => stats.n4_internal_accesses += 1,
                            NodeType::N4_LEAF => stats.n4_leaf_accesses += 1,
                            _ => {}
                        }
                    }
                    
                    // Linear search for Node4
                    for i in 0..children_len {
                        let child_key = self.data[children_start + i];
                        if child_key == next_key_byte {
                            found_child = Some(i);
                            break;
                        }
                    }
                }
                NodeType::N16_INTERNAL | NodeType::N16_LEAF => {

                    #[cfg(feature = "access-stats")]
                    if let Ok(mut stats) = self.access_stats.lock() {
                        stats.n16_accesses += 1;
                        match node_type {
                            NodeType::N16_INTERNAL => stats.n16_internal_accesses += 1,
                            NodeType::N16_LEAF => stats.n16_leaf_accesses += 1,
                            _ => {}
                        }
                    }
                    found_child = self.linear_search_node16(children_start, children_len, next_key_byte, node_type);
                    // SIMD search for Node16
                    // #[cfg(target_arch = "x86_64")]
                    // {
                    //     if is_x86_feature_detected!("sse2") {
                    //         // println!("SSE2 detected, using SIMD search");
                    //         found_child = self.simd_search_node16(children_start, children_len, next_key_byte, node_type);
                    //     } else {
                    //         // println!("SSE2 not detected, using linear search");
                    //         found_child = self.linear_search_node16(children_start, children_len, next_key_byte, node_type);
                    //     }
                    // }
                    // #[cfg(not(target_arch = "x86_64"))]
                    // {
                    //     println!("Not x86_64, using linear search");
                    //     found_child = self.linear_search_node16(children_start, children_len, next_key_byte, node_type);
                    // }
                }
                NodeType::N48_INTERNAL => {
                    // Track access
                    #[cfg(feature = "access-stats")]
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
                    #[cfg(feature = "access-stats")]
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
                    #[cfg(feature = "access-stats")]
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
                    #[cfg(feature = "access-stats")]
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
                            // For N48_INTERNAL: child_idx is 1-based index into child_offsets array
                            let child_offsets_start = children_start + 256; // After key array
                            let child_offset_location = child_offsets_start + (child_idx - 1) * 4; // Convert to 0-based
                            let next_node_offset = u32::from_le_bytes([
                                self.data[child_offset_location],
                                self.data[child_offset_location + 1],
                                self.data[child_offset_location + 2],
                                self.data[child_offset_location + 3],
                            ]) as usize;
                            
                            if next_node_offset == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_offset = next_node_offset;
                                key_pos += 1;
                            }
                        }
                        NodeType::N256_INTERNAL => {
                            // For N256_INTERNAL: we read the node_offset directly
                            let direct_offset_location = children_start + next_key_byte as usize * 4;
                            let next_node_offset = u32::from_le_bytes([
                                self.data[direct_offset_location],
                                self.data[direct_offset_location + 1],
                                self.data[direct_offset_location + 2],
                                self.data[direct_offset_location + 3],
                            ]) as usize;
                            
                            if next_node_offset == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_offset = next_node_offset;
                                key_pos += 1;
                            }
                        }
                        _ => {
                            // N4/N16 internal nodes: [keys][offsets] layout
                            let header = unsafe { *(self.data.as_ptr().add(current_node_offset) as *const crate::congee_compact_v2::NodeHeader) };
                            let children_len = header.children_len as usize;
                            let offset_start = children_start + children_len;
                            let offset_index = offset_start + child_idx * 4;
                            let next_node_offset = u32::from_le_bytes([
                                self.data[offset_index],
                                self.data[offset_index + 1],
                                self.data[offset_index + 2],
                                self.data[offset_index + 3],
                            ]) as usize;
                            
                            if next_node_offset == 0 {
                                // Found stored value at this position
                                return key_pos + 1 == key.len();
                            } else {
                                current_node_offset = next_node_offset;
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
        println!("Total nodes: {}", self.node_count());
        println!("Total data size: {} bytes", self.data.len());
        
        // Iterate through nodes using offset calculation
        let mut node_index = 0;
        let mut offset = 0;
        
        while offset + 4 <= self.data.len() {
            let header = unsafe { *(self.data.as_ptr().add(offset) as *const NodeHeader) };
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            
            // Get prefix
            let prefix_start = offset + 4;
            let prefix = &self.data[prefix_start..prefix_start + prefix_len];
            
            println!("Node[{}] @ offset {}: type={}, prefix={:?}, children={}",
                     node_index, offset, header.node_type, prefix, children_len);
            
            // Print children - we'll skip detailed child parsing for now in debug
            // This would require recreating the child parsing logic here
            println!("  -> {} children", children_len);
            
            // Calculate node size to advance to next node
            let children_size = match header.node_type {
                NodeType::N48_INTERNAL => 256 + children_len * 4,
                NodeType::N48_LEAF => 256,
                NodeType::N256_INTERNAL => 256 * 4,
                NodeType::N256_LEAF => 256,
                NodeType::N4_LEAF | NodeType::N16_LEAF => children_len,
                _ => children_len * 5, // N4/N16 internal: key + offset pairs
            };
            
            offset += 4 + prefix_len + children_size; // header + prefix + children
            node_index += 1;
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
        // Count nodes by parsing through the data
        let mut count = 0;
        let mut offset = 0;
        
        while offset + 4 <= self.data.len() {
            count += 1;
            
            // Read node header to calculate size
            let header = unsafe { *(self.data.as_ptr().add(offset) as *const NodeHeader) };
            let prefix_len = header.prefix_len as usize;
            let children_len = header.children_len as usize;
            
            // Calculate children size based on node type
            let children_size = match header.node_type {
                NodeType::N48_INTERNAL => 256 + children_len * 4,
                NodeType::N48_LEAF => 256,
                NodeType::N256_INTERNAL => 256 * 4,
                NodeType::N256_LEAF => 256,
                NodeType::N4_LEAF | NodeType::N16_LEAF => children_len,
                _ => children_len * 5, // key + offset pairs
            };
            
            offset += 4 + prefix_len + children_size;
        }
        
        count
    }
    
    /// Returns total memory usage - just the data array (offsets now embedded)
    pub fn total_memory_bytes(&self) -> usize {
        self.data.len() // No separate offset array needed!
    }
    
    #[cfg(feature = "access-stats")]
    pub fn get_access_stats(&self) -> AccessStats {
        self.access_stats.lock().map(|stats| stats.clone()).unwrap_or_else(|_| AccessStats::default())
    }
    
    #[cfg(feature = "access-stats")]
    pub fn reset_access_stats(&self) {
        if let Ok(mut stats) = self.access_stats.lock() {
            *stats = AccessStats::default();
        }
    }

    pub fn stats(&self) -> CompactV2Stats {
        let mut stats = CompactV2Stats::default();
        stats.total_data_size = self.total_memory_bytes(); // Just the data array now
        stats.total_nodes = self.node_count();
        
        // Iterate through nodes by offset, not index
        let mut offset = 0;
        while offset + 4 <= self.data.len() {
            let header = *self.get_node_header(offset);
            let prefix = self.get_node_prefix(offset);
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
                    stats.children_bytes += 256; // 256 byte presence array
                    stats.total_children += children_len;
                }
                NodeType::N256_LEAF => {
                    stats.n256_leaf_count += 1;
                    stats.children_bytes += 256; // 256 byte presence array
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
                    stats.children_bytes += 256 + children_len * 4; // 256 key array + 4 bytes per child offset
                    stats.total_children += children_len;
                }
                NodeType::N256_INTERNAL => {
                    stats.n256_internal_count += 1;
                    stats.children_bytes += 256 * 4; // 256 * 4 bytes direct offsets
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
            
            // Advance to next node
            let children_size = match header.node_type {
                NodeType::N48_INTERNAL => 256 + children_len * 4,
                NodeType::N48_LEAF => 256,
                NodeType::N256_INTERNAL => 256 * 4,
                NodeType::N256_LEAF => 256,
                NodeType::N4_LEAF | NodeType::N16_LEAF => children_len,
                _ => children_len * 5, // key + offset pairs
            };
            offset += 4 + prefix.len() + children_size;
        }
        
        // Include access statistics
        #[cfg(feature = "access-stats")]
        {
            let access_stats = self.get_access_stats();
            stats.n4_accesses = access_stats.n4_accesses;
            stats.n16_accesses = access_stats.n16_accesses;
            stats.n48_accesses = access_stats.n48_accesses;
            stats.n256_accesses = access_stats.n256_accesses;
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
    #[cfg(feature = "access-stats")]
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

    #[test]
    fn test_various_patterns() {
        // Test different key patterns that exercise different node types
        
        // Pattern 1: Powers of 2 (sparse distribution)
        let tree1 = CongeeSet::<usize>::default();
        let guard1 = tree1.pin();
        let powers_of_2: Vec<usize> = (0..20).map(|i| 1 << i).collect();
        
        for &key in &powers_of_2 {
            tree1.insert(key, &guard1).unwrap();
        }
        
        let data1 = tree1.to_compact_v2();
        let compact1 = CongeeCompactV2::new(&data1);
        
        for &key in &powers_of_2 {
            let key_bytes = key.to_be_bytes();
            assert!(compact1.contains(&key_bytes), "Power of 2 key {} should exist", key);
        }
        
        // Pattern 2: Sequential with gaps (0, 2, 4, 6, 8...)
        let tree2 = CongeeSet::<usize>::default();
        let guard2 = tree2.pin();
        let even_keys: Vec<usize> = (0..50).map(|i| i * 2).collect();
        
        for &key in &even_keys {
            tree2.insert(key, &guard2).unwrap();
        }
        
        let data2 = tree2.to_compact_v2();
        let compact2 = CongeeCompactV2::new(&data2);
        
        for &key in &even_keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact2.contains(&key_bytes), "Even key {} should exist", key);
        }
        
        // Verify odd keys don't exist
        for i in 0..25 {
            let odd_key: usize = i * 2 + 1;
            let key_bytes = odd_key.to_be_bytes();
            assert!(!compact2.contains(&key_bytes), "Odd key {} should not exist", odd_key);
        }
    }

    #[test]
    fn test_large_key_space() {
        // Test with keys spread across a large key space to force different node types
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        let large_keys = vec![
            0x0000000000000001usize,
            0x0000000010000000usize,
            0x0000001000000000usize,
            0x0000100000000000usize,
            0x0010000000000000usize,
            0x1000000000000000usize,
            0x8000000000000000usize,
            0xFFFFFFFFFFFFFFFFusize,
        ];
        
        for &key in &large_keys {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all large keys exist
        for &key in &large_keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Large key 0x{:016x} should exist", key);
        }
        
        // Test some intermediate values don't exist
        let non_existent = [
            0x0000000000000002usize,
            0x0000000020000000usize,
            0x7FFFFFFFFFFFFFFFusize,
        ];
        
        for &key in &non_existent {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Non-existent key 0x{:016x} should not exist", key);
        }
    }

    #[test]
    fn test_dense_ranges() {
        // Test dense ranges that will create N48 and N256 nodes
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert a dense range to force larger node types
        for i in 0x1000..0x1100 {  // 256 consecutive values
            tree.insert(i, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all dense range keys exist
        for i in 0x1000usize..0x1100usize {
            let key_bytes = i.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Dense key {} should exist", i);
        }
        
        // Test boundaries don't exist
        let boundary_keys = [0x0FFFusize, 0x1100usize, 0x1101usize];
        for &key in &boundary_keys {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Boundary key {} should not exist", key);
        }
        
        let stats = compact.stats();
        println!("Dense range stats:\n{}", stats);
        
        // Should have created some N48 or N256 nodes
        assert!(stats.n48_internal_count > 0 || stats.n48_leaf_count > 0 || 
                stats.n256_internal_count > 0 || stats.n256_leaf_count > 0, 
                "Dense range should create N48 or N256 nodes");
    }

    #[test]
    fn test_key_boundary_conditions() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Test boundary conditions
        let boundary_keys = [
            0usize,                    // Minimum value
            usize::MAX,                // Maximum value 
            1,                         // Just above minimum
            usize::MAX - 1,            // Just below maximum
            0x7FFFFFFFFFFFFFFFusize,   // Max signed value
            0x8000000000000000usize,   // Min "negative" value
        ];
        
        for &key in &boundary_keys {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all boundary keys exist
        for &key in &boundary_keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Boundary key {} should exist", key);
        }
    }

    #[test]
    fn test_random_keys() {
        use std::collections::HashSet;
        
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Generate pseudo-random keys using a simple LCG
        let mut random_keys = HashSet::new();
        let mut seed = 12345usize;
        
        for _ in 0..500 {
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            random_keys.insert(seed);
        }
        
        let random_vec: Vec<usize> = random_keys.into_iter().collect();
        
        // Insert random keys
        for &key in &random_vec {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all random keys exist
        for &key in &random_vec {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Random key {} should exist", key);
        }
        
        // Test some definitely non-existent keys
        let non_existent = [2usize, 4, 6, 8, 10]; // Very unlikely to collide with LCG
        for &key in &non_existent {
            let key_bytes = key.to_be_bytes();
            // Only assert if we're sure it doesn't exist
            if !random_vec.contains(&key) {
                assert!(!compact.contains(&key_bytes), "Non-random key {} should not exist", key);
            }
        }
        
        let stats = compact.stats();
        println!("Random keys test with {} keys, {} nodes", random_vec.len(), compact.node_count());
        println!("Node distribution: N4:{} N16:{} N48:{} N256:{}", 
                 stats.n4_internal_count + stats.n4_leaf_count,
                 stats.n16_internal_count + stats.n16_leaf_count,
                 stats.n48_internal_count + stats.n48_leaf_count,
                 stats.n256_internal_count + stats.n256_leaf_count);
    }

    #[test]
    fn test_memory_stats_accuracy() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert a known pattern
        for i in 1..=100 {
            tree.insert(i, &guard).unwrap();
        }
        
        let original_stats = tree.stats();
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        let compact_stats = compact.stats();
        
        // Verify that compact version shows significant memory savings
        let efficiency = compact_stats.memory_efficiency_vs_congee_set(original_stats.total_memory_bytes());
        
        println!("Original memory: {} bytes", original_stats.total_memory_bytes());
        println!("Compact memory: {} bytes", compact_stats.total_data_size);
        println!("Memory efficiency: {:.2}x", efficiency);
        
        assert!(efficiency > 1.0, "Compact format should use less memory than original");
        assert!(compact_stats.kv_pairs == 100, "Should have exactly 100 key-value pairs");
    }

    #[test]
    fn test_partial_key_matches() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Insert keys that share prefixes but differ in later bytes
        let keys = [
            0x1234567890ABCDEFusize,
            0x1234567890ABCDEEusize,  // Differs in last byte
            0x1234567890ABCDDDusize,  // Differs in last 2 bytes
            0x1234567890ABBBBBusize,  // Differs in last 4 bytes
        ];
        
        for &key in &keys {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all keys exist
        for &key in &keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key 0x{:016x} should exist", key);
        }
        
        // Test partial matches that should NOT exist
        let partial_keys = [
            0x1234567890ABCDECusize,  // Close to first key
            0x1234567890ABCDEDusize,  // Close to first key  
            0x1234567890ABCDDCusize,  // Close to third key
        ];
        
        for &key in &partial_keys {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Partial key 0x{:016x} should not exist", key);
        }
    }

    #[test]
    fn test_node_type_transitions() {
        // Test that creates different node types by gradually adding keys
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Start with keys that create N4 nodes
        let base = 0x1000usize;
        
        // Add 2 keys (should be N4)
        tree.insert(base, &guard).unwrap();
        tree.insert(base + 1, &guard).unwrap();
        
        // Add more to force N16
        for i in 2..10 {
            tree.insert(base + i, &guard).unwrap();
        }
        
        // Add more to potentially force N48
        for i in 10..30 {
            tree.insert(base + i, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Verify all keys exist
        for i in 0..30 {
            let key = base + i;
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key {} should exist", key);
        }
        
        let stats = compact.stats();
        println!("Node type transitions test:");
        println!("N4: {} internal, {} leaf", stats.n4_internal_count, stats.n4_leaf_count);
        println!("N16: {} internal, {} leaf", stats.n16_internal_count, stats.n16_leaf_count);
        println!("N48: {} internal, {} leaf", stats.n48_internal_count, stats.n48_leaf_count);
        
        // Should have created various node types
        assert!(stats.total_nodes() > 0, "Should have some nodes");
    }

    #[test]
    fn test_key_length_edge_cases() {
        // Test keys that exercise different path lengths in the tree
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Keys with same prefix but different suffixes to test path compression
        let keys = [
            0x1111111111111111usize,  // All same nibbles
            0x1111111111111112usize,  // Differs in last nibble
            0x1111111111111121usize,  // Differs in 2nd last nibble
            0x1111111111112111usize,  // Differs in 3rd last nibble
            0x1111111111211111usize,  // And so on...
            0x1111111112111111usize,
            0x1111111211111111usize,
            0x1111112111111111usize,
        ];
        
        for &key in &keys {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // All keys should exist
        for &key in &keys {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key 0x{:016x} should exist", key);
        }
        
        // Similar keys that don't exist
        let non_existent = [
            0x1111111111111113usize,
            0x1111111111111131usize,
            0x1111111111111311usize,
        ];
        
        for &key in &non_existent {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key 0x{:016x} should not exist", key);
        }
        
        compact.debug_print();
    }

    #[test]
    fn test_sibling_nodes() {
        // Test case that creates sibling nodes at different levels
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Create a pattern that will create multiple branches
        let base_keys = [
            0x1000000000000000usize,
            0x2000000000000000usize,
            0x3000000000000000usize,
            0x4000000000000000usize,
        ];
        
        // For each base, add some variations
        for &base in &base_keys {
            for i in 0..5 {
                tree.insert(base + i, &guard).unwrap();
            }
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all combinations exist
        for &base in &base_keys {
            for i in 0..5 {
                let key = base + i;
                let key_bytes = key.to_be_bytes();
                assert!(compact.contains(&key_bytes), "Key 0x{:016x} should exist", key);
            }
        }
        
        let stats = compact.stats();
        println!("Sibling nodes test stats:\n{}", stats);
    }

    #[test]
    fn test_minimal_differences() {
        // Test keys that differ by minimal amounts
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        let base = 0x5555555555555555usize;
        let variations = [
            base,                    // Original
            base ^ 0x0000000000000001, // Flip LSB
            base ^ 0x0000000000000002, // Flip bit 1
            base ^ 0x0000000000000004, // Flip bit 2
            base ^ 0x0000000000000008, // Flip bit 3
            base ^ 0x0000000000000010, // Flip bit 4
            base ^ 0x0000000000000080, // Flip bit 7
            base ^ 0x0000000000008000, // Flip bit 15
            base ^ 0x8000000000000000, // Flip MSB
        ];
        
        for &key in &variations {
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // All variations should exist
        for &key in &variations {
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Variation 0x{:016x} should exist", key);
        }
        
        // Test some other bit patterns don't exist
        let non_patterns = [
            base ^ 0x0000000000000003, // Flip bits 0 and 1
            base ^ 0x000000000000000C, // Flip bits 2 and 3
            base ^ 0x00000000000000F0, // Flip bits 4-7
        ];
        
        for &key in &non_patterns {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Non-pattern 0x{:016x} should not exist", key);
        }
    }

    #[test]
    fn test_stress_deep_tree() {
        // Create a deep tree by using keys with long common prefixes
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Base prefix - same for first 7 bytes
        let prefix = 0x1122334455667700usize;
        
        // Add keys that differ only in the last byte
        for i in 0u8..=255 {
            let key = prefix | (i as usize);
            tree.insert(key, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all 256 keys exist
        for i in 0u8..=255 {
            let key = prefix | (i as usize);
            let key_bytes = key.to_be_bytes();
            assert!(compact.contains(&key_bytes), "Key with suffix {} should exist", i);
        }
        
        // Test some keys with different prefixes don't exist
        let different_prefixes = [
            0x1122334455667800usize,  // Different in last byte (0x00 -> 0x00 but with 0x78 -> 0x80) 
            0x1122334455668000usize,  // Different in third-to-last byte
            0x1122334455660000usize,  // Different in fourth-to-last byte
        ];
        
        for &key in &different_prefixes {
            let key_bytes = key.to_be_bytes();
            assert!(!compact.contains(&key_bytes), "Key with different prefix 0x{:016x} should not exist", key);
        }
        
        let stats = compact.stats();
        println!("Deep tree test stats:\n{}", stats);
        
        // Should definitely create N256 nodes for this pattern
        assert!(stats.n256_leaf_count > 0 || stats.n256_internal_count > 0, 
               "Deep tree with 256 keys should create N256 nodes");
    }

    #[test]
    fn test_alternating_patterns() {
        // Test alternating bit patterns that might stress the tree structure
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        let patterns = [
            0xAAAAAAAAAAAAAAAAusize, // 10101010... pattern
            0x5555555555555555usize, // 01010101... pattern
            0xCCCCCCCCCCCCCCCCusize, // 11001100... pattern
            0x3333333333333333usize, // 00110011... pattern
            0xF0F0F0F0F0F0F0F0usize, // 11110000... pattern
            0x0F0F0F0F0F0F0F0Fusize, // 00001111... pattern
        ];
        
        // Add each pattern and some variations
        for &pattern in &patterns {
            tree.insert(pattern, &guard).unwrap();
            // Add some nearby values
            tree.insert(pattern.wrapping_add(1), &guard).unwrap();
            tree.insert(pattern.wrapping_add(2), &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // Test all patterns and variations exist
        for &pattern in &patterns {
            for offset in 0..=2 {
                let key = pattern.wrapping_add(offset);
                let key_bytes = key.to_be_bytes();
                assert!(compact.contains(&key_bytes), "Pattern 0x{:016x} + {} should exist", pattern, offset);
            }
        }
    }

    #[test]
    fn test_debug_functions() {
        // Test the debug and analysis functions
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Create a varied dataset
        for i in 1..=50 {
            tree.insert(i, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        // These should not panic
        compact.debug_print();
        compact.debug_raw_bytes();
        
        let stats = compact.stats();
        println!("Debug test stats:\n{}", stats);
        
        // Basic sanity checks on stats
        assert!(stats.total_data_size > 0);
        assert!(stats.total_nodes > 0);
        assert!(stats.kv_pairs == 50);
        assert!(stats.header_bytes > 0);
        assert!(stats.bytes_per_key() > 0.0);
        assert!(stats.memory_efficiency_vs_original() > 0.0);
    }

    #[test]
    #[cfg(feature = "access-stats")]
    fn test_comprehensive_access_tracking() {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Create a tree with different node types
        for i in 1..=200 {
            tree.insert(i, &guard).unwrap();
        }
        
        let data = tree.to_compact_v2();
        let compact = CongeeCompactV2::new(&data);
        
        compact.reset_access_stats();
        
        // Perform various lookups
        for i in 1..=200 {
            let key_bytes = i.to_be_bytes();
            let _ = compact.contains(&key_bytes);
        }
        
        // Look for some non-existent keys
        for i in 201..=220 {
            let key_bytes = i.to_be_bytes();
            let _ = compact.contains(&key_bytes);
        }
        
        let access_stats = compact.get_access_stats();
        let stats = compact.stats();
        
        println!("Comprehensive access tracking:");
        println!("Total accesses: {}", access_stats.n4_accesses + access_stats.n16_accesses + 
                                       access_stats.n48_accesses + access_stats.n256_accesses);
        println!("{}", stats);
        
        // Should have recorded some accesses
        let total_accesses = access_stats.n4_accesses + access_stats.n16_accesses + 
                            access_stats.n48_accesses + access_stats.n256_accesses;
        assert!(total_accesses > 0, "Should have recorded node accesses");
        
        // Access ratios should be computed
        let (n4_ratio, n16_ratio, n48_ratio, n256_ratio) = stats.access_ratios();
        assert!(n4_ratio >= 0.0 && n16_ratio >= 0.0 && n48_ratio >= 0.0 && n256_ratio >= 0.0);
        
        // Access distribution should sum to ~100%
        let (n4_dist, n16_dist, n48_dist, n256_dist) = stats.access_distribution();
        let total_dist = n4_dist + n16_dist + n48_dist + n256_dist;
        assert!((total_dist - 100.0).abs() < 0.1, "Access distribution should sum to ~100%");
    }
}