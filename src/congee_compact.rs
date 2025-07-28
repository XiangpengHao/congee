#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
    N4Leaf = 4,
    N16Leaf = 5,
    N48Leaf = 6,
    N256Leaf = 7,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeType::N4,
            1 => NodeType::N16,
            2 => NodeType::N48,
            3 => NodeType::N256,
            4 => NodeType::N4Leaf,
            5 => NodeType::N16Leaf,
            6 => NodeType::N48Leaf,
            7 => NodeType::N256Leaf,
            _ => panic!("Invalid node type: {}", value),
        }
    }
}

pub struct CongeeCompact<'a> {
    data: &'a [u8],
    num_nodes: u32,
    prefix_data_len: u32,
    children_data_len: u32,
    // Calculated offsets into data
    node_types_offset: usize,
    prefix_offsets_offset: usize,
    children_offsets_offset: usize,
    prefix_data_offset: usize,
    children_keys_offset: usize,
    children_indices_offset: usize,
}

impl<'a> CongeeCompact<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        // Verify magic
        let magic = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(magic, 0x434F4D50414354u64, "Invalid magic number"); // "COMPACT"
        
        // Read header
        let num_nodes = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let prefix_data_len = u32::from_le_bytes(data[12..16].try_into().unwrap());
        let children_data_len = u32::from_le_bytes(data[16..20].try_into().unwrap());
        
        // Calculate section offsets
        let mut offset = 32; // Header size
        
        let node_types_offset = offset;
        offset += num_nodes as usize; // 1 byte per node
        
        let prefix_offsets_offset = offset;
        offset += num_nodes as usize * 4; // 4 bytes per u32
        
        let children_offsets_offset = offset;
        offset += num_nodes as usize * 4; // 4 bytes per u32
        
        let prefix_data_offset = offset;
        offset += prefix_data_len as usize;
        
        let children_keys_offset = offset;
        offset += children_data_len as usize;
        
        let children_indices_offset = offset;
        // children_indices has 4 bytes per children_data_len
        
        println!("CongeeCompact: {} nodes, {} prefix bytes, {} children", 
                 num_nodes, prefix_data_len, children_data_len);
        
        Self {
            data,
            num_nodes,
            prefix_data_len,
            children_data_len,
            node_types_offset,
            prefix_offsets_offset,
            children_offsets_offset,
            prefix_data_offset,
            children_keys_offset,
            children_indices_offset,
        }
    }

    pub fn debug_print(&self) {
        println!("\n=== CongeeCompact Debug Structure ===");
        for i in 0..self.num_nodes as usize {
            let node_type = self.get_node_type(i);
            
            // Print prefix
            let prefix = self.get_prefix(i);
            let prefix_str = format!("{:?}", prefix);
            
            // Print children keys in same line
            let (children_start, children_end) = self.get_children_range(i);
            let children_str = if children_start < children_end {
                let mut keys = Vec::new();
                for j in children_start..children_end {
                    let key = self.get_child_key(j);
                    let node_index = self.get_child_node_index(j);
                    if node_index == 0 {
                        keys.push(format!("0x{:02x}", key));
                    } else {
                        keys.push(format!("0x{:02x}→{}", key, node_index));
                    }
                }
                format!("({}): [{}]", children_end - children_start, keys.join(", "))
            } else {
                "none".to_string()
            };
            
            println!("Node[{}]: type={:?}, prefix={}, children={}", i, node_type, prefix_str, children_str);
        }
        println!("=== End Debug Structure ===\n");
    }

    fn get_node_type(&self, index: usize) -> NodeType {
        let byte = self.data[self.node_types_offset + index];
        NodeType::from(byte)
    }

    fn get_prefix_offset(&self, index: usize) -> u32 {
        let offset = self.prefix_offsets_offset + index * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    fn get_children_offset(&self, index: usize) -> u32 {
        let offset = self.children_offsets_offset + index * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    fn get_prefix(&self, node_index: usize) -> &[u8] {
        let prefix_start = if node_index == 0 { 0 } else { self.get_prefix_offset(node_index - 1) as usize };
        let prefix_end = self.get_prefix_offset(node_index) as usize;
        &self.data[self.prefix_data_offset + prefix_start..self.prefix_data_offset + prefix_end]
    }

    fn get_child_key(&self, child_index: usize) -> u8 {
        self.data[self.children_keys_offset + child_index]
    }

    fn get_child_node_index(&self, child_index: usize) -> u32 {
        let offset = self.children_indices_offset + child_index * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    fn get_children_range(&self, node_index: usize) -> (usize, usize) {
        let children_start = if node_index == 0 { 0 } else { self.get_children_offset(node_index - 1) as usize };
        let children_end = self.get_children_offset(node_index) as usize;
        (children_start, children_end)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let mut current_node_index = 0;
        let mut key_pos = 0;

        loop {
            if current_node_index >= self.num_nodes as usize {
                return false;
            }
            
            let node_type = self.get_node_type(current_node_index);
            
            // Check prefix
            let prefix = self.get_prefix(current_node_index);
            if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
                return false;
            }
            key_pos += prefix.len();

            // If we've consumed the entire key, check if this is a valid termination point
            if key_pos >= key.len() {
                let (children_start, children_end) = self.get_children_range(current_node_index);
                
                // Check if any child has node_index 0 (indicating a stored value)
                for j in children_start..children_end {
                    if self.get_child_node_index(j) == 0 {
                        return true;
                    }
                }
                return false;
            }

            let next_key_byte = key[key_pos];
            let (children_start, children_end) = self.get_children_range(current_node_index);
            
            // Binary search for child
            let mut low = children_start;
            let mut high = children_end;
            let mut found_child = None;

            while low < high {
                let mid_index = low + (high - low) / 2;
                let mid_key = self.get_child_key(mid_index);
                match mid_key.cmp(&next_key_byte) {
                    std::cmp::Ordering::Less => low = mid_index + 1,
                    std::cmp::Ordering::Greater => high = mid_index,
                    std::cmp::Ordering::Equal => {
                        found_child = Some(mid_index);
                        break;
                    }
                }
            }

            match found_child {
                Some(child_index) => {
                    let next_node_index = self.get_child_node_index(child_index);
                    key_pos += 1;

                    match node_type {
                        NodeType::N4Leaf | NodeType::N16Leaf | NodeType::N48Leaf | NodeType::N256Leaf => {
                            // We are at a leaf. For a successful match, we should have consumed
                            // the entire key and the child should point to a value (node_index 0)
                            return key_pos == key.len() && next_node_index == 0;
                        }
                        _ => { // Internal nodes
                            if next_node_index > 0 {
                                current_node_index = next_node_index as usize;
                            } else {
                                // Found a stored value at this exact position
                                return key_pos == key.len();
                            }
                        }
                    }
                }
                None => return false,
            }
        }
    }
}