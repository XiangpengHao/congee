use flatbuffers::Vector;
use crate::congee_flat_generated::congee_flat::{self as Columnar, Child, NodeType};

pub struct CongeeFlat<'a> {
    node_types: Vector<'a, NodeType>,
    prefix_bytes: Vector<'a, u8>,
    prefix_offsets: Vector<'a, u32>,
    children_data: Vector<'a, Child>,
    children_offsets: Vector<'a, u32>,
}

impl<'a> CongeeFlat<'a> {
    pub fn new(flatbuffer_data: &'a [u8]) -> Self {
        let cfr = Columnar::root_as_congee_flat(flatbuffer_data).unwrap();
        println!("node_types len: {} size: {}", cfr.node_types().unwrap().len(), cfr.node_types().unwrap().len() * std::mem::size_of::<u8>());
        println!("prefix_bytes len: {} size: {}", cfr.prefix_bytes().unwrap().len(), cfr.prefix_bytes().unwrap().len() * std::mem::size_of::<u8>());
        println!("prefix_offsets len: {} size: {}", cfr.prefix_offsets().unwrap().len(), cfr.prefix_offsets().unwrap().len() * std::mem::size_of::<u32>());
        println!("children_data len: {} size: {}", cfr.children_data().unwrap().len(), cfr.children_data().unwrap().len() * std::mem::size_of::<Child>());
        println!("children_offsets len: {} size: {}", cfr.children_offsets().unwrap().len(), cfr.children_offsets().unwrap().len() * std::mem::size_of::<u32>());
        println!("total size: {}", cfr.node_types().unwrap().len() * std::mem::size_of::<u8>() + cfr.prefix_bytes().unwrap().len() * std::mem::size_of::<u8>() + cfr.prefix_offsets().unwrap().len() * std::mem::size_of::<u32>() + cfr.children_data().unwrap().len() * std::mem::size_of::<Child>() + cfr.children_offsets().unwrap().len() * std::mem::size_of::<u32>());
        Self {
            node_types: cfr.node_types().unwrap(),
            prefix_bytes: cfr.prefix_bytes().unwrap(),
            prefix_offsets: cfr.prefix_offsets().unwrap(),
            children_data: cfr.children_data().unwrap(),
            children_offsets: cfr.children_offsets().unwrap(),
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let mut current_node_index = 0;
        let mut key_pos = 0;

        loop {
            
            if current_node_index >= self.node_types.len() {
                return false;
            }
            
            let node_type = self.node_types.get(current_node_index);
            
            if current_node_index >= self.prefix_offsets.len() {
                return false;
            }
            
            let prefix_start = if current_node_index == 0 { 0 } else { self.prefix_offsets.get(current_node_index - 1) as usize };
            let prefix_end = self.prefix_offsets.get(current_node_index) as usize;
            let prefix = &self.prefix_bytes.bytes()[prefix_start..prefix_end];

            // Check if the remaining key starts with this node's prefix
            if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
                return false;
            }
            key_pos += prefix.len();

            // If we've consumed the entire key, check if this is a valid termination point
            if key_pos >= key.len() {
                if current_node_index >= self.children_offsets.len() {
                    return false;
                }
                
                let children_start = if current_node_index == 0 { 0 } else { self.children_offsets.get(current_node_index - 1) as usize };
                let children_end = self.children_offsets.get(current_node_index) as usize;
                
                // Check if any child has node_index 0 (indicating a stored value)
                for j in children_start..children_end {
                    let child = self.children_data.get(j);
                    if child.node_index() == 0 {
                        return true;
                    }
                }
                return false;
            }

            let next_key_byte = key[key_pos];

            if current_node_index >= self.children_offsets.len() {
                return false;
            }

            let children_start = if current_node_index == 0 { 0 } else { self.children_offsets.get(current_node_index - 1) as usize };
            let children_end = self.children_offsets.get(current_node_index) as usize;
            
            // binary search
            let mut low = children_start;
            let mut high = children_end;
            let mut child_opt = None;

            while low < high {
                let mid_index = low + (high - low) / 2;
                let mid_child = self.children_data.get(mid_index);
                match mid_child.key().cmp(&next_key_byte) {
                    std::cmp::Ordering::Less => low = mid_index + 1,
                    std::cmp::Ordering::Greater => high = mid_index,
                    std::cmp::Ordering::Equal => {
                        child_opt = Some(mid_child);
                        break;
                    }
                }
            }

            match child_opt {
                Some(child) => {
                    let next_node_index = child.node_index();
                    key_pos += 1;

                    
                    match node_type {
                        NodeType::N4_LEAF | NodeType::N16_LEAF | NodeType::N48_LEAF | NodeType::N256_LEAF => {
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