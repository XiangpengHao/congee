use flatbuffers::Vector;
use crate::congee_flat_struct_generated::congee_flat::{self as Struct, Child, Node, NodeType};

pub struct CongeeFlatStruct<'a> {
    nodes: Vector<'a, flatbuffers::ForwardsUOffset<Node<'a>>>,
}

impl<'a> CongeeFlatStruct<'a> {
    pub fn new(flatbuffer_data: &'a [u8]) -> Self {
        let cfr = Struct::root_as_congee_flat(flatbuffer_data).unwrap();
        println!("nodes len: {}", cfr.nodes().unwrap().len());
        Self {
            nodes: cfr.nodes().unwrap(),
        }
    }

    pub fn debug_print(&self) {
        println!("\n=== CongeeFlatStruct Debug Structure ===");
        for i in 0..self.nodes.len() {
            let node = self.nodes.get(i);
            let node_type = node.node_type();
            
            // Print prefix
            let prefix_str = if let Some(prefix_vec) = node.prefix() {
                let prefix = prefix_vec.bytes();
                format!("{:?}", prefix)
            } else {
                "[]".to_string()
            };
            
            // Print children keys in same line
            let children_str = if let Some(children) = node.children() {
                let mut keys = Vec::new();
                for j in 0..children.len() {
                    let child = children.get(j);
                    let key = child.key();
                    let node_index = child.node_index();
                    if node_index == 0 {
                        keys.push(format!("0x{:02x}", key));
                    } else {
                        keys.push(format!("0x{:02x}→{}", key, node_index));
                    }
                }
                if keys.is_empty() {
                    "none".to_string()
                } else {
                    format!("({}): [{}]", children.len(), keys.join(", "))
                }
            } else {
                "none".to_string()
            };
            
            println!("Node[{}]: type={:?}, prefix={}, children={}", i, node_type, prefix_str, children_str);
        }
        println!("=== End Debug Structure ===\n");
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let mut current_node_index = 0;
        let mut key_pos = 0;

        loop {
            if current_node_index >= self.nodes.len() {
                return false;
            }
            
            let node = self.nodes.get(current_node_index);
            let node_type = node.node_type();
            
            // Get prefix and check it matches
            if let Some(prefix_vec) = node.prefix() {
                let prefix = prefix_vec.bytes();
                if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
                    return false;
                }
                key_pos += prefix.len();
            }

            // If we've consumed the entire key, check if this is a valid termination point
            if key_pos >= key.len() {
                if let Some(children) = node.children() {
                    // Check if any child has node_index 0 (indicating a stored value)
                    for i in 0..children.len() {
                        let child = children.get(i);
                        if child.node_index() == 0 {
                            return true;
                        }
                    }
                }
                return false;
            }

            let next_key_byte = key[key_pos];

            // Find the child with matching key
            if let Some(children) = node.children() {
                let mut child_opt = None;
                
                // Binary search through children
                let mut low = 0;
                let mut high = children.len();
                
                while low < high {
                    let mid_index = low + (high - low) / 2;
                    let mid_child = children.get(mid_index);
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

                        // Handle leaf vs internal nodes
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
            } else {
                return false; // No children
            }
        }
    }
}