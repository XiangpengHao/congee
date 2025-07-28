// src/main.rs part 1 of 4: imports
mod congee_flat_struct_generated;
mod congee_flat_columnar_generated;

use congee_flat_columnar_generated::congee_flat_columnar as Columar;

use std::fs::read;
use std::time::Instant;
use ahash::AHashMap;

// use crossbeam_epoch::Pointable;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use congee_flat_struct_generated::congee_flat::{CongeeFlatArgs, finish_congee_flat_buffer, root_as_congee_flat, Child};
use congee::{CongeeRaw, CongeeSet, DefaultAllocator, CongeeFlatColumnar};

use crate::congee_flat_struct_generated::congee_flat::{CongeeFlat, Node, NodeArgs, NodeType};


pub fn make_congee_flat(bldr: &mut FlatBufferBuilder, dest: &mut Vec<u8>) {
    dest.clear();
    bldr.reset();

    let mut children= Vec::new();

    children.push(Child::new(1,0));
    children.push(Child::new(2,0));
    children.push(Child::new(3,0));
    children.push(Child::new(4,0));
    children.push(Child::new(5,0));


    let children_wip = bldr.create_vector(&children);
    let prefix: Vec<u8> = vec![1,2,3,4,5,6];
    let prefix_wip = bldr.create_vector(&prefix);

    let node_args = NodeArgs { 
        node_type: NodeType::N16_LEAF, 
        prefix: Some(prefix_wip), 
        children: Some(children_wip) 
    };

    let node1 = Node::create(bldr, &node_args);

    let mut children= Vec::new();
    children.push(Child::new(1,0));
    let children_wip = bldr.create_vector(&children);
    let prefix: Vec<u8> = vec![1,2,3,4,5,6];
    let prefix_wip = bldr.create_vector(&prefix);

    let node_args = NodeArgs { 
        node_type: NodeType::N4_LEAF, 
        prefix: Some(prefix_wip), 
        children: Some(children_wip) 
    };

    let node2 = Node::create(bldr, &node_args);

    let root_prefix: Vec<u8> = vec![];
    let root_prefix_wip = bldr.create_vector(&root_prefix);


    let root_children = vec![Child::new(1, 1), Child::new(2, 2)];
    let root_children_wip = bldr.create_vector(&root_children);
    let root_node_args = NodeArgs{
        node_type: NodeType::N4_INTERNAL,
        prefix: Some(root_prefix_wip),
        children: Some(root_children_wip) 
    };

    let root_node = Node::create(bldr, &root_node_args);
    // let nodes = [root_node, node];

    let nodes_wip = bldr.create_vector(&[root_node, node1, node2]);
    
    let congee_flat_args = CongeeFlatArgs {
        nodes: Some(nodes_wip)
    };

    let congee_flat_offset = CongeeFlat::create(bldr, &congee_flat_args);

    finish_congee_flat_buffer(bldr, congee_flat_offset);

    // Copy the serialized FlatBuffers data to our own byte buffer.
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

fn find_key(buf: &[u8], key: &[u8]) -> bool {
    let cfr = root_as_congee_flat(buf).unwrap();
    let nodes = cfr.nodes().unwrap();

    let mut current_node_index = 0; // Convention: root is at index 0
    let mut key_pos = 0;

    loop {
        let node = nodes.get(current_node_index);

        // 1. Check prefix
        let prefix = node.prefix().unwrap();
        if !key[key_pos..].starts_with(prefix.bytes()) {
            return false;
        }
        key_pos += prefix.len();

        // 2. Are we done?
        if key_pos >= key.len() {
            // Key exhausted. This implies the key is a prefix of another key, but doesn't exist itself.
            return false;
        }

        // 3. Find next child
        let next_key_byte = key[key_pos];
        
        let children = node.children().unwrap();
        
        // OPTIMIZATION: Manually implement binary search on the FlatBuffers Vector.
        // This is a major performance win, as children are sorted by key.
        let mut low = 0;
        let mut high = children.len();
        let mut child_opt = None;
        while low < high {
            let mid = low + (high - low) / 2;
            let child = children.get(mid);
            match child.key().cmp(&next_key_byte) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    child_opt = Some(child);
                    break;
                }
            }
        }

        match child_opt {
            Some(child) => {
                let next_node_index = child.node_index();
                key_pos += 1;

                match node.node_type() {
                    NodeType::N4_LEAF | NodeType::N16_LEAF => {
                        // We are at a leaf. The key should be fully consumed now.
                        // The child's node_index should also be 0.
                        return key_pos == key.len() && next_node_index == 0;
                    }
                    NodeType::N4_INTERNAL => {
                        if next_node_index > 0 {
                            current_node_index = next_node_index as usize;
                        } else {
                            // Internal node's child must point to another node.
                            return false;
                        }
                    }
                    _ => { 
                        if next_node_index > 0 {
                            current_node_index = next_node_index as usize;
                        } else {
                            return false;
                        }
                    }
                }
            }
            None => {
                // No child matches the next key byte.
                return false;
            }
        }
    }
}


fn read_congee_flat(buf: &[u8]) {
    // let key: [u8; 8] = [1,1,2,3,4,5,6,1];
    // let cfr = root_as_congee_flat(buf).unwrap();
    
    // let nodes = cfr.nodes().unwrap();

    // for (i, node) in nodes.iter().enumerate() {
    //     print!("Node: {i}\n");
    //     print!("node type: {:?}\n", NodeType::variant_name(node.node_type()));
    //     print!("Prefix: {:?}\n", node.prefix().unwrap());

    //     print!("Children: {}\n", node.children().unwrap().len());

    //     for child in node.children().unwrap() {
    //         print!("Child key, node index: {} - {}", child.key(), child.node_index());
    //     }
    // }

    let key_to_find: &[u8] = &[1, 1, 2, 3, 4, 5, 6, 1];
    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = find_key(buf, key_to_find);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    let missing_key: &[u8] = &[1, 1, 2, 3, 4, 5, 6, 99]; // This key will fail at leaf
    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = find_key(buf, missing_key);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);
    
}

fn read_congee_flat_columnar(buf: &[u8], key: &[u8]) -> bool {

    // All these can be moved out. these are one time operations.
    let cfr = Columar::root_as_congee_flat(buf).unwrap();
    let node_types = cfr.node_types().unwrap();
    let prefix_bytes = cfr.prefix_bytes().unwrap();
    let prefix_offsets = cfr.prefix_offsets().unwrap();
    let children_data = cfr.children_data().unwrap();
    let children_offsets = cfr.children_offsets().unwrap();

    let mut current_node_index = 0;
    let mut key_pos = 0;

    loop {
        // --- Read from columnar arrays ---
        let node_type = node_types.get(current_node_index);
        
        let prefix_start = if current_node_index == 0 { 0 } else { prefix_offsets.get(current_node_index - 1) as usize };
        let prefix_end = prefix_offsets.get(current_node_index) as usize;
        let prefix = &prefix_bytes.bytes()[prefix_start..prefix_end];

        // Check if the remaining key starts with this node's prefix
        if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
            return false;
        }
        key_pos += prefix.len();

        // If we've consumed the entire key, check if this is a valid termination point
        if key_pos >= key.len() {
            // For CongeeSet, we need to check if this node represents a complete key
            // In the real implementation, leaf nodes with children having node_index=0 indicate stored values
            let children_start = if current_node_index == 0 { 0 } else { children_offsets.get(current_node_index - 1) as usize };
            let children_end = children_offsets.get(current_node_index) as usize;
            
            if node_type < 4 {
                return false;
            }
            // Check if any child has node_index 0 (indicating a stored value)
            for j in children_start..children_end {
                let child = children_data.get(j);
                if child.node_index() == 0 {
                    return true;
                }
            }
            return false;
        }

        let next_key_byte = key[key_pos];

        let children_start = if current_node_index == 0 { 0 } else { children_offsets.get(current_node_index - 1) as usize };
        let children_end = children_offsets.get(current_node_index) as usize;
        
        // OPTIMIZATION: Manually implement binary search on the logical slice of the columnar vector.
        let mut low = children_start;
        let mut high = children_end;
        let mut child_opt = None;

        while low < high {
            let mid_index = low + (high - low) / 2;
            let mid_child = children_data.get(mid_index);
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
                    Columar::NodeType::N4_LEAF | Columar::NodeType::N16_LEAF => {
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

fn main() {
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();

    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 2]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 3]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 4]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 5]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();

    // let fb_vec = tree.to_flatbuffer();


    let mut bldr = FlatBufferBuilder::new();
    let mut bytes: Vec<u8> = Vec::new();

    make_congee_flat(&mut bldr, &mut bytes);

    println!("Size of congee flat: {}", bytes.len());

    read_congee_flat(&bytes);

    let key_to_find: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 1];
    let missing_key: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 99];

    println!("\n\n--- Manual Columnar FlatBuffer Lookup ---");
    let mut bldr_col = FlatBufferBuilder::new();
    let columnar_bytes = tree.to_flatbuffer_columnar(&mut bldr_col);
    println!("Size of columnar congee flat: {}", columnar_bytes.len());

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = read_congee_flat_columnar(&columnar_bytes, &key_to_find);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = read_congee_flat_columnar(&columnar_bytes, &missing_key);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    println!("\n\n--- CongeeFlatColumnar Implementation Lookup ---");
    let congee_columnar = CongeeFlatColumnar::from_congee_set(&tree);
    println!("Size of CongeeFlatColumnar: {} bytes", congee_columnar.memory_usage());

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = congee_columnar.contains(&usize::from_be_bytes(key_to_find));
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = congee_columnar.contains(&usize::from_be_bytes(missing_key));
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    println!("\n\n--- CongeeSet Lookup ---");

    println!("Size of CongeeSet: {} bytes", tree.stats().total_memory_bytes());


    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = tree.contains(&usize::from_be_bytes(key_to_find), &guard);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);
    
    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = tree.contains(&usize::from_be_bytes(missing_key), &guard);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    println!("\n\n--- AHashMap Lookup ---");
    let mut map = AHashMap::new();
    map.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 1]), ());
    map.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 2]), ());
    map.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 3]), ());
    map.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 4]), ());
    map.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 5]), ());
    map.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 1]), ());

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = map.contains_key(&usize::from_be_bytes(key_to_find));
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = map.contains_key(&usize::from_be_bytes(missing_key));
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    println!("\n\n--- Memory Usage Comparison ---");
    println!("CongeeSet:           {} bytes", tree.stats().total_memory_bytes());
    println!("Manual FlatBuffer:   {} bytes", bytes.len());
    println!("Manual Columnar FB:  {} bytes", columnar_bytes.len());
    println!("CongeeFlatColumnar:  {} bytes", congee_columnar.memory_usage());
    println!("AHashMap:            ~{} bytes (estimate)", map.len() * (8 + 8 + 16)); // key + value + overhead

    println!("\n--- Efficiency Analysis ---");
    let theoretical_min = 6 * 8; // 6 keys * 8 bytes each
    println!("Theoretical minimum: {} bytes", theoretical_min);
    
    let congee_overhead = tree.stats().total_memory_bytes() as f64 / theoretical_min as f64;
    let columnar_overhead = congee_columnar.memory_usage() as f64 / theoretical_min as f64;
    
    println!("CongeeSet overhead:        {:.2}x theoretical minimum", congee_overhead);
    println!("CongeeFlatColumnar overhead: {:.2}x theoretical minimum", columnar_overhead);
    
    let memory_savings = ((tree.stats().total_memory_bytes() - congee_columnar.memory_usage()) as f64 / tree.stats().total_memory_bytes() as f64) * 100.0;
    let efficiency_improvement = tree.stats().total_memory_bytes() as f64 / congee_columnar.memory_usage() as f64;
    
    println!("Memory savings:      {:.1}% ({} bytes → {} bytes)", 
             memory_savings, tree.stats().total_memory_bytes(), congee_columnar.memory_usage());
    println!("Efficiency improvement: {:.2}x vs CongeeSet", efficiency_improvement);
}
