use std::fs::read;
use std::time::Instant;
use ahash::AHashMap;
use rand::Rng;
use flatbuffers::*;

use congee::{CongeeSet, CongeeFlat};

// use crate::congee_flat_generated::congee_flat::{CongeeFlat, Node, NodeArgs, NodeType};
use congee::congee_flat_generated::congee_flat::{self as Columnar, Child, NodeType};

// fn read_congee_flat_columnar(node_types: &Vector<'_,NodeType>, prefix_bytes: &Vector<'_,u8>, prefix_offsets: &Vector<'_,u32>, children_data: &Vector<'_,Child>, children_offsets: &Vector<'_,u32>, key: &[u8]) -> bool {

//     // All these can be moved out. these are one time operations.
//     
//     let mut current_node_index = 0;
//     let mut key_pos = 0;
// 
//     loop {
//         // --- Read from columnar arrays ---
//         let node_type = node_types.get(current_node_index);
//         
//         let prefix_start = if current_node_index == 0 { 0 } else { prefix_offsets.get(current_node_index - 1) as usize };
//         let prefix_end = prefix_offsets.get(current_node_index) as usize;
//         let prefix = &prefix_bytes.bytes()[prefix_start..prefix_end];
// 
//         // Check if the remaining key starts with this node's prefix
//         if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
//             return false;
//         }
//         key_pos += prefix.len();
// 
//         // If we've consumed the entire key, check if this is a valid termination point
//         if key_pos >= key.len() {
//             // For CongeeSet, we need to check if this node represents a complete key
//             // In the real implementation, leaf nodes with children having node_index=0 indicate stored values
//             let children_start = if current_node_index == 0 { 0 } else { children_offsets.get(current_node_index - 1) as usize };
//             let children_end = children_offsets.get(current_node_index) as usize;
//             
//             // if node_type < 4 {
//             //     return false;
//             // }
//             // Check if any child has node_index 0 (indicating a stored value)
//             for j in children_start..children_end {
//                 let child = children_data.get(j);
//                 if child.node_index() == 0 {
//                     return true;
//                 }
//             }
//             return false;
//         }
// 
//         let next_key_byte = key[key_pos];
// 
//         let children_start = if current_node_index == 0 { 0 } else { children_offsets.get(current_node_index - 1) as usize };
//         let children_end = children_offsets.get(current_node_index) as usize;
//         
//         // OPTIMIZATION: Manually implement binary search on the logical slice of the columnar vector.
//         let mut low = children_start;
//         let mut high = children_end;
//         let mut child_opt = None;
// 
//         while low < high {
//             let mid_index = low + (high - low) / 2;
//             let mid_child = children_data.get(mid_index);
//             match mid_child.key().cmp(&next_key_byte) {
//                 std::cmp::Ordering::Less => low = mid_index + 1,
//                 std::cmp::Ordering::Greater => high = mid_index,
//                 std::cmp::Ordering::Equal => {
//                     child_opt = Some(mid_child);
//                     break;
//                 }
//             }
//         }
// 
//         match child_opt {
//             Some(child) => {
//                 let next_node_index = child.node_index();
//                 key_pos += 1;
// 
//                 // Handle leaf vs internal nodes
//                 match node_type {
//                     Columnar::NodeType::N4_LEAF | Columnar::NodeType::N16_LEAF | Columnar::NodeType::N48_LEAF | Columnar::NodeType::N256_LEAF => {
//                         // We are at a leaf. For a successful match, we should have consumed
//                         // the entire key and the child should point to a value (node_index 0)
//                         return key_pos == key.len() && next_node_index == 0;
//                     }
//                     _ => { // Internal nodes
//                         if next_node_index > 0 {
//                             current_node_index = next_node_index as usize;
//                         } else {
//                             // Found a stored value at this exact position
//                             return key_pos == key.len();
//                         }
//                     }
//                 }
//             }
//             None => return false,
//         }
//     }
// }


fn main() {
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();

    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 2]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 3]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 4]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 5]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 2]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 3]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 4]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 5]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 6]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 7]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 8]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 9]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 10]), &guard).unwrap();

    
    // random keys
    // let mut rng = rand::thread_rng();
    // for _ in 0..10000 {
    //     let mut key = [0u8; 8];
    //     rng.fill(&mut key);
    //     // Insert the key with a dummy value (e.g., 42)
    //     tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    // }

    // sequential keys
    for i in 0..10 {
        let key = (i as u64).to_be_bytes();

        // Insert the key with a dummy value (e.g., 42)
        tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    }

    let set_size = tree.stats();

    let columnar_bytes = tree.to_flatbuffer();

    // let cfr = Columnar::root_as_congee_flat(&columnar_bytes).unwrap();
    // let node_types = cfr.node_types().unwrap();
    // let prefix_bytes = cfr.prefix_bytes().unwrap();
    // let prefix_offsets = cfr.prefix_offsets().unwrap();
    // let children_data = cfr.children_data().unwrap();
    // let children_offsets = cfr.children_offsets().unwrap();
    
    let congee_flat = CongeeFlat::new(&columnar_bytes);

    // println!("node_types: {:?}", node_types);
    // println!("prefix_bytes: {:?}", prefix_bytes);
    // println!("prefix_offsets: {:?}", prefix_offsets);
    // println!("children_data: {:?}", children_data);
    // println!("children_offsets: {:?}", children_offsets);
    
    println!("Size of congee flat: {}", columnar_bytes.len());
    let key_to_find: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 1];
    let key_to_find_2: [u8; 8] = (1023 as u64).to_be_bytes();
    let missing_key: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 99];

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    // let found = read_congee_flat_columnar(&node_types, &prefix_bytes, &prefix_offsets, &children_data, &children_offsets, &key_to_find);
    let found = congee_flat.contains(&key_to_find);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key 2: {:?}", key_to_find_2);
    let start = Instant::now();
    // let found = read_congee_flat_columnar(&node_types, &prefix_bytes, &prefix_offsets, &children_data, &children_offsets, &key_to_find_2);
    let found = congee_flat.contains(&key_to_find_2);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    // let found = read_congee_flat_columnar(&node_types, &prefix_bytes, &prefix_offsets, &children_data, &children_offsets, &missing_key);
    let found = congee_flat.contains(&missing_key);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);
    
    for _key in 4.. 10 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        // let found = read_congee_flat_columnar(&node_types, &prefix_bytes, &prefix_offsets, &children_data, &children_offsets, &key);
        let found = congee_flat.contains(&key);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }

    println!(" ---- Congee Set ----");
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

    for _key in 4.. 9 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        let found = tree.contains(&usize::from_be_bytes(key), &guard);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }
    println!();
    println!("Congee set stats: \n{}", set_size);
}
