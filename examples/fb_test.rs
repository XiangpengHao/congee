use std::fs::read;
use std::time::Instant;
use ahash::AHashMap;
use rand::Rng;
use flatbuffers::*;

use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct};

// use crate::congee_flat_generated::congee_flat::{CongeeFlat, Node, NodeArgs, NodeType};
use congee::congee_flat_generated::congee_flat::{self as Columnar, Child, NodeType};

// fn read_congee_flat_columnar(node_types: &Vector<'_,NodeType>, prefix_bytes: &Vector<'_,u8>, prefix_offsets: &Vector<'_,u32>, children_data: &Vector<'_,Child>, children_offsets: &Vector<'_,u32>, key: &[u8]) -> bool {

//     // All these can be moved out. these are one time operations.
//     
//     let mut current_node_index = 0;
//     let mut key_pos = 0;
// 
//     loop {

//         let node_type = node_types.get(current_node_index);
//         
//         let prefix_start = if current_node_index == 0 { 0 } else { prefix_offsets.get(current_node_index - 1) as usize };
//         let prefix_end = prefix_offsets.get(current_node_index) as usize;
//         let prefix = &prefix_bytes.bytes()[prefix_start..prefix_end];
// 
//         if key_pos + prefix.len() > key.len() || !key[key_pos..key_pos + prefix.len()].eq(prefix) {
//             return false;
//         }
//         key_pos += prefix.len();
// 
//         
//         if key_pos >= key.len() {
//             let children_start = if current_node_index == 0 { 0 } else { children_offsets.get(current_node_index - 1) as usize };
//             let children_end = children_offsets.get(current_node_index) as usize;
//             
//             // if node_type < 4 {
//             //     return false;
//             // }
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
//                 match node_type {
//                     Columnar::NodeType::N4_LEAF | Columnar::NodeType::N16_LEAF | Columnar::NodeType::N48_LEAF | Columnar::NodeType::N256_LEAF => {
//                         return key_pos == key.len() && next_node_index == 0;
//                     }
//                     _ => { // Internal nodes
//                         if next_node_index > 0 {
//                             current_node_index = next_node_index as usize;
//                         } else {
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
    // for _ in 0..16000 {
    //     let mut key = [0u8; 8];
    //     rng.fill(&mut key);
    //     tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    // }

    // sequential keys
    for i in 0..16000 {
        let key = (i as u64).to_be_bytes();
        tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    }

    let set_stats = tree.stats();
    println!("Congee set stats: \n{}", set_stats);


    let columnar_bytes = tree.to_flatbuffer();
    let congee_flat = CongeeFlat::new(&columnar_bytes);
    
    let struct_bytes = tree.to_flatbuffer_struct();
    let congee_flat_struct = CongeeFlatStruct::new(&struct_bytes);

    // println!("node_types: {:?}", node_types);
    // println!("prefix_bytes: {:?}", prefix_bytes);
    // println!("prefix_offsets: {:?}", prefix_offsets);
    // println!("children_data: {:?}", children_data);
    // println!("children_offsets: {:?}", children_offsets);
    
    println!("Size of congee flat: {}", columnar_bytes.len());
    let key_to_find: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 1];
    // let key_to_find_2: [u8; 8] = (1000023 as u64).to_be_bytes();
    let key_to_find_2: [u8; 8] = [2, 1, 2, 3, 4, 5, 6, 8];
    let missing_key: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 99];

    println!(" *** congee flat ***");

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
    assert!(found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    // let found = read_congee_flat_columnar(&node_types, &prefix_bytes, &prefix_offsets, &children_data, &children_offsets, &missing_key);
    let found = congee_flat.contains(&missing_key);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);
    
    for _key in 114..124 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        let found = congee_flat.contains(&key);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }

    println!(" *** congee flat struct ***");

    let struct_bytes = tree.to_flatbuffer_struct();
    let congee_flat_struct = CongeeFlatStruct::new(&struct_bytes);
    
    println!("Size of congee flat struct: {}", struct_bytes.len());
    
    // congee_flat_struct.debug_print();
    
    let key_to_find: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 1];
    // let key_to_find_2: [u8; 8] = (1000023 as u64).to_be_bytes();
    let key_to_find_2: [u8; 8] = [2, 1, 2, 3, 4, 5, 6, 8];
    let missing_key: [u8; 8] = [1, 1, 2, 3, 4, 5, 6, 99];

    println!(" *** congee flat struct ***");

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = congee_flat_struct.contains(&key_to_find);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key 2: {:?}", key_to_find_2);
    let start = Instant::now();
    let found = congee_flat_struct.contains(&key_to_find_2);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = congee_flat_struct.contains(&missing_key);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    for _key in 1014..1024 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        let found = congee_flat_struct.contains(&key);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }

    println!(" *** congee set ***");
    println!("Size of CongeeSet: {} bytes", tree.stats().total_memory_bytes());

    println!("\nSearching for key: {:?}", key_to_find);
    let start = Instant::now();
    let found = tree.contains(&usize::from_be_bytes(key_to_find), &guard);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);

    println!("\nSearching for key: {:?}", key_to_find_2);
    let start = Instant::now();
    let found = tree.contains(&usize::from_be_bytes(key_to_find_2), &guard);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(found);
    
    println!("\nSearching for key: {:?}", missing_key);
    let start = Instant::now();
    let found = tree.contains(&usize::from_be_bytes(missing_key), &guard);
    let duration = start.elapsed();
    println!("Found: {} in {:?}", found, duration);
    assert!(!found);

    for _key in 114.. 124 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        let found = tree.contains(&usize::from_be_bytes(key), &guard);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }
    println!();

    println!("\n*** memory comparison ***");
    println!("CongeeSet memory: {} bytes", set_stats.total_memory_bytes());
    println!("CongeeFlat memory: {} bytes - {:.1}% reduction ({:.2}x smaller)", columnar_bytes.len(), (1.0 - columnar_bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0, set_stats.total_memory_bytes() as f64 / columnar_bytes.len() as f64);
    println!("CongeeFlatStruct memory: {} bytes - {:.1}% reduction ({:.2}x smaller)", struct_bytes.len(), (1.0 - struct_bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0, set_stats.total_memory_bytes() as f64 / struct_bytes.len() as f64);

    println!("\n*** performance tests ***");
    let iterations = 100000;
    let test_keys: Vec<[u8; 8]> = (0..1000).map(|i| (i as u64).to_be_bytes()).collect();

    // CongeeFlat (columnar) performance test
    // println!("Running CongeeFlat performance test...");
    // let start = Instant::now();
    // for _ in 0..iterations {
    //     for key in &test_keys {
    //         let _ = congee_flat.contains(key);
    //     }
    // }
    // let duration = start.elapsed();
    // println!("CongeeFlat: {} contains() calls in {:?}", iterations * test_keys.len(), duration);

    // CongeeFlatStruct performance test
    // println!("Running CongeeFlatStruct performance test...");
    // let start = Instant::now();
    // for _ in 0..iterations {
    //     for key in &test_keys {
    //         let _ = congee_flat_struct.contains(key);
    //     }
    // }
    // let duration = start.elapsed();
    // println!("CongeeFlatStruct: {} contains() calls in {:?}", iterations * test_keys.len(), duration);

    // // CongeeSet performance test
    println!("Running CongeeSet performance test...");
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &test_keys {
            let _ = tree.contains(&usize::from_be_bytes(*key), &guard);
        }
    }
    let duration = start.elapsed();
    println!("CongeeSet: {} contains() calls in {:?}", iterations * test_keys.len(), duration);
    asdewer
}
