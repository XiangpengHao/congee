use std::time::Instant;
use congee::{CongeeSet, CongeeFlatStruct};

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

    // sequential keys
    for i in 0..200 {
        let key = (i as u64).to_be_bytes();
        tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    }

    let set_stats = tree.stats();
    println!("Congee set stats: \n{}", set_stats);

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

    for _key in 114..124 {
        let key = (_key as u64).to_be_bytes();
        println!("\nSearching for key: {:?}", key);
        let start = Instant::now();
        let found = congee_flat_struct.contains(&key);
        let duration = start.elapsed();
        println!("Found: {} in {:?}", found, duration);
        assert!(found);
    }

    println!(" ** congee set **");
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
    println!("CongeeFlatStruct memory: {} bytes", struct_bytes.len());
    println!("Memory reduction: {:.1}% ({:.2}x smaller)", 
             (1.0 - struct_bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0,
             set_stats.total_memory_bytes() as f64 / struct_bytes.len() as f64);
}