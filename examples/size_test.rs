use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct};

fn main() {
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();

    // Insert your test keys: 1,2,3,4,5 and 1000-1014 and 10002-10004
    for i in 1..=5 {
        tree.insert(i, &guard).unwrap();
    }
    for i in 1000..=1014 {
        tree.insert(i, &guard).unwrap();
    }
    for i in 10002..=10004 {
        tree.insert(i, &guard).unwrap();
    }

    println!("Total keys inserted: {}", 5 + 15 + 3);

    let columnar_bytes = tree.to_flatbuffer();
    let struct_bytes = tree.to_flatbuffer_struct();
    
    println!("CongeeFlat (columnar) size: {} bytes", columnar_bytes.len());
    println!("CongeeFlatStruct size: {} bytes", struct_bytes.len());
    println!("Your custom format estimate: 124 bytes");
    
    println!("\nMemory savings vs FlatBuffers:");
    println!("vs CongeeFlat: {:.1}x smaller", columnar_bytes.len() as f64 / 124.0);
    println!("vs CongeeFlatStruct: {:.1}x smaller", struct_bytes.len() as f64 / 124.0);
}