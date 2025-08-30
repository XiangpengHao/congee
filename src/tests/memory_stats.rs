use crate::CongeeRaw;

#[test]
fn test_memory_stats_simple() {
    let tree = CongeeRaw::<usize, usize>::default();
    let guard = tree.pin();

    // Empty tree will have a root node, which is 56 bytes
    let stats = tree.stats();

    let empty_memory = stats.total_memory_bytes();
    assert_eq!(empty_memory, 56);
    assert_eq!(stats.kv_pairs(), 0);

    tree.insert(1, 42, &guard).unwrap();
    let stats = tree.stats();

    // After inserting one value, we should have more memory and at least one more node
    let memory_increase = stats.total_memory_bytes() - empty_memory;
    assert_eq!(memory_increase, 56);
    assert_eq!(stats.total_nodes(), 2);
    assert_eq!(stats.kv_pairs(), 1);
}

#[test]
fn test_memory_stats_expected_sizes() {
    let tree = CongeeRaw::<usize, usize>::default();
    let guard = tree.pin();

    tree.insert(0x1000, 1, &guard).unwrap();
    tree.insert(0x2000, 2, &guard).unwrap(); // Different prefix
    tree.insert(0x1001, 3, &guard).unwrap(); // Same prefix as first

    let stats = tree.stats();
    let (n4_mem, n16_mem, n48_mem, n256_mem) = stats.memory_by_node_type();

    // Verify the sum matches total
    assert_eq!(
        stats.total_memory_bytes(),
        n4_mem + n16_mem + n48_mem + n256_mem
    );

    assert!(stats.total_memory_bytes() > 0);
    assert_eq!(stats.kv_pairs(), 3);
}
