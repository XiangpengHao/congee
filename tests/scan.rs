use con_art_rust::{tree::Tree, Key};

#[test]
fn basic_scan() {
    let tree = Tree::new();
    let key_cnt = 1000;

    for i in 0..key_cnt {
        tree.insert(Key::from(i), i);
    }

    let low_key = Key::from(200);
    let high_key = Key::from(300);

    let mut results = [0; 120];
    let scan_r = tree
        .look_up_range(&low_key, &high_key, &mut results)
        .unwrap();

    assert_eq!(scan_r, 100);
    for i in 0..scan_r {
        assert_eq!(results[i], 200 + i);
    }
}
