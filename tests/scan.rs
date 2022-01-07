use con_art_rust::{tree::Tree, Key};

#[test]
fn basic_scan() {
    let tree = Tree::new();
    let key_cnt = 1000;

    for i in 0..key_cnt {
        tree.insert(Key::from(i), i);
    }

    let scan_cnt = 10;
    let low_v = 200;
    let low_key = Key::from(low_v);
    let high_key = Key::from(low_v + scan_cnt);

    let mut results = [0; 20];
    let scan_r = tree
        .look_up_range(&low_key, &high_key, &mut results)
        .unwrap();

    assert_eq!(scan_r, scan_cnt);
    for i in 0..scan_r {
        assert_eq!(results[i], low_v + i);
    }
}
