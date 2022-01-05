use con_art_rust::{tree::Tree, Key};

#[test]
fn test_simple() {
    let tree = Tree::new();
    let key_cnt = 1000;

    for i in 0..key_cnt {
        tree.insert(Key::from(i), i);
    }

    for i in 0..key_cnt {
        let v = tree.look_up(&Key::from(i)).unwrap();
        assert_eq!(v, i);
    }
    println!("it works");
}

#[test]
fn test_insert_read_back() {
    let key_cnt = 1000000;
    let tree = Tree::new();

    for i in 0..key_cnt {
        tree.insert(Key::from(i), i);
    }

    for i in 0..key_cnt {
        let v = tree.look_up(&Key::from(i)).unwrap();
        assert_eq!(v, i);
    }
    println!("it works");
}
