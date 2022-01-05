use std::sync::Arc;
use std::thread;

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

#[test]
fn test_rng_insert_read_back() {
    let key_cnt = 30000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let tree = Tree::new();

    for v in key_space.iter() {
        tree.insert(Key::from(*v), *v);
    }

    for i in 0..key_cnt {
        let v = tree.look_up(&Key::from(i)).unwrap();
        assert_eq!(v, i);
    }
}

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{thread_rng, SeedableRng};

#[test]
fn test_concurrent_insert() {
    let key_cnt_per_thread = 100000;
    let n_thread = 1;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * n_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    key_space.shuffle(&mut thread_rng());

    let key_space = Arc::new(key_space);

    let tree = Arc::new(Tree::new());

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(Key::from(val), val);
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    for v in key_space.iter() {
        let val = tree.look_up(&Key::from(*v)).unwrap();
        assert_eq!(val, *v);
    }
}
