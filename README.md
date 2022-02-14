# Concurrent ART (adaptive radix tree) 
[![con-art-rust](https://github.com/XiangpengHao/con-art-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/XiangpengHao/con-art-rust/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/con-art-rust.svg)](
https://crates.io/crates/con-art-rust)
[![dependency status](https://deps.rs/crate/con-art-rust/0.1.8/status.svg)](https://deps.rs/crate/con-art-rust/0.1.8)
[![codecov](https://codecov.io/gh/XiangpengHao/con-art-rust/branch/main/graph/badge.svg?token=x0PSjQrqyR)](https://codecov.io/gh/XiangpengHao/con-art-rust)

A Rust implementation of ART-OLC [concurrent adaptive radix tree](https://db.in.tum.de/~leis/papers/artsync.pdf).
It implements the optimistic lock coupling with proper SIMD support.

The code is based on its [C++ implementation](https://github.com/flode/ARTSynchronized), with many bug fixes.

It only support (and optimized for) 8 byte key;
due to this specialization, this ART has great performance -- basic operations are only 20% slower than [flurry](https://github.com/jonhoo/flurry) hash table, range scan is an order of magnitude faster, of course :)

The code is extensively tested with [{address|leak} sanitizer](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html) as well as [libfuzzer](https://llvm.org/docs/LibFuzzer.html) (use BTreeMap as oracle).

### Why this library?
- Need an ordered container, i.e. need range scan; otherwise you should consider a hash table, e.g. [flurry](https://github.com/jonhoo/flurry).
- Restricted on memory consumption. Hash tables often have exponential bucket size growth, which often lead to low load factors. ART is more space efficient.

### Example:
```rust
let art = Art::new();
let guard = art.pin(); // enter an epoch

art.insert(UsizeKey::new(0), 42, &guard); // insert a value

let val = art.get(&UsizeKey::new(0)).unwrap(); // read the value
assert_eq!(val, 42);

let mut scan_buffer = vec![0; 8];
let scan_result = art.look_up_range(&UsizeKey::new(0), &UsizeKey::new(10), &mut art_scan_buffer); // scan values

assert_eq!(scan_result.unwrap(), 1);
```
