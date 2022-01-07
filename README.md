# Concurrent ART (adaptive radix tree) 
[![con-art-rust](https://github.com/XiangpengHao/con-art-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/XiangpengHao/con-art-rust/actions/workflows/ci.yml)

A Rust implementation of ART-OLC [concurrent adaptive radix tree](https://db.in.tum.de/~leis/papers/artsync.pdf).

The code is a direct translation of its [C++ implementation](https://github.com/flode/ARTSynchronized), with minor bug fixes.

TODO:

- Implement delete
- Implement epoch based memory reclaim 
- Align the API with the BTree's in std.

