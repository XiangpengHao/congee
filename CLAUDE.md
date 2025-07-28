# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Congee is a Rust implementation of ART-OLC (Adaptive Radix Tree with Optimistic Lock Coupling), a high-performance concurrent data structure. It specializes in 8-byte fixed keys and provides fast concurrent operations with SIMD support.

### Experimental Feature: CongeeFlatColumnar

CongeeFlatColumnar is an experimental feature that uses FlatBuffers to create a space-efficient, read-only set implementation. It provides significant memory savings compared to the standard CongeeSet while maintaining good read performance. Key characteristics:

**Purpose**: Optimized for read-heavy workloads where memory efficiency is critical, primarily intended for use as a set rather than a hashmap.

**Architecture**: Uses a columnar (Struct-of-Arrays) layout with FlatBuffers serialization:
- Node types stored in parallel arrays
- Prefix data stored separately with offsets
- Children data stored with corresponding offsets
- Enables compact binary representation

**Performance**: Provides substantial memory savings (50%+ reduction verified) with ~4x slower lookup times compared to the in-memory CongeeSet. Trade-off optimized for memory-constrained environments.

**Usage**: Convert existing CongeeSet to columnar format using `tree.to_flatbuffer()`.
## Common Development Commands

### Testing
- `cargo test` - Run all tests
- `cargo test --features shuttle` - Run tests with Shuttle scheduler for detecting concurrency bugs
- `cargo test --features stats` - Run tests with statistics enabled

### Benchmarking
- `cargo bench` - Run all benchmarks
- `cargo bench --bench basic` - Run basic operation benchmarks  
- `cargo bench --bench scan` - Run range scan benchmarks
- `cargo bench --bench node_size` - Run node size benchmarks
- `cargo run --example fb_child_as_struct` - CongeeFlatColumnar performance comparison

### Building
- `cargo build` - Standard build
- `cargo build --release` - Optimized release build
- `cargo build --all-features` - Build with all features enabled

### Linting and Formatting
- `cargo fmt` - Format code
- `cargo clippy` - Run Clippy lints

### Special Features
- `--features flamegraph` - Enable flamegraph profiling
- `--features perf` - Enable perf profiling  
- `--features stats` - Enable runtime statistics collection
- `--features shuttle` - Enable Shuttle testing for concurrency validation

## Architecture

### Core Components

**CongeeInner** (`src/congee_inner.rs`): The core tree implementation containing the ART logic, lock coupling, and SIMD operations.

**Public APIs**:
- **Congee** (`src/congee.rs`): Arc-based concurrent map for reference-counted values
- **CongeeRaw** (`src/congee_raw.rs`): Low-level API for raw u64 keys and values
- **CongeeSet** (`src/congee_set.rs`): Set implementation


**Node Types** (`src/nodes/`): Adaptive node implementations:
- `Node4` - For 1-4 children 
- `Node16` - For 5-16 children
- `Node48` - For 17-48 children  
- `Node256` - For 49-256 children

**Memory Management**:
- Uses crossbeam-epoch for lock-free memory reclamation
- Custom allocator support via `Allocator` trait
- `DefaultAllocator` and `MemoryStatsAllocator` implementations

### Key Concepts

**Epoch-based Memory Management**: All operations require an `epoch::Guard` obtained via `tree.pin()`. Guards should be reused when possible as they can be expensive to create.

**Optimistic Lock Coupling**: The tree uses optimistic concurrency control with lock coupling for high-performance concurrent access.

**Fixed 8-byte Keys**: All keys are converted to/from `[u8; 8]` arrays using big-endian byte ordering for consistent ordering.

**SIMD Support**: Node operations use SIMD instructions for fast key comparison and searching.

## Memory Statistics

The stats system provides detailed memory usage information:

**Basic Usage**:
```rust
let tree = CongeeRaw::default();
let stats = tree.stats();
println!("Total memory: {} bytes", stats.total_memory_bytes());
```

**Available Methods**:
- `stats.total_memory_bytes()` - Total memory consumed in bytes
- `stats.total_nodes()` - Number of nodes in the tree  
- `stats.kv_pairs()` - Number of key-value pairs
- `stats.memory_by_node_type()` - Memory breakdown by node type (N4, N16, N48, N256)

**Node Sizes**:
- Node4: 56 bytes each
- Node16: 160 bytes each  
- Node48: 664 bytes each
- Node256: 2096 bytes each

The stats provide both raw data methods and a pretty-printed display format showing memory usage per tree level and node type.

## Testing Strategy

The codebase uses extensive testing including:
- Unit tests for all public APIs
- Concurrency tests with multiple threads
- Memory sanitizer testing (address/leak sanitizers)
- Fuzzing with libfuzzer (`fuzz/` directory)
- Shuttle-based concurrency testing for bug detection

When adding new features, ensure comprehensive test coverage including concurrent scenarios.

## Memory Safety

- All raw pointer operations are carefully documented with safety comments
- Arc reference counting is managed explicitly in the high-level API
- Epoch-based reclamation prevents use-after-free bugs
- Extensive use of `unsafe` blocks are all documented with safety rationale

## Next tasks 

- Verify if the logic in to_flatbuffer() is correct. Identify any bugs or missed scenarios.
- Move the read_congee_flat_columnar logic to congee_flat.rs in congee/src. It should be a simple contains(). The node_types, prefix_bytes, ... should already be initialized before calling contains. We don't want to initialize those for every single contains() call. 

Note: Keep the code as simple as possible. Don't complicate. The moving logic should be straightforward. I don't think it will have any surprises. 

