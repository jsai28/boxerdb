# BoxerDB

Lightweight embedded B‑tree storage written in Rust for educational purposes.

Overview
- Small, self-contained B‑tree storage engine implemented as a Rust crate.
- Core storage code lives under the `storage` module: [src/storage/mod.rs](src/storage/mod.rs).
- Logical B‑tree structure in [`storage::BTree`](src/storage/btree.rs).
- Node representation and (de)serialization in [`storage::Node`](src/storage/node.rs) — see [`Node::encode_node`](src/storage/node.rs) and [`Node::decode_node`](src/storage/node.rs).
- Disk I/O and page management in [src/storage/diskmanager.rs](src/storage/diskmanager.rs).
- Configuration and constants in [`storage::StorageConfig`](src/storage/configs.rs).
- Minimal example of a storage engine to learn B‑tree internals, disk layout, and simple persistence.
- Good starting point for experimenting with concurrency, WAL, or more advanced indexing.
