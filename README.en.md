# nvme-state-db

A **key-value** storage engine in [Rust](https://www.rust-lang.org/), aimed at low-latency **blockchain state** access on **NVMe**.

This is not a network server or a SQL database. It is the LSM core: a 4K-aligned WAL with `O_DIRECT`, a lock-free MemTable in RAM, and SSTable reads via `mmap` (the SST reader does not copy values).

[Versión en español](README.md) · Implementation plan: [`FASES.md`](FASES.md) (Spanish)

---

## What it is for

A block execution engine issues many small `get`/`put` calls and cannot afford to:

- go through the Linux page cache on every durable write, or
- stall the execution thread while a large file is flushed to disk.

This crate writes the **WAL** first (durability), then applies the **MemTable**. Flushing to an **SSTable** runs on a background worker. `get` checks RAM (active and immutable tables) and then `.sst` files from newest to oldest.

---

## Architecture

```text
put(k,v) → WAL (disk, O_DIRECT + O_SYNC) → MemTable (SkipList)
                    ↓ background flush
             SSTable (.sst, mmap + SIMD Bloom)
                    ↑
get(k)  → active MemTable → immutable MemTable → Bloom → SST
```

| Piece | Role |
|--------|------|
| **WAL** | Append-only log. 4096-byte pages. Written **before** RAM. |
| **MemTable** | Concurrent SkipList. A tombstone is not “missing key”: it stops an old on-disk value from coming back. |
| **SSTable** | Immutable sorted file: blocks + index + Bloom + footer. |
| **Engine** | Wires the pieces together. `put`/`get` do not wait on `.sst` I/O. |

There is **no** compaction, snapshots, transactions, or networking. See “Fuera de alcance” in `FASES.md`.

---

## Requirements

- Linux x86_64 (the WAL uses `posix_memalign` and Unix open flags).
- Rust (developed on nightly; 2021 edition).
- A real NVMe device is not required for tests; `O_DIRECT` may fall back to aligned I/O depending on the filesystem.

---

## Quick start

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo bench --bench state_access   # latency, IOPS, write amplification
```

Open a data directory (creates `wal` and `.sst` files there):

```bash
cargo run -- /path/to/datadir
```

Library API (`nvme_state_db`):

```rust
use nvme_state_db::Engine;

let db = Engine::open("/tmp/statedb")?;
db.put(b"account", b"balance")?;
let hit = db.get(b"account")?;
db.flush()?; // rotate the MemTable and wait for the worker
```

Public types: `Engine`, `Key`, `Value`, `SeqNum`, `Error` (`thiserror` in the library; `anyhow` only in the binary).

---

## Repository layout

```text
src/
  engine.rs          # put / get / background flush
  memtable.rs        # lock-free SkipList
  wal.rs             # O_DIRECT, 4K
  sstable/           # writer + mmap reader
  index/bloom.rs     # blocked Bloom, AVX2 when available
  types.rs, error.rs
benches/state_access.rs
examples/lookup.rs
```

Sample numbers on the development machine (put ~5 ms because of `O_SYNC`+4K; MemTable get ~tens of ns; SST get ~hundreds of ns; WAL write amplification ≫ 1 from alignment): see [`FASES.md`](FASES.md) (phase 9). HTML reports: `target/criterion/` after `cargo bench`.

---

## Status

Plan phases **1–9** are implemented (crate → MemTable → WAL → Bloom → SST write/read → engine → background flush → benches). **Phase 10 (compaction)** is optional and out of the current scope.

---

## License

MIT OR Apache-2.0
