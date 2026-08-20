# nvme-state-db

Motor de almacenamiento **clave-valor** en [Rust](https://www.rust-lang.org/), pensado para acceso de baja latencia al **estado de una blockchain** sobre discos **NVMe**.

No es un servidor de red ni una base SQL. Es el núcleo LSM: escribe en un WAL alineado a 4K con `O_DIRECT`, mantiene una MemTable lock-free en RAM y lee SSTables con `mmap` (sin copiar el valor en el reader).

[English version](README.en.md) · Plan de implementación: [`FASES.md`](FASES.md)

---

## Qué problema resuelve

Un ejecutor de bloques hace muchos `get`/`put` pequeños y no puede permitirse:

- pasar por el page cache de Linux en cada write durable, o
- bloquear el hilo de ejecución mientras se vuelca un archivo grande a disco.

Este crate escribe primero el **WAL** (durabilidad), aplica después la **MemTable**, y el flush a **SSTable** corre en un worker. El `get` mira RAM (tabla activa e inmutable) y luego archivos `.sst` de más nuevos a más viejos.

---

## Arquitectura

```text
put(k,v) → WAL (disco, O_DIRECT + O_SYNC) → MemTable (SkipList)
                    ↓ flush en background
             SSTable (.sst, mmap + Bloom SIMD)
                    ↑
get(k)  → MemTable activa → MemTable inmutable → Bloom → SST
```

| Pieza | Rol |
|--------|-----|
| **WAL** | Diario append-only. Páginas de 4096 B. Se escribe **antes** que RAM. |
| **MemTable** | SkipList concurrente. Un tombstone no es “clave ausente”: evita resucitar un valor viejo en disco. |
| **SSTable** | Archivo inmutable ordenado: bloques + índice + Bloom + footer. |
| **Engine** | Une las piezas. `put`/`get` no esperan al I/O del `.sst`. |

**No hay** compactación, snapshots, transacciones ni red. Ver “Fuera de alcance” en `FASES.md`.

---

## Requisitos

- Linux x86_64 (el WAL usa `posix_memalign` y flags Unix).
- Rust (el repo se desarrolla con nightly; edition 2021).
- NVMe real no es obligatorio para tests; `O_DIRECT` puede caer a un fallback alineado según el filesystem.

---

## Uso rápido

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo bench --bench state_access   # latencia, IOPS, write amplification
```

Abrir un directorio de datos (crea `wal` y `.sst` allí):

```bash
cargo run -- /ruta/al/datadir
```

API de la biblioteca (`nvme_state_db`):

```rust
use nvme_state_db::Engine;

let db = Engine::open("/tmp/statedb")?;
db.put(b"account", b"balance")?;
let hit = db.get(b"account")?;
db.flush()?; // rota MemTable y espera al worker
```

Tipos públicos: `Engine`, `Key`, `Value`, `SeqNum`, `Error` (`thiserror` en la lib; `anyhow` solo en el binario).

---

## Layout del repo

```text
src/
  engine.rs          # put / get / flush en background
  memtable.rs        # SkipList lock-free
  wal.rs             # O_DIRECT, 4K
  sstable/           # writer + reader mmap
  index/bloom.rs     # Bloom bloqueado, AVX2 si hay
  types.rs, error.rs
benches/state_access.rs
examples/lookup.rs
```

Números de una pasada en esta máquina (put ~5 ms por `O_SYNC`+4K; get MemTable ~decenas de ns; get SST ~cientos de ns; WA del WAL ≫ 1 por alineación): detalle en [`FASES.md`](FASES.md) (fase 9). Informes HTML: `target/criterion/` tras `cargo bench`.

---

## Estado

Fases **1–9** del plan están implementadas (crate → MemTable → WAL → Bloom → SST write/read → motor → flush background → benches). Una **fase 10 (compactación)** es opcional y no forma parte del alcance actual.

---

## Licencia

MIT OR Apache-2.0
