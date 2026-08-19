# Plan por fases — nvme-state-db

Motor KV de baja latencia en Rust para estado blockchain en NVMe.
Cada fase debe ser **autorizada** antes de implementarse. El objetivo es completar la fase y entender cómo funciona.

Estado actual del repositorio: **Fase 6 completa**. Crate, MemTable, WAL, Bloom, writer y reader mmap de SSTable.

---

## Protocolo

1. Autorizar por escrito: **autoriza fase N**.
2. Implementar **solo esa fase**, con TDD (`mod tests` primero, luego código).
3. Verificar con `cargo test` y `cargo clippy`.
4. Resumir qué se construyó, qué se aprendió y qué queda.
5. No adelantar fases.

Reglas de código vigentes: `.cursorrules` (I/O directo, layout, concurrencia) y `rust.cursorrules` (ownership, `thiserror`, TDD, sin `unwrap` en producción).

---

## Arquitectura objetivo

Un motor de estilo LSM (Log-Structured Merge):

```text
put(k,v) → WAL (durabilidad) → MemTable (RAM, lock-free)
                ↓ flush en background
         SSTable en disco (mmap, zero-copy)
                ↑
get(k)  → MemTable → Bloom → SSTable mmap
```

Estructura de directorios prevista:

```text
nvme-state-db/
├── Cargo.toml
├── FASES.md
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── error.rs
│   ├── memtable.rs
│   ├── wal.rs
│   ├── engine.rs
│   ├── sstable/
│   │   ├── mod.rs
│   │   ├── writer.rs
│   │   └── reader.rs
│   └── index/
│       ├── mod.rs
│       └── bloom.rs
└── benches/
    └── state_access.rs
```

---

## Fase 1 — Crate, tipos y errores

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `Cargo.toml`, `src/lib.rs`, `src/error.rs`, newtypes (`Key`, `Value`, `SeqNum`).

**Qué se aprende:** layout de una crate Rust, `thiserror` vs `anyhow`, por qué un newtype es más seguro que `[u8]`.

**Criterio de cierre:** `cargo test` y `cargo clippy` pasan. No hay WAL ni MemTable todavía.

**Hecho:** `Cargo.toml`, `src/lib.rs`, `src/main.rs`, `src/error.rs`, `src/types.rs`, `.gitignore`.

---

## Fase 2 — MemTable lock-free

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `src/memtable.rs` con `crossbeam-skiplist`: `put`, `get`, `delete` (tombstone), iteración ordenada, estimación de tamaño para flush.

**Qué se aprende:** por qué una SkipList permite lecturas concurrentes sin mutex; qué es un tombstone; cuándo la MemTable está llena.

**Criterio de cierre:** tests de put/get/delete, overwrite, concurrencia básica y tamaño.

**Hecho:** `src/memtable.rs`, dependencia `crossbeam-skiplist`. `put`/`get`/`delete`, tombstones, iteración ordenada, `approx_bytes` / `is_full`.

---

## Fase 3 — WAL con `O_DIRECT` y alineación 4K

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `src/wal.rs`: header `#[repr(C)]`, buffers alineados (`posix_memalign` / `aligned-vec`), append de records, replay para recovery.

**Qué se aprende:** por qué `File::write` normal rompe `O_DIRECT`; qué es un sector de 4096; por qué el WAL se escribe **antes** de la MemTable; cómo se reconstruye el estado tras un crash.

**Criterio de cierre:** tests de append + replay; un test que compruebe que el buffer está alineado a 4096. `unsafe` solo aquí, documentado.

**Hecho:** `src/wal.rs` (`WALHeader` 4K, `posix_memalign`, append/replay, CRC). Replay aplica a MemTable en tests de recovery.

**Nota:** en desarrollo se puede usar un directorio normal; NVMe real no es obligatorio para aprender.

---

## Fase 4 — Bloom filter SIMD

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `src/index/mod.rs` y `src/index/bloom.rs`: insert, `may_contain`, serialización a disco.

**Qué se aprende:** un Bloom **nunca** da falso negativo; sí puede dar falso positivo (y por eso ahorra I/O en SSTables). Qué gana SIMD.

**Criterio de cierre:** tests de pertenencia, falsos negativos = 0, y (de)serializar el filtro.

**Hecho:** `src/index/mod.rs`, `src/index/bloom.rs`. Bloom por bloques de 256 bits, `may_contain` con AVX2, `to_bytes` / `from_bytes`.

---

## Fase 5 — SSTable writer (flush)

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `src/sstable/mod.rs` y `writer.rs`: formato on-disk (bloques, `BlockHeader`, `IndexEntry`), flush de MemTable inmutable → archivo `.sst`.

**Qué se aprende:** por qué los datos en LSM se escriben ordenados y en bloques; qué es write amplification a este nivel.

**Criterio de cierre:** flush de una MemTable produce un archivo válido con índice y Bloom.

**Hecho:** `src/sstable/mod.rs` (layout `BlockHeader` / `IndexEntry` / `SstFooter`) y `writer.rs` (`flush_memtable`).

---

## Fase 6 — SSTable reader zero-copy (`mmap`)

**Estado:** completa (`cargo test` + `cargo clippy -D warnings`)

**Qué se construye:** `src/sstable/reader.rs`: mapear el archivo, buscar por índice, devolver slices **sin copiar**.

**Qué se aprende:** `mmap` vs leer a un `Vec`; por qué el lifetime del valor está atado al archivo mapeado; el camino caliente de lookup.

**Criterio de cierre:** round-trip writer → reader; `get` encuentra claves y no encuentra las que no existen.

**Hecho:** `SstReader` (`memmap2`) + `SstLookup` (`Alive` / `Deleted` / `Missing`). Bloom → índice binario → bloque; el valor es un subslice del mapping.

---

## Fase 7 — Motor: `put` / `get` / flush síncrono

**Estado:** pendiente de autorización

**Qué se construye:** `src/engine.rs` (y CLI mínimo en `main.rs` si aporta): WAL + MemTable + lista de SSTables. `put` durable, `get` con orden MemTable → SSTables nuevas→viejas.

**Qué se aprende:** el camino completo de una clave; por qué el orden de búsqueda importa; cuándo hace falta flush.

**Criterio de cierre:** test de integración: put → “crash” (reabrir) → get; flush manual → get desde SSTable.

---

## Fase 8 — Flush en background

**Estado:** pendiente de autorización

**Qué se construye:** rotación de MemTable (activa ↔ inmutable) y worker que escribe SSTables **sin bloquear** `put`/`get`.

**Qué se aprende:** por qué no se puede bloquear el motor de ejecución; el trade-off de una MemTable inmutable pendiente de flush.

**Criterio de cierre:** writes continúan mientras hay un flush; `get` ve datos en la MemTable inmutable.

---

## Fase 9 — Benchmarks

**Estado:** pendiente de autorización

**Qué se construye:** `benches/state_access.rs` con Criterion: latencia de `get`/`put`, IOPS, write amplification.

**Qué se aprende:** qué medir (p50/p99, no solo media) y qué números importan en NVMe.

**Criterio de cierre:** benches corren y dejan una línea base documentada en el resumen de la fase.

---

## Fuera de alcance

No hay compactación, snapshots, transacciones ni red. El `.cursorrules` no los pide.

Si más adelante se quiere, se puede añadir una **Fase 10 (compactación)** al cerrar la 9.

---

## Cómo autorizar

En el chat:

- `autoriza fase N` — se implementa esa fase
- `ajusta el plan` — cambios a este documento
- `explica más la fase N` — diseño detallado antes de tocar código
