# Diagrama de clases

Modelo de los **tipos públicos** (y algunos internos relevantes). En Rust no hay clases: `struct` / `enum` + `impl`. El diagrama usa notación UML por costumbre.

```mermaid
classDiagram
    direction TB

    class Engine {
        +open(dir) Result~Engine~
        +put(key, value) Result
        +delete(key) Result
        +get(key) Result~EngineLookup~
        +flush() Result
        +schedule_flush() Result
        +wait_flush() Result
        +needs_flush() Result~bool~
    }

    class EngineOptions {
        +mem_capacity_bytes usize
    }

    class EngineLookup {
        <<enumeration>>
        Alive
        Deleted
        Missing
    }

    class EngineValue {
        <<enumeration>>
        Mem
        Frozen
        Sst
    }

    class Inner {
        <<internal>>
        dir PathBuf
        wal Mutex~Option~Wal~~
        snapshot RwLock~Snapshot~
        next_seq AtomicU64
        next_sst AtomicU64
    }

    class Snapshot {
        <<internal>>
        active Arc~MemTable~
        frozen Option~Arc~MemTable~~
        sstables Vec~SstReader~
    }

    class MemTable {
        +put(key, value, seq) bool
        +delete(key, seq) bool
        +get(key) Lookup
        +is_full() bool
        +iter()
    }

    class Lookup {
        <<enumeration>>
        Alive
        Deleted
        Missing
    }

    class Wal {
        +create(path) Result~Wal~
        +open(path) Result~Wal~
        +append(seq, op) Result
        +replay() Result~Vec~WalRecord~~
    }

    class WalOp {
        <<enumeration>>
        Put
        Delete
    }

    class WalRecord {
        +seq SeqNum
        +op WalOp
    }

    class SstWriter {
        +create(path, expected) Result
        +add(key, seq, value) Result
        +finish() Result~SstMeta~
    }

    class SstReader {
        +open(path) Result~SstReader~
        +get(key) Result~SstLookup~
    }

    class SstLookup {
        <<enumeration>>
        Alive
        Deleted
        Missing
    }

    class Bloom {
        +insert(key)
        +may_contain(key) bool
        +to_bytes() / from_bytes()
    }

    class Key {
        +new(bytes) Result~Key~
        +as_bytes()
    }

    class Value {
        +new(bytes) Value
        +as_bytes()
    }

    class SeqNum {
        +new(n) SeqNum
        +next() Result~SeqNum~
    }

    class Error {
        <<enumeration>>
        Io EmptyKey SequenceOverflow
        WalCorrupt SstCorrupt LockPoisoned Flush
    }

    Engine *-- EngineOptions : open_with
    Engine *-- Inner : Arc
    Inner *-- Snapshot
    Inner *-- Wal : Mutex
    Snapshot o-- MemTable : active / frozen
    Snapshot o-- SstReader : sstables
    Engine --> EngineLookup : get
    EngineLookup *-- EngineValue : Alive
    Engine --> Wal : put
    Engine --> MemTable : put / get
    Engine --> SstWriter : flush_memtable
    Wal --> WalOp
    Wal --> WalRecord
    MemTable --> Lookup
    MemTable --> Key
    MemTable --> Value
    MemTable --> SeqNum
    SstReader --> SstLookup
    SstReader --> Bloom : deserializado
    SstWriter --> Bloom : al escribir
    Engine ..> Error : Result
```

## Capas (sin UML)

```text
types (Key, Value, SeqNum, Error)
        ↑
memtable / wal / index::Bloom / sstable
        ↑
     engine
```

## Notas

- `Engine` es la fachada; el estado compartido vive en `Inner` + `Snapshot`.
- `Lookup` / `SstLookup` / `EngineLookup` hablan el mismo idioma: vivo / tombstone / ausente.
- El reader de SST guarda un `mmap`; el `get` del engine **copia** a `Value` para no atar lifetimes al `RwLock`.

Ver también: [flujograma](flujograma.md) · [diagrama de flujo](diagrama-de-flujo.md)
