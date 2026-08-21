# Flujograma del sistema

Vista de **arquitectura y flujo de datos** de `nvme-state-db`: cómo se mueven las escrituras y lecturas entre piezas.

```mermaid
flowchart TB
    subgraph Cliente["Cliente / ejecutor"]
        API["Engine::put / get / delete / flush"]
    end

    subgraph Motor["Engine"]
        SEQ["AtomicU64 next_seq"]
        WALM["Mutex&lt;Wal&gt;"]
        SNAP["RwLock&lt;Snapshot&gt;"]
        WORK["Worker flush<br/>hilo nvme-sst-flush"]
        TX["Canal mpsc Job"]
    end

    subgraph Snapshot["Snapshot"]
        ACT["MemTable activa<br/>Arc"]
        FRZ["MemTable inmutable<br/>Option&lt;Arc&gt;"]
        SST["Vec&lt;SstReader&gt;<br/>viejas → nuevas"]
    end

    subgraph Disco["Directorio de datos"]
        WFILE["wal"]
        WFLUSH["wal.flush"]
        SSTF["000001.sst …"]
    end

    API --> SEQ
    API -->|"append O_DIRECT"| WALM
    API -->|"put / get"| SNAP
    SNAP --> ACT
    SNAP --> FRZ
    SNAP --> SST

    WALM <-->|"sync"| WFILE
    API -->|"schedule_flush"| TX
    TX --> WORK
    WORK -->|"flush_memtable"| SSTF
    WORK -->|"rota WAL"| WFLUSH
    WORK -->|"instala reader"| SST
    SST -->|"mmap"| SSTF
```

## Lectura rápida

| Dirección | Qué pasa |
|-----------|----------|
| **Write** | `put` → WAL en disco → MemTable activa |
| **Flush** | Activa se congela → worker escribe `.sst` → se suelta la inmutable |
| **Read** | Activa → inmutable (si hay) → SSTables de más nueva a más vieja |

## Relación con otros diagramas

- [Diagrama de clases](diagrama-de-clases.md) — tipos y composición
- [Diagrama de flujo](diagrama-de-flujo.md) — decisiones paso a paso (`put` / `get` / `flush`)
