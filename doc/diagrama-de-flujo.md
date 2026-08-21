# Diagrama de flujo

Flujos **paso a paso** con decisiones. Complementa el [flujograma](flujograma.md) (arquitectura) y el [diagrama de clases](diagrama-de-clases.md).

---

## 1. `put` (escritura durable)

```mermaid
flowchart TD
    A([put key, value]) --> B{key vacía?}
    B -->|sí| E1([Error EmptyKey])
    B -->|no| C[Asignar next_seq atómico]
    C --> D[Lock Mutex del WAL]
    D --> F[Wal::append Put + O_SYNC]
    F -->|falla| E2([Error WAL / Io])
    F -->|ok| G[MemTable activa.put]
    G --> H([Ok])
```

Orden fijo: **primero disco (WAL), después RAM**. Si el proceso muere entre F y G, el replay reconstruye la MemTable.

---

## 2. `get` (lectura)

```mermaid
flowchart TD
    A([get key]) --> B{key vacía?}
    B -->|sí| E1([Error EmptyKey])
    B -->|no| C[Leer Snapshot RwLock]
    C --> D[Buscar en MemTable activa]
    D --> D1{resultado}
    D1 -->|Alive| R1([Alive Mem — copia Value])
    D1 -->|Deleted| R2([Deleted — fin])
    D1 -->|Missing| E{¿hay inmutable?}
    E -->|sí| F[Buscar en MemTable frozen]
    F --> F1{resultado}
    F1 -->|Alive| R3([Alive Frozen])
    F1 -->|Deleted| R2
    F1 -->|Missing| G
    E -->|no| G[SSTables de más nueva a más vieja]
    G --> H{¿alguna conoce la clave?}
    H -->|Alive| R4([Alive Sst — copia Value])
    H -->|Deleted| R2
    H -->|ninguna| R5([Missing])
```

Un **Deleted** corta la búsqueda: no se mira disco más viejo (si no, “resucitaría” un put antiguo).

---

## 3. Flush en background

```mermaid
flowchart TD
    A([schedule_flush / flush]) --> B[wait_flush si hay job previo]
    B --> C{activa vacía?}
    C -->|sí y sin frozen| Z([Ok nada que hacer])
    C -->|no| D[write Snapshot]
    D --> E[active → frozen]
    E --> F[Nueva MemTable activa vacía]
    F --> G[Renombrar wal → wal.flush]
    G --> H[Crear wal nuevo]
    H --> I[Encolar Job::Flush al worker]
    I --> J([schedule Ok — put sigue])

    subgraph Worker["Hilo nvme-sst-flush"]
        W1[Recibir Job] --> W2[flush_memtable → .sst.tmp]
        W2 --> W3[rename → NNNNNN.sst]
        W3 --> W4[SstReader::open mmap]
        W4 --> W5[Push a sstables; frozen = None]
        W5 --> W6[Borrar wal.flush]
        W6 --> W7[Señal Condvar in_flight=false]
    end

    I -.-> W1
    W7 --> K{¿llamaron flush?}
    K -->|sí wait_flush| L([Ok flush completo])
    K -->|solo schedule| M([Caller no espera])
```

`flush()` = `schedule_flush` + `wait_flush` en bucle hasta vaciar activa e inmutable.

---

## 4. Lookup dentro de una SSTable

```mermaid
flowchart TD
    A([SstReader::get]) --> B{Bloom may_contain?}
    B -->|false| M([Missing])
    B -->|true| C[Búsqueda binaria en índice]
    C --> D{¿hay bloque candidato?}
    D -->|no| M
    D -->|sí| E[Leer bloque + validar CRC]
    E -->|CRC malo| Err([SstCorrupt])
    E -->|ok| F[Escanear registros ordenados]
    F --> G{clave}
    G -->|put| Alive([Alive slice mmap])
    G -->|tombstone| Del([Deleted])
    G -->|no está / pasó de largo| M
```

---

## 5. Apertura / recovery

```mermaid
flowchart TD
    A([Engine::open]) --> B[create_dir_all]
    B --> C[Cargar *.sst → SstReader]
    C --> D{existe wal.flush?}
    D -->|sí| E[Replay wal.flush → MemTable]
    D -->|no| F
    E --> F{existe wal?}
    F -->|sí| G[Replay wal → MemTable]
    F -->|no| H[Wal::create]
    G --> I[Si hubo wal.flush: checkpoint + WAL nuevo]
    H --> J[Arrancar worker]
    I --> J
    J --> K([Engine listo])
```

Ver también: [flujograma](flujograma.md) · [diagrama de clases](diagrama-de-clases.md)
