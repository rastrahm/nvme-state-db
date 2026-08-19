//! `nvme-state-db`: motor KV de baja latencia para estado blockchain en NVMe.
//!
//! Esta crate es la **biblioteca**. Los errores públicos son [`Error`] (`thiserror`):
//! un enum que el llamador puede `match` sin asignar. El binario (`src/main.rs`)
//! no vive aquí: usa `anyhow` para contexto de aplicación.
//!
//! Fase 9: motor completo y benches de latencia / IOPS / write amplification.

#![deny(missing_docs)]

pub mod engine;
pub mod error;
pub mod index;
pub mod memtable;
pub mod sstable;
pub mod types;
pub mod wal;

pub use engine::{Engine, EngineLookup, EngineOptions, EngineValue};
pub use error::Error;
pub use index::Bloom;
pub use memtable::{Lookup, MemTable, MemValue};
pub use sstable::{flush_memtable, SstFooter, SstLookup, SstMeta, SstReader, SstWriter};
pub use types::{Key, SeqNum, Value};
pub use wal::{Wal, WalOp, WalRecord, WAL_ALIGN};

#[cfg(test)]
mod tests {
    use super::{Error, Key, Lookup, MemTable, SeqNum, Value};

    #[test]
    fn public_api_reexports_compile() {
        let key = Key::new(b"k").expect("clave");
        let value = Value::new(b"v");
        let seq = SeqNum::ZERO;
        assert_eq!(key.as_bytes(), b"k");
        assert_eq!(value.as_bytes(), b"v");
        assert_eq!(seq.get(), 0);
        let _err: Error = Error::EmptyKey;
        let table = MemTable::new(64);
        assert!(table.get(b"k").is_missing());
        let _lookup: Lookup<'_> = Lookup::Missing;
    }
}
