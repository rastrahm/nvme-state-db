//! `nvme-state-db`: motor KV de baja latencia para estado blockchain en NVMe.
//!
//! Esta crate es la **biblioteca**. Los errores públicos son [`Error`] (`thiserror`):
//! un enum que el llamador puede `match` sin asignar. El binario (`src/main.rs`)
//! no vive aquí: usa `anyhow` para contexto de aplicación.
//!
//! Fase 2: tipos de dominio, errores y MemTable lock-free ([`MemTable`]).

#![deny(missing_docs)]

pub mod error;
pub mod memtable;
pub mod types;

pub use error::Error;
pub use memtable::{Lookup, MemTable, MemValue};
pub use types::{Key, SeqNum, Value};

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
