//! Índice en RAM y on-disk: hoy el [Bloom filter](bloom::Bloom).
//!
//! En la fase 5 el `IndexEntry` de las SSTables vivirá aquí también.
//! El Bloom se consulta **antes** de tocar mmap: si dice “no”, la clave
//! no está (cero falsos negativos) y nos ahorramos un I/O.

pub mod bloom;

pub use bloom::Bloom;
