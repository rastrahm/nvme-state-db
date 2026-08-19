//! Reader de SSTable: `mmap` + índice + Bloom, valores como slices del mapa.
//!
//! `mmap` pide al kernel que el archivo aparezca como un `&[u8]` en el espacio
//! de direcciones. Un `get` no hace `read` ni copia el valor a un `Vec`: apunta
//! al byte dentro del mapping. Por eso el lifetime de [`SstLookup::Alive`] está
//! atado a `&SstReader`: si el reader se cae, el mapping también.
//!
//! Camino caliente: Bloom (`false` → no está) → búsqueda binaria en el índice
//! → escanear el bloque (claves ordenadas) → slice del valor.

use crate::error::Error;
use crate::index::Bloom;
use crate::sstable::{
    kind_put, kind_tombstone, read_u32, read_u64, BlockHeader, IndexEntry, SstFooter,
    BLOCK_HEADER_SIZE, FOOTER_SIZE, INDEX_ENTRY_SIZE,
};
use crate::types::SeqNum;
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

// =============================================================================
// LOOKUP  (mismo idioma que MemTable: Alive / Deleted / Missing)
// =============================================================================

/// Resultado de un `get` sobre un `.sst` mapeado.
///
/// El valor vivo es un subslice del `mmap`: cero copias.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SstLookup<'a> {
    /// Put: `value` apunta al mapping (puede ser vacío).
    Alive {
        /// Seq del registro.
        seq: SeqNum,
        /// Bytes del valor dentro del `mmap`.
        value: &'a [u8],
    },
    /// Tombstone en este archivo.
    Deleted(SeqNum),
    /// Ni put ni tombstone (Bloom dijo no, o el bloque no la tiene).
    Missing,
}

impl<'a> SstLookup<'a> {
    /// Purpose: distingue ausencia de tombstone.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `true` solo en [`SstLookup::Missing`].
    #[inline(always)]
    pub fn is_missing(self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Purpose: valor vivo, si hay put.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `Some` solo en [`SstLookup::Alive`].
    #[inline(always)]
    pub fn value(self) -> Option<&'a [u8]> {
        match self {
            Self::Alive { value, .. } => Some(value),
            Self::Deleted(_) | Self::Missing => None,
        }
    }
}

// -----------------------------------------------------------------------------
// Índice en offsets (first-key sigue en el mmap, no se copia)
// -----------------------------------------------------------------------------

struct IndexSlot {
    first_key_off: usize,
    first_key_len: usize,
    block_offset: u64,
    block_len: u32,
}

// =============================================================================
// SstReader  (struct = mapping + metadatos; impl = open / get)
// =============================================================================

/// SSTable inmutable mapeada en memoria.
pub struct SstReader {
    mmap: Mmap,
    bloom: Bloom,
    index: Vec<IndexSlot>,
}

impl SstReader {
    /// Purpose: abre un `.sst`, lo mapea y valida footer / índice / Bloom.
    ///
    /// Inputs: `path` — archivo escrito por [`crate::sstable::SstWriter`].
    ///
    /// Returns: reader listo para `get`.
    ///
    /// # Safety (del `mmap`)
    ///
    /// El mapping asume que el archivo **no se trunca ni se reescribe** mientras
    /// viva el reader. Un `.sst` es inmutable; no lo abras en escritura.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = File::open(path.as_ref())?;
        // SAFETY: SST inmutable; `file` puede caerse tras el map en Unix.
        let mmap = unsafe { Mmap::map(&file)? };
        Self::from_mmap(mmap)
    }

    fn from_mmap(mmap: Mmap) -> Result<Self, Error> {
        if mmap.len() < FOOTER_SIZE {
            return Err(Error::SstCorrupt("archivo más corto que el footer"));
        }
        let footer_off = mmap.len() - FOOTER_SIZE;
        let footer = SstFooter::decode(&mmap[footer_off..])?;
        let index_bytes = slice_range(&mmap, footer.index_offset, footer.index_len)?;
        let bloom_bytes = slice_range(&mmap, footer.bloom_offset, footer.bloom_len)?;
        let bloom = Bloom::from_bytes(bloom_bytes)?;
        let index = parse_index(index_bytes, footer.index_offset)?;
        if index.len() as u64 != footer.block_count {
            return Err(Error::SstCorrupt("block_count no coincide con el índice"));
        }
        Ok(Self { mmap, bloom, index })
    }

    /// Purpose: busca `key` sin copiar el valor.
    ///
    /// Inputs: `self` — mapping vivo; `key` — bytes de búsqueda.
    ///
    /// Returns: [`SstLookup`] con slices atados a `self`.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Result<SstLookup<'_>, Error> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if !self.bloom.may_contain(key) {
            return Ok(SstLookup::Missing);
        }
        let Some(slot) = self.find_block(key) else {
            return Ok(SstLookup::Missing);
        };
        self.lookup_in_block(slot, key)
    }

    /// Primera clave del bloque `i`, vista sobre el mmap.
    #[inline(always)]
    fn first_key(&self, i: usize) -> &[u8] {
        let slot = &self.index[i];
        &self.mmap[slot.first_key_off..slot.first_key_off + slot.first_key_len]
    }

    /// Último bloque cuya first-key es `<= key` (búsqueda binaria).
    #[inline(always)]
    fn find_block(&self, key: &[u8]) -> Option<&IndexSlot> {
        if self.index.is_empty() {
            return None;
        }
        let mut lo = 0_usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.first_key(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            None
        } else {
            Some(&self.index[lo - 1])
        }
    }

    /// Purpose: busca `key` dentro de un bloque ya elegido por el índice.
    ///
    /// Inputs: `self` — mapping; `slot` — offset/len del bloque; `key` — bytes a comparar.
    ///
    /// Returns: lookup con slices atados a `'a` (`&self`). Error si el bloque está
    /// corrupto (rango, magic, CRC).
    fn lookup_in_block<'a>(&'a self, slot: &IndexSlot, key: &[u8]) -> Result<SstLookup<'a>, Error> {
        let start = usize_off(slot.block_offset)?;
        let end = start
            .checked_add(slot.block_len as usize)
            .ok_or(Error::SstCorrupt("block_len"))?;
        let block = self
            .mmap
            .get(start..end)
            .ok_or(Error::SstCorrupt("bloque fuera de rango"))?;
        let header = BlockHeader::decode(block)?;
        let payload_end = BLOCK_HEADER_SIZE
            .checked_add(header.payload_len as usize)
            .ok_or(Error::SstCorrupt("payload_len"))?;
        let payload = block
            .get(BLOCK_HEADER_SIZE..payload_end)
            .ok_or(Error::SstCorrupt("payload truncado"))?;
        if crc32fast::hash(payload) != header.crc32 {
            return Err(Error::SstCorrupt("CRC de bloque"));
        }
        scan_sorted_payload(payload, key)
    }
}

/// Purpose: convierte offset `u64` a `usize` (Linux x86_64).
fn usize_off(n: u64) -> Result<usize, Error> {
    usize::try_from(n).map_err(|_| Error::SstCorrupt("offset no cabe en usize"))
}

/// Purpose: toma un subslice del mapping en `[offset, offset+len)`.
///
/// Inputs: `mmap` — archivo mapeado; `offset` / `len` — rango on-disk (`u64`).
///
/// Returns: bytes dentro del mmap, o `SstCorrupt` si el rango no cabe.
fn slice_range(mmap: &Mmap, offset: u64, len: u64) -> Result<&[u8], Error> {
    let start = usize_off(offset)?;
    let n = usize_off(len)?;
    let end = start
        .checked_add(n)
        .ok_or(Error::SstCorrupt("rango overflow"))?;
    mmap.get(start..end)
        .ok_or(Error::SstCorrupt("rango fuera del mmap"))
}

/// Purpose: recorre el blob de índice y arma slots con offsets al mmap.
///
/// Inputs: `bytes` — subslice del índice; `index_file_offset` — dónde empieza
/// ese blob en el archivo (para calcular `first_key_off` absoluto).
///
/// Returns: un `IndexSlot` por bloque (first-key **no** se copia; solo offset/len).
fn parse_index(bytes: &[u8], index_file_offset: u64) -> Result<Vec<IndexSlot>, Error> {
    let mut slots = Vec::new();
    let mut off = 0_usize;
    let base = usize_off(index_file_offset)?;
    while off < bytes.len() {
        let rest = bytes
            .get(off..)
            .ok_or(Error::SstCorrupt("índice truncado"))?;
        let entry = IndexEntry::decode(rest)?;
        off += INDEX_ENTRY_SIZE;
        let klen = entry.first_key_len as usize;
        let key_end = off
            .checked_add(klen)
            .ok_or(Error::SstCorrupt("first_key_len"))?;
        if key_end > bytes.len() {
            return Err(Error::SstCorrupt("first-key truncada"));
        }
        let first_key_off = base
            .checked_add(off)
            .ok_or(Error::SstCorrupt("offset de first-key"))?;
        slots.push(IndexSlot {
            first_key_off,
            first_key_len: klen,
            block_offset: entry.block_offset,
            block_len: entry.block_len,
        });
        off = key_end;
    }
    Ok(slots)
}

/// Recorre registros ordenados; `value`/`key` son subslices de `payload`.
fn scan_sorted_payload<'a>(payload: &'a [u8], key: &[u8]) -> Result<SstLookup<'a>, Error> {
    let mut off = 0_usize;
    while off < payload.len() {
        if payload.len().saturating_sub(off) < 17 {
            return Err(Error::SstCorrupt("registro truncado"));
        }
        let key_len = read_u32(payload, off)? as usize;
        let value_len = read_u32(payload, off + 4)? as usize;
        let seq = SeqNum::new(read_u64(payload, off + 8)?);
        let kind = payload[off + 16];
        off += 17;
        let key_end = off
            .checked_add(key_len)
            .ok_or(Error::SstCorrupt("clave de registro"))?;
        let val_end = key_end
            .checked_add(value_len)
            .ok_or(Error::SstCorrupt("valor de registro"))?;
        let rec_key = payload
            .get(off..key_end)
            .ok_or(Error::SstCorrupt("clave truncada"))?;
        let rec_val = payload
            .get(key_end..val_end)
            .ok_or(Error::SstCorrupt("valor truncado"))?;
        off = val_end;

        if rec_key == key {
            if kind == kind_put() {
                return Ok(SstLookup::Alive {
                    seq,
                    value: rec_val,
                });
            }
            if kind == kind_tombstone() {
                return Ok(SstLookup::Deleted(seq));
            }
            return Err(Error::SstCorrupt("kind de registro desconocido"));
        }
        if rec_key > key {
            return Ok(SstLookup::Missing);
        }
    }
    Ok(SstLookup::Missing)
}

#[cfg(test)]
mod tests {
    use super::{SstLookup, SstReader};
    use crate::memtable::MemTable;
    use crate::sstable::{flush_memtable, flush_memtable_with, SstWriteOptions};
    use crate::types::{Key, SeqNum, Value};
    use std::fs;
    use std::path::PathBuf;

    fn temp_sst() -> (tempfile::TempDir, PathBuf) {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("sst-tests");
        fs::create_dir_all(&base).expect("mkdir");
        let dir = tempfile::TempDir::new_in(&base).expect("tempdir");
        let path = dir.path().join("table.sst");
        (dir, path)
    }

    fn key(bytes: &[u8]) -> Key {
        Key::new(bytes).expect("key")
    }

    #[test]
    fn round_trip_put_and_missing() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(64 * 1024);
        table.put(key(b"alpha"), Value::new(b"uno"), SeqNum::new(1));
        table.put(key(b"beta"), Value::new(b""), SeqNum::new(2));
        flush_memtable(&table, &path).expect("flush");

        let reader = SstReader::open(&path).expect("open");
        match reader.get(b"alpha").expect("get") {
            SstLookup::Alive { seq, value } => {
                assert_eq!(seq.get(), 1);
                assert_eq!(value, b"uno");
            }
            other => panic!("esperado Alive, {other:?}"),
        }
        match reader.get(b"beta").expect("get") {
            SstLookup::Alive { value, .. } => assert!(value.is_empty()),
            other => panic!("valor vacío debe ser Alive, {other:?}"),
        }
        assert!(reader.get(b"zzz").expect("miss").is_missing());
        assert!(reader.get(b"aaa").expect("antes").is_missing());
    }

    #[test]
    fn tombstone_is_deleted_not_missing() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(1024);
        table.delete(key(b"gone"), SeqNum::new(9));
        flush_memtable(&table, &path).expect("flush");
        let reader = SstReader::open(&path).expect("open");
        assert_eq!(
            reader.get(b"gone").expect("get"),
            SstLookup::Deleted(SeqNum::new(9))
        );
    }

    #[test]
    fn empty_sst_get_is_missing() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(1024);
        flush_memtable(&table, &path).expect("flush");
        let reader = SstReader::open(&path).expect("open");
        assert!(reader.get(b"k").expect("get").is_missing());
    }

    #[test]
    fn value_slice_is_inside_mmap() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(1024);
        table.put(key(b"k"), Value::new(b"payload"), SeqNum::new(1));
        flush_memtable(&table, &path).expect("flush");
        let reader = SstReader::open(&path).expect("open");
        let lookup = reader.get(b"k").expect("get");
        let value = lookup.value().expect("alive");
        let map = reader.mmap.as_ref();
        let start = map.as_ptr() as usize;
        let end = start + map.len();
        let p = value.as_ptr() as usize;
        assert!(
            p >= start && p + value.len() <= end,
            "el valor debe vivir en el mmap"
        );
    }

    #[test]
    fn multi_block_gets_every_key() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(64 * 1024);
        for i in 0..8_u64 {
            let k = Key::new(format!("k{i}").as_bytes()).expect("k");
            table.put(
                k,
                Value::new(format!("v{i}").as_bytes()),
                SeqNum::new(i + 1),
            );
        }
        let opts = SstWriteOptions {
            block_size: 40,
            bits_per_key: 10,
        };
        let meta = flush_memtable_with(&table, &path, opts).expect("flush");
        assert!(meta.block_count >= 2);
        let reader = SstReader::open(&path).expect("open");
        for i in 0..8_u64 {
            let k = format!("k{i}");
            let v = format!("v{i}");
            match reader.get(k.as_bytes()).expect("get") {
                SstLookup::Alive { seq, value } => {
                    assert_eq!(seq.get(), i + 1);
                    assert_eq!(value, v.as_bytes());
                }
                other => panic!("k{i}: {other:?}"),
            }
        }
        assert!(reader.get(b"k9").expect("miss").is_missing());
    }

    #[test]
    fn empty_key_is_error() {
        let (_dir, path) = temp_sst();
        flush_memtable(&MemTable::new(64), &path).expect("flush");
        let reader = SstReader::open(&path).expect("open");
        assert!(matches!(reader.get(b""), Err(crate::Error::EmptyKey)));
    }

    #[test]
    fn open_rejects_truncated_file() {
        let (_dir, path) = temp_sst();
        fs::write(&path, b"corto").expect("write");
        assert!(SstReader::open(&path).is_err());
    }
}
