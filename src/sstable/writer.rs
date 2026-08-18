//! Flush: MemTable ordenada → archivo `.sst` (bloques + índice + Bloom + footer).
//!
//! El writer es de un solo hilo (`&mut self`). La MemTable sigue sirviendo
//! `get`/`put` en paralelo; el motor (fase 8) rotará a una tabla inmutable
//! antes de llamar aquí.

use crate::error::Error;
use crate::index::Bloom;
use crate::memtable::{MemTable, MemValue};
use crate::sstable::{
    encode_record, kind_put, kind_tombstone, BlockHeader, IndexEntry, SstFooter, BLOCK_HEADER_SIZE,
    BLOCK_MAGIC, DEFAULT_BLOCK_SIZE, FOOTER_SIZE, SST_MAGIC, SST_VERSION,
};
use crate::types::{Key, SeqNum};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(test)]
use std::io::{Read, Seek, SeekFrom};

/// Opciones del flush.
#[derive(Clone, Copy, Debug)]
pub struct SstWriteOptions {
    /// Tamaño objetivo del payload de cada bloque de datos, en bytes.
    pub block_size: usize,
    /// Densidad del Bloom (1..=16).
    pub bits_per_key: u8,
}

impl Default for SstWriteOptions {
    /// Purpose: 4K por bloque y 10 bits/clave (~1% de falsos positivos).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: opciones por defecto.
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            bits_per_key: 10,
        }
    }
}

/// Resumen tras un flush exitoso.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstMeta {
    /// Ruta del archivo escrito.
    pub path: PathBuf,
    /// Puts + tombstones.
    pub entry_count: u64,
    /// Bloques de datos.
    pub block_count: u64,
    /// Tamaño total, incluido el footer.
    pub file_size: u64,
}

// -----------------------------------------------------------------------------
// SstWriter  (struct = estado del flush, impl = add / finish)
// -----------------------------------------------------------------------------

/// Acumulador de una SSTable en construcción.
pub struct SstWriter {
    path: PathBuf,
    file: File,
    offset: u64,
    block_size: usize,
    payload: Vec<u8>,
    records_in_block: u32,
    first_key: Option<Vec<u8>>,
    index: Vec<(Vec<u8>, IndexEntry)>,
    bloom: Bloom,
    entry_count: u64,
    min_seq: Option<u64>,
    max_seq: Option<u64>,
}

impl SstWriter {
    /// Purpose: crea el archivo y un Bloom dimensionado a `expected_keys`.
    ///
    /// Inputs: `path` — destino `.sst`; `expected_keys` — para el Bloom.
    ///
    /// Returns: writer vacío listo para [`SstWriter::add`].
    pub fn create(path: impl AsRef<Path>, expected_keys: usize) -> Result<Self, Error> {
        Self::create_with(path, expected_keys, SstWriteOptions::default())
    }

    /// Purpose: como [`SstWriter::create`] con tamaño de bloque configurable.
    ///
    /// Inputs: `path`, `expected_keys`, `opts`.
    ///
    /// Returns: writer vacío.
    pub fn create_with(
        path: impl AsRef<Path>,
        expected_keys: usize,
        opts: SstWriteOptions,
    ) -> Result<Self, Error> {
        if opts.block_size == 0 {
            return Err(Error::SstCorrupt("block_size no puede ser 0"));
        }
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let bloom = Bloom::new(expected_keys.max(1), opts.bits_per_key)?;
        Ok(Self {
            path,
            file,
            offset: 0,
            block_size: opts.block_size,
            payload: Vec::new(),
            records_in_block: 0,
            first_key: None,
            index: Vec::new(),
            bloom,
            entry_count: 0,
            min_seq: None,
            max_seq: None,
        })
    }

    /// Purpose: añade un registro (put o tombstone), flusheando el bloque si llena.
    ///
    /// Inputs: `key`, `seq`, `value` — de la MemTable, ya en orden.
    ///
    /// Returns: `Ok` si quedó en el buffer o se escribió un bloque.
    pub fn add(&mut self, key: &Key, seq: SeqNum, value: MemValue<'_>) -> Result<(), Error> {
        let seq_raw = seq.get();
        let (kind, val_bytes) = match value {
            MemValue::Put(v) => (kind_put(), v.as_bytes()),
            MemValue::Tombstone => (kind_tombstone(), &[][..]),
        };
        let rec_len = 17_usize
            .saturating_add(key.len())
            .saturating_add(val_bytes.len());
        if !self.payload.is_empty() && self.payload.len().saturating_add(rec_len) > self.block_size
        {
            self.flush_block()?;
        }
        if self.payload.is_empty() {
            self.first_key = Some(key.as_bytes().to_vec());
        }
        encode_record(&mut self.payload, key.as_bytes(), seq_raw, kind, val_bytes)?;
        self.records_in_block = self.records_in_block.saturating_add(1);
        self.bloom.insert(key.as_bytes());
        self.entry_count = self.entry_count.saturating_add(1);
        self.min_seq = Some(self.min_seq.map_or(seq_raw, |m| m.min(seq_raw)));
        self.max_seq = Some(self.max_seq.map_or(seq_raw, |m| m.max(seq_raw)));
        Ok(())
    }

    /// Purpose: vuelca el último bloque, índice, Bloom y footer; `fsync`.
    ///
    /// Inputs: `self` — se consume; no se puede seguir haciendo `add`.
    ///
    /// Returns: metadatos del archivo cerrado.
    pub fn finish(mut self) -> Result<SstMeta, Error> {
        if !self.payload.is_empty() {
            self.flush_block()?;
        }
        let index_offset = self.offset;
        let mut index_buf = Vec::new();
        for (first_key, entry) in &self.index {
            index_buf.extend_from_slice(&entry.encode()?);
            index_buf.extend_from_slice(first_key);
        }
        self.file.write_all(&index_buf)?;
        self.offset = self.offset.saturating_add(index_buf.len() as u64);
        let index_len = index_buf.len() as u64;

        let bloom_offset = self.offset;
        let bloom_bytes = self.bloom.to_bytes()?;
        self.file.write_all(&bloom_bytes)?;
        self.offset = self.offset.saturating_add(bloom_bytes.len() as u64);
        let bloom_len = bloom_bytes.len() as u64;

        let footer = SstFooter {
            magic: SST_MAGIC,
            version: SST_VERSION,
            flags: 0,
            index_offset,
            index_len,
            bloom_offset,
            bloom_len,
            entry_count: self.entry_count,
            block_count: self.index.len() as u64,
            min_seq: self.min_seq.unwrap_or(0),
            max_seq: self.max_seq.unwrap_or(0),
            checksum: 0,
            _pad: [0; 52],
        };
        let footer_bytes = footer.encode()?;
        self.file.write_all(&footer_bytes)?;
        self.offset = self.offset.saturating_add(FOOTER_SIZE as u64);
        self.file.sync_all()?;
        Ok(SstMeta {
            path: self.path,
            entry_count: self.entry_count,
            block_count: footer.block_count,
            file_size: self.offset,
        })
    }

    /// Purpose: escribe BlockHeader + payload y anota el índice.
    fn flush_block(&mut self) -> Result<(), Error> {
        let payload_len =
            u32::try_from(self.payload.len()).map_err(|_| Error::SstRecordTooLarge {
                size: self.payload.len(),
            })?;
        let crc = crc32fast::hash(&self.payload);
        let header = BlockHeader {
            magic: BLOCK_MAGIC,
            crc32: crc,
            payload_len,
            record_count: self.records_in_block,
        };
        let header_bytes = header.encode()?;
        let block_len = u32::try_from(BLOCK_HEADER_SIZE.saturating_add(self.payload.len()))
            .map_err(|_| Error::SstRecordTooLarge {
                size: BLOCK_HEADER_SIZE.saturating_add(self.payload.len()),
            })?;
        let first_key = self
            .first_key
            .take()
            .ok_or(Error::SstCorrupt("bloque sin first-key"))?;
        let first_key_len =
            u32::try_from(first_key.len()).map_err(|_| Error::SstRecordTooLarge {
                size: first_key.len(),
            })?;
        self.file.write_all(&header_bytes)?;
        self.file.write_all(&self.payload)?;
        self.index.push((
            first_key,
            IndexEntry {
                block_offset: self.offset,
                block_len,
                first_key_len,
            },
        ));
        self.offset = self.offset.saturating_add(u64::from(block_len));
        self.payload.clear();
        self.records_in_block = 0;
        Ok(())
    }
}

/// Purpose: vuelca toda la MemTable (orden lexicográfico) a `path`.
///
/// Inputs: `table` — se lee con `iter`, no se modifica; `path` — archivo destino.
///
/// Returns: [`SstMeta`] del `.sst` cerrado.
pub fn flush_memtable(table: &MemTable, path: impl AsRef<Path>) -> Result<SstMeta, Error> {
    flush_memtable_with(table, path, SstWriteOptions::default())
}

/// Purpose: como [`flush_memtable`] con opciones (tests de varios bloques).
///
/// Inputs: `table`, `path`, `opts`.
///
/// Returns: metadatos.
pub fn flush_memtable_with(
    table: &MemTable,
    path: impl AsRef<Path>,
    opts: SstWriteOptions,
) -> Result<SstMeta, Error> {
    let mut writer = SstWriter::create_with(path, table.len(), opts)?;
    for item in table.iter() {
        writer.add(item.key(), item.seq(), item.mem_value())?;
    }
    writer.finish()
}

/// Purpose: lee un rango del archivo a un `Vec` (solo para tests / inspector).
#[cfg(test)]
fn read_at(path: &Path, offset: u64, len: usize) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0_u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::{flush_memtable, flush_memtable_with, read_at, SstWriteOptions};
    use crate::index::Bloom;
    use crate::memtable::MemTable;
    use crate::sstable::{
        decode_index, decode_records, BlockHeader, SstFooter, BLOCK_HEADER_SIZE, FOOTER_SIZE,
        SST_MAGIC,
    };
    use crate::types::{Key, SeqNum, Value};
    use std::fs;
    use std::path::PathBuf;

    fn key(bytes: &[u8]) -> Key {
        Key::new(bytes).expect("clave")
    }

    fn temp_sst() -> (tempfile::TempDir, PathBuf) {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("sst-tests");
        fs::create_dir_all(&base).expect("mkdir");
        let dir = tempfile::TempDir::new_in(&base).expect("tempdir");
        let path = dir.path().join("table.sst");
        (dir, path)
    }

    #[test]
    fn flush_empty_memtable_writes_valid_footer() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(1024);
        let meta = flush_memtable(&table, &path).expect("flush");
        assert_eq!(meta.entry_count, 0);
        assert_eq!(meta.block_count, 0);
        assert!(meta.file_size >= FOOTER_SIZE as u64);
        let footer = SstFooter::read_from_file(&path).expect("footer");
        assert_eq!(footer.magic, SST_MAGIC);
        assert_eq!(footer.entry_count, 0);
        assert_eq!(footer.block_count, 0);
        let bloom = Bloom::from_bytes(
            &read_at(&path, footer.bloom_offset, footer.bloom_len as usize).expect("bloom"),
        )
        .expect("bloom parse");
        assert!(!bloom.may_contain(b"nada"));
    }

    #[test]
    fn flush_puts_and_tombstone_roundtrip_structure() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(64 * 1024);
        table.put(key(b"c"), Value::new(b"3"), SeqNum::new(1));
        table.put(key(b"a"), Value::new(b"1"), SeqNum::new(2));
        table.delete(key(b"b"), SeqNum::new(3));
        let meta = flush_memtable(&table, &path).expect("flush");
        assert_eq!(meta.entry_count, 3);
        assert_eq!(meta.block_count, 1);

        let footer = SstFooter::read_from_file(&path).expect("footer");
        assert_eq!(footer.min_seq, 1);
        assert_eq!(footer.max_seq, 3);

        let bloom_bytes =
            read_at(&path, footer.bloom_offset, footer.bloom_len as usize).expect("b");
        let bloom = Bloom::from_bytes(&bloom_bytes).expect("bloom");
        assert!(bloom.may_contain(b"a"));
        assert!(bloom.may_contain(b"b"));
        assert!(bloom.may_contain(b"c"));
        assert!(!bloom.may_contain(b"zzz"));

        let index_bytes =
            read_at(&path, footer.index_offset, footer.index_len as usize).expect("idx");
        let index = decode_index(&index_bytes).expect("index");
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].0, b"a");

        let (first_key, entry) = &index[0];
        assert_eq!(first_key.as_slice(), b"a");
        let block = read_at(&path, entry.block_offset, entry.block_len as usize).expect("blk");
        let header = BlockHeader::decode(&block).expect("hdr");
        assert_eq!(header.record_count, 3);
        let payload = &block[BLOCK_HEADER_SIZE..];
        assert_eq!(crc32fast::hash(payload), header.crc32);
        let recs = decode_records(payload).expect("recs");
        assert_eq!(recs[0].key, b"a");
        assert_eq!(recs[1].key, b"b");
        assert_eq!(recs[2].key, b"c");
        assert!(recs[0].value.is_some());
        assert!(recs[1].value.is_none());
        assert_eq!(recs[2].value.as_deref(), Some(b"3".as_ref()));
    }

    #[test]
    fn small_blocks_produce_multiple_index_entries() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(64 * 1024);
        for i in 0..8_u64 {
            let k = Key::new(format!("k{i}").as_bytes()).expect("k");
            table.put(k, Value::new(b"xxxx"), SeqNum::new(i + 1));
        }
        let opts = SstWriteOptions {
            block_size: 40,
            bits_per_key: 10,
        };
        let meta = flush_memtable_with(&table, &path, opts).expect("flush");
        assert!(meta.block_count >= 2, "block_count={}", meta.block_count);
        let footer = SstFooter::read_from_file(&path).expect("footer");
        let index = decode_index(
            &read_at(&path, footer.index_offset, footer.index_len as usize).expect("idx"),
        )
        .expect("index");
        assert_eq!(index.len() as u64, meta.block_count);
        let mut prev = Vec::new();
        for (first, _) in &index {
            assert!(first >= &prev, "índice desordenado");
            prev = first.clone();
        }
    }

    #[test]
    fn file_ends_with_footer_magic() {
        let (_dir, path) = temp_sst();
        let table = MemTable::new(1024);
        table.put(key(b"k"), Value::new(b"v"), SeqNum::new(1));
        let meta = flush_memtable(&table, &path).expect("flush");
        let raw = fs::read(&path).expect("read");
        assert_eq!(raw.len() as u64, meta.file_size);
        let tail = &raw[raw.len() - FOOTER_SIZE..];
        assert_eq!(&tail[0..4], &SST_MAGIC);
        SstFooter::decode(tail).expect("decode");
    }
}
