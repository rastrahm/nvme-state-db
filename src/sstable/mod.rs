//! SSTable: archivo inmutable, claves **ordenadas**, pensado para `mmap`.
//!
//! El writer (fase 5) vuelca una MemTable. El reader (fase 6) mapeará el mismo
//! layout. Escribir ordenado permite búsqueda binaria por índice de bloques;
//! escribir en bloques de ~4K limita cuánto hay que tocar en un `get`.
//!
//! **Write amplification (aquí):** cada flush copia todas las claves de RAM a
//! un archivo nuevo. Más adelante la compactación volverá a copiar SSTables
//! viejas a otras nuevas. No se actualiza un valor in-place.

mod writer;

pub use writer::{flush_memtable, SstMeta, SstWriteOptions, SstWriter};

use crate::error::Error;

/// Magic del archivo (footer).
pub const SST_MAGIC: [u8; 4] = *b"SST1";
/// Magic de un bloque de datos.
pub const BLOCK_MAGIC: [u8; 4] = *b"BLK1";
/// Versión de formato.
pub const SST_VERSION: u16 = 1;
/// Bytes de [`BlockHeader`].
pub const BLOCK_HEADER_SIZE: usize = 16;
/// Bytes de [`IndexEntry`] (la clave va a continuación).
pub const INDEX_ENTRY_SIZE: usize = 16;
/// Bytes de [`SstFooter`], siempre al final del archivo.
pub const FOOTER_SIZE: usize = 128;
/// Tamaño objetivo de bloque de datos (payload), 4K.
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

const KIND_PUT: u8 = 1;
const KIND_TOMBSTONE: u8 = 2;

// =============================================================================
// TIPOS ON-DISK  (repr(C) = layout fijo; no son clases)
// =============================================================================

/// Cabecera de un bloque de datos. 16 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockHeader {
    /// `BLK1`.
    pub magic: [u8; 4],
    /// CRC32 del payload (lo que sigue a esta cabecera).
    pub crc32: u32,
    /// Bytes de payload.
    pub payload_len: u32,
    /// Número de registros en el bloque.
    pub record_count: u32,
}

const _: () = assert!(std::mem::size_of::<BlockHeader>() == BLOCK_HEADER_SIZE);

/// Puntero a un bloque de datos. 16 bytes; la first-key va justo después.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexEntry {
    /// Offset del [`BlockHeader`] desde el inicio del archivo.
    pub block_offset: u64,
    /// Tamaño de cabecera + payload, en bytes.
    pub block_len: u32,
    /// Bytes de la primera clave del bloque, que siguen a esta struct.
    pub first_key_len: u32,
}

const _: () = assert!(std::mem::size_of::<IndexEntry>() == INDEX_ENTRY_SIZE);

/// Cola fija del `.sst` (128 bytes). El reader la busca en `len - 128`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SstFooter {
    /// `SST1`.
    pub magic: [u8; 4],
    /// Versión de formato.
    pub version: u16,
    /// Reservado.
    pub flags: u16,
    /// Offset del índice.
    pub index_offset: u64,
    /// Longitud del índice en bytes.
    pub index_len: u64,
    /// Offset del blob Bloom.
    pub bloom_offset: u64,
    /// Longitud del Bloom.
    pub bloom_len: u64,
    /// Registros (puts + tombstones).
    pub entry_count: u64,
    /// Bloques de datos.
    pub block_count: u64,
    /// Seq mínimo visto (0 si vacío).
    pub min_seq: u64,
    /// Seq máximo visto (0 si vacío).
    pub max_seq: u64,
    /// CRC32 de los primeros 72 bytes de este footer.
    pub checksum: u32,
    /// Relleno hasta 128.
    pub _pad: [u8; 52],
}

const _: () = assert!(std::mem::size_of::<SstFooter>() == FOOTER_SIZE);

impl SstFooter {
    /// Purpose: serializa el footer a 128 bytes, calculando el CRC.
    ///
    /// Inputs: `self` — campos ya rellenos (`checksum` se ignora y se recalcula).
    ///
    /// Returns: página de footer lista para `write`.
    pub fn encode(&self) -> Result<[u8; FOOTER_SIZE], Error> {
        let mut buf = [0_u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        write_u16(&mut buf, 4, self.version)?;
        write_u16(&mut buf, 6, self.flags)?;
        write_u64(&mut buf, 8, self.index_offset)?;
        write_u64(&mut buf, 16, self.index_len)?;
        write_u64(&mut buf, 24, self.bloom_offset)?;
        write_u64(&mut buf, 32, self.bloom_len)?;
        write_u64(&mut buf, 40, self.entry_count)?;
        write_u64(&mut buf, 48, self.block_count)?;
        write_u64(&mut buf, 56, self.min_seq)?;
        write_u64(&mut buf, 64, self.max_seq)?;
        let crc = crc32fast::hash(&buf[0..72]);
        write_u32(&mut buf, 72, crc)?;
        Ok(buf)
    }

    /// Purpose: parsea 128 bytes de cola y valida magic, versión y CRC.
    ///
    /// Inputs: `bytes` — últimos [`FOOTER_SIZE`] bytes del archivo.
    ///
    /// Returns: footer, o [`Error::SstCorrupt`].
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < FOOTER_SIZE {
            return Err(Error::SstCorrupt("footer truncado"));
        }
        if bytes[0..4] != SST_MAGIC {
            return Err(Error::SstCorrupt("magic de footer"));
        }
        let version = read_u16(bytes, 4)?;
        if version != SST_VERSION {
            return Err(Error::SstCorrupt("versión de SST"));
        }
        let got = read_u32(bytes, 72)?;
        let expect = crc32fast::hash(&bytes[0..72]);
        if got != expect {
            return Err(Error::SstCorrupt("checksum de footer"));
        }
        Ok(Self {
            magic: SST_MAGIC,
            version,
            flags: read_u16(bytes, 6)?,
            index_offset: read_u64(bytes, 8)?,
            index_len: read_u64(bytes, 16)?,
            bloom_offset: read_u64(bytes, 24)?,
            bloom_len: read_u64(bytes, 32)?,
            entry_count: read_u64(bytes, 40)?,
            block_count: read_u64(bytes, 48)?,
            min_seq: read_u64(bytes, 56)?,
            max_seq: read_u64(bytes, 64)?,
            checksum: got,
            _pad: [0; 52],
        })
    }

    /// Purpose: lee el footer desde el final de un archivo `.sst`.
    ///
    /// Inputs: `path` — SSTable ya cerrada.
    ///
    /// Returns: footer validado.
    pub fn read_from_file(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path.as_ref())?;
        let len = file.metadata()?.len();
        if len < FOOTER_SIZE as u64 {
            return Err(Error::SstCorrupt("archivo más corto que el footer"));
        }
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf = [0_u8; FOOTER_SIZE];
        file.read_exact(&mut buf)?;
        Self::decode(&buf)
    }
}

impl BlockHeader {
    /// Purpose: serializa 16 bytes de cabecera de bloque.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: array fijo.
    pub fn encode(&self) -> Result<[u8; BLOCK_HEADER_SIZE], Error> {
        let mut buf = [0_u8; BLOCK_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        write_u32(&mut buf, 4, self.crc32)?;
        write_u32(&mut buf, 8, self.payload_len)?;
        write_u32(&mut buf, 12, self.record_count)?;
        Ok(buf)
    }

    /// Purpose: parsea una cabecera de bloque.
    ///
    /// Inputs: `bytes` — al menos 16 bytes.
    ///
    /// Returns: header, o error si el magic no es `BLK1`.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < BLOCK_HEADER_SIZE {
            return Err(Error::SstCorrupt("BlockHeader truncada"));
        }
        if bytes[0..4] != BLOCK_MAGIC {
            return Err(Error::SstCorrupt("magic de bloque"));
        }
        Ok(Self {
            magic: BLOCK_MAGIC,
            crc32: read_u32(bytes, 4)?,
            payload_len: read_u32(bytes, 8)?,
            record_count: read_u32(bytes, 12)?,
        })
    }
}

impl IndexEntry {
    /// Purpose: serializa los 16 bytes fijos (sin la clave).
    ///
    /// Inputs: `self`.
    ///
    /// Returns: array fijo.
    pub fn encode(&self) -> Result<[u8; INDEX_ENTRY_SIZE], Error> {
        let mut buf = [0_u8; INDEX_ENTRY_SIZE];
        write_u64(&mut buf, 0, self.block_offset)?;
        write_u32(&mut buf, 8, self.block_len)?;
        write_u32(&mut buf, 12, self.first_key_len)?;
        Ok(buf)
    }

    /// Purpose: parsea 16 bytes de índice.
    ///
    /// Inputs: `bytes`.
    ///
    /// Returns: entrada; la clave hay que leerla aparte.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < INDEX_ENTRY_SIZE {
            return Err(Error::SstCorrupt("IndexEntry truncada"));
        }
        Ok(Self {
            block_offset: read_u64(bytes, 0)?,
            block_len: read_u32(bytes, 8)?,
            first_key_len: read_u32(bytes, 12)?,
        })
    }
}

/// Purpose: recorre el blob de índice (entrada + first-key) en orden.
///
/// Inputs: `bytes` — `footer.index_len` bytes desde `index_offset`.
///
/// Returns: pares (first_key, entrada).
pub fn decode_index(bytes: &[u8]) -> Result<Vec<(Vec<u8>, IndexEntry)>, Error> {
    let mut out = Vec::new();
    let mut off = 0_usize;
    while off < bytes.len() {
        let rest = bytes
            .get(off..)
            .ok_or(Error::SstCorrupt("índice fuera de rango"))?;
        let entry = IndexEntry::decode(rest)?;
        off = off
            .checked_add(INDEX_ENTRY_SIZE)
            .ok_or(Error::SstCorrupt("offset de índice"))?;
        let end = off
            .checked_add(entry.first_key_len as usize)
            .ok_or(Error::SstCorrupt("clave de índice"))?;
        let key = bytes
            .get(off..end)
            .ok_or(Error::SstCorrupt("first-key truncada"))?
            .to_vec();
        off = end;
        out.push((key, entry));
    }
    Ok(out)
}

pub(crate) fn encode_record(
    dst: &mut Vec<u8>,
    key: &[u8],
    seq: u64,
    kind: u8,
    value: &[u8],
) -> Result<(), Error> {
    let key_len = u32_len(key.len())?;
    let value_len = u32_len(value.len())?;
    dst.extend_from_slice(&key_len.to_le_bytes());
    dst.extend_from_slice(&value_len.to_le_bytes());
    dst.extend_from_slice(&seq.to_le_bytes());
    dst.push(kind);
    dst.extend_from_slice(key);
    dst.extend_from_slice(value);
    Ok(())
}

pub(crate) fn kind_put() -> u8 {
    KIND_PUT
}

pub(crate) fn kind_tombstone() -> u8 {
    KIND_TOMBSTONE
}

/// Registro decodificado desde un bloque (copia; el reader de la fase 6 no copiará).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRecord {
    /// Clave.
    pub key: Vec<u8>,
    /// Secuencia.
    pub seq: u64,
    /// `Some` si es put; `None` si es tombstone.
    pub value: Option<Vec<u8>>,
}

/// Purpose: decodifica el payload de un bloque (sin la BlockHeader).
///
/// Inputs: `payload` — bytes cuyo CRC ya se validó (o tests).
///
/// Returns: registros en orden; `value == None` es tombstone.
pub fn decode_records(payload: &[u8]) -> Result<Vec<DecodedRecord>, Error> {
    let mut out = Vec::new();
    let mut off = 0_usize;
    while off < payload.len() {
        if payload.len().saturating_sub(off) < 17 {
            return Err(Error::SstCorrupt("registro truncado"));
        }
        let key_len = read_u32(payload, off)? as usize;
        let value_len = read_u32(payload, off + 4)? as usize;
        let seq = read_u64(payload, off + 8)?;
        let kind = payload[off + 16];
        off += 17;
        let key_end = off
            .checked_add(key_len)
            .ok_or(Error::SstCorrupt("clave de registro"))?;
        let val_end = key_end
            .checked_add(value_len)
            .ok_or(Error::SstCorrupt("valor de registro"))?;
        let key = payload
            .get(off..key_end)
            .ok_or(Error::SstCorrupt("clave truncada"))?
            .to_vec();
        let value = payload
            .get(key_end..val_end)
            .ok_or(Error::SstCorrupt("valor truncado"))?
            .to_vec();
        off = val_end;
        match kind {
            KIND_PUT => out.push(DecodedRecord {
                key,
                seq,
                value: Some(value),
            }),
            KIND_TOMBSTONE => {
                if value_len != 0 {
                    return Err(Error::SstCorrupt("tombstone con valor"));
                }
                out.push(DecodedRecord {
                    key,
                    seq,
                    value: None,
                });
            }
            _ => return Err(Error::SstCorrupt("kind de registro")),
        }
    }
    Ok(out)
}

fn u32_len(n: usize) -> Result<u32, Error> {
    u32::try_from(n).map_err(|_| Error::SstRecordTooLarge { size: n })
}

fn write_u16(dst: &mut [u8], off: usize, v: u16) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 2)
        .ok_or(Error::SstCorrupt("encode u16"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn write_u32(dst: &mut [u8], off: usize, v: u32) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 4)
        .ok_or(Error::SstCorrupt("encode u32"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn write_u64(dst: &mut [u8], off: usize, v: u64) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 8)
        .ok_or(Error::SstCorrupt("encode u64"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn read_u16(src: &[u8], off: usize) -> Result<u16, Error> {
    let slot = src
        .get(off..off + 2)
        .ok_or(Error::SstCorrupt("decode u16"))?;
    Ok(u16::from_le_bytes([slot[0], slot[1]]))
}

fn read_u32(src: &[u8], off: usize) -> Result<u32, Error> {
    let slot = src
        .get(off..off + 4)
        .ok_or(Error::SstCorrupt("decode u32"))?;
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(slot);
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(src: &[u8], off: usize) -> Result<u64, Error> {
    let slot = src
        .get(off..off + 8)
        .ok_or(Error::SstCorrupt("decode u64"))?;
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(slot);
    Ok(u64::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::{
        BlockHeader, IndexEntry, SstFooter, BLOCK_HEADER_SIZE, FOOTER_SIZE, INDEX_ENTRY_SIZE,
    };

    #[test]
    fn on_disk_headers_have_fixed_size() {
        assert_eq!(std::mem::size_of::<BlockHeader>(), BLOCK_HEADER_SIZE);
        assert_eq!(std::mem::size_of::<IndexEntry>(), INDEX_ENTRY_SIZE);
        assert_eq!(std::mem::size_of::<SstFooter>(), FOOTER_SIZE);
    }
}
