//! Write-Ahead Log con `O_DIRECT` y páginas de 4096 bytes.
//!
//! ## Por qué no vale un `File::write` cualquiera
//!
//! Con `O_DIRECT` el kernel **no** usa el page cache. Cada `write` debe
//! cumplir tres alineaciones a la vez:
//!
//! 1. el **puntero** del buffer (de ahí `posix_memalign(4096, …)`),
//! 2. el **tamaño** de la transferencia (múltiplo de 4096),
//! 3. el **offset** del archivo (múltiplo de 4096).
//!
//! Un `Vec<u8>` típico no garantiza (1). El kernel responde `EINVAL`.
//!
//! ## Durabilidad vs MemTable
//!
//! El orden correcto es: **primero** `append` al WAL, **después** `put`/`delete`
//! en la MemTable. Si el proceso muere entre ambos, el replay reconstruye RAM.
//! Si se escribiera primero RAM y luego el WAL, un crash perdería el put.
//!
//! Cada `append` se abre con `O_SYNC`, así que al retornar `Ok` los bytes ya
//! están en disco (o el fallback alineado + `O_SYNC` si `O_DIRECT` no está
//! disponible en ese filesystem).

use crate::error::Error;
use crate::types::{Key, SeqNum, Value};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Result as IoResult};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::ptr::{self, NonNull};
use std::slice;

/// Tamaño de página / sector lógico que usa este WAL.
pub const WAL_ALIGN: usize = 4096;

/// Magic ASCII `NSDB` al inicio del archivo.
pub const WAL_MAGIC: [u8; 4] = *b"NSDB";

/// Versión de formato on-disk.
pub const WAL_VERSION: u16 = 1;

/// Bit 0 de `WALHeader::flags`: el archivo se abrió (o se intentó) con `O_DIRECT`.
pub const WAL_FLAG_DIRECT: u16 = 1 << 0;

/// Tope defensivo de un registro ya empaquetado (payload + cabecera de frame).
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

const KIND_PUT: u8 = 1;
const KIND_DELETE: u8 = 2;
const FRAME_HEADER: usize = 8; // crc32 + payload_len

// =============================================================================
// TIPOS (no son "clases": en Rust se llama struct / enum + impl)
// =============================================================================
//
// struct / enum  →  campos (datos). No se dice "propiedades".
// impl Tipo      →  funciones asociadas (`Tipo::create`) y métodos (`self.append`).
// Drop           →  destructor, cuando el valor se va de ámbito.
//
// Un valor concreto (`let wal = Wal::create(...)`) es una *instancia* o
// simplemente un valor de ese tipo. Rust no tiene "objetos" ni `new` de lenguaje.

/// Cabecera on-disk: exactamente una página.
///
/// `repr(C)` fija el layout byte a byte (magic, version, flags, checksum, pad).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALHeader {
    /// Identificador `NSDB`.
    pub magic: [u8; 4],
    /// Versión de formato.
    pub version: u16,
    /// Banderas (ver [`WAL_FLAG_DIRECT`]).
    pub flags: u16,
    /// CRC32 de los 8 bytes anteriores (`magic`..`flags`).
    pub checksum: u32,
    /// Relleno hasta 4096. Sin datos; debe ser cero.
    pub _pad: [u8; 4084],
}

const _: () = assert!(std::mem::size_of::<WALHeader>() == WAL_ALIGN);

/// Operación durable registrada en el log.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WalOp {
    /// Inserta o sobreescribe un valor.
    Put {
        /// Clave afectada.
        key: Key,
        /// Valor vivo (no es tombstone).
        value: Value,
    },
    /// Tombstone: la clave queda borrada a partir de este `seq`.
    Delete {
        /// Clave afectada.
        key: Key,
    },
}

/// Registro ya decodificado, listo para aplicar a la MemTable.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WalRecord {
    /// Número de secuencia de este write.
    pub seq: SeqNum,
    /// Put o delete.
    pub op: WalOp,
}

// -----------------------------------------------------------------------------
// AlignedBuf  (struct = campos, impl = métodos, Drop = free)
// -----------------------------------------------------------------------------

/// Buffer en heap alineado a [`WAL_ALIGN`], obtenido con `posix_memalign`.
struct AlignedBuf {
    ptr: NonNull<u8>,
    cap: usize,
}

// SAFETY: dueño exclusivo del bloque de `posix_memalign`. Mover el WAL a
// otro hilo (Mutex + worker de flush) no comparte el puntero. No implementamos
// `Sync`: las escrituras usan `&mut self`.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Purpose: reserva `bytes` (redondeados a páginas de 4K) alineados.
    ///
    /// Inputs: `bytes` — capacidad mínima pedida, en bytes.
    ///
    /// Returns: buffer puesto a cero, o [`Error::AllocFailed`] / [`Error::Unaligned`].
    fn with_capacity(bytes: usize) -> Result<Self, Error> {
        Self::allocate(bytes)
    }

    /// Purpose: llama a `posix_memalign` y comprueba la alineación.
    ///
    /// Inputs: `bytes` — tamaño mínimo; se redondea a múltiplo de [`WAL_ALIGN`].
    ///
    /// Returns: buffer válido para `O_DIRECT`.
    fn allocate(bytes: usize) -> Result<Self, Error> {
        let cap = align_up(bytes.max(1));
        let mut raw: *mut libc::c_void = ptr::null_mut();
        // SAFETY: `raw` es un out-ptr propio; alignment es potencia de 2 y
        // múltiplo de sizeof(void*); `cap` es múltiplo de alignment (POSIX).
        let rc = unsafe { libc::posix_memalign(&mut raw, WAL_ALIGN, cap) };
        if rc != 0 {
            return Err(Error::AllocFailed(rc));
        }
        let Some(ptr) = NonNull::new(raw.cast::<u8>()) else {
            return Err(Error::AllocFailed(-1));
        };
        if !(ptr.as_ptr() as usize).is_multiple_of(WAL_ALIGN) {
            // SAFETY: `raw` salió de posix_memalign en este mismo frame.
            unsafe { libc::free(raw) };
            return Err(Error::Unaligned {
                required: WAL_ALIGN,
            });
        }
        // SAFETY: `ptr` apunta a `cap` bytes recién reservados y únicos.
        unsafe { ptr::write_bytes(ptr.as_ptr(), 0, cap) };
        Ok(Self { ptr, cap })
    }

    /// Purpose: garantiza capacidad, copiando los primeros `preserve` bytes.
    ///
    /// Inputs: `need` — nueva capacidad mínima; `preserve` — bytes a conservar.
    ///
    /// Returns: `Ok(())` si ya cabía o si el realloc alineado tuvo éxito.
    fn ensure(&mut self, need: usize, preserve: usize) -> Result<(), Error> {
        if self.cap >= need {
            return Ok(());
        }
        let next = Self::allocate(need)?;
        let copy = preserve.min(self.cap).min(next.cap);
        // SAFETY: ambos buffers son únicos, `copy` no supera ninguno.
        unsafe {
            ptr::copy_nonoverlapping(self.ptr.as_ptr(), next.ptr.as_ptr(), copy);
        }
        *self = next;
        Ok(())
    }

    /// Purpose: expone el puntero para comprobar alineación en tests.
    ///
    /// Inputs: `self` — buffer vivo.
    ///
    /// Returns: dirección del primer byte.
    #[cfg(test)]
    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Purpose: capacidad en bytes (siempre múltiplo de 4K).
    ///
    /// Inputs: `self` — buffer vivo.
    ///
    /// Returns: `cap` reservado por `posix_memalign`.
    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.cap
    }

    /// Purpose: vista mutable de todo el buffer.
    ///
    /// Inputs: `self` — préstamo exclusivo.
    ///
    /// Returns: slice de exactamente `cap` bytes.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: préstamo exclusivo; `cap` es el tamaño pastido a posix_memalign.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }

    /// Purpose: vista inmutable de todo el buffer.
    ///
    /// Inputs: `self` — préstamo compartido.
    ///
    /// Returns: slice de exactamente `cap` bytes.
    fn as_slice(&self) -> &[u8] {
        // SAFETY: el buffer está vivo y no se realloca bajo esta referencia.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr` proviene de `posix_memalign` y no se ha liberado aún.
        unsafe { libc::free(self.ptr.as_ptr().cast()) };
    }
}

// -----------------------------------------------------------------------------
// Wal  ← aquí se DEFINE el tipo (campos). La instancia se CREA en `create`/`open`.
// -----------------------------------------------------------------------------

/// WAL durable de un solo escritor (`append` toma `&mut self`).
///
/// Tipo = `struct Wal` (campos) + `impl Wal` (constructores y métodos).
/// No es una clase: no hay herencia ni `new` como palabra clave.
pub struct Wal {
    file: File,
    offset: u64,
    direct: bool,
    buf: AlignedBuf,
}

// Métodos y constructores del tipo `Wal` (`Self` = `Wal`).
impl Wal {
    /// Purpose: crea un WAL nuevo (trunca si el archivo existía) y escribe la cabecera.
    ///
    /// Inputs: `path` — archivo a crear (el directorio padre debe existir).
    ///
    /// Returns: WAL listo para `append`, con offset = 4096.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let (file, direct) = open_wal(path.as_ref(), true)?;
        // Construcción del valor: literal de struct (`Self { campos }`).
        // Aquí nace `wal`; `buf` se reserva con posix_memalign.
        let mut wal = Self {
            file,
            offset: 0,
            direct,
            buf: AlignedBuf::with_capacity(WAL_ALIGN)?,
        };
        wal.write_header()?;
        wal.offset = WAL_ALIGN as u64;
        Ok(wal)
    }

    /// Purpose: abre un WAL existente, valida la cabecera y localiza el final válido.
    ///
    /// Inputs: `path` — archivo creado por [`Wal::create`].
    ///
    /// Returns: WAL posicionado para seguir haciendo `append` (recovery).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let (file, direct) = open_wal(path.as_ref(), false)?;
        // Segunda vía de construcción (recovery): mismo literal de struct.
        let mut wal = Self {
            file,
            offset: WAL_ALIGN as u64,
            direct,
            buf: AlignedBuf::with_capacity(WAL_ALIGN)?,
        };
        wal.read_header()?;
        wal.offset = wal.scan_end()?;
        Ok(wal)
    }

    /// Purpose: indica si el descriptor se abrió con `O_DIRECT`.
    ///
    /// Inputs: `self` — WAL abierto.
    ///
    /// Returns: `true` si el kernel aceptó `O_DIRECT`; si no, hay fallback alineado.
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    /// Purpose: offset de la próxima escritura, siempre múltiplo de 4K.
    ///
    /// Inputs: `self` — WAL abierto.
    ///
    /// Returns: posición absoluta en el archivo, en bytes.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Purpose: serializa y volca un put/delete en una o más páginas de 4K.
    ///
    /// Inputs: `seq` — secuencia de este write; `op` — put o delete.
    ///
    /// Returns: `Ok(())` cuando el frame está en disco (`O_SYNC`). Entonces
    /// el llamador puede aplicar `op` a la MemTable.
    pub fn append(&mut self, seq: SeqNum, op: WalOp) -> Result<(), Error> {
        let payload = payload_size(&op);
        let framed = align_up(FRAME_HEADER.saturating_add(payload));
        if framed > MAX_RECORD_BYTES {
            return Err(Error::WalRecordTooLarge {
                size: framed,
                max: MAX_RECORD_BYTES,
            });
        }
        self.buf.ensure(framed, 0)?;
        let buf = self.buf.as_mut_slice();
        buf[..framed].fill(0);
        encode_payload(&mut buf[FRAME_HEADER..FRAME_HEADER + payload], seq, &op)?;
        let crc = crc32fast::hash(&buf[FRAME_HEADER..FRAME_HEADER + payload]);
        write_u32(buf, 0, crc)?;
        write_u32(buf, 4, u32_from_usize(payload)?)?;
        write_aligned(&self.file, &buf[..framed], self.offset)?;
        self.offset += framed as u64;
        Ok(())
    }

    /// Purpose: relee los registros válidos desde la página 1 hasta [`Wal::offset`].
    ///
    /// Inputs: `self` — WAL abierto (no muta el offset de append).
    ///
    /// Returns: registros en orden; un CRC fallido o una página rota se
    /// interpretan como fin del log (crash a mitad de write).
    pub fn replay(&self) -> Result<Vec<WalRecord>, Error> {
        let mut buf = AlignedBuf::with_capacity(WAL_ALIGN)?;
        let mut offset = WAL_ALIGN as u64;
        let mut out = Vec::new();
        let end = self.offset;
        while offset < end {
            match read_one(&self.file, &mut buf, offset, end)? {
                None => break,
                Some((record, next)) => {
                    out.push(record);
                    offset = next;
                }
            }
        }
        Ok(out)
    }

    /// Purpose: `fsync` explícito (redundante con `O_SYNC`, útil en el fallback).
    ///
    /// Inputs: `self` — WAL abierto.
    ///
    /// Returns: `Ok(())` si el kernel confirma los datos en disco.
    pub fn sync(&self) -> Result<(), Error> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Purpose: escribe la página 0 (`WALHeader`) en el buffer alineado.
    ///
    /// Inputs: `self` — WAL con offset 0 y buffer ≥ 4K.
    ///
    /// Returns: cabecera durable en offset 0.
    fn write_header(&mut self) -> Result<(), Error> {
        self.buf.ensure(WAL_ALIGN, 0)?;
        let flags = if self.direct { WAL_FLAG_DIRECT } else { 0 };
        let buf = self.buf.as_mut_slice();
        buf[..WAL_ALIGN].fill(0);
        buf[0..4].copy_from_slice(&WAL_MAGIC);
        write_u16(buf, 4, WAL_VERSION)?;
        write_u16(buf, 6, flags)?;
        let checksum = crc32fast::hash(&buf[0..8]);
        write_u32(buf, 8, checksum)?;
        write_aligned(&self.file, &buf[..WAL_ALIGN], 0)?;
        Ok(())
    }

    /// Purpose: lee y valida la página 0.
    ///
    /// Inputs: `self` — archivo existente.
    ///
    /// Returns: `Ok` si magic, versión y checksum coinciden.
    fn read_header(&mut self) -> Result<(), Error> {
        self.buf.ensure(WAL_ALIGN, 0)?;
        let n = read_aligned(&self.file, &mut self.buf.as_mut_slice()[..WAL_ALIGN], 0)?;
        if n < WAL_ALIGN {
            return Err(Error::WalCorrupt("cabecera truncada"));
        }
        let buf = self.buf.as_slice();
        if buf[0..4] != WAL_MAGIC {
            return Err(Error::WalCorrupt("magic inválida"));
        }
        let version = read_u16(buf, 4)?;
        if version != WAL_VERSION {
            return Err(Error::UnsupportedWalVersion(version));
        }
        let expected = crc32fast::hash(&buf[0..8]);
        let got = read_u32(buf, 8)?;
        if expected != got {
            return Err(Error::WalCorrupt("checksum de cabecera"));
        }
        Ok(())
    }

    /// Purpose: avanza página a página hasta el primer frame inválido o EOF.
    ///
    /// Inputs: `self` — cabecera ya validada.
    ///
    /// Returns: offset (múltiplo de 4K) donde debe ir el próximo `append`.
    fn scan_end(&mut self) -> Result<u64, Error> {
        let file_len = self.file.metadata()?.len();
        let mut offset = WAL_ALIGN as u64;
        while offset < file_len {
            match read_one(&self.file, &mut self.buf, offset, file_len)? {
                None => break,
                Some((_, next)) => offset = next,
            }
        }
        Ok(offset)
    }
}

/// Purpose: abre el archivo con `O_DIRECT | O_SYNC`; si el FS no lo permite, fallback.
///
/// Inputs: `path` — destino; `create` — truncar y crear vs solo abrir.
///
/// Returns: `(File, direct)` donde `direct` indica si `O_DIRECT` quedó activo.
fn open_wal(path: &Path, create: bool) -> Result<(File, bool), Error> {
    match open_with_flags(path, create, libc::O_DIRECT | libc::O_SYNC) {
        Ok(file) => Ok((file, true)),
        Err(err) if direct_unsupported(&err) && !create_missing(&err, create) => {
            let file = open_with_flags(path, create, libc::O_SYNC)?;
            Ok((file, false))
        }
        Err(err) => Err(err.into()),
    }
}

/// Purpose: distingue "FS no soporta O_DIRECT" de errores reales (ENOENT, EACCES).
///
/// Inputs: `err` — error de `open`.
///
/// Returns: `true` si conviene reintentar sin `O_DIRECT`.
fn direct_unsupported(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    )
}

/// Purpose: no enmascara ENOENT al abrir un WAL que debería existir.
///
/// Inputs: `err` — error de `open`; `create` — si estábamos creando.
///
/// Returns: `true` si hay que propagar el error sin fallback.
fn create_missing(err: &std::io::Error, create: bool) -> bool {
    !create && err.kind() == ErrorKind::NotFound
}

/// Purpose: `open(2)` con flags POSIX extra (`O_DIRECT`, `O_SYNC`).
///
/// Inputs: `path`, `create`, `custom` — bits de `open`.
///
/// Returns: `File` de std, o error de I/O.
fn open_with_flags(path: &Path, create: bool, custom: i32) -> IoResult<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).custom_flags(custom);
    if create {
        opts.create(true).truncate(true);
    }
    opts.open(path)
}

/// Purpose: escribe un slice ya alineado en tamaño, puntero y offset.
///
/// Inputs: `file` — WAL; `buf` — longitud múltiplo de 4K; `offset` — múltiplo de 4K.
///
/// Returns: `Ok` si se escribieron todos los bytes; `Unaligned` si el slice no cumple.
fn write_aligned(file: &File, buf: &[u8], offset: u64) -> Result<(), Error> {
    check_aligned_slice(buf, offset)?;
    file.write_all_at(buf, offset)?;
    Ok(())
}

/// Purpose: lee en un slice alineado. EOF corto se interpreta como 0 bytes.
///
/// Inputs: `file`, `buf` (tamaño múltiplo de 4K), `offset` múltiplo de 4K.
///
/// Returns: número de bytes leídos (0 en EOF).
fn read_aligned(file: &File, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
    check_aligned_slice(buf, offset)?;
    match file.read_exact_at(buf, offset) {
        Ok(()) => Ok(buf.len()),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(0),
        Err(err) => Err(err.into()),
    }
}

/// Purpose: rechaza transferencias que `O_DIRECT` no aceptaría.
///
/// Inputs: `buf` — memoria a transferir; `offset` — posición en el archivo.
///
/// Returns: `Unaligned` si puntero, longitud u offset no son múltiplos de 4K.
fn check_aligned_slice(buf: &[u8], offset: u64) -> Result<(), Error> {
    let ptr = buf.as_ptr() as usize;
    if !ptr.is_multiple_of(WAL_ALIGN)
        || !buf.len().is_multiple_of(WAL_ALIGN)
        || !offset.is_multiple_of(WAL_ALIGN as u64)
    {
        return Err(Error::Unaligned {
            required: WAL_ALIGN,
        });
    }
    Ok(())
}

/// Purpose: lee un frame en `offset` o señala fin de log.
///
/// Inputs: `file`, `buf` reutilizable, `offset` actual, `file_end` (no leer más allá).
///
/// Returns: `None` si EOF, página cero, CRC mala o frame truncado (crash).
fn read_one(
    file: &File,
    buf: &mut AlignedBuf,
    offset: u64,
    file_end: u64,
) -> Result<Option<(WalRecord, u64)>, Error> {
    if offset + WAL_ALIGN as u64 > file_end {
        return Ok(None);
    }
    buf.ensure(WAL_ALIGN, 0)?;
    let n = read_aligned(file, &mut buf.as_mut_slice()[..WAL_ALIGN], offset)?;
    if n < WAL_ALIGN {
        return Ok(None);
    }
    let crc = read_u32(buf.as_slice(), 0)?;
    let payload_len = read_u32(buf.as_slice(), 4)? as usize;
    if payload_len == 0 {
        return Ok(None);
    }
    let framed = align_up(FRAME_HEADER.saturating_add(payload_len));
    if framed > MAX_RECORD_BYTES {
        return Err(Error::WalRecordTooLarge {
            size: framed,
            max: MAX_RECORD_BYTES,
        });
    }
    if offset + framed as u64 > file_end {
        return Ok(None);
    }
    if framed > WAL_ALIGN {
        buf.ensure(framed, WAL_ALIGN)?;
        let rest = framed - WAL_ALIGN;
        let n = read_aligned(
            file,
            &mut buf.as_mut_slice()[WAL_ALIGN..framed],
            offset + WAL_ALIGN as u64,
        )?;
        if n < rest {
            return Ok(None);
        }
    }
    let payload = &buf.as_slice()[FRAME_HEADER..FRAME_HEADER + payload_len];
    if crc32fast::hash(payload) != crc {
        return Ok(None);
    }
    let record = decode_payload(payload)?;
    Ok(Some((record, offset + framed as u64)))
}

/// Purpose: bytes del payload (sin CRC ni padding de página).
///
/// Inputs: `op` — put o delete.
///
/// Returns: 17 + clave + (valor si put).
fn payload_size(op: &WalOp) -> usize {
    let key_len = match op {
        WalOp::Put { key, .. } | WalOp::Delete { key } => key.len(),
    };
    let value_len = match op {
        WalOp::Put { value, .. } => value.len(),
        WalOp::Delete { .. } => 0,
    };
    8 + 1 + 4 + 4 + key_len + value_len
}

/// Purpose: escribe `seq` + kind + key/value en `dst`.
///
/// Inputs: `dst` — exactamente `payload_size` bytes; `seq`, `op`.
///
/// Returns: `Ok` o error si `dst` es corto (bug interno).
fn encode_payload(dst: &mut [u8], seq: SeqNum, op: &WalOp) -> Result<(), Error> {
    let (kind, key, value) = match op {
        WalOp::Put { key, value } => (KIND_PUT, key, Some(value)),
        WalOp::Delete { key } => (KIND_DELETE, key, None),
    };
    write_u64(dst, 0, seq.get())?;
    write_u8(dst, 8, kind)?;
    write_u32(dst, 9, u32_from_usize(key.len())?)?;
    write_u32(dst, 13, u32_from_usize(value.map(Value::len).unwrap_or(0))?)?;
    let mut off = 17;
    off = write_bytes(dst, off, key.as_bytes())?;
    if let Some(value) = value {
        write_bytes(dst, off, value.as_bytes())?;
    }
    Ok(())
}

/// Purpose: parsea un payload con CRC ya verificado.
///
/// Inputs: `src` — bytes del payload.
///
/// Returns: [`WalRecord`] o [`Error::WalCorrupt`] si el layout no cuadra.
fn decode_payload(src: &[u8]) -> Result<WalRecord, Error> {
    if src.len() < 17 {
        return Err(Error::WalCorrupt("payload corto"));
    }
    let seq = SeqNum::new(read_u64(src, 0)?);
    let kind = read_u8(src, 8)?;
    let key_len = read_u32(src, 9)? as usize;
    let value_len = read_u32(src, 13)? as usize;
    let key_start: usize = 17;
    let key_end = key_start.saturating_add(key_len);
    let val_end = key_end.saturating_add(value_len);
    if val_end != src.len() {
        return Err(Error::WalCorrupt("longitudes de clave/valor"));
    }
    let key = Key::from_vec(src[key_start..key_end].to_vec())?;
    match kind {
        KIND_PUT => {
            let value = Value::from_vec(src[key_end..val_end].to_vec());
            Ok(WalRecord {
                seq,
                op: WalOp::Put { key, value },
            })
        }
        KIND_DELETE => {
            if value_len != 0 {
                return Err(Error::WalCorrupt("delete con valor"));
            }
            Ok(WalRecord {
                seq,
                op: WalOp::Delete { key },
            })
        }
        _ => Err(Error::WalCorrupt("kind desconocido")),
    }
}

/// Purpose: redondea hacia el próximo múltiplo de [`WAL_ALIGN`].
///
/// Inputs: `n` — tamaño en bytes (ya ≥ 1 en los llamadores de alloc).
///
/// Returns: `n` si ya es múltiplo; si no, el siguiente múltiplo. Mínimo 4096
/// si `n == 0`.
fn align_up(n: usize) -> usize {
    if n == 0 {
        WAL_ALIGN
    } else {
        n.div_ceil(WAL_ALIGN) * WAL_ALIGN
    }
}

/// Purpose: convierte `usize` a `u32` para longitudes on-disk.
///
/// Inputs: `n` — longitud de clave o valor.
///
/// Returns: `u32` o [`Error::WalRecordTooLarge`] si no cabe.
fn u32_from_usize(n: usize) -> Result<u32, Error> {
    u32::try_from(n).map_err(|_| Error::WalRecordTooLarge {
        size: n,
        max: u32::MAX as usize,
    })
}

/// Purpose: escribe un `u8` en `dst[off]`.
fn write_u8(dst: &mut [u8], off: usize, v: u8) -> Result<(), Error> {
    *dst.get_mut(off)
        .ok_or(Error::WalCorrupt("encode u8 fuera de rango"))? = v;
    Ok(())
}

/// Purpose: escribe un `u16` little-endian.
fn write_u16(dst: &mut [u8], off: usize, v: u16) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 2)
        .ok_or(Error::WalCorrupt("encode u16 fuera de rango"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

/// Purpose: escribe un `u32` little-endian.
fn write_u32(dst: &mut [u8], off: usize, v: u32) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 4)
        .ok_or(Error::WalCorrupt("encode u32 fuera de rango"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

/// Purpose: escribe un `u64` little-endian.
fn write_u64(dst: &mut [u8], off: usize, v: u64) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 8)
        .ok_or(Error::WalCorrupt("encode u64 fuera de rango"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

/// Purpose: copia `bytes` en `dst[off..]`.
///
/// Returns: el offset posterior a la copia.
fn write_bytes(dst: &mut [u8], off: usize, bytes: &[u8]) -> Result<usize, Error> {
    let end = off.saturating_add(bytes.len());
    let slot = dst
        .get_mut(off..end)
        .ok_or(Error::WalCorrupt("encode bytes fuera de rango"))?;
    slot.copy_from_slice(bytes);
    Ok(end)
}

/// Purpose: lee un `u8`.
fn read_u8(src: &[u8], off: usize) -> Result<u8, Error> {
    src.get(off)
        .copied()
        .ok_or(Error::WalCorrupt("decode u8 fuera de rango"))
}

/// Purpose: lee un `u16` little-endian.
fn read_u16(src: &[u8], off: usize) -> Result<u16, Error> {
    let slot = src
        .get(off..off + 2)
        .ok_or(Error::WalCorrupt("decode u16 fuera de rango"))?;
    Ok(u16::from_le_bytes([slot[0], slot[1]]))
}

/// Purpose: lee un `u32` little-endian.
fn read_u32(src: &[u8], off: usize) -> Result<u32, Error> {
    let slot = src
        .get(off..off + 4)
        .ok_or(Error::WalCorrupt("decode u32 fuera de rango"))?;
    Ok(u32::from_le_bytes([slot[0], slot[1], slot[2], slot[3]]))
}

/// Purpose: lee un `u64` little-endian.
fn read_u64(src: &[u8], off: usize) -> Result<u64, Error> {
    let slot = src
        .get(off..off + 8)
        .ok_or(Error::WalCorrupt("decode u64 fuera de rango"))?;
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(slot);
    Ok(u64::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::{align_up, AlignedBuf, WALHeader, Wal, WalOp, WAL_ALIGN, WAL_MAGIC, WAL_VERSION};
    use crate::error::Error;
    use crate::memtable::{Lookup, MemTable};
    use crate::types::{Key, SeqNum, Value};
    use std::fs;
    use std::path::{Path, PathBuf};

    fn key(bytes: &[u8]) -> Key {
        Key::new(bytes).expect("clave")
    }

    fn temp_wal_path() -> (tempfile::TempDir, PathBuf) {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("wal-tests");
        fs::create_dir_all(&base).expect("target/wal-tests");
        let dir = tempfile::TempDir::new_in(&base).expect("tempdir en ext4");
        let path = dir.path().join("log.wal");
        (dir, path)
    }

    #[test]
    fn header_is_exactly_one_page() {
        assert_eq!(std::mem::size_of::<WALHeader>(), WAL_ALIGN);
        assert_eq!(WAL_MAGIC, *b"NSDB");
        assert_eq!(WAL_VERSION, 1);
    }

    #[test]
    fn posix_memalign_buffer_is_4k_aligned() {
        let buf = AlignedBuf::with_capacity(1).expect("alloc");
        let addr = buf.as_ptr() as usize;
        assert!(addr.is_multiple_of(WAL_ALIGN), "ptr={addr:#x}");
        assert!(buf.capacity().is_multiple_of(WAL_ALIGN));
        assert!(buf.capacity() >= WAL_ALIGN);
    }

    #[test]
    fn align_up_rounds_to_pages() {
        assert_eq!(align_up(0), WAL_ALIGN);
        assert_eq!(align_up(1), WAL_ALIGN);
        assert_eq!(align_up(WAL_ALIGN), WAL_ALIGN);
        assert_eq!(align_up(WAL_ALIGN + 1), WAL_ALIGN * 2);
    }

    #[test]
    fn unaligned_slice_is_rejected() {
        let mut heap = vec![0_u8; WAL_ALIGN + 1];
        // Un Vec no garantiza alineación 4K; desplazamos 1 byte para forzar desalineado.
        let slice = &mut heap[1..1 + WAL_ALIGN];
        let err = super::check_aligned_slice(slice, WAL_ALIGN as u64)
            .expect_err("un Vec desplazado no debería pasar");
        assert!(matches!(
            err,
            Error::Unaligned {
                required: WAL_ALIGN
            }
        ));
    }

    #[test]
    fn append_put_then_replay() {
        let (_dir, path) = temp_wal_path();
        let mut wal = Wal::create(&path).expect("create");
        wal.append(
            SeqNum::new(1),
            WalOp::Put {
                key: key(b"balance"),
                value: Value::new(b"100"),
            },
        )
        .expect("append");
        assert!(wal.offset().is_multiple_of(WAL_ALIGN as u64));
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq, SeqNum::new(1));
        match &records[0].op {
            WalOp::Put { key, value } => {
                assert_eq!(key.as_bytes(), b"balance");
                assert_eq!(value.as_bytes(), b"100");
            }
            WalOp::Delete { .. } => panic!("esperado Put"),
        }
    }

    #[test]
    fn append_delete_replays_as_delete() {
        let (_dir, path) = temp_wal_path();
        let mut wal = Wal::create(&path).expect("create");
        wal.append(
            SeqNum::new(1),
            WalOp::Delete {
                key: key(b"balance"),
            },
        )
        .expect("append");
        let records = wal.replay().expect("replay");
        assert!(matches!(records[0].op, WalOp::Delete { .. }));
    }

    #[test]
    fn file_size_is_multiple_of_4k() {
        let (_dir, path) = temp_wal_path();
        let mut wal = Wal::create(&path).expect("create");
        wal.append(
            SeqNum::new(1),
            WalOp::Put {
                key: key(b"k"),
                value: Value::new(b"v"),
            },
        )
        .expect("append");
        drop(wal);
        let len = fs::metadata(&path).expect("meta").len();
        assert!(len.is_multiple_of(WAL_ALIGN as u64));
        assert_eq!(len, (WAL_ALIGN * 2) as u64);
    }

    #[test]
    fn crash_reopen_replays_same_ops() {
        let (_dir, path) = temp_wal_path();
        {
            let mut wal = Wal::create(&path).expect("create");
            wal.append(
                SeqNum::new(1),
                WalOp::Put {
                    key: key(b"a"),
                    value: Value::new(b"1"),
                },
            )
            .expect("a");
            wal.append(
                SeqNum::new(2),
                WalOp::Put {
                    key: key(b"b"),
                    value: Value::new(b"2"),
                },
            )
            .expect("b");
            wal.append(SeqNum::new(3), WalOp::Delete { key: key(b"a") })
                .expect("del");
        }
        let wal = Wal::open(&path).expect("open after crash");
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 3);
        assert!(matches!(records[2].op, WalOp::Delete { .. }));
    }

    #[test]
    fn wal_then_memtable_survives_reopen() {
        let (_dir, path) = temp_wal_path();
        {
            let mut wal = Wal::create(&path).expect("create");
            // Orden durable: WAL primero, MemTable después (aquí solo WAL:
            // simulamos crash antes de tocar RAM).
            wal.append(
                SeqNum::new(1),
                WalOp::Put {
                    key: key(b"k"),
                    value: Value::new(b"old"),
                },
            )
            .expect("p1");
            wal.append(
                SeqNum::new(2),
                WalOp::Put {
                    key: key(b"k"),
                    value: Value::new(b"new"),
                },
            )
            .expect("p2");
        }
        let wal = Wal::open(&path).expect("open");
        let table = MemTable::new(1024);
        for rec in wal.replay().expect("replay") {
            match rec.op {
                WalOp::Put { key, value } => {
                    table.put(key, value, rec.seq);
                }
                WalOp::Delete { key } => {
                    table.delete(key, rec.seq);
                }
            }
        }
        let got = table.get(b"k");
        match got {
            Lookup::Alive(pinned) => assert_eq!(pinned.value().as_bytes(), b"new"),
            Lookup::Deleted(_) | Lookup::Missing => panic!("k debería estar Alive"),
        }
    }

    #[test]
    fn large_value_spans_two_pages() {
        let (_dir, path) = temp_wal_path();
        let mut wal = Wal::create(&path).expect("create");
        let big = vec![0xAB; 5000];
        wal.append(
            SeqNum::new(1),
            WalOp::Put {
                key: key(b"blob"),
                value: Value::from_vec(big.clone()),
            },
        )
        .expect("big");
        assert!(wal.offset() >= (WAL_ALIGN * 3) as u64);
        let recs = wal.replay().expect("replay");
        match &recs[0].op {
            WalOp::Put { value, .. } => assert_eq!(value.as_bytes(), big.as_slice()),
            WalOp::Delete { .. } => panic!("put"),
        }
    }

    #[test]
    fn corrupt_second_record_stops_replay() {
        let (_dir, path) = temp_wal_path();
        {
            let mut wal = Wal::create(&path).expect("create");
            wal.append(
                SeqNum::new(1),
                WalOp::Put {
                    key: key(b"keep"),
                    value: Value::new(b"ok"),
                },
            )
            .expect("1");
            wal.append(
                SeqNum::new(2),
                WalOp::Put {
                    key: key(b"drop"),
                    value: Value::new(b"xx"),
                },
            )
            .expect("2");
        }
        flip_byte(&path, (WAL_ALIGN * 2) as u64 + 20);
        let wal = Wal::open(&path).expect("open");
        let recs = wal.replay().expect("replay");
        assert_eq!(recs.len(), 1);
        match &recs[0].op {
            WalOp::Put { key, .. } => assert_eq!(key.as_bytes(), b"keep"),
            WalOp::Delete { .. } => panic!("keep"),
        }
    }

    #[test]
    fn create_uses_direct_io_on_this_nvme() {
        let (_dir, path) = temp_wal_path();
        let wal = Wal::create(&path).expect("create");
        assert!(
            wal.is_direct(),
            "este workspace está en ext4/NVMe; O_DIRECT debería activarse"
        );
    }

    /// Purpose: corrompe un byte del WAL usando un buffer alineado (`O_DIRECT`).
    fn flip_byte(path: &Path, at: u64) {
        use std::os::unix::fs::{FileExt, OpenOptionsExt};

        let page_off = at / WAL_ALIGN as u64 * WAL_ALIGN as u64;
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .expect("open direct");
        let mut buf = AlignedBuf::with_capacity(WAL_ALIGN).expect("buf");
        file.read_exact_at(&mut buf.as_mut_slice()[..WAL_ALIGN], page_off)
            .expect("read page");
        let idx = (at - page_off) as usize;
        buf.as_mut_slice()[idx] ^= 0xFF;
        file.write_all_at(&buf.as_slice()[..WAL_ALIGN], page_off)
            .expect("write page");
    }
}
