//! Motor síncrono: WAL → MemTable → SSTables.
//!
//! ## Camino de un `put`
//!
//! 1. Asignar `seq`.
//! 2. `append` al WAL (disco, `O_SYNC`). Si esto falla, RAM no cambia.
//! 3. Aplicar a la MemTable. Si el proceso muere entre 2 y 3, el replay
//!    reconstruye RAM.
//!
//! ## Camino de un `get`
//!
//! 1. MemTable. `Alive` / `Deleted` **terminan** la búsqueda (el tombstone
//!    no puede dejarse “atravesar” hacia un valor viejo en disco).
//! 2. SSTables de **más nueva a más vieja**. La primera que conoce la clave
//!    gana: un put reciente en `000002.sst` debe tapar `000001.sst`.
//!
//! El flush de esta fase es **síncrono y explícito**: bloquea al llamador,
//! escribe un `.sst`, vacía la MemTable y rota el WAL. El flush en background
//! es la fase 8.

use crate::error::Error;
use crate::memtable::{Lookup, MemTable, PinnedValue};
use crate::sstable::{flush_memtable, SstFooter, SstLookup, SstReader};
use crate::types::{Key, SeqNum, Value};
use crate::wal::{Wal, WalOp};
use std::fs;
use std::path::{Path, PathBuf};

/// Nombre del archivo WAL dentro del directorio del motor.
const WAL_FILE: &str = "wal";

/// Capacidad por defecto de la MemTable (señal de flush), en bytes.
const DEFAULT_MEM_CAPACITY: usize = 64 * 1024;

// =============================================================================
// OPCIONES / LOOKUP
// =============================================================================

/// Opciones al abrir el motor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineOptions {
    /// Umbral de [`MemTable::is_full`] (el flush sigue siendo manual).
    pub mem_capacity_bytes: usize,
}

impl Default for EngineOptions {
    /// Purpose: 64 KiB de MemTable; suficiente para tests y un CLI mínimo.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: opciones por defecto.
    fn default() -> Self {
        Self {
            mem_capacity_bytes: DEFAULT_MEM_CAPACITY,
        }
    }
}

/// Bytes vivos de un `get`: MemTable (SkipList) o mmap de una SSTable.
pub enum EngineValue<'a> {
    /// Anclado al nodo de la MemTable.
    Mem(PinnedValue<'a>),
    /// Subslice del `mmap` de una SSTable.
    Sst {
        /// Seq del registro en disco.
        seq: SeqNum,
        /// Bytes del valor (sin copiar).
        bytes: &'a [u8],
    },
}

impl<'a> EngineValue<'a> {
    /// Purpose: expone los bytes sin copiar.
    ///
    /// Inputs: `self` — put vivo.
    ///
    /// Returns: slice de la MemTable o del mmap.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Mem(pinned) => pinned.value().as_bytes(),
            Self::Sst { bytes, .. } => bytes,
        }
    }

    /// Purpose: seq del put que ganó el `get`.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: seq de MemTable o de SST.
    #[inline(always)]
    pub fn seq(&self) -> SeqNum {
        match self {
            Self::Mem(pinned) => pinned.seq(),
            Self::Sst { seq, .. } => *seq,
        }
    }
}

/// Resultado de [`Engine::get`]: MemTable y SSTables usan el mismo idioma.
pub enum EngineLookup<'a> {
    /// Valor vivo (RAM o mmap).
    Alive(EngineValue<'a>),
    /// Tombstone: no seguir buscando versiones más viejas.
    Deleted(SeqNum),
    /// Nadie conoce la clave.
    Missing,
}

impl<'a> EngineLookup<'a> {
    /// Purpose: ¿la clave no está en MemTable ni en SSTables?
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `true` solo en [`EngineLookup::Missing`].
    #[inline(always)]
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Purpose: ¿hay un tombstone más reciente que cualquier SST más vieja?
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `true` solo en [`EngineLookup::Deleted`].
    #[inline(always)]
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    /// Purpose: bytes del put, si está vivo.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `Some` solo en [`EngineLookup::Alive`].
    #[inline(always)]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Alive(v) => Some(v.as_bytes()),
            Self::Deleted(_) | Self::Missing => None,
        }
    }
}

// =============================================================================
// Engine  (struct = estado; impl = open / put / get / flush)
// =============================================================================

/// Motor KV sobre un directorio: un WAL, una MemTable, N SSTables inmutables.
pub struct Engine {
    dir: PathBuf,
    wal_path: PathBuf,
    /// `None` solo entre `take` y `create` al rotar el WAL.
    wal: Option<Wal>,
    mem: MemTable,
    /// Índice 0 = más vieja; la última = más nueva (se busca al revés).
    sstables: Vec<SstReader>,
    next_sst: u64,
    next_seq: SeqNum,
    mem_capacity_bytes: usize,
}

impl Engine {
    /// Purpose: abre (o crea) un directorio de datos y recupera WAL + SSTables.
    ///
    /// Inputs: `dir` — carpeta del motor; se crea si no existe.
    ///
    /// Returns: motor con MemTable reconstruida por replay.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with(dir, EngineOptions::default())
    }

    /// Purpose: como [`Engine::open`] con capacidad de MemTable configurable.
    ///
    /// Inputs: `dir`; `opts` — umbral de [`Engine::needs_flush`].
    ///
    /// Returns: motor recuperado.
    pub fn open_with(dir: impl AsRef<Path>, opts: EngineOptions) -> Result<Self, Error> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let wal_path = dir.join(WAL_FILE);
        let sst_files = list_sst_files(&dir)?;
        let mut max_seq = 0_u64;
        let mut sstables = Vec::with_capacity(sst_files.len());
        let mut next_sst = 1_u64;
        for (id, path) in &sst_files {
            let footer = SstFooter::read_from_file(path)?;
            max_seq = max_seq.max(footer.max_seq);
            sstables.push(SstReader::open(path)?);
            next_sst = next_sst.max(id.saturating_add(1));
        }
        let wal = if wal_path.exists() {
            Wal::open(&wal_path)?
        } else {
            Wal::create(&wal_path)?
        };
        let mem = MemTable::new(opts.mem_capacity_bytes);
        for rec in wal.replay()? {
            max_seq = max_seq.max(rec.seq.get());
            match rec.op {
                WalOp::Put { key, value } => {
                    let _ = mem.put(key, value, rec.seq);
                }
                WalOp::Delete { key } => {
                    let _ = mem.delete(key, rec.seq);
                }
            }
        }
        let next_seq = SeqNum::new(max_seq).next()?;
        Ok(Self {
            dir,
            wal_path,
            wal: Some(wal),
            mem,
            sstables,
            next_sst,
            next_seq,
            mem_capacity_bytes: opts.mem_capacity_bytes,
        })
    }

    /// Purpose: directorio de datos.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: path pasado a [`Engine::open`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Purpose: ¿la MemTable alcanzó el umbral? El llamador decide `flush`.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: [`MemTable::is_full`].
    pub fn needs_flush(&self) -> bool {
        self.mem.is_full()
    }

    /// Purpose: número de SSTables abiertas (de más vieja a más nueva).
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `sstables.len()`.
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Purpose: escribe un put durable y luego lo aplica en RAM.
    ///
    /// Inputs: `key` / `value` — bytes; la clave no puede ser vacía.
    ///
    /// Returns: `Ok` cuando el WAL confirmó y la MemTable ve el valor.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key = Key::new(key)?;
        let value = Value::new(value);
        let seq = self.next_seq;
        self.wal_mut()?.append(
            seq,
            WalOp::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )?;
        let _ = self.mem.put(key, value, seq);
        self.next_seq = seq.next()?;
        Ok(())
    }

    /// Purpose: tombstone durable (el `get` no resucita SSTables más viejas).
    ///
    /// Inputs: `key` — bytes no vacíos.
    ///
    /// Returns: `Ok` tras WAL + MemTable.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let key = Key::new(key)?;
        let seq = self.next_seq;
        self.wal_mut()?
            .append(seq, WalOp::Delete { key: key.clone() })?;
        let _ = self.mem.delete(key, seq);
        self.next_seq = seq.next()?;
        Ok(())
    }

    /// Purpose: busca MemTable y luego SSTables nuevas→viejas, sin copiar el valor.
    ///
    /// Inputs: `self` — dueño de RAM y mmaps; `key` — bytes de búsqueda.
    ///
    /// Returns: [`EngineLookup`] atado a `&self`.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Result<EngineLookup<'_>, Error> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        match self.mem.get(key) {
            Lookup::Alive(pinned) => return Ok(EngineLookup::Alive(EngineValue::Mem(pinned))),
            Lookup::Deleted(seq) => return Ok(EngineLookup::Deleted(seq)),
            Lookup::Missing => {}
        }
        for sst in self.sstables.iter().rev() {
            match sst.get(key)? {
                SstLookup::Alive { seq, value } => {
                    return Ok(EngineLookup::Alive(EngineValue::Sst { seq, bytes: value }));
                }
                SstLookup::Deleted(seq) => return Ok(EngineLookup::Deleted(seq)),
                SstLookup::Missing => {}
            }
        }
        Ok(EngineLookup::Missing)
    }

    /// Purpose: vuelca la MemTable a un `.sst` nuevo, abre el reader y rota el WAL.
    ///
    /// Inputs: `self` — se muta (nuevo mmap, MemTable vacía, WAL truncado).
    ///
    /// Returns: `Ok` si no había nada o si el flush terminó; cualquier `SstLookup`
    /// previo caduca porque este método es `&mut self`.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let path = self.dir.join(format!("{:06}.sst", self.next_sst));
        flush_memtable(&self.mem, &path)?;
        self.sstables.push(SstReader::open(&path)?);
        self.next_sst = self.next_sst.saturating_add(1);
        self.mem = MemTable::new(self.mem_capacity_bytes);
        self.rotate_wal()?;
        Ok(())
    }

    /// Cierra el WAL actual y crea uno vacío en el mismo path (post-flush).
    fn rotate_wal(&mut self) -> Result<(), Error> {
        drop(self.wal.take());
        self.wal = Some(Wal::create(&self.wal_path)?);
        Ok(())
    }

    fn wal_mut(&mut self) -> Result<&mut Wal, Error> {
        self.wal
            .as_mut()
            .ok_or(Error::WalCorrupt("WAL no inicializado"))
    }
}

/// Purpose: lista `{id}.sst` en `dir`, ordenados por `id` ascendente.
///
/// Inputs: `dir` — directorio del motor.
///
/// Returns: pares (id, path); ignora nombres que no sean dígitos + `.sst`.
fn list_sst_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, Error> {
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        let path = ent.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".sst") else {
            continue;
        };
        let Ok(id) = stem.parse::<u64>() else {
            continue;
        };
        out.push((id, path));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{Engine, EngineOptions};
    use std::fs;
    use std::path::PathBuf;

    fn temp_engine_dir() -> (tempfile::TempDir, PathBuf) {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("engine-tests");
        fs::create_dir_all(&base).expect("mkdir");
        let dir = tempfile::TempDir::new_in(&base).expect("tempdir");
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    #[test]
    fn put_then_get_from_memtable() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        db.put(b"k", b"v").expect("put");
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"v".as_ref()));
        assert!(db.get(b"no").expect("miss").is_missing());
        assert_eq!(db.sstable_count(), 0);
    }

    #[test]
    fn crash_reopen_replays_wal() {
        let (_tmp, dir) = temp_engine_dir();
        {
            let mut db = Engine::open(&dir).expect("open");
            db.put(b"alpha", b"uno").expect("put");
            db.put(b"beta", b"dos").expect("put");
        }
        let db = Engine::open(&dir).expect("reopen");
        assert_eq!(
            db.get(b"alpha").expect("a").as_bytes(),
            Some(b"uno".as_ref())
        );
        assert_eq!(
            db.get(b"beta").expect("b").as_bytes(),
            Some(b"dos".as_ref())
        );
        assert_eq!(db.sstable_count(), 0);
    }

    #[test]
    fn flush_then_get_from_sstable() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        db.put(b"k", b"sst").expect("put");
        db.flush().expect("flush");
        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"sst".as_ref()));
    }

    #[test]
    fn flush_then_reopen_reads_sstable_not_wal() {
        let (_tmp, dir) = temp_engine_dir();
        {
            let mut db = Engine::open(&dir).expect("open");
            db.put(b"k", b"disco").expect("put");
            db.flush().expect("flush");
        }
        let db = Engine::open(&dir).expect("reopen");
        assert_eq!(db.sstable_count(), 1);
        assert_eq!(
            db.get(b"k").expect("get").as_bytes(),
            Some(b"disco".as_ref())
        );
    }

    #[test]
    fn newer_sstable_wins_over_older() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        db.put(b"k", b"viejo").expect("put1");
        db.flush().expect("f1");
        db.put(b"k", b"nuevo").expect("put2");
        db.flush().expect("f2");
        assert_eq!(db.sstable_count(), 2);
        assert_eq!(
            db.get(b"k").expect("get").as_bytes(),
            Some(b"nuevo".as_ref())
        );
    }

    #[test]
    fn tombstone_hides_sstable_value() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        db.put(b"k", b"vivo").expect("put");
        db.flush().expect("flush");
        db.delete(b"k").expect("del");
        assert!(db.get(b"k").expect("get").is_deleted());
    }

    #[test]
    fn memtable_overrides_sstable_until_flush() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        db.put(b"k", b"sst").expect("put");
        db.flush().expect("flush");
        db.put(b"k", b"ram").expect("put2");
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"ram".as_ref()));
    }

    #[test]
    fn needs_flush_after_small_capacity() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open_with(
            &dir,
            EngineOptions {
                mem_capacity_bytes: 8,
            },
        )
        .expect("open");
        db.put(b"k", b"12345678").expect("put");
        assert!(db.needs_flush());
    }

    #[test]
    fn empty_key_is_error() {
        let (_tmp, dir) = temp_engine_dir();
        let mut db = Engine::open(&dir).expect("open");
        assert!(matches!(db.put(b"", b"v"), Err(crate::Error::EmptyKey)));
        assert!(matches!(db.get(b""), Err(crate::Error::EmptyKey)));
    }
}
