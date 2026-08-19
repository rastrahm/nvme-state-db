//! Motor: WAL → MemTable activa / inmutable → SSTables.
//!
//! ## Por qué no se puede bloquear el ejecutor
//!
//! Escribir un `.sst` es I/O (ms). Un `put` del estado blockchain no puede
//! esperar eso. Se **rota** la MemTable: la llena pasa a inmutable (solo
//! lecturas) y una nueva activa recibe writes. Un worker vuelca la inmutable.
//!
//! ## `get` durante el flush
//!
//! Orden: **activa** → **inmutable** → SSTables nuevas→viejas. Si no se
//! mirara la inmutable, un `get` perdería claves que aún no están en el mmap.
//!
//! El `get` del motor **copia** el valor a un [`Value`]. Así el worker puede
//! soltar la tabla inmutable sin lifetimes atados a un `RwLock`. MemTable y
//! SST siguen siendo zero-copy por dentro.

use crate::error::Error;
use crate::memtable::{Lookup, MemTable};
use crate::sstable::{flush_memtable, SstFooter, SstLookup, SstReader};
use crate::types::{Key, SeqNum, Value};
use crate::wal::{Wal, WalOp};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, JoinHandle};

/// Nombre del WAL de la MemTable activa.
const WAL_FILE: &str = "wal";
/// WAL de la tabla que se está flusheando (tras rotar).
const WAL_FLUSH_FILE: &str = "wal.flush";
/// Capacidad por defecto de la MemTable (señal de flush), en bytes.
const DEFAULT_MEM_CAPACITY: usize = 64 * 1024;

// =============================================================================
// OPCIONES / LOOKUP
// =============================================================================

/// Opciones al abrir el motor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineOptions {
    /// Umbral de [`MemTable::is_full`] de la tabla **activa**.
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

/// Bytes vivos de un `get` (copia en el borde del motor).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineValue {
    /// Hit en la MemTable activa.
    Mem {
        /// Seq del put.
        seq: SeqNum,
        /// Copia del valor.
        value: Value,
    },
    /// Hit en la MemTable inmutable (flush en curso).
    Frozen {
        /// Seq del put.
        seq: SeqNum,
        /// Copia del valor.
        value: Value,
    },
    /// Hit en una SSTable.
    Sst {
        /// Seq del registro.
        seq: SeqNum,
        /// Copia del valor (el mmap sigue zero-copy hasta aquí).
        value: Value,
    },
}

impl EngineValue {
    /// Purpose: expone los bytes.
    ///
    /// Inputs: `self` — put vivo.
    ///
    /// Returns: slice del [`Value`] copiado.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Mem { value, .. } | Self::Frozen { value, .. } | Self::Sst { value, .. } => {
                value.as_bytes()
            }
        }
    }

    /// Purpose: seq del put que ganó el `get`.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: seq de RAM o de SST.
    #[inline(always)]
    pub fn seq(&self) -> SeqNum {
        match self {
            Self::Mem { seq, .. } | Self::Frozen { seq, .. } | Self::Sst { seq, .. } => *seq,
        }
    }
}

/// Resultado de [`Engine::get`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineLookup {
    /// Valor vivo.
    Alive(EngineValue),
    /// Tombstone: no seguir buscando versiones más viejas.
    Deleted(SeqNum),
    /// Nadie conoce la clave.
    Missing,
}

impl EngineLookup {
    /// Purpose: ¿la clave no está en MemTable ni en SSTables?
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `true` solo en [`EngineLookup::Missing`].
    #[inline(always)]
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Purpose: ¿hay un tombstone más reciente que cualquier versión más vieja?
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

// -----------------------------------------------------------------------------
// Estado compartido + worker
// -----------------------------------------------------------------------------

struct Snapshot {
    active: Arc<MemTable>,
    frozen: Option<Arc<MemTable>>,
    sstables: Vec<SstReader>,
}

struct FlushCtl {
    in_flight: bool,
    last_err: Option<String>,
}

enum Job {
    Flush {
        frozen: Arc<MemTable>,
        sst_id: u64,
        flush_wal: PathBuf,
    },
    Stop,
}

struct Inner {
    dir: PathBuf,
    wal_path: PathBuf,
    flush_wal_path: PathBuf,
    wal: Mutex<Option<Wal>>,
    snapshot: RwLock<Snapshot>,
    next_seq: AtomicU64,
    next_sst: AtomicU64,
    mem_capacity_bytes: usize,
    flush: Mutex<FlushCtl>,
    flush_cv: Condvar,
    job_tx: Sender<Job>,
    #[cfg(test)]
    flush_gate: Mutex<()>,
}

// =============================================================================
// Engine
// =============================================================================

/// Motor KV: `put`/`get` no esperan al I/O del `.sst`.
pub struct Engine {
    inner: Arc<Inner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Engine {
    /// Purpose: abre (o crea) un directorio de datos y arranca el worker de flush.
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
        let flush_wal_path = dir.join(WAL_FLUSH_FILE);

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

        let mem = MemTable::new(opts.mem_capacity_bytes);
        recover_wals(&wal_path, &flush_wal_path, &mem, &mut max_seq)?;

        let wal = if wal_path.exists() {
            Wal::open(&wal_path)?
        } else {
            Wal::create(&wal_path)?
        };

        let next_seq = SeqNum::new(max_seq).next()?.get();
        let (job_tx, job_rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            dir,
            wal_path,
            flush_wal_path,
            wal: Mutex::new(Some(wal)),
            snapshot: RwLock::new(Snapshot {
                active: Arc::new(mem),
                frozen: None,
                sstables,
            }),
            next_seq: AtomicU64::new(next_seq),
            next_sst: AtomicU64::new(next_sst),
            mem_capacity_bytes: opts.mem_capacity_bytes,
            flush: Mutex::new(FlushCtl {
                in_flight: false,
                last_err: None,
            }),
            flush_cv: Condvar::new(),
            job_tx,
            #[cfg(test)]
            flush_gate: Mutex::new(()),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("nvme-sst-flush".into())
            .spawn(move || run_worker(worker_inner, job_rx))
            .map_err(Error::from)?;
        Ok(Self {
            inner,
            worker: Mutex::new(Some(worker)),
        })
    }

    /// Purpose: directorio de datos.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: path pasado a [`Engine::open`].
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// Purpose: ¿la MemTable **activa** alcanzó el umbral?
    ///
    /// Inputs: `self`.
    ///
    /// Returns: [`MemTable::is_full`] de la tabla que recibe writes.
    pub fn needs_flush(&self) -> Result<bool, Error> {
        Ok(read_snap(&self.inner.snapshot)?.active.is_full())
    }

    /// Purpose: hay una MemTable inmutable esperando o en medio del worker.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `true` si `get` aún debe mirar la tabla congelada.
    pub fn has_frozen_memtable(&self) -> Result<bool, Error> {
        Ok(read_snap(&self.inner.snapshot)?.frozen.is_some())
    }

    /// Purpose: número de SSTables ya instaladas.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: longitud de la lista (las inmutables en RAM no cuentan).
    pub fn sstable_count(&self) -> Result<usize, Error> {
        Ok(read_snap(&self.inner.snapshot)?.sstables.len())
    }

    /// Purpose: escribe un put durable y lo aplica a la MemTable **activa**.
    ///
    /// Inputs: `key` / `value` — bytes; la clave no puede ser vacía.
    ///
    /// Returns: `Ok` cuando el WAL confirmó y la activa ve el valor. No espera
    /// al worker de SST.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key = Key::new(key)?;
        let value = Value::new(value);
        self.write_op(WalOp::Put {
            key: key.clone(),
            value: value.clone(),
        })?;
        Ok(())
    }

    /// Purpose: tombstone durable sobre la tabla activa.
    ///
    /// Inputs: `key` — bytes no vacíos.
    ///
    /// Returns: `Ok` tras WAL + MemTable activa.
    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let key = Key::new(key)?;
        self.write_op(WalOp::Delete { key })?;
        Ok(())
    }

    /// Purpose: busca activa → inmutable → SSTables nuevas→viejas.
    ///
    /// Inputs: `self`; `key` — bytes de búsqueda.
    ///
    /// Returns: [`EngineLookup`] con valor copiado (no presta el mmap).
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Result<EngineLookup, Error> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let snap = read_snap(&self.inner.snapshot)?;
        if let Some(hit) = lookup_mem(&snap.active, key, false) {
            return Ok(hit);
        }
        if let Some(frozen) = &snap.frozen {
            if let Some(hit) = lookup_mem(frozen, key, true) {
                return Ok(hit);
            }
        }
        for sst in snap.sstables.iter().rev() {
            match sst.get(key)? {
                SstLookup::Alive { seq, value } => {
                    return Ok(EngineLookup::Alive(EngineValue::Sst {
                        seq,
                        value: Value::new(value),
                    }));
                }
                SstLookup::Deleted(seq) => return Ok(EngineLookup::Deleted(seq)),
                SstLookup::Missing => {}
            }
        }
        Ok(EngineLookup::Missing)
    }

    /// Purpose: rota si hace falta y espera a que el worker instale el `.sst`.
    ///
    /// Inputs: `self` — no bloquea **otros** `put`/`get` durante el I/O (solo
    /// espera el llamador de `flush`).
    ///
    /// Returns: `Ok` cuando no queda nada en activa ni inmutable.
    pub fn flush(&self) -> Result<(), Error> {
        loop {
            self.wait_flush()?;
            let empty = {
                let snap = read_snap(&self.inner.snapshot)?;
                snap.active.is_empty() && snap.frozen.is_none()
            };
            if empty {
                return Ok(());
            }
            self.schedule_flush()?;
            self.wait_flush()?;
        }
    }

    /// Purpose: congela la activa y encola el volcado; no espera al disco.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `Ok` si no había datos o si el job se envió.
    pub fn schedule_flush(&self) -> Result<(), Error> {
        self.wait_flush()?;
        let job = {
            let mut snap = write_snap(&self.inner.snapshot)?;
            if snap.frozen.is_some() {
                return Ok(());
            }
            if snap.active.is_empty() {
                return Ok(());
            }
            let frozen = Arc::clone(&snap.active);
            snap.active = Arc::new(MemTable::new(self.inner.mem_capacity_bytes));
            snap.frozen = Some(Arc::clone(&frozen));
            let sst_id = self.inner.next_sst.fetch_add(1, Ordering::SeqCst);
            drop(snap);
            self.rotate_wal()?;
            Job::Flush {
                frozen,
                sst_id,
                flush_wal: self.inner.flush_wal_path.clone(),
            }
        };
        {
            let mut ctl = lock_mutex(&self.inner.flush)?;
            ctl.in_flight = true;
            ctl.last_err = None;
        }
        self.inner
            .job_tx
            .send(job)
            .map_err(|_| Error::Flush("worker de flush no está vivo".into()))?;
        Ok(())
    }

    /// Purpose: espera a que termine el job de flush en curso (si hay).
    ///
    /// Inputs: `self`.
    ///
    /// Returns: error del worker si el volcado falló.
    pub fn wait_flush(&self) -> Result<(), Error> {
        let mut ctl = lock_mutex(&self.inner.flush)?;
        while ctl.in_flight {
            ctl = self
                .inner
                .flush_cv
                .wait(ctl)
                .map_err(|_| Error::LockPoisoned)?;
        }
        if let Some(msg) = ctl.last_err.take() {
            return Err(Error::Flush(msg));
        }
        Ok(())
    }

    fn write_op(&self, op: WalOp) -> Result<(), Error> {
        let snap = read_snap(&self.inner.snapshot)?;
        let mut wal_slot = lock_mutex(&self.inner.wal)?;
        let wal = wal_slot
            .as_mut()
            .ok_or(Error::WalCorrupt("WAL no inicializado"))?;
        let seq_raw = self.inner.next_seq.fetch_add(1, Ordering::SeqCst);
        if seq_raw == 0 {
            return Err(Error::SequenceOverflow);
        }
        let seq = SeqNum::new(seq_raw);
        match &op {
            WalOp::Put { key, value } => {
                wal.append(seq, op.clone())?;
                let _ = snap.active.put(key.clone(), value.clone(), seq);
            }
            WalOp::Delete { key } => {
                wal.append(seq, op.clone())?;
                let _ = snap.active.delete(key.clone(), seq);
            }
        }
        Ok(())
    }

    fn rotate_wal(&self) -> Result<(), Error> {
        let mut slot = lock_mutex(&self.inner.wal)?;
        drop(slot.take());
        if self.inner.flush_wal_path.exists() {
            fs::remove_file(&self.inner.flush_wal_path)?;
        }
        fs::rename(&self.inner.wal_path, &self.inner.flush_wal_path)?;
        *slot = Some(Wal::create(&self.inner.wal_path)?);
        Ok(())
    }

    #[cfg(test)]
    fn hold_flush_worker(&self) -> Result<MutexGuard<'_, ()>, Error> {
        lock_mutex(&self.inner.flush_gate)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.inner.job_tx.send(Job::Stop);
        if let Ok(mut slot) = self.worker.lock() {
            if let Some(handle) = slot.take() {
                let _ = handle.join();
            }
        }
    }
}

fn run_worker(inner: Arc<Inner>, rx: Receiver<Job>) {
    while let Ok(job) = rx.recv() {
        match job {
            Job::Stop => break,
            Job::Flush {
                frozen,
                sst_id,
                flush_wal,
            } => {
                #[cfg(test)]
                let _gate = inner.flush_gate.lock();
                let result = install_sst(&inner, frozen, sst_id, &flush_wal);
                let mut ctl = match inner.flush.lock() {
                    Ok(c) => c,
                    Err(_) => break,
                };
                ctl.in_flight = false;
                ctl.last_err = result.err().map(|e| e.to_string());
                inner.flush_cv.notify_all();
            }
        }
    }
}

fn install_sst(
    inner: &Inner,
    frozen: Arc<MemTable>,
    sst_id: u64,
    flush_wal: &Path,
) -> Result<(), Error> {
    let tmp = inner.dir.join(format!("{sst_id:06}.sst.tmp"));
    let final_path = inner.dir.join(format!("{sst_id:06}.sst"));
    flush_memtable(&frozen, &tmp)?;
    fs::rename(&tmp, &final_path)?;
    let reader = SstReader::open(&final_path)?;
    {
        let mut snap = write_snap(&inner.snapshot)?;
        snap.sstables.push(reader);
        snap.frozen = None;
    }
    if flush_wal.exists() {
        fs::remove_file(flush_wal)?;
    }
    Ok(())
}

fn lookup_mem(table: &MemTable, key: &[u8], frozen: bool) -> Option<EngineLookup> {
    match table.get(key) {
        Lookup::Alive(pinned) => {
            let seq = pinned.seq();
            let value = pinned.value().clone();
            let inner = if frozen {
                EngineValue::Frozen { seq, value }
            } else {
                EngineValue::Mem { seq, value }
            };
            Some(EngineLookup::Alive(inner))
        }
        Lookup::Deleted(seq) => Some(EngineLookup::Deleted(seq)),
        Lookup::Missing => None,
    }
}

/// Replays `wal.flush` (si existe) y luego `wal`; checkpoint si hubo flush file.
fn recover_wals(
    wal_path: &Path,
    flush_wal_path: &Path,
    mem: &MemTable,
    max_seq: &mut u64,
) -> Result<(), Error> {
    let had_flush = flush_wal_path.exists();
    if had_flush {
        let w = Wal::open(flush_wal_path)?;
        apply_replay(&w, mem, max_seq)?;
    }
    if wal_path.exists() {
        let w = Wal::open(wal_path)?;
        apply_replay(&w, mem, max_seq)?;
    }
    if had_flush {
        drop_wal_files(wal_path, flush_wal_path)?;
        let mut wal = Wal::create(wal_path)?;
        checkpoint_mem(mem, &mut wal)?;
    }
    Ok(())
}

fn apply_replay(wal: &Wal, mem: &MemTable, max_seq: &mut u64) -> Result<(), Error> {
    for rec in wal.replay()? {
        *max_seq = (*max_seq).max(rec.seq.get());
        match rec.op {
            WalOp::Put { key, value } => {
                let _ = mem.put(key, value, rec.seq);
            }
            WalOp::Delete { key } => {
                let _ = mem.delete(key, rec.seq);
            }
        }
    }
    Ok(())
}

fn checkpoint_mem(mem: &MemTable, wal: &mut Wal) -> Result<(), Error> {
    for item in mem.iter() {
        let op = match item.mem_value() {
            crate::memtable::MemValue::Put(v) => WalOp::Put {
                key: item.key().clone(),
                value: v.clone(),
            },
            crate::memtable::MemValue::Tombstone => WalOp::Delete {
                key: item.key().clone(),
            },
        };
        wal.append(item.seq(), op)?;
    }
    Ok(())
}

fn drop_wal_files(wal_path: &Path, flush_wal_path: &Path) -> Result<(), Error> {
    if wal_path.exists() {
        fs::remove_file(wal_path)?;
    }
    if flush_wal_path.exists() {
        fs::remove_file(flush_wal_path)?;
    }
    Ok(())
}

fn lock_mutex<T>(m: &Mutex<T>) -> Result<MutexGuard<'_, T>, Error> {
    m.lock().map_err(|_| Error::LockPoisoned)
}

fn read_snap(s: &RwLock<Snapshot>) -> Result<RwLockReadGuard<'_, Snapshot>, Error> {
    s.read().map_err(|_| Error::LockPoisoned)
}

fn write_snap(s: &RwLock<Snapshot>) -> Result<RwLockWriteGuard<'_, Snapshot>, Error> {
    s.write().map_err(|_| Error::LockPoisoned)
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
    use super::{Engine, EngineOptions, EngineValue};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;

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
        let db = Engine::open(&dir).expect("open");
        db.put(b"k", b"v").expect("put");
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"v".as_ref()));
        assert!(db.get(b"no").expect("miss").is_missing());
        assert_eq!(db.sstable_count().expect("n"), 0);
    }

    #[test]
    fn crash_reopen_replays_wal() {
        let (_tmp, dir) = temp_engine_dir();
        {
            let db = Engine::open(&dir).expect("open");
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
        assert_eq!(db.sstable_count().expect("n"), 0);
    }

    #[test]
    fn flush_then_get_from_sstable() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        db.put(b"k", b"sst").expect("put");
        db.flush().expect("flush");
        assert_eq!(db.sstable_count().expect("n"), 1);
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"sst".as_ref()));
    }

    #[test]
    fn flush_then_reopen_reads_sstable_not_wal() {
        let (_tmp, dir) = temp_engine_dir();
        {
            let db = Engine::open(&dir).expect("open");
            db.put(b"k", b"disco").expect("put");
            db.flush().expect("flush");
        }
        let db = Engine::open(&dir).expect("reopen");
        assert_eq!(db.sstable_count().expect("n"), 1);
        assert_eq!(
            db.get(b"k").expect("get").as_bytes(),
            Some(b"disco".as_ref())
        );
    }

    #[test]
    fn newer_sstable_wins_over_older() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        db.put(b"k", b"viejo").expect("put1");
        db.flush().expect("f1");
        db.put(b"k", b"nuevo").expect("put2");
        db.flush().expect("f2");
        assert_eq!(db.sstable_count().expect("n"), 2);
        assert_eq!(
            db.get(b"k").expect("get").as_bytes(),
            Some(b"nuevo".as_ref())
        );
    }

    #[test]
    fn tombstone_hides_sstable_value() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        db.put(b"k", b"vivo").expect("put");
        db.flush().expect("flush");
        db.delete(b"k").expect("del");
        assert!(db.get(b"k").expect("get").is_deleted());
    }

    #[test]
    fn memtable_overrides_sstable_until_flush() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        db.put(b"k", b"sst").expect("put");
        db.flush().expect("flush");
        db.put(b"k", b"ram").expect("put2");
        assert_eq!(db.get(b"k").expect("get").as_bytes(), Some(b"ram".as_ref()));
    }

    #[test]
    fn needs_flush_after_small_capacity() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open_with(
            &dir,
            EngineOptions {
                mem_capacity_bytes: 8,
            },
        )
        .expect("open");
        db.put(b"k", b"12345678").expect("put");
        assert!(db.needs_flush().expect("full"));
    }

    #[test]
    fn empty_key_is_error() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        assert!(matches!(db.put(b"", b"v"), Err(crate::Error::EmptyKey)));
        assert!(matches!(db.get(b""), Err(crate::Error::EmptyKey)));
    }

    #[test]
    fn get_sees_frozen_memtable_while_worker_blocked() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Engine::open(&dir).expect("open");
        db.put(b"old", b"frozen").expect("put");
        let _gate = db.hold_flush_worker().expect("gate");
        db.schedule_flush().expect("schedule");
        assert!(db.has_frozen_memtable().expect("frozen"));
        assert_eq!(db.sstable_count().expect("n"), 0);
        db.put(b"new", b"active").expect("put during flush");
        match db.get(b"old").expect("old") {
            super::EngineLookup::Alive(EngineValue::Frozen { value, .. }) => {
                assert_eq!(value.as_bytes(), b"frozen");
            }
            other => panic!("old debía estar en inmutable, no {other:?}"),
        }
        match db.get(b"new").expect("new") {
            super::EngineLookup::Alive(EngineValue::Mem { value, .. }) => {
                assert_eq!(value.as_bytes(), b"active");
            }
            other => panic!("new debía estar en activa, no {other:?}"),
        }
        drop(_gate);
        db.wait_flush().expect("wait");
        assert!(!db.has_frozen_memtable().expect("gone"));
        assert_eq!(db.sstable_count().expect("n"), 1);
        assert_eq!(
            db.get(b"old").expect("old sst").as_bytes(),
            Some(b"frozen".as_ref())
        );
        assert_eq!(
            db.get(b"new").expect("new mem").as_bytes(),
            Some(b"active".as_ref())
        );
    }

    #[test]
    fn puts_proceed_on_another_thread_during_flush() {
        let (_tmp, dir) = temp_engine_dir();
        let db = Arc::new(Engine::open(&dir).expect("open"));
        db.put(b"k", b"v").expect("seed");
        let gate = db.hold_flush_worker().expect("gate");
        db.schedule_flush().expect("schedule");
        let writer = Arc::clone(&db);
        let h = thread::spawn(move || {
            for i in 0..32_u32 {
                let k = format!("n{i}");
                writer.put(k.as_bytes(), b"x").expect("put");
            }
        });
        h.join().expect("join");
        drop(gate);
        db.wait_flush().expect("wait");
        assert_eq!(db.get(b"k").expect("k").as_bytes(), Some(b"v".as_ref()));
        assert_eq!(db.get(b"n0").expect("n0").as_bytes(), Some(b"x".as_ref()));
    }
}
