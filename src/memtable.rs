//! MemTable concurrente: SkipList lock-free en RAM.
//!
//! Las mutaciones toman `&self` (no `&mut self`): varios hilos pueden hacer
//! `put`/`get`/`delete` a la vez sin un mutex global. El orden de las claves
//! es el de [`Key`] (lexicográfico), el mismo que usarán las SSTables al hacer flush.
//!
//! Un **tombstone** es un borrado explícito. No es lo mismo que “la clave no
//! está”: si solo estuviera ausente, el `get` del motor (fase 7) seguiría
//! buscando en SSTables y **resucitaría** un valor ya borrado.

use crate::types::{Key, SeqNum, Value};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bytes contados de más por entrada, además de clave y valor.
///
/// Cubre `SeqNum` (8) y la etiqueta put/tombstone. No incluye punteros internos
/// del SkipList: [`MemTable::approx_bytes`] es una cota inferior, suficiente
/// para decidir un flush.
const ENTRY_OVERHEAD: usize = 16;

/// Registro interno: secuencia + payload. El `Value` vacío en un tombstone
/// no se publica; evita `unwrap` al leer un put.
struct Record {
    seq: SeqNum,
    tombstone: bool,
    value: Value,
}

impl Record {
    /// Purpose: estima los bytes de esta entrada más la clave asociada.
    ///
    /// Inputs: `key` — clave almacenada; `self` — put o tombstone.
    ///
    /// Returns: `key.len() + (value.len() si put) + ENTRY_OVERHEAD`.
    #[inline(always)]
    fn encoded_size(&self, key: &Key) -> usize {
        let payload = if self.tombstone { 0 } else { self.value.len() };
        key.len()
            .saturating_add(payload)
            .saturating_add(ENTRY_OVERHEAD)
    }
}

/// Resultado de un `get` en la MemTable: tres estados, no un `Option`.
///
/// `Option<Value>` colapsaría tombstone y ausencia, y el motor no podría
/// saber si debe seguir buscando en disco.
pub enum Lookup<'a> {
    /// La clave está viva. [`PinnedValue`] mantiene el nodo del SkipList
    /// anclado: `&Value` no copia los bytes.
    Alive(PinnedValue<'a>),
    /// Borrado explícito. El motor **no** debe mirar SSTables más viejas.
    Deleted(SeqNum),
    /// La MemTable no tiene la clave. El motor **sí** puede mirar SSTables.
    Missing,
}

impl<'a> Lookup<'a> {
    /// Purpose: obtiene el valor si la clave está viva.
    ///
    /// Inputs: `self` — resultado de [`MemTable::get`].
    ///
    /// Returns: `Some` solo en [`Lookup::Alive`]; `None` si está borrada o ausente.
    #[inline(always)]
    pub fn value(&self) -> Option<&Value> {
        match self {
            Lookup::Alive(pinned) => Some(pinned.value()),
            Lookup::Deleted(_) | Lookup::Missing => None,
        }
    }

    /// Purpose: indica si este `get` encontró un tombstone.
    ///
    /// Inputs: `self` — resultado de [`MemTable::get`].
    ///
    /// Returns: `true` solo para [`Lookup::Deleted`].
    #[inline(always)]
    pub fn is_deleted(&self) -> bool {
        matches!(self, Lookup::Deleted(_))
    }

    /// Purpose: indica si la MemTable no conoce la clave.
    ///
    /// Inputs: `self` — resultado de [`MemTable::get`].
    ///
    /// Returns: `true` solo para [`Lookup::Missing`].
    #[inline(always)]
    pub fn is_missing(&self) -> bool {
        matches!(self, Lookup::Missing)
    }
}

/// Valor anclado al nodo del SkipList: no copia el registro.
pub struct PinnedValue<'a> {
    entry: Entry<'a, Key, Record>,
}

impl<'a> PinnedValue<'a> {
    /// Purpose: borra el ancla y expone los bytes vivos.
    ///
    /// Inputs: `self` — solo se construye para un put, nunca para tombstone.
    ///
    /// Returns: referencia al [`Value`] cuyo lifetime es el de `self`.
    #[inline(always)]
    pub fn value(&self) -> &Value {
        &self.entry.value().value
    }

    /// Purpose: número de secuencia de este put.
    ///
    /// Inputs: `self` — entrada viva anclada.
    ///
    /// Returns: el [`SeqNum`] almacenado junto al valor.
    #[inline(always)]
    pub fn seq(&self) -> SeqNum {
        self.entry.value().seq
    }

    /// Purpose: clave de esta entrada, sin copiar.
    ///
    /// Inputs: `self` — entrada viva anclada.
    ///
    /// Returns: referencia a la [`Key`] del nodo.
    #[inline(always)]
    pub fn key(&self) -> &Key {
        self.entry.key()
    }
}

/// Payload público de una entrada (put o tombstone), para flush e iteración.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MemValue<'a> {
    /// Valor vivo.
    Put(&'a Value),
    /// Borrado explícito.
    Tombstone,
}

/// Una entrada durante la iteración ordenada (incluye tombstones).
pub struct IterItem<'a> {
    entry: Entry<'a, Key, Record>,
}

impl IterItem<'_> {
    /// Purpose: clave de la entrada, en orden lexicográfico del iterador.
    ///
    /// Inputs: `self` — item producido por [`MemTable::iter`].
    ///
    /// Returns: referencia a la [`Key`] del nodo.
    #[inline(always)]
    pub fn key(&self) -> &Key {
        self.entry.key()
    }

    /// Purpose: secuencia de esta versión.
    ///
    /// Inputs: `self` — item de iteración.
    ///
    /// Returns: [`SeqNum`] del put o del tombstone.
    #[inline(always)]
    pub fn seq(&self) -> SeqNum {
        self.entry.value().seq
    }

    /// Purpose: interpreta put vs tombstone sin copiar el valor.
    ///
    /// Inputs: `self` — item de iteración.
    ///
    /// Returns: [`MemValue::Put`] o [`MemValue::Tombstone`].
    #[inline(always)]
    pub fn mem_value(&self) -> MemValue<'_> {
        let record = self.entry.value();
        if record.tombstone {
            MemValue::Tombstone
        } else {
            MemValue::Put(&record.value)
        }
    }
}

/// Tabla en memoria, compartible entre hilos (`Send + Sync`).
///
/// No rechaza writes al llenarse: [`MemTable::is_full`] es una señal para el
/// motor (rotar a una MemTable inmutable y hacer flush). Seguir aceptando puts
/// evita bloquear al ejecutor.
pub struct MemTable {
    map: SkipMap<Key, Record>,
    capacity_bytes: usize,
    approx_bytes: AtomicUsize,
}

impl MemTable {
    /// Purpose: crea una MemTable vacía con umbral de flush en bytes.
    ///
    /// Inputs: `capacity_bytes` — tamaño aproximado a partir del cual
    /// [`MemTable::is_full`] es verdadero. `0` hace que esté llena desde el
    /// primer write de tamaño > 0; no se rechaza el write.
    ///
    /// Returns: una MemTable vacía, lista para compartir con `Arc`.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            map: SkipMap::new(),
            capacity_bytes,
            approx_bytes: AtomicUsize::new(0),
        }
    }

    /// Purpose: umbral de flush configurado al construir.
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: el `capacity_bytes` pasado a [`MemTable::new`].
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity_bytes
    }

    /// Purpose: estimación actual de bytes ocupados (clave + valor + overhead).
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: contador relajado; bajo concurrencia es aproximado.
    #[inline(always)]
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Purpose: indica si conviene hacer flush.
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: `true` si [`MemTable::approx_bytes`] ≥ capacidad. No bloquea
    /// ni rechaza writes posteriores.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.approx_bytes() >= self.capacity_bytes
    }

    /// Purpose: número aproximado de claves distintas (puts y tombstones).
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: `SkipMap::len`, aproximado si hay writes concurrentes.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Purpose: indica si no hay entradas.
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: `true` si el SkipMap está vacío.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Purpose: inserta o sobreescribe un valor vivo.
    ///
    /// Inputs:
    /// - `key` — clave ya validada (no vacía).
    /// - `value` — bytes a almacenar; vacío no es un borrado.
    /// - `seq` — secuencia de este write (el WAL la asignará en la fase 3).
    ///
    /// Returns: `true` si este `seq` quedó instalado; `false` si ya había un
    /// `seq` estrictamente mayor (write obsoleto, no pisa).
    pub fn put(&self, key: Key, value: Value, seq: SeqNum) -> bool {
        self.apply(
            key,
            Record {
                seq,
                tombstone: false,
                value,
            },
        )
    }

    /// Purpose: registra un tombstone (borrado explícito).
    ///
    /// Inputs:
    /// - `key` — clave a borrar; si no existía, igual se inserta el tombstone
    ///   (puede haber un valor más viejo en una SSTable).
    /// - `seq` — secuencia de este delete.
    ///
    /// Returns: `true` si este `seq` quedó instalado; `false` si es obsoleto.
    pub fn delete(&self, key: Key, seq: SeqNum) -> bool {
        self.apply(
            key,
            Record {
                seq,
                tombstone: true,
                value: Value::default(),
            },
        )
    }

    /// Purpose: busca una clave sin copiar el valor.
    ///
    /// Inputs: `key` — bytes de la clave (p. ej. [`Key::as_bytes`]).
    ///
    /// Returns: [`Lookup::Alive`], [`Lookup::Deleted`] o [`Lookup::Missing`].
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Lookup<'_> {
        match self.map.get(key) {
            None => Lookup::Missing,
            Some(entry) => {
                if entry.value().tombstone {
                    Lookup::Deleted(entry.value().seq)
                } else {
                    Lookup::Alive(PinnedValue { entry })
                }
            }
        }
    }

    /// Purpose: recorre las entradas en orden de [`Key`], incluidos tombstones.
    ///
    /// Inputs: `self` — tabla.
    ///
    /// Returns: iterador de [`IterItem`] válido mientras viva la MemTable.
    pub fn iter(&self) -> impl Iterator<Item = IterItem<'_>> + '_ {
        self.map.iter().map(|entry| IterItem { entry })
    }

    /// Purpose: instala `record` si su seq no es más viejo que el actual.
    ///
    /// Inputs: `key` — clave poseída; `record` — put o tombstone a instalar.
    ///
    /// Returns: `true` si el registro instalado tiene exactamente `record.seq`.
    fn apply(&self, key: Key, record: Record) -> bool {
        let new_size = record.encoded_size(&key);
        let old_size = self
            .map
            .get(key.as_bytes())
            .map(|e| e.value().encoded_size(e.key()))
            .unwrap_or(0);
        let seq = record.seq;
        let entry = self.map.compare_insert(key, record, |old| seq >= old.seq);
        let applied = entry.value().seq == seq;
        if applied {
            let _ =
                self.approx_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(old_size).saturating_add(new_size))
                    });
        }
        applied
    }
}

#[cfg(test)]
mod tests {
    use super::{Lookup, MemTable, MemValue};
    use crate::types::{Key, SeqNum, Value};
    use std::sync::Arc;

    fn key(bytes: &[u8]) -> Key {
        Key::new(bytes).expect("clave de test")
    }

    #[test]
    fn empty_memtable_is_empty() {
        let table = MemTable::new(1024);
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert_eq!(table.approx_bytes(), 0);
        assert!(!table.is_full());
        assert!(table.get(b"missing").is_missing());
    }

    #[test]
    fn put_then_get_returns_same_bytes() {
        let table = MemTable::new(1024);
        assert!(table.put(key(b"k"), Value::new(b"v"), SeqNum::new(1)));
        let got = table.get(b"k");
        match got {
            Lookup::Alive(pinned) => {
                assert_eq!(pinned.value().as_bytes(), b"v");
                assert_eq!(pinned.seq(), SeqNum::new(1));
                assert_eq!(pinned.key().as_bytes(), b"k");
            }
            other => panic!(
                "esperado Alive, got deleted={} missing={}",
                other.is_deleted(),
                other.is_missing()
            ),
        }
    }

    #[test]
    fn overwrite_replaces_value() {
        let table = MemTable::new(1024);
        table.put(key(b"k"), Value::new(b"old"), SeqNum::new(1));
        table.put(key(b"k"), Value::new(b"new"), SeqNum::new(2));
        assert_eq!(
            table.get(b"k").value().map(Value::as_bytes),
            Some(b"new".as_ref())
        );
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn stale_seq_does_not_clobber() {
        let table = MemTable::new(1024);
        assert!(table.put(key(b"k"), Value::new(b"new"), SeqNum::new(5)));
        assert!(!table.put(key(b"k"), Value::new(b"old"), SeqNum::new(3)));
        assert_eq!(
            table.get(b"k").value().map(Value::as_bytes),
            Some(b"new".as_ref())
        );
    }

    #[test]
    fn delete_is_deleted_not_missing() {
        let table = MemTable::new(1024);
        table.put(key(b"k"), Value::new(b"v"), SeqNum::new(1));
        assert!(table.delete(key(b"k"), SeqNum::new(2)));
        let got = table.get(b"k");
        match got {
            Lookup::Deleted(seq) => assert_eq!(seq, SeqNum::new(2)),
            Lookup::Alive(_) => panic!("un tombstone no es Alive"),
            Lookup::Missing => panic!("un tombstone no es Missing: el motor iría a disco"),
        }
        assert!(table.get(b"k").is_deleted());
        assert!(table.get(b"k").value().is_none());
    }

    #[test]
    fn delete_of_unknown_key_still_writes_tombstone() {
        let table = MemTable::new(1024);
        assert!(table.delete(key(b"ghost"), SeqNum::new(1)));
        assert!(table.get(b"ghost").is_deleted());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn put_after_delete_resurrects() {
        let table = MemTable::new(1024);
        table.delete(key(b"k"), SeqNum::new(1));
        table.put(key(b"k"), Value::new(b"back"), SeqNum::new(2));
        assert_eq!(
            table.get(b"k").value().map(Value::as_bytes),
            Some(b"back".as_ref())
        );
    }

    #[test]
    fn empty_value_is_alive_not_tombstone() {
        let table = MemTable::new(1024);
        table.put(key(b"k"), Value::default(), SeqNum::new(1));
        let got = table.get(b"k");
        match got {
            Lookup::Alive(pinned) => assert!(pinned.value().is_empty()),
            Lookup::Deleted(_) => panic!("Value vacío no es tombstone"),
            Lookup::Missing => panic!("la clave debería existir"),
        }
    }

    #[test]
    fn iter_is_lexicographic_and_includes_tombstones() {
        let table = MemTable::new(4096);
        table.put(key(b"c"), Value::new(b"3"), SeqNum::new(1));
        table.put(key(b"a"), Value::new(b"1"), SeqNum::new(2));
        table.delete(key(b"b"), SeqNum::new(3));

        let keys: Vec<Vec<u8>> = table
            .iter()
            .map(|item| item.key().as_bytes().to_vec())
            .collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        let kinds: Vec<Option<Vec<u8>>> = table
            .iter()
            .map(|item| match item.mem_value() {
                MemValue::Put(v) => Some(v.as_bytes().to_vec()),
                MemValue::Tombstone => None,
            })
            .collect();
        assert_eq!(kinds[0].as_deref(), Some(b"1".as_ref()));
        assert!(kinds[1].is_none());
        assert_eq!(kinds[2].as_deref(), Some(b"3".as_ref()));
    }

    #[test]
    fn approx_bytes_grows_with_put() {
        let table = MemTable::new(10_000);
        assert_eq!(table.approx_bytes(), 0);
        table.put(key(b"abc"), Value::new(b"xyz"), SeqNum::new(1));
        let size = table.approx_bytes();
        assert!(
            size >= 3 + 3 + ENTRY_OVERHEAD_FOR_TEST,
            "size={size} debería incluir clave, valor y overhead"
        );
        table.put(key(b"abc"), Value::new(b"xyz"), SeqNum::new(2));
        assert_eq!(table.len(), 1);
        assert!(table.approx_bytes() > 0);
    }

    const ENTRY_OVERHEAD_FOR_TEST: usize = 16;

    #[test]
    fn is_full_does_not_reject_writes() {
        let table = MemTable::new(1);
        assert!(!table.is_full());
        table.put(key(b"k"), Value::new(b"value"), SeqNum::new(1));
        assert!(table.is_full());
        assert!(table.put(key(b"k2"), Value::new(b"more"), SeqNum::new(2)));
        assert_eq!(
            table.get(b"k2").value().map(Value::as_bytes),
            Some(b"more".as_ref())
        );
    }

    #[test]
    fn concurrent_puts_are_all_visible() {
        let table = Arc::new(MemTable::new(1 << 20));
        std::thread::scope(|scope| {
            for thread_id in 0..4_u64 {
                let table = Arc::clone(&table);
                scope.spawn(move || {
                    for i in 0..100_u64 {
                        let bytes = format!("t{thread_id}-k{i}");
                        let k = Key::new(bytes.as_bytes()).expect("clave");
                        let seq = SeqNum::new(thread_id * 100 + i + 1);
                        assert!(table.put(k, Value::new(b"v"), seq));
                    }
                });
            }
        });
        assert_eq!(table.len(), 400);
        for thread_id in 0..4 {
            for i in 0..100 {
                let bytes = format!("t{thread_id}-k{i}");
                assert!(!table.get(bytes.as_bytes()).is_missing(), "falta {bytes}");
            }
        }
    }

    #[test]
    fn concurrent_gets_during_puts() {
        let table = Arc::new(MemTable::new(1 << 20));
        table.put(key(b"stable"), Value::new(b"yes"), SeqNum::new(1));
        std::thread::scope(|scope| {
            let readers = Arc::clone(&table);
            scope.spawn(move || {
                for _ in 0..1_000 {
                    assert_eq!(
                        readers.get(b"stable").value().map(Value::as_bytes),
                        Some(b"yes".as_ref())
                    );
                }
            });
            let writer = Arc::clone(&table);
            scope.spawn(move || {
                for i in 0..1_000_u64 {
                    let k = Key::new(format!("w{i}").as_bytes()).expect("k");
                    writer.put(k, Value::new(b"x"), SeqNum::new(i + 2));
                }
            });
        });
        assert!(!table.is_empty());
    }

    #[test]
    fn memtable_is_send_sync() {
        fn assert_bounds<T: Send + Sync>() {}
        assert_bounds::<MemTable>();
    }
}
