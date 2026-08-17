//! Newtypes de dominio: [`Key`], [`Value`] y [`SeqNum`].
//!
//! Un `&[u8]` suelto admite cualquier cosa (clave vacía, mezcla de claves y
//! valores, alias accidental). El newtype hace ilegal ese mezclado en el
//! sistema de tipos y concentra la validación en el constructor.

use crate::error::Error;
use std::borrow::Borrow;

/// Clave de estado: secuencia de bytes no vacía, ordenada lexicográficamente.
///
/// El orden (`Ord`) coincide con el de las SSTables: las claves se recorren
/// y se compactan en orden de bytes, no con un hash.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Key(Vec<u8>);

impl Key {
    /// Purpose: construye una clave copiando bytes del llamador.
    ///
    /// Inputs: `bytes` — slice o tipo convertible a slice; no puede estar vacío.
    ///
    /// Returns: `Ok(Key)` si hay al menos un byte; [`Error::EmptyKey`] en caso contrario.
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, Error> {
        Self::from_vec(bytes.as_ref().to_vec())
    }

    /// Purpose: construye una clave tomando posesión del `Vec` (sin copia extra).
    ///
    /// Inputs: `bytes` — buffer propiedad del llamador; no puede estar vacío.
    ///
    /// Returns: `Ok(Key)` si hay al menos un byte; [`Error::EmptyKey`] en caso contrario.
    pub fn from_vec(bytes: Vec<u8>) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Err(Error::EmptyKey);
        }
        Ok(Self(bytes))
    }

    /// Purpose: expone los bytes de la clave sin copiarlos.
    ///
    /// Inputs: `self` — clave ya validada.
    ///
    /// Returns: slice prestado cuya vida está atada a la `Key`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Purpose: longitud en bytes de la clave.
    ///
    /// Inputs: `self` — clave ya validada.
    ///
    /// Returns: número de bytes; siempre ≥ 1.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Purpose: indica si la clave está vacía.
    ///
    /// Inputs: `self` — clave ya validada.
    ///
    /// Returns: siempre `false`. El constructor rechaza el slice vacío; el
    /// método existe para satisfacer el convenio `len` / `is_empty` de Rust.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for Key {
    /// Purpose: permite usar `Key` donde se espera un slice de bytes.
    ///
    /// Inputs: `self` — clave ya validada.
    ///
    /// Returns: los mismos bytes que [`Key::as_bytes`].
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for Key {
    /// Purpose: permite buscar en mapas/SkipLists con un `&[u8]` sin poseer `Key`.
    ///
    /// Inputs: `self` — clave ya validada.
    ///
    /// Returns: slice de bytes comparable con `Ord`.
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for Key {
    type Error = Error;

    /// Purpose: convierte un `Vec<u8>` en `Key` validando el invariante.
    ///
    /// Inputs: `bytes` — buffer a tomar en posesión.
    ///
    /// Returns: [`Key`] o [`Error::EmptyKey`].
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_vec(bytes)
    }
}

impl From<Key> for Vec<u8> {
    /// Purpose: recupera el `Vec` interior sin copiar.
    ///
    /// Inputs: `key` — clave poseída.
    ///
    /// Returns: los bytes originales.
    fn from(key: Key) -> Self {
        key.0
    }
}

/// Valor de estado: secuencia de bytes, posiblemente vacía.
///
/// Un valor vacío **no** es un borrado. El borrado es un tombstone en
/// [`crate::memtable::MemTable::delete`]: son tipos distintos a propósito.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Value(Vec<u8>);

impl Value {
    /// Purpose: construye un valor copiando bytes del llamador.
    ///
    /// Inputs: `bytes` — slice o tipo convertible a slice; puede estar vacío.
    ///
    /// Returns: un [`Value`] poseído. Nunca falla: cualquier longitud es válida.
    pub fn new(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }

    /// Purpose: construye un valor tomando posesión del `Vec` (sin copia extra).
    ///
    /// Inputs: `bytes` — buffer propiedad del llamador; puede estar vacío.
    ///
    /// Returns: un [`Value`] que envuelve exactamente esos bytes.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Purpose: expone los bytes del valor sin copiarlos.
    ///
    /// Inputs: `self` — valor poseído.
    ///
    /// Returns: slice prestado cuya vida está atada al `Value`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Purpose: longitud en bytes del valor.
    ///
    /// Inputs: `self` — valor poseído.
    ///
    /// Returns: número de bytes, incluido 0.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Purpose: indica si el valor no contiene bytes.
    ///
    /// Inputs: `self` — valor poseído.
    ///
    /// Returns: `true` si y solo si la longitud es 0. No implica borrado.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for Value {
    /// Purpose: el valor vacío, útil como default de tests y huecos.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: [`Value`] de longitud 0.
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl AsRef<[u8]> for Value {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_vec(bytes)
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Self {
        value.0
    }
}

/// Número de secuencia monotónico (`u64`) para ordenar writes en WAL y MemTable.
///
/// Empieza en 0. Cada `put`/`delete` futuro tomará un número estrictamente mayor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SeqNum(u64);

impl SeqNum {
    /// Secuencia inicial antes del primer write.
    pub const ZERO: Self = Self(0);

    /// Purpose: envuelve un `u64` ya conocido (p. ej. leído del WAL).
    ///
    /// Inputs: `n` — valor crudo de 64 bits; cualquier valor es representable.
    ///
    /// Returns: el [`SeqNum`] correspondiente.
    #[inline(always)]
    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    /// Purpose: obtiene el entero crudo.
    ///
    /// Inputs: `self` — copia del newtype.
    ///
    /// Returns: el `u64` interior.
    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Purpose: incrementa en uno sin wrapping silenciosa.
    ///
    /// Inputs: `self` — secuencia actual.
    ///
    /// Returns: `Ok(siguiente)` o [`Error::SequenceOverflow`] si `self` es `u64::MAX`.
    pub fn next(self) -> Result<Self, Error> {
        match self.0.checked_add(1) {
            Some(n) => Ok(Self(n)),
            None => Err(Error::SequenceOverflow),
        }
    }
}

impl From<SeqNum> for u64 {
    fn from(seq: SeqNum) -> Self {
        seq.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Key, SeqNum, Value};
    use crate::error::Error;
    use std::borrow::Borrow;
    use std::convert::TryFrom;

    #[test]
    fn key_rejects_empty_slice() {
        let err = Key::new([]).expect_err("una clave vacía debe fallar");
        assert!(matches!(err, Error::EmptyKey));
    }

    #[test]
    fn key_rejects_empty_vec() {
        let err = Key::from_vec(Vec::new()).expect_err("Vec vacío debe fallar");
        assert!(matches!(err, Error::EmptyKey));
    }

    #[test]
    fn key_accepts_non_empty_bytes() {
        let key = Key::new(b"account/0xabc").expect("clave válida");
        assert_eq!(key.as_bytes(), b"account/0xabc");
        assert_eq!(key.len(), 13);
        assert!(!key.is_empty());
    }

    #[test]
    fn key_from_vec_takes_ownership_without_changing_bytes() {
        let raw = vec![0x00, 0xff];
        let key = Key::from_vec(raw).expect("clave válida");
        assert_eq!(key.as_bytes(), &[0x00, 0xff]);
        let back: Vec<u8> = key.into();
        assert_eq!(back, vec![0x00, 0xff]);
    }

    #[test]
    fn key_try_from_vec() {
        assert!(Key::try_from(Vec::<u8>::new()).is_err());
        assert!(Key::try_from(vec![1_u8]).is_ok());
    }

    #[test]
    fn key_ord_is_lexicographic() {
        let a = Key::new(b"aa").expect("a");
        let b = Key::new(b"ab").expect("b");
        let c = Key::new(b"b").expect("c");
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn key_borrow_matches_as_bytes() {
        let key = Key::new(b"k").expect("k");
        let borrowed: &[u8] = key.borrow();
        assert_eq!(borrowed, key.as_bytes());
    }

    #[test]
    fn value_allows_empty() {
        let empty = Value::new([]);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(Value::default().as_bytes(), b"");
    }

    #[test]
    fn value_round_trip_vec() {
        let value = Value::from_vec(vec![1, 2, 3]);
        assert_eq!(value.as_bytes(), &[1, 2, 3]);
        let raw: Vec<u8> = value.into();
        assert_eq!(raw, vec![1, 2, 3]);
    }

    #[test]
    fn empty_value_is_not_a_tombstone_type() {
        // El valor vacío existe como Value; el borrado no se representa aquí.
        let _stored: Value = Value::new([]);
        // Si existiera Tombstone, no compilamos una conversión implícita.
        fn takes_value(_: &Value) {}
        takes_value(&Value::default());
    }

    #[test]
    fn seqnum_starts_at_zero_and_increments() {
        let zero = SeqNum::ZERO;
        assert_eq!(zero.get(), 0);
        let one = zero.next().expect("0 + 1 cabe en u64");
        assert_eq!(one.get(), 1);
        assert!(zero < one);
    }

    #[test]
    fn seqnum_new_wraps_raw_integer() {
        let seq = SeqNum::new(42);
        assert_eq!(u64::from(seq), 42);
    }

    #[test]
    fn seqnum_overflow_is_an_error() {
        let max = SeqNum::new(u64::MAX);
        let err = max.next().expect_err("MAX no puede incrementarse");
        assert!(matches!(err, Error::SequenceOverflow));
    }
}
