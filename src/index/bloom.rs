//! Bloom filter por bloques de 256 bits, consulta con SIMD (AVX2).
//!
//! ## Por qué existe
//!
//! Antes de hacer `mmap`/`get` en una SSTable, preguntamos al Bloom:
//! - **`false`** → la clave **no está**. Cero falsos negativos. No hay I/O.
//! - **`true`** → *puede* estar. A veces es un falso positivo; entonces
//!   sí leemos disco y no encontramos nada. Eso es aceptable.
//!
//! ## Bloques + SIMD
//!
//! Un Bloom clásico salta por todo el bitmap (mal para caché). Aquí cada clave
//! cae en **un bloque de 256 bits** (4 × `u64`). `may_contain` carga el bloque,
//! arma una máscara de `k` bits y pregunta con AVX2 si ` (bloque & máscara) == máscara `
//! en un solo registro de 256 bits.

use crate::error::Error;

/// Bits por bloque (una línea pequeña, cabe en YMM / 4 u64).
const BLOCK_BITS: usize = 256;
/// Palabras `u64` por bloque.
const WORDS_PER_BLOCK: usize = BLOCK_BITS / 64;
/// Tope defensivo: 8 Mi de palabras → 64 MiB de bitmap.
const MAX_WORDS: usize = 8 * 1024 * 1024;
/// Magic on-disk `BLM1`.
const BLOOM_MAGIC: [u8; 4] = *b"BLM1";
/// Bytes de cabecera serializada.
const HEADER_LEN: usize = 12;

// =============================================================================
// TIPO Bloom  (struct = campos, impl = insert / may_contain / (de)serializar)
// =============================================================================

/// Filtro de Bloom bloqueado. No es una clase: tipo + `impl`.
#[derive(Debug)]
pub struct Bloom {
    /// Bitmap: `num_blocks * 4` palabras little-endian en RAM (host order).
    words: Vec<u64>,
    /// Número de bloques de 256 bits.
    num_blocks: usize,
    /// Hashes por clave (bits que se encienden dentro del bloque), 1..=8.
    k: u8,
    /// Parámetro de construcción, se conserva al serializar.
    bits_per_key: u8,
}

impl Bloom {
    /// Purpose: reserva un filtro vacío dimensionado para `expected_keys`.
    ///
    /// Inputs:
    /// - `expected_keys` — claves que se insertarán (0 ⇒ un bloque mínimo).
    /// - `bits_per_key` — densidad, 1..=16. Más bits ⇒ menos falsos positivos.
    ///
    /// Returns: Bloom vacío, o [`Error::BloomInvalid`] si los parámetros no valen.
    pub fn new(expected_keys: usize, bits_per_key: u8) -> Result<Self, Error> {
        if !(1..=16).contains(&bits_per_key) {
            return Err(Error::BloomInvalid("bits_per_key debe estar en 1..=16"));
        }
        let k = bits_per_key.clamp(1, 8);
        let total_bits = expected_keys
            .saturating_mul(usize::from(bits_per_key))
            .max(BLOCK_BITS);
        let num_blocks = total_bits.div_ceil(BLOCK_BITS).max(1);
        let num_words = num_blocks.saturating_mul(WORDS_PER_BLOCK);
        if num_words > MAX_WORDS || num_words == 0 {
            return Err(Error::BloomInvalid("filtro demasiado grande"));
        }
        Ok(Self {
            words: vec![0_u64; num_words],
            num_blocks,
            k,
            bits_per_key,
        })
    }

    /// Purpose: enciende los `k` bits de `key` en su bloque.
    ///
    /// Inputs: `key` — bytes de la clave (p. ej. [`crate::Key::as_bytes`]).
    ///
    /// Returns: nada. Insertar dos veces la misma clave es idempotente.
    #[inline(always)]
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = hash2(key);
        let block_idx = block_index(h1, self.num_blocks);
        let mask = probe_mask(h1, h2, self.k);
        let Some(block) = block_mut(&mut self.words, block_idx) else {
            return;
        };
        for (word, bit) in block.iter_mut().zip(mask.iter()) {
            *word |= *bit;
        }
    }

    /// Purpose: ¿puede estar `key`? Camino caliente del lookup en SSTable.
    ///
    /// Inputs: `key` — bytes a consultar.
    ///
    /// Returns: `false` si **seguro** no está (no hay I/O). `true` si *puede*
    /// estar (falso positivo posible). Nunca `false` tras un `insert` de esa clave.
    #[inline(always)]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = hash2(key);
        let block_idx = block_index(h1, self.num_blocks);
        let mask = probe_mask(h1, h2, self.k);
        let Some(block) = block_ref(&self.words, block_idx) else {
            // Fallo interno: devolver true evita un falso negativo.
            return true;
        };
        mask_is_subset(&block, &mask)
    }

    /// Purpose: número de hashes por clave.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `k` en 1..=8.
    pub fn k(&self) -> u8 {
        self.k
    }

    /// Purpose: bloques de 256 bits.
    ///
    /// Inputs: `self`.
    ///
    /// Returns: `num_blocks` ≥ 1.
    pub fn num_blocks(&self) -> usize {
        self.num_blocks
    }

    /// Purpose: serializa cabecera + bitmap a un `Vec` (fase 5 lo meterá en el .sst).
    ///
    /// Inputs: `self` — filtro ya poblado o vacío.
    ///
    /// Returns: bytes little-endian. No usa `O_DIRECT`; es un blob para el writer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let bitmap_bytes = self.words.len().saturating_mul(8);
        let mut out = vec![0_u8; HEADER_LEN.saturating_add(bitmap_bytes)];
        out[0..4].copy_from_slice(&BLOOM_MAGIC);
        out[4] = self.k;
        out[5] = self.bits_per_key;
        write_u32_at(&mut out, 8, u32_from_usize(self.num_blocks)?)?;
        for (i, word) in self.words.iter().enumerate() {
            let off = HEADER_LEN.saturating_add(i.saturating_mul(8));
            write_u64_at(&mut out, off, *word)?;
        }
        Ok(out)
    }

    /// Purpose: reconstruye un Bloom desde bytes de [`Bloom::to_bytes`].
    ///
    /// Inputs: `bytes` — blob on-disk.
    ///
    /// Returns: el mismo filtro, o [`Error::BloomCorrupt`] si magic/tamaños no cuadran.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::BloomCorrupt("cabecera truncada"));
        }
        if bytes[0..4] != BLOOM_MAGIC {
            return Err(Error::BloomCorrupt("magic inválida"));
        }
        let k = bytes[4];
        let bits_per_key = bytes[5];
        if !(1..=8).contains(&k) {
            return Err(Error::BloomCorrupt("k fuera de rango"));
        }
        if !(1..=16).contains(&bits_per_key) {
            return Err(Error::BloomCorrupt("bits_per_key fuera de rango"));
        }
        let num_blocks = read_u32_at(bytes, 8)? as usize;
        if num_blocks == 0 || num_blocks > MAX_WORDS / WORDS_PER_BLOCK {
            return Err(Error::BloomCorrupt("num_blocks inválido"));
        }
        let num_words = num_blocks.saturating_mul(WORDS_PER_BLOCK);
        let need = HEADER_LEN.saturating_add(num_words.saturating_mul(8));
        if bytes.len() != need {
            return Err(Error::BloomCorrupt("tamaño de bitmap"));
        }
        let mut words = vec![0_u64; num_words];
        for (i, word) in words.iter_mut().enumerate() {
            let off = HEADER_LEN.saturating_add(i.saturating_mul(8));
            *word = read_u64_at(bytes, off)?;
        }
        Ok(Self {
            words,
            num_blocks,
            k,
            bits_per_key,
        })
    }
}

// -----------------------------------------------------------------------------
// Hash, máscara de k bits, SIMD
// -----------------------------------------------------------------------------

/// Purpose: elige el bloque 0..num_blocks a partir de h1.
#[inline(always)]
fn block_index(h1: u64, num_blocks: usize) -> usize {
    let n = num_blocks.max(1) as u64;
    (h1 % n) as usize
}

/// Purpose: dos hashes 64-bit; el segundo se fuerza impar (double hashing).
///
/// Inputs: `key` — bytes.
///
/// Returns: `(h1, h2)` determinista para la misma clave.
#[inline(always)]
fn hash2(key: &[u8]) -> (u64, u64) {
    let mut a = 0x9E37_79B9_7F4A_7C15_u64 ^ key.len() as u64;
    let mut b = 0xC2B2_AE3D_27D4_EB4F_u64 ^ (key.len() as u64).rotate_left(17);
    let mut chunks = key.chunks_exact(8);
    for chunk in chunks.by_ref() {
        let mut raw = [0_u8; 8];
        raw.copy_from_slice(chunk);
        let x = u64::from_le_bytes(raw);
        a = a
            .wrapping_add(x)
            .rotate_left(13)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9);
        b ^= x.wrapping_add(a);
        b = b.rotate_left(29).wrapping_mul(0x94D0_49BB_1331_11EB);
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut raw = [0_u8; 8];
        raw[..rem.len()].copy_from_slice(rem);
        let x = u64::from_le_bytes(raw);
        a ^= x;
        b = b.wrapping_add(x);
    }
    a ^= a >> 30;
    a = a.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    b ^= b >> 27;
    b = b.wrapping_mul(0x94D0_49BB_1331_11EB);
    (a, b | 1)
}

/// Purpose: máscara de 256 bits con los `k` bits a probar/encender.
///
/// Inputs: `h1`, `h2` (h2 impar), `k` hashes.
///
/// Returns: 4 palabras; el bit `i` está en `mask[bit/64]`.
#[inline(always)]
fn probe_mask(h1: u64, h2: u64, k: u8) -> [u64; WORDS_PER_BLOCK] {
    let mut mask = [0_u64; WORDS_PER_BLOCK];
    let probes = usize::from(k.min(8));
    for i in 0..probes {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let bit = (h as usize) & (BLOCK_BITS - 1);
        let word = bit / 64;
        let offset = bit % 64;
        mask[word] |= 1_u64 << offset;
    }
    mask
}

/// Purpose: `(block & mask) == mask` en escalar (referencia y fallback).
#[inline(always)]
fn mask_is_subset_scalar(block: &[u64; 4], mask: &[u64; 4]) -> bool {
    block
        .iter()
        .zip(mask.iter())
        .all(|(word, bit)| word & bit == *bit)
}

/// Purpose: misma prueba; AVX2 si el CPU lo tiene.
#[inline(always)]
fn mask_is_subset(block: &[u64; 4], mask: &[u64; 4]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: acabamos de detectar AVX2; `block`/`mask` son 32 bytes.
            return unsafe { mask_is_subset_avx2(block, mask) };
        }
    }
    mask_is_subset_scalar(block, mask)
}

/// Purpose: AND + compare de 256 bits en un registro YMM.
///
/// Inputs: bloque cargado y máscara de probes.
///
/// Returns: `true` si todos los bits de `mask` están encendidos en `block`.
///
/// # Safety
/// El llamador garantiza AVX2. Los punteros cubren 32 bytes válidos.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn mask_is_subset_avx2(block: &[u64; 4], mask: &[u64; 4]) -> bool {
    use core::arch::x86_64::{
        __m256i, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8,
    };
    // SAFETY: `loadu` admite desalineado; 4×u64 = 32 bytes.
    let b = unsafe { _mm256_loadu_si256(block.as_ptr().cast::<__m256i>()) };
    let m = unsafe { _mm256_loadu_si256(mask.as_ptr().cast::<__m256i>()) };
    let anded = _mm256_and_si256(b, m);
    let eq = _mm256_cmpeq_epi8(anded, m);
    _mm256_movemask_epi8(eq) == -1
}

/// Purpose: vista de un bloque; `None` si el índice está fuera (bug de tamaño).
#[inline(always)]
fn block_ref(words: &[u64], block_idx: usize) -> Option<[u64; 4]> {
    let start = block_idx.checked_mul(WORDS_PER_BLOCK)?;
    Some([
        *words.get(start)?,
        *words.get(start + 1)?,
        *words.get(start + 2)?,
        *words.get(start + 3)?,
    ])
}

/// Purpose: vista mutable de un bloque.
#[inline(always)]
fn block_mut(words: &mut [u64], block_idx: usize) -> Option<&mut [u64]> {
    let start = block_idx.checked_mul(WORDS_PER_BLOCK)?;
    let end = start.checked_add(WORDS_PER_BLOCK)?;
    words.get_mut(start..end)
}

/// Purpose: `usize` → `u32` para la cabecera on-disk.
fn u32_from_usize(n: usize) -> Result<u32, Error> {
    u32::try_from(n).map_err(|_| Error::BloomInvalid("num_blocks no cabe en u32"))
}

fn write_u32_at(dst: &mut [u8], off: usize, v: u32) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 4)
        .ok_or(Error::BloomCorrupt("encode u32"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn write_u64_at(dst: &mut [u8], off: usize, v: u64) -> Result<(), Error> {
    let slot = dst
        .get_mut(off..off + 8)
        .ok_or(Error::BloomCorrupt("encode u64"))?;
    slot.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn read_u32_at(src: &[u8], off: usize) -> Result<u32, Error> {
    let slot = src
        .get(off..off + 4)
        .ok_or(Error::BloomCorrupt("decode u32"))?;
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(slot);
    Ok(u32::from_le_bytes(raw))
}

fn read_u64_at(src: &[u8], off: usize) -> Result<u64, Error> {
    let slot = src
        .get(off..off + 8)
        .ok_or(Error::BloomCorrupt("decode u64"))?;
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(slot);
    Ok(u64::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::{
        hash2, mask_is_subset, mask_is_subset_avx2, mask_is_subset_scalar, probe_mask, Bloom,
        HEADER_LEN, WORDS_PER_BLOCK,
    };
    use crate::error::Error;
    use crate::types::Key;

    #[test]
    fn empty_bloom_contains_nothing() {
        let bloom = Bloom::new(100, 10).expect("new");
        assert!(!bloom.may_contain(b"account/0xabc"));
        assert!(!bloom.may_contain(b""));
    }

    #[test]
    fn insert_then_may_contain() {
        let mut bloom = Bloom::new(16, 10).expect("new");
        let key = Key::new(b"balance/0xabc").expect("key");
        bloom.insert(key.as_bytes());
        assert!(
            bloom.may_contain(key.as_bytes()),
            "falso negativo: una clave insertada tiene que dar true"
        );
    }

    #[test]
    fn no_false_negatives_on_many_keys() {
        let n = 2_000_usize;
        let mut bloom = Bloom::new(n, 10).expect("new");
        let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i}").into_bytes()).collect();
        for k in &keys {
            bloom.insert(k);
        }
        for k in &keys {
            assert!(
                bloom.may_contain(k),
                "falso negativo en {}",
                String::from_utf8_lossy(k)
            );
        }
    }

    #[test]
    fn false_positives_are_possible_but_rare() {
        let n = 1_000_usize;
        let mut bloom = Bloom::new(n, 10).expect("new");
        for i in 0..n {
            bloom.insert(format!("in{i}").as_bytes());
        }
        let probes = 1_000_usize;
        let fps = (0..probes)
            .filter(|i| bloom.may_contain(format!("out{i}").as_bytes()))
            .count();
        // 10 bits/key ≈ 1% teórico; 50% indicaría un filtro roto.
        assert!(
            fps < probes / 2,
            "demasiados falsos positivos: {fps}/{probes}"
        );
    }

    #[test]
    fn roundtrip_bytes_preserves_membership() {
        let mut bloom = Bloom::new(64, 8).expect("new");
        bloom.insert(b"alpha");
        bloom.insert(b"beta");
        let bytes = bloom.to_bytes().expect("ser");
        assert!(bytes.len() > HEADER_LEN);
        let restored = Bloom::from_bytes(&bytes).expect("de");
        assert_eq!(restored.k(), bloom.k());
        assert_eq!(restored.num_blocks(), bloom.num_blocks());
        assert!(restored.may_contain(b"alpha"));
        assert!(restored.may_contain(b"beta"));
        assert!(!restored.may_contain(b"no-estoy"));
    }

    #[test]
    fn from_bytes_rejects_bad_magic() {
        let mut bloom = Bloom::new(8, 8).expect("new");
        bloom.insert(b"x");
        let mut bytes = bloom.to_bytes().expect("ser");
        bytes[0] = b'X';
        let err = Bloom::from_bytes(&bytes).expect_err("magic");
        assert!(matches!(err, Error::BloomCorrupt(_)));
    }

    #[test]
    fn new_rejects_bad_bits_per_key() {
        assert!(matches!(Bloom::new(10, 0), Err(Error::BloomInvalid(_))));
        assert!(matches!(Bloom::new(10, 17), Err(Error::BloomInvalid(_))));
    }

    #[test]
    fn hash2_is_stable_and_h2_odd() {
        let (a, b) = hash2(b"same");
        let (c, d) = hash2(b"same");
        assert_eq!((a, b), (c, d));
        assert_eq!(b & 1, 1);
        assert_ne!(hash2(b"same"), hash2(b"other"));
    }

    #[test]
    fn avx2_matches_scalar_when_available() {
        let mask = probe_mask(0x1234_5678_9ABC_DEF0, 0x1111_1111_1111_1111 | 1, 7);
        let mut block = [0_u64; WORDS_PER_BLOCK];
        for (w, m) in block.iter_mut().zip(mask.iter()) {
            *w |= *m;
        }
        assert!(mask_is_subset_scalar(&block, &mask));
        assert!(mask_is_subset(&block, &mask));
        block[0] &= !mask[0];
        if mask[0] != 0 {
            assert!(!mask_is_subset_scalar(&block, &mask));
            assert!(!mask_is_subset(&block, &mask));
        }
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                let full = probe_mask(1, 3, 8);
                let present = full;
                // SAFETY: AVX2 detectado; 32 bytes válidos.
                assert!(unsafe { mask_is_subset_avx2(&present, &full) });
                let mut missing = present;
                missing[0] = 0;
                missing[1] = 0;
                missing[2] = 0;
                missing[3] = 0;
                assert!(!unsafe { mask_is_subset_avx2(&missing, &full) });
            }
        }
    }
}
