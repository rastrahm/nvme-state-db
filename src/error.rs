//! Errores de la biblioteca (`thiserror`).
//!
//! La crate de librería expone un [`Error`] enumerable y de coste cero en el
//! camino feliz. El binario (`main.rs`) usa `anyhow` para ensamblar contexto
//! de aplicación: esa es la frontera intencionada entre ambos crates de error.

use thiserror::Error;

/// Error recuperable de la biblioteca.
///
/// Las variantes se irán ampliando en fases posteriores (SSTable, motor).
#[derive(Debug, Error)]
pub enum Error {
    /// Fallo del sistema de archivos o del kernel.
    #[error("error de E/S: {0}")]
    Io(#[from] std::io::Error),

    /// Una clave no puede ser un slice vacío: no es ordenable de forma útil
    /// en un LSM y suele indicar un bug del llamador.
    #[error("la clave no puede estar vacía")]
    EmptyKey,

    /// El contador monotónico del WAL/MemTable alcanzó `u64::MAX`.
    #[error("desbordamiento del número de secuencia")]
    SequenceOverflow,

    /// El puntero del buffer no cumple la alineación que exige `O_DIRECT`.
    #[error("buffer WAL no alineado a {required} bytes")]
    Unaligned {
        /// Alineación exigida, en bytes (4096).
        required: usize,
    },

    /// `posix_memalign` devolvió un código distinto de 0.
    #[error("posix_memalign falló con código {0}")]
    AllocFailed(i32),

    /// Magic, checksum de cabecera o payload con CRC válido pero ilegible.
    #[error("WAL corrupto: {0}")]
    WalCorrupt(&'static str),

    /// El archivo declara una versión de formato que este binario no entiende.
    #[error("versión de WAL no soportada: {0}")]
    UnsupportedWalVersion(u16),

    /// El registro no cabe en el límite defensivo (evita mapear gigabytes).
    #[error("registro WAL demasiado grande ({size} bytes, máximo {max})")]
    WalRecordTooLarge {
        /// Tamaño que se intentó escribir o leer.
        size: usize,
        /// Tope actual del motor.
        max: usize,
    },

    /// Capacidad o `bits_per_key` fuera de rango al construir el filtro.
    #[error("parámetros de Bloom inválidos: {0}")]
    BloomInvalid(&'static str),

    /// Magic, tamaños o `k` ilegibles al deserializar un Bloom.
    #[error("Bloom corrupto: {0}")]
    BloomCorrupt(&'static str),
}

#[cfg(test)]
mod tests {
    use super::Error;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn empty_key_display_is_explicit() {
        let message = Error::EmptyKey.to_string();
        assert!(
            message.contains("vacía"),
            "el mensaje debe documentar el invariante, got: {message}"
        );
    }

    #[test]
    fn io_error_converts_with_from() {
        let io = IoError::new(ErrorKind::NotFound, "archivo inexistente");
        let err = Error::from(io);
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("E/S"));
    }

    #[test]
    fn sequence_overflow_display() {
        assert_eq!(
            Error::SequenceOverflow.to_string(),
            "desbordamiento del número de secuencia"
        );
    }

    #[test]
    fn error_is_send_sync_static() {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<Error>();
    }

    #[test]
    fn wal_corrupt_display() {
        assert_eq!(
            Error::WalCorrupt("magic inválida").to_string(),
            "WAL corrupto: magic inválida"
        );
    }

    #[test]
    fn bloom_error_display() {
        assert_eq!(
            Error::BloomInvalid("bits_per_key").to_string(),
            "parámetros de Bloom inválidos: bits_per_key"
        );
        assert_eq!(
            Error::BloomCorrupt("magic").to_string(),
            "Bloom corrupto: magic"
        );
    }
}
