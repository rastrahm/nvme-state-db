//! Errores de la biblioteca (`thiserror`).
//!
//! La crate de librería expone un [`Error`] enumerable y de coste cero en el
//! camino feliz. El binario (`main.rs`) usa `anyhow` para ensamblar contexto
//! de aplicación: esa es la frontera intencionada entre ambos crates de error.

use thiserror::Error;

/// Error recuperable de la biblioteca.
///
/// Las variantes se irán ampliando en fases posteriores (WAL, SSTable, motor).
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
}
