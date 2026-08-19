//! Punto de entrada del binario `nvme-state-db`.
//!
//! `anyhow::Result` es el tipo de error de **aplicación**: permite añadir
//! contexto (`.context()`) a lo largo del `main`. La biblioteca expone
//! `nvme_state_db::Error` (`thiserror`) para que el motor siga siendo
//! enumerable.

use anyhow::{Context, Result};
use nvme_state_db::Engine;

/// Purpose: abre un directorio de datos si se pasa por argv; si no, no-op.
///
/// Inputs: `argv[1]` opcional — path del motor.
///
/// Returns: `Ok(())` si no hay args o si [`Engine::open`] termina bien.
fn main() -> Result<()> {
    let Some(dir) = std::env::args().nth(1) else {
        return Ok(());
    };
    let _engine = Engine::open(&dir).context("abrir el motor")?;
    Ok(())
}
