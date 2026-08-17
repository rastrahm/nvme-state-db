//! Punto de entrada del binario `nvme-state-db`.
//!
//! `anyhow::Result` es el tipo de error de **aplicación**: permite añadir
//! contexto (`.context()`) a lo largo del `main`. La biblioteca expone
//! `nvme_state_db::Error` (`thiserror`) para que el motor siga siendo
//! enumerable. En fases posteriores, el binario convertirá `Error` → `anyhow`.

use anyhow::Result;

/// Purpose: arranca el proceso CLI.
///
/// Inputs: ninguno en esta fase (argv queda para un CLI futuro).
///
/// Returns: `Ok(())` si el proceso termina limpio; `Err` si arrancar falla.
fn main() -> Result<()> {
    Ok(())
}
