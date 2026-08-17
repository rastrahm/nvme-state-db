//! Recorrido de `put` / `delete` / `get` sobre la MemTable.
//!
//! Ejecutar: `cargo run --example lookup`
//!
//! Muestra los tres resultados de [`nvme_state_db::Lookup`]:
//! Missing (no está), Alive (valor vivo) y Deleted (tombstone).

use anyhow::{Context, Result};
use nvme_state_db::{Key, Lookup, MemTable, SeqNum, Value};

fn main() -> Result<()> {
    let table = MemTable::new(64 * 1024);
    let clave = Key::new(b"balance/0xabc").context("clave de ejemplo")?;

    println!("=== MemTable vacía ===");
    describe("get(balance/0xabc)", table.get(clave.as_bytes()));
    println!("  → el motor (fase 7) SÍ iría a SSTables: quizá exista en disco.\n");

    println!("=== put(balance/0xabc, 100) seq=1 ===");
    table.put(clave.clone(), Value::new(b"100"), SeqNum::new(1));
    describe("get", table.get(clave.as_bytes()));
    println!("  → el motor NO va a disco: la versión nueva está en RAM.\n");

    println!("=== put(balance/0xabc, 250) seq=2  (overwrite) ===");
    table.put(clave.clone(), Value::new(b"250"), SeqNum::new(2));
    describe("get", table.get(clave.as_bytes()));
    println!("  → un put no crea otra clave: sustituye el valor.\n");

    println!("=== delete(balance/0xabc) seq=3  (tombstone) ===");
    table.delete(clave.clone(), SeqNum::new(3));
    describe("get", table.get(clave.as_bytes()));
    println!("  → no es Missing. Si lo fuera, el motor resucitaría el 250 de una SSTable.\n");

    println!("=== get de una clave que nunca existió ===");
    describe("get(nonce/0xabc)", table.get(b"nonce/0xabc"));
    println!("  → Missing de verdad: nunca hubo put ni delete de esta clave.\n");

    println!("=== put(balance/0xabc, 1) seq=4  (resucita) ===");
    table.put(clave.clone(), Value::new(b"1"), SeqNum::new(4));
    describe("get", table.get(clave.as_bytes()));
    println!("  → un put posterior al tombstone vuelve a Alive.\n");

    println!("=== put con Value vacío seq=5  (no es delete) ===");
    table.put(clave, Value::default(), SeqNum::new(5));
    describe("get", table.get(b"balance/0xabc"));
    println!("  → Alive con 0 bytes. El borrado se hace con delete(), no con un valor vacío.\n");

    println!("=== iteración ordenada (como irá al flush) ===");
    table.put(
        Key::new(b"account/0xaaa")?,
        Value::new(b"aaa"),
        SeqNum::new(6),
    );
    table.delete(Key::new(b"account/0xbbb")?, SeqNum::new(7));
    for item in table.iter() {
        let k = String::from_utf8_lossy(item.key().as_bytes());
        let payload = match item.mem_value() {
            nvme_state_db::MemValue::Put(v) => {
                format!("Put({})", String::from_utf8_lossy(v.as_bytes()))
            }
            nvme_state_db::MemValue::Tombstone => "Tombstone".to_string(),
        };
        println!("  {k}  seq={}  {payload}", item.seq().get());
    }

    Ok(())
}

/// Purpose: imprime la variante de Lookup y, si aplica, el valor.
///
/// Inputs: `label` — texto del paso; `got` — resultado de MemTable::get.
///
/// Returns: nada; solo escribe a stdout.
fn describe(label: &str, got: Lookup<'_>) {
    match got {
        Lookup::Alive(pinned) => {
            let bytes = pinned.value().as_bytes();
            let shown = if bytes.is_empty() {
                "(vacío)".to_string()
            } else {
                String::from_utf8_lossy(bytes).into_owned()
            };
            println!("  {label} → Alive  valor={shown:?}  seq={}", pinned.seq().get());
        }
        Lookup::Deleted(seq) => {
            println!("  {label} → Deleted (tombstone)  seq={}", seq.get());
        }
        Lookup::Missing => {
            println!("  {label} → Missing");
        }
    }
}
