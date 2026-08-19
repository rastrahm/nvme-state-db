//! Benchmarks de acceso a estado: latencia, IOPS, write amplification.
//!
//! ## Qué medir (y qué no)
//!
//! La **media** esconde la cola: un `put` con `O_SYNC` a 200 µs de media puede
//! tener p99 de varios ms cuando el NVMe está ocupado. En un ejecutor de
//! bloques importa el **p99** (casi peor caso), no el promedio de laboratorio.
//!
//! - **p50** — la mitad de las ops fueron más rápidas.
//! - **p99** — el 1 % más lento; es lo que “siente” el peor bloque.
//! - **IOPS** — ops / segundo (aquí: `get` sobre MemTable o SST).
//! - **Write amplification** — bytes que tocan disco / bytes del usuario.
//!   El WAL alinea a 4K: un put de 40 B puede escribir 4096 B (WA ≫ 1).
//!
//! Correr: `cargo bench --bench state_access`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nvme_state_db::Engine;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const VALUE: &[u8] = b"0123456789abcdef0123456789abcdef"; // 32 B

fn bench_dir() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("bench-state");
    fs::create_dir_all(&base).expect("mkdir bench");
    tempfile::TempDir::new_in(&base).expect("tempdir").keep()
}

fn key_of(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn percentile(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted_ns.len() as f64 - 1.0)).round() as usize;
    sorted_ns[idx.min(sorted_ns.len() - 1)]
}

/// Muestra p50/p99 en ns (además del reporte de Criterion, que usa IC de la media).
fn print_percentiles(label: &str, mut samples_ns: Vec<u64>) {
    samples_ns.sort_unstable();
    let p50 = percentile(&samples_ns, 50.0);
    let p99 = percentile(&samples_ns, 99.0);
    eprintln!("{label}: n={} p50={p50} ns p99={p99} ns", samples_ns.len());
}

fn timed_puts(db: &Engine, from: u64, count: u64) -> Vec<u64> {
    let mut ns = Vec::with_capacity(count as usize);
    for i in from..from.saturating_add(count) {
        let k = key_of(i);
        let t0 = Instant::now();
        db.put(&k, VALUE).expect("put");
        ns.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    ns
}

fn timed_gets(db: &Engine, from: u64, count: u64) -> Vec<u64> {
    let mut ns = Vec::with_capacity(count as usize);
    for i in from..from.saturating_add(count) {
        let k = key_of(i);
        let t0 = Instant::now();
        let hit = db.get(&k).expect("get");
        black_box(hit.as_bytes());
        ns.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    ns
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let mut sum = 0_u64;
    let Ok(rd) = fs::read_dir(path) else {
        return 0;
    };
    for ent in rd.flatten() {
        if let Ok(meta) = ent.metadata() {
            if meta.is_file() {
                sum = sum.saturating_add(meta.len());
            }
        }
    }
    sum
}

fn put_latency(c: &mut Criterion) {
    let dir = bench_dir();
    let db = Engine::open(&dir).expect("open");
    print_percentiles("put_warmup", timed_puts(&db, 0, 32));

    let mut n = 100_u64;
    let mut group = c.benchmark_group("put");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(40);
    group.bench_function("put_8B_key_32B_val", |b| {
        b.iter(|| {
            n = n.saturating_add(1);
            db.put(&key_of(n), black_box(VALUE)).expect("put");
        });
    });
    group.finish();
}

fn get_memtable(c: &mut Criterion) {
    let dir = bench_dir();
    let db = Engine::open(&dir).expect("open");
    for i in 0..512_u64 {
        db.put(&key_of(i), VALUE).expect("seed");
    }
    print_percentiles("get_memtable", timed_gets(&db, 0, 512));

    let mut group = c.benchmark_group("get_memtable");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("get_hit_active", |b| {
        let mut i = 0_u64;
        b.iter(|| {
            i = (i + 1) % 512;
            black_box(db.get(&key_of(i)).expect("get"));
        });
    });
    group.finish();
}

fn get_sstable(c: &mut Criterion) {
    let dir = bench_dir();
    let db = Engine::open(&dir).expect("open");
    for i in 0..512_u64 {
        db.put(&key_of(i), VALUE).expect("seed");
    }
    db.flush().expect("flush");
    print_percentiles("get_sstable", timed_gets(&db, 0, 512));

    let mut group = c.benchmark_group("get_sstable");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("get_hit_sst", |b| {
        let mut i = 0_u64;
        b.iter(|| {
            i = (i + 1) % 512;
            black_box(db.get(&key_of(i)).expect("get"));
        });
    });
    group.finish();
}

fn write_amplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_amplification");
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);
    group.bench_function("wal_then_flush_ratio", |b| {
        b.iter(|| {
            let dir = bench_dir();
            let db = Engine::open(&dir).expect("open");
            const N: u64 = 64;
            const USER: u64 = 8 + 32;
            for i in 0..N {
                db.put(&key_of(i), VALUE).expect("put");
            }
            let after_wal = dir_bytes(&dir);
            db.flush().expect("flush");
            db.wait_flush().expect("wait");
            let after_sst = dir_bytes(&dir);
            let user = N.saturating_mul(USER);
            let wa_wal = after_wal as f64 / user as f64;
            let wa_all = after_sst as f64 / user as f64;
            eprintln!(
                "WA user={user} B disk_wal={after_wal} B (×{wa_wal:.1}) disk_after_flush={after_sst} B (×{wa_all:.1})"
            );
            black_box((wa_wal, wa_all));
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    put_latency,
    get_memtable,
    get_sstable,
    write_amplification
);
criterion_main!(benches);
