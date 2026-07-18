//! Criterion benchmarks for `ShardedDatabase`.
//!
//! Run with:
//!   cargo bench -p nexrade-core
//!
//! HTML reports are written to `target/criterion/`.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexrade_core::store::{Entry, ShardedDatabase};
use nexrade_core::types::DataType;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn string_entry(value: &[u8]) -> Entry {
    Entry::new(DataType::String(Bytes::copy_from_slice(value)))
}

/// Build a key from a u64 — small, cache-friendly.
#[inline]
fn make_key(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

// ── 1. Single-threaded SET ────────────────────────────────────────────────────
//
// Measures the raw cost of acquiring a shard write lock, inserting an entry,
// and bumping the key_version — i.e. the amortised per-key cost of a SET.

fn bench_set(c: &mut Criterion) {
    let sdb = ShardedDatabase::new(16);
    let mut g = c.benchmark_group("set_single_thread");
    g.throughput(Throughput::Elements(1));
    g.bench_function("write_for+insert", |b| {
        let mut n: u64 = 0;
        b.iter(|| {
            let key = make_key(n);
            sdb.write_for(&key)
                .insert(key.clone(), string_entry(b"value"));
            n += 1;
        });
    });
    g.finish();
}

// ── 2. Single-threaded GET ────────────────────────────────────────────────────
//
// Measures read_for (shard hash + shared lock acquisition) + get_ro.
// Pre-populates 100 k keys so the HashMap is realistically sized.

fn bench_get(c: &mut Criterion) {
    const KEYS: u64 = 100_000;
    let sdb = ShardedDatabase::new(16);
    for i in 0..KEYS {
        let key = make_key(i);
        sdb.write_for(&key)
            .insert(key.clone(), string_entry(b"value"));
    }
    let mut g = c.benchmark_group("get_single_thread");
    g.throughput(Throughput::Elements(1));
    g.bench_function("read_for+get_ro", |b| {
        let mut n: u64 = 0;
        b.iter(|| {
            let key = make_key(n % KEYS);
            let _ = black_box(sdb.read_for(&key).get_ro(&key));
            n += 1;
        });
    });
    g.finish();
}

// ── 3. Concurrent writes — unique keys ────────────────────────────────────────
//
// Each thread writes to keys that are disjoint from every other thread's keys.
// Because FNV-1a distributes keys uniformly across shards, contention is
// proportional to 1/num_shards rather than 1 (one global lock).
//
// With 16 shards and 4 threads the expected contention fraction is ~25 % —
// so throughput should scale nearly linearly up to num_shards threads.

fn bench_concurrent_writes(c: &mut Criterion) {
    const OPS: u64 = 10_000;
    let mut g = c.benchmark_group("concurrent_writes_unique_keys");
    for &threads in &[1usize, 2, 4, 8] {
        g.throughput(Throughput::Elements(OPS * threads as u64));
        g.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let sdb = Arc::new(ShardedDatabase::new(16));
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let sdb = Arc::clone(&sdb);
                            thread::spawn(move || {
                                for i in 0..OPS {
                                    let key = make_key(t as u64 * OPS + i);
                                    sdb.write_for(&key).insert(key.clone(), string_entry(b"v"));
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    g.finish();
}

// ── 4. Concurrent reads — shared dataset ─────────────────────────────────────
//
// All threads read from the same 100 k-key dataset.  Multiple threads can
// hold shared read guards on the same shard simultaneously, so this should
// scale perfectly regardless of shard count.

fn bench_concurrent_reads(c: &mut Criterion) {
    const KEYS: u64 = 100_000;
    const OPS: u64 = 10_000;
    let sdb = Arc::new(ShardedDatabase::new(16));
    for i in 0..KEYS {
        let key = make_key(i);
        sdb.write_for(&key)
            .insert(key.clone(), string_entry(b"value"));
    }
    let mut g = c.benchmark_group("concurrent_reads_shared_dataset");
    for &threads in &[1usize, 2, 4, 8] {
        g.throughput(Throughput::Elements(OPS * threads as u64));
        g.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let sdb = Arc::clone(&sdb);
                            thread::spawn(move || {
                                for i in 0..OPS {
                                    let key = make_key((t as u64 * OPS + i) % KEYS);
                                    let _ = black_box(sdb.read_for(&key).get_ro(&key));
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    g.finish();
}

// ── 5. Cross-shard rename ─────────────────────────────────────────────────────
//
// RENAME must lock two shards in sorted-index order to avoid deadlocks.
// This benchmark quantifies that overhead relative to a single-shard write.

fn bench_rename(c: &mut Criterion) {
    let sdb = ShardedDatabase::new(16);
    // These two keys almost certainly land in different shards (FNV-1a distributes well).
    let a: Vec<u8> = b"bench:rename:alpha".to_vec();
    let b_key: Vec<u8> = b"bench:rename:beta".to_vec();
    sdb.write_for(&a).insert(a.clone(), string_entry(b"value"));
    sdb.write_for(&b_key)
        .insert(b_key.clone(), string_entry(b"value"));

    let mut g = c.benchmark_group("cross_shard_rename");
    g.throughput(Throughput::Elements(1));
    g.bench_function("round_trip", |b| {
        b.iter(|| {
            sdb.rename(&a, b_key.clone());
            sdb.rename(&b_key, a.clone());
        });
    });
    g.finish();
}

// ── 6. Shard count vs throughput ─────────────────────────────────────────────
//
// Fixes the thread count at 4 and varies the number of shards: 1 (baseline,
// simulates the old single-lock behaviour), 4, 16, 64.
//
// 1 shard  → all 4 threads always contend on the same lock.
// 4 shards → each thread statistically owns its own shard most of the time.
// 16/64   → further reduces residual contention.

fn bench_shard_scaling(c: &mut Criterion) {
    const THREADS: usize = 4;
    const OPS: u64 = 10_000;
    let mut g = c.benchmark_group("shard_count_vs_throughput");
    g.throughput(Throughput::Elements(OPS * THREADS as u64));
    for &shards in &[1usize, 4, 16, 64] {
        g.bench_with_input(BenchmarkId::new("shards", shards), &shards, |b, &shards| {
            b.iter(|| {
                let sdb = Arc::new(ShardedDatabase::new(shards));
                let handles: Vec<_> = (0..THREADS)
                    .map(|t| {
                        let sdb = Arc::clone(&sdb);
                        thread::spawn(move || {
                            for i in 0..OPS {
                                let key = make_key(t as u64 * OPS + i);
                                sdb.write_for(&key).insert(key.clone(), string_entry(b"v"));
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }
    g.finish();
}

// ── 7. Concurrent INCR — single hot key vs disjoint keys ─────────────────────
//
// The read-lock CAS fast path (`ShardedDatabase::incr_int`) exists to fix
// single-hot-key contention: N threads all incrementing the *same* key used
// to fully serialize on that key's shard write lock regardless of critical-
// section length. `single_hot_key` measures that case directly. The sibling
// `disjoint_keys` group is a sanity check that the common case (each thread
// on its own key) is unaffected — it should scale the same way
// `bench_concurrent_writes` already does.

fn bench_concurrent_incr(c: &mut Criterion) {
    const OPS: u64 = 10_000;
    let mut g = c.benchmark_group("concurrent_incr_single_hot_key");
    for &threads in &[1usize, 2, 4, 8, 16, 50] {
        g.throughput(Throughput::Elements(OPS * threads as u64));
        g.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let sdb = Arc::new(ShardedDatabase::new(16));
                    // Promote once up front so every thread hits the fast
                    // path from the first increment.
                    sdb.incr_int(b"hot", 0).unwrap();
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let sdb = Arc::clone(&sdb);
                            thread::spawn(move || {
                                for _ in 0..OPS {
                                    sdb.incr_int(b"hot", 1).unwrap();
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    g.finish();

    let mut g = c.benchmark_group("concurrent_incr_disjoint_keys");
    for &threads in &[1usize, 2, 4, 8, 16, 50] {
        g.throughput(Throughput::Elements(OPS * threads as u64));
        g.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let sdb = Arc::new(ShardedDatabase::new(16));
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let sdb = Arc::clone(&sdb);
                            thread::spawn(move || {
                                let key = make_key(t as u64);
                                for _ in 0..OPS {
                                    sdb.incr_int(&key, 1).unwrap();
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_concurrent_writes,
    bench_concurrent_reads,
    bench_rename,
    bench_shard_scaling,
    bench_concurrent_incr,
);
criterion_main!(benches);
