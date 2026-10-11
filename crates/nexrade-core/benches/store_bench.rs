//! Criterion benchmarks for `ShardedDatabase`.
//!
//! Run with:
//!   cargo bench -p nexrade-core
//!
//! HTML reports are written to `target/criterion/`.
//!
//! # Reading the results (1.4.0)
//!
//! **A `p < 0.05` "Performance has regressed" line from this harness is not
//! by itself evidence of a regression.** Measured on an idle 24-core host,
//! six consecutive runs of the *same binary* with the settings CI used
//! (`--measurement-time 3`) reported, in order: regressed (p=0.00),
//! no change, no change, no change, **improved** (p=0.01), regressed
//! (p=0.00) — a ±7% swing on code that did not change.
//!
//! Criterion's `p` value answers "did the sample distributions differ?", not
//! "did the code get slower?". On a shared machine, run-to-run variation in
//! CPU frequency, scheduler placement, and binary layout differs enough to
//! make that answer "yes" for identical code.
//!
//! Two mitigations are configured in [`bench_config`] below:
//!
//! * a **5% `noise_threshold`**, so criterion reports "Change within noise
//!   threshold" rather than "regressed" for sub-5% deltas, and
//! * longer default warm-up and measurement windows, which narrowed the
//!   observed spread from ~15% to ~5%.
//!
//! Neither makes a single run authoritative. See
//! comparison procedure — in short: build both binaries at the *same*
//! filesystem path, and establish this session's noise floor by repeating
//! the same build before trusting any delta.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexrade_core::store::{Entry, ShardedDatabase};
use nexrade_core::types::DataType;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Shared criterion configuration for every group in this file.
///
/// The defaults are deliberately more conservative than criterion's own:
///
/// * `noise_threshold(0.05)` — criterion still computes significance, but
///   labels anything inside ±5% as noise instead of a regression. This
///   directly matches the measured floor: sub-5% deltas from this harness
///   are not distinguishable from re-running the same binary.
/// * `warm_up_time(3s)` / `measurement_time(10s)` — the 1s/3s settings CI
///   previously used produced roughly three times the spread.
///
/// `--warm-up-time` / `--measurement-time` on the command line still
/// override these, so a quick local run is unaffected.
fn bench_config() -> Criterion {
    Criterion::default()
        .noise_threshold(0.05)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10))
}

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
//
// NOTE (1.5.0): this walks keys **sequentially** (`n % KEYS`), which is
// prefetch-friendly in a way real key access is not. Measured directly, the
// same map probed with a randomised order costs ~3× more (19 ns sequential
// vs 62 ns randomised at 100 k keys) and shows a larger response to hasher
// and layout changes. Kept as-is for continuity with historical results;
// `get_working_set` below is the honest shape.

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

// ── 2b. GET vs working-set size, randomised access (1.5.0) ────────────────────
//
// The gap this closes: nothing in the suite varied dataset size, yet that is
// the single largest determinant of read cost. Measured with a standalone
// probe, shrinking the working set from 100 000 keys to 1 000 — changing
// nothing else — took a read from 136.2 ns to 26.7 ns, while shard-lock cost
// stayed flat at 11.3 ns. In other words the read path is dominated by cache
// residency, not by locking, and the suite could not see that at all.
//
// Two deliberate choices:
//
// * **Randomised probe order.** Multiplying the counter by a large odd
//   constant walks the keyspace in a fixed but scattered order — reproducible
//   run to run (no RNG, no `Math.random` equivalent) while defeating the
//   hardware prefetcher. `bench_get` above walks sequentially and understates
//   both absolute cost and the size of any improvement.
// * **Keys pre-generated outside the timed closure.** `make_key` allocates a
//   `Vec<u8>`; doing that per iteration measures the allocator as much as the
//   store.
//
// Use the 1k rows for cache-resident behaviour and the 1M rows for
// out-of-cache. A change that helps one and not the other is a locality
// change, which is exactly the distinction the 1.5.0 locality investigation
// needed and did not have.

fn bench_get_working_set(c: &mut Criterion) {
    // Odd multiplier coprime with every power-of-two table size, so the walk
    // visits every index before repeating.
    const STRIDE: u64 = 2_654_435_761;

    let mut g = c.benchmark_group("get_working_set_random");
    for &keys in &[1_000u64, 10_000, 100_000, 1_000_000] {
        let sdb = ShardedDatabase::new(64);
        let all: Vec<Vec<u8>> = (0..keys).map(make_key).collect();
        for k in &all {
            sdb.write_for(k).insert(k.clone(), string_entry(b"value"));
        }
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::new("keys", keys), &keys, |b, &keys| {
            let mut n: u64 = 0;
            b.iter(|| {
                let k = &all[(n.wrapping_mul(STRIDE) % keys) as usize];
                let hit = black_box(sdb.read_for(k).get_ro(k)).is_some();
                debug_assert!(hit, "working-set bench must be 100% hits");
                n = n.wrapping_add(1);
            });
        });
    }
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
//
// **Do not read the reported scaling as an absolute** (1.5.0). Threads are
// spawned *inside* `b.iter()`, so every iteration pays OS thread
// creation and join. Measured on the reference host, bare spawn+join of 8
// threads doing nothing costs 268.9 µs against this benchmark's own
// ~1657 µs — about 16% of the measurement. That drags apparent 8-thread
// efficiency down to ~35%; with persistent threads and a start barrier the
// same workload scales at 62–78% (rising with shard count).
//
// It is left as-is deliberately: as a *relative* A/B measure both arms pay
// the identical overhead, and restructuring it would invalidate comparison
// against every historical result. But it is why this benchmark is among
// the noisier ones in the 1.4.0 floor table (CV 4.6% at threads/8), so
// treat single-digit percentage deltas here as inside the noise band
// rather than as signal.

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

// ── 10. KeyHasher A/B — SipHash (std default) vs foldhash ────────────────────
//
// `KeyHasher` in `store.rs` is `foldhash::fast::RandomState` rather than the
// std default of SipHash-1-3, on the claim that hashing was the dominant cost
// on the read path. That claim was previously sourced to a document that does
// not exist in this repository, so it could not be checked or re-run by
// anyone. This benchmark is that check: it measures the two hashers against
// each other on the three key shapes the claim named, so the decision is
// reproducible and a future regression is visible.
//
// Method notes:
//   * Keys are built outside the timed closure — `make_key` allocates, and
//     timing the allocator instead of the hasher would make the numbers
//     meaningless.
//   * Probe order is scattered by an odd multiplier rather than sequential, so
//     the hardware prefetcher does not hide the cost (`bench_get` above
//     documents the sequential/randomised gap).
//   * The value is a `u64` so this isolates the *map probe* — the hash and
//     bucket walk — rather than entry-size effects.

fn bench_hasher_ab(c: &mut Criterion) {
    use nexrade_core::store::KeyHasher;
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;

    const KEYS: usize = 100_000;
    // Large odd constant: walks the keyspace in a scattered but deterministic
    // order, so runs are comparable while defeating prefetch.
    const STRIDE: usize = 2_654_435_761;

    fn key_u64_le(n: u64) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }
    fn key_user(n: u64) -> Vec<u8> {
        format!("user:{n}").into_bytes()
    }
    fn key_64byte(n: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(64);
        k.extend_from_slice(&n.to_le_bytes());
        while k.len() < 64 {
            let x = ((k.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ n) as u8;
            k.push(x.wrapping_add(k.len() as u8));
        }
        k
    }

    fn probe<S: std::hash::BuildHasher + Default>(keys: &[Vec<u8>]) -> u64 {
        let mut m: HashMap<&[u8], u64, S> = HashMap::with_hasher(S::default());
        for (i, k) in keys.iter().enumerate() {
            m.insert(k.as_slice(), i as u64);
        }
        let mut hits = 0u64;
        for i in 0..keys.len() {
            let idx = i.wrapping_mul(STRIDE) % keys.len();
            hits += *m.get(keys[idx].as_slice()).unwrap_or(&0);
        }
        hits
    }

    /// Builds the `n`th key of one key shape. Each shape is a different
    /// realistic key width so the A/B covers short numeric keys, common
    /// colon-delimited names, and maximum-length keys.
    type KeyMaker = fn(u64) -> Vec<u8>;

    let shapes: [(&str, KeyMaker); 3] = [
        ("u64_le", key_u64_le),
        ("user_n", key_user),
        ("64byte", key_64byte),
    ];

    let mut g = c.benchmark_group("hasher_ab");
    g.throughput(Throughput::Elements(KEYS as u64));
    for (shape_name, make) in shapes {
        let keys: Vec<Vec<u8>> = (0..KEYS as u64).map(make).collect();
        g.bench_function(format!("{shape_name}/siphash"), |b| {
            b.iter(|| black_box(probe::<RandomState>(&keys)))
        });
        g.bench_function(format!("{shape_name}/foldhash"), |b| {
            b.iter(|| black_box(probe::<KeyHasher>(&keys)))
        });
    }
    g.finish();
}

criterion_group!(
    name = benches;
    config = bench_config();
    targets =
        bench_set,
        bench_get,
        bench_get_working_set,
        bench_concurrent_writes,
        bench_concurrent_reads,
        bench_rename,
        bench_shard_scaling,
        bench_concurrent_incr,
        bench_hasher_ab,
);
criterion_main!(benches);
