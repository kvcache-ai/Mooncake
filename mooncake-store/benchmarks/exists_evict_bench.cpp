#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_uint64(num_objects, 10000000, "Number of unique random keys to create");
DEFINE_uint64(prefill_threads, 8, "Concurrent PutStart/PutEnd threads");
DEFINE_uint64(lookup_threads, 8, "Concurrent BatchExistKey threads");
DEFINE_uint64(batch_size, 200, "Random keys per existence request");
DEFINE_uint64(baseline_sec, 10, "Measured lookup-only baseline duration");
DEFINE_uint64(duration_sec, 60, "Measured duration with periodic eviction");
DEFINE_uint64(eviction_interval_sec, 10, "Seconds between eviction starts");
DEFINE_double(eviction_ratio, 0.05, "Target fraction of objects to evict");
DEFINE_double(eviction_lowerbound, 0.025, "Fallback eviction fraction");
DEFINE_uint64(lease_ms, 1000,
              "Read lease granted by successful existence calls");
DEFINE_double(lookup_fraction, 0.8,
              "Lookup threads draw keys only from this fraction of the key "
              "space; the rest are never looked up after they are written "
              "(1 = read every key)");
DEFINE_string(lookup_subset, "interleaved",
              "Which keys --lookup_fraction selects: prefix = the first "
              "fraction of key indices (written first during prefill); "
              "interleaved = a hash-chosen subset spread evenly over write "
              "order");
DEFINE_double(put_rate, 50000,
              "New keys written per second after prefill, across all writer "
              "threads (0 = no writes). New keys get indices beyond "
              "--num_objects; interleaved lookups sample them too");
DEFINE_uint64(put_threads, 4, "Writer threads used by --put_rate");
DEFINE_double(segment_headroom, 0.125,
              "Segment capacity beyond num_objects x 1 KiB, as a fraction; "
              "with --put_rate this is how far writes can run ahead of "
              "eviction before PutStart fails");
DEFINE_uint32(eviction_worker_threads, 0,
              "MasterServiceConfig::eviction_worker_threads (0 = automatic: "
              "one worker per 16M filed keys, clamped to [1, 64])");
DEFINE_uint64(seed, 1, "Reproducible key permutation and lookup RNG seed");
DEFINE_string(lookup_mode, "exist",
              "exist: BatchExistKey (grants a read lease); probe: "
              "BatchProbeKey with no lease");
DEFINE_bool(skip_eviction, false,
            "Never run eviction; measures lookups (and puts) alone as a "
            "control");
DEFINE_string(key_prefix, "",
              "Prefix prepended to every key; use a production-length prefix "
              "(~150 chars) so key strings are heap-allocated like real keys");

namespace {
using Clock = std::chrono::steady_clock;
using Microseconds = std::chrono::microseconds;
const mooncake::TenantId kDefaultTenant = mooncake::TenantId::Default();

// SplitMix64's invertible permutation guarantees unique, randomized key IDs.
// The compact base64 representation stays inside std::string's small buffer.
std::string Key(uint64_t index) {
    uint64_t value = index + FLAGS_seed + 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    value ^= value >> 31;
    constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string key(12, 'k');
    for (size_t i = 1; i < key.size(); ++i) {
        key[i] = alphabet[value & 63];
        value >>= 6;
    }
    if (!FLAGS_key_prefix.empty()) return FLAGS_key_prefix + key;
    return key;
}

uint64_t Us(Clock::duration duration) {
    return std::chrono::duration_cast<Microseconds>(duration).count();
}

struct Sample {
    uint64_t begin_us;
    uint64_t end_us;
    uint64_t hits;
    uint64_t errors;
};

struct alignas(64) ReaderStats {
    std::vector<Sample> samples;
};

struct Eviction {
    uint64_t begin_us;
    uint64_t end_us;
};

struct Summary {
    std::vector<uint64_t> latencies;
    uint64_t hits{0};
    uint64_t errors{0};

    void Add(const Sample& sample) {
        latencies.push_back(sample.end_us - sample.begin_us);
        hits += sample.hits;
        errors += sample.errors;
    }

    void Print(const char* phase, double seconds) {
        std::sort(latencies.begin(), latencies.end());
        auto percentile = [&](double fraction) -> uint64_t {
            if (latencies.empty()) return 0;
            const size_t rank =
                static_cast<size_t>(std::ceil(fraction * latencies.size()));
            return latencies[rank - 1];
        };
        const uint64_t keys = latencies.size() * FLAGS_batch_size;
        std::cout << "RESULT phase=" << phase
                  << " requests=" << latencies.size()
                  << " seconds=" << std::fixed << std::setprecision(3)
                  << seconds << " batches_per_sec="
                  << (seconds > 0 ? latencies.size() / seconds : 0)
                  << " p50_us=" << percentile(0.50)
                  << " p95_us=" << percentile(0.95)
                  << " p99_us=" << percentile(0.99)
                  << " max_us=" << percentile(1.0)
                  << " hit_percent=" << (keys > 0 ? 100.0 * hits / keys : 0)
                  << " errors=" << errors << std::endl;
    }
};

bool Prefill(mooncake::MasterService& service,
             const mooncake::UUID& client_id) {
    std::atomic<uint64_t> completed{0};
    std::atomic<bool> failed{false};
    std::vector<std::thread> writers;
    const auto begin = Clock::now();
    for (uint64_t thread = 0; thread < FLAGS_prefill_threads; ++thread) {
        writers.emplace_back([&, thread] {
            mooncake::ReplicateConfig config;
            config.replica_num = 1;
            config.preferred_segment = "exists_evict_bench_segment";
            for (uint64_t index = thread;
                 index < FLAGS_num_objects && !failed.load();
                 index += FLAGS_prefill_threads) {
                const auto key = Key(index);
                auto start = service.PutStart(client_id, key, kDefaultTenant,
                                              1024, config);
                if (!start || start->size() != 1) {
                    LOG(ERROR) << "PutStart failed at index=" << index;
                    failed.store(true);
                    return;
                }
                auto end = service.PutEnd(client_id, key, kDefaultTenant,
                                          mooncake::ReplicaType::MEMORY);
                if (!end) {
                    LOG(ERROR) << "PutEnd failed at index=" << index;
                    failed.store(true);
                    return;
                }
                const uint64_t count = completed.fetch_add(1) + 1;
                if (count % 1000000 == 0) {
                    std::cout
                        << "PREFILL completed=" << count
                        << " elapsed_sec=" << Us(Clock::now() - begin) / 1e6
                        << std::endl;
                }
            }
        });
    }
    for (auto& writer : writers) writer.join();
    std::cout << "PREFILL completed=" << completed.load()
              << " live_keys=" << service.GetKeyCount()
              << " elapsed_sec=" << Us(Clock::now() - begin) / 1e6 << std::endl;
    return !failed.load() && service.GetKeyCount() == FLAGS_num_objects;
}

bool Run() {
    auto config =
        mooncake::MasterServiceConfig::builder()
            .set_memory_allocator(mooncake::BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_default_kv_lease_ttl(FLAGS_lease_ms)
            .set_client_live_ttl_sec(86400)
            .set_enable_ha(false)
            .build();
    config.eviction_worker_threads = FLAGS_eviction_worker_threads;
    mooncake::MasterService service(config);
    const auto client_id = mooncake::generate_uuid();
    mooncake::Segment segment;
    segment.id = mooncake::generate_uuid();
    segment.name = "exists_evict_bench_segment";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000ULL;
    segment.size =
        static_cast<uint64_t>(static_cast<double>(FLAGS_num_objects) * 1024.0 *
                              (1.0 + FLAGS_segment_headroom)) +
        1048576;
    // Like batch_evict_bench, the segment is a synthetic address range. Real
    // metadata/allocator handles are created; payload bytes and RPC are absent.
    if (!service.MountSegment(segment, client_id)) return false;
    if (!Prefill(service, client_id)) return false;

    std::cout << "CONFIG mode=in_process_metadata_no_payload_io"
              << " objects=" << FLAGS_num_objects
              << " threads=" << FLAGS_lookup_threads
              << " batch_size=" << FLAGS_batch_size
              << " baseline_sec=" << FLAGS_baseline_sec
              << " duration_sec=" << FLAGS_duration_sec
              << " eviction_interval_sec=" << FLAGS_eviction_interval_sec
              << " eviction_ratio=" << FLAGS_eviction_ratio
              << " eviction_lowerbound=" << FLAGS_eviction_lowerbound
              << " lookup_mode=" << FLAGS_lookup_mode
              << " lookup_fraction=" << FLAGS_lookup_fraction
              << " lookup_subset=" << FLAGS_lookup_subset
              << " eviction_worker_threads=" << FLAGS_eviction_worker_threads
              << " lease_ms=" << FLAGS_lease_ms << " seed=" << FLAGS_seed
              << std::endl;

    std::atomic<bool> stop{false};
    // Keys written so far: prefilled keys plus those the writer threads add.
    std::atomic<uint64_t> written{FLAGS_num_objects};
    std::vector<ReaderStats> stats(FLAGS_lookup_threads);
    std::vector<std::thread> readers;
    std::barrier gate(static_cast<std::ptrdiff_t>(FLAGS_lookup_threads + 1));
    Clock::time_point origin;
    for (uint64_t thread = 0; thread < FLAGS_lookup_threads; ++thread) {
        readers.emplace_back([&, thread] {
            std::mt19937_64 rng(FLAGS_seed + thread + 1);
            const uint64_t lookup_keys = std::max<uint64_t>(
                1,
                static_cast<uint64_t>(FLAGS_lookup_fraction *
                                      static_cast<double>(FLAGS_num_objects)));
            std::uniform_int_distribution<uint64_t> index(0, lookup_keys - 1);
            std::uniform_int_distribution<uint64_t> any_index(
                0, FLAGS_num_objects - 1);
            const bool interleaved = FLAGS_lookup_subset == "interleaved";
            // Hot or cold by a hash of the index, independent of Key()'s
            // permutation and of prefill order (which follows the index).
            const uint64_t hot_cutoff =
                static_cast<uint64_t>(FLAGS_lookup_fraction * 1000000.0);
            auto is_hot = [&](uint64_t i) {
                uint64_t z = i + 0xD1B54A32D192ED03ULL;
                z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
                z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
                z ^= z >> 31;
                return z % 1000000 < hot_cutoff;
            };
            auto pick = [&]() -> uint64_t {
                if (!interleaved) return index(rng);
                for (;;) {
                    // Draw from every key written so far, including keys the
                    // writer threads added after prefill.
                    const uint64_t n = written.load(std::memory_order_acquire);
                    const uint64_t i =
                        std::uniform_int_distribution<uint64_t>(0, n - 1)(rng);
                    if (is_hot(i)) return i;
                }
            };
            std::vector<std::string> keys(FLAGS_batch_size);
            auto& samples = stats[thread].samples;
            samples.reserve(65536);
            gate.arrive_and_wait();
            while (!stop.load(std::memory_order_relaxed)) {
                for (auto& key : keys) {
                    key = Key(pick());
                }
                const auto begin = Clock::now();
                auto results =
                    FLAGS_lookup_mode == "probe"
                        ? service.BatchProbeKey(mooncake::GrantLeasePolicy{},
                                                keys, kDefaultTenant)
                        : service.BatchExistKey(keys, kDefaultTenant);
                const auto end = Clock::now();
                uint64_t hits = 0;
                uint64_t errors = 0;
                if (results.size() != keys.size()) {
                    errors = keys.size();
                } else {
                    for (const auto& result : results) {
                        if (!result) {
                            ++errors;
                        } else {
                            hits += result.value();
                        }
                    }
                }
                samples.push_back(
                    {Us(begin - origin), Us(end - origin), hits, errors});
            }
        });
    }
    origin = Clock::now();
    gate.arrive_and_wait();

    // Writer threads: a steady stream of new keys for the whole run.
    struct PutSample {
        uint64_t begin_us;
        uint64_t end_us;
        bool ok;
    };
    std::vector<std::vector<PutSample>> put_samples(FLAGS_put_threads);
    std::vector<std::thread> writers;
    if (FLAGS_put_rate > 0) {
        for (uint64_t w = 0; w < FLAGS_put_threads; ++w) {
            writers.emplace_back([&, w] {
                mooncake::ReplicateConfig config;
                config.replica_num = 1;
                config.preferred_segment = "exists_evict_bench_segment";
                const auto interval =
                    std::chrono::duration_cast<Clock::duration>(
                        std::chrono::duration<double>(
                            static_cast<double>(FLAGS_put_threads) /
                            FLAGS_put_rate));
                auto& samples = put_samples[w];
                samples.reserve(1 << 20);
                auto next = Clock::now();
                while (!stop.load(std::memory_order_relaxed)) {
                    std::this_thread::sleep_until(next);
                    next += interval;
                    // After a stall, resume the rate instead of bursting.
                    if (Clock::now() - next > std::chrono::seconds(1)) {
                        next = Clock::now();
                    }
                    const uint64_t idx = written.fetch_add(1);
                    const auto key = Key(idx);
                    const auto begin = Clock::now();
                    bool ok = false;
                    auto start = service.PutStart(client_id, key,
                                                  kDefaultTenant, 1024, config);
                    if (start && start->size() == 1) {
                        ok = service
                                 .PutEnd(client_id, key, kDefaultTenant,
                                         mooncake::ReplicaType::MEMORY)
                                 .has_value();
                    }
                    const auto end = Clock::now();
                    samples.push_back(
                        {Us(begin - origin), Us(end - origin), ok});
                }
            });
        }
    }
    const auto baseline_end = origin + std::chrono::seconds(FLAGS_baseline_sec);
    const auto deadline =
        baseline_end + std::chrono::seconds(FLAGS_duration_sec);
    const auto interval = std::chrono::seconds(FLAGS_eviction_interval_sec);
    auto next = baseline_end + interval;
    std::vector<Eviction> evictions;
    while (next < deadline) {
        std::this_thread::sleep_until(next);
        if (Clock::now() >= deadline) break;
        if (!FLAGS_skip_eviction) {
            const auto before = service.GetKeyCount();
            const auto begin = Clock::now();
            mooncake::test::MasterServiceTestPeer(service)
                .RunBatchEvictForTesting(FLAGS_eviction_ratio,
                                         FLAGS_eviction_lowerbound);
            const auto end = Clock::now();
            evictions.push_back({Us(begin - origin), Us(end - origin)});
            std::cout << "EVICTION cycle=" << evictions.size()
                      << " begin_sec=" << Us(begin - origin) / 1e6
                      << " duration_ms=" << Us(end - begin) / 1000.0
                      << " keys_before=" << before
                      << " keys_after=" << service.GetKeyCount() << std::endl;
        }
        // Skip missed ticks instead of issuing overlapping/catch-up evictions.
        do {
            next += interval;
        } while (next <= Clock::now());
    }
    std::this_thread::sleep_until(deadline);
    stop.store(true, std::memory_order_relaxed);
    for (auto& reader : readers) reader.join();
    for (auto& writer : writers) writer.join();
    const double measured_sec = Us(Clock::now() - baseline_end) / 1e6;

    Summary baseline, all, during, between;
    uint64_t eviction_us = 0;
    for (const auto& eviction : evictions) {
        eviction_us += eviction.end_us - eviction.begin_us;
    }
    for (const auto& reader : stats) {
        for (const auto& sample : reader.samples) {
            if (sample.begin_us < FLAGS_baseline_sec * 1000000) {
                baseline.Add(sample);
                continue;
            }
            all.Add(sample);
            const bool overlaps = std::any_of(
                evictions.begin(), evictions.end(), [&](const Eviction& e) {
                    return sample.begin_us < e.end_us &&
                           sample.end_us > e.begin_us;
                });
            (overlaps ? during : between).Add(sample);
        }
    }
    if (FLAGS_put_rate > 0) {
        std::vector<uint64_t> lat_all, lat_during;
        uint64_t attempted = 0, failed = 0, attempted_during = 0,
                 failed_during = 0;
        double first_fail_sec = -1;
        for (const auto& per_writer : put_samples) {
            for (const auto& p : per_writer) {
                ++attempted;
                const uint64_t lat = p.end_us - p.begin_us;
                lat_all.push_back(lat);
                const bool overlaps = std::any_of(
                    evictions.begin(), evictions.end(), [&](const Eviction& e) {
                        return p.begin_us < e.end_us && p.end_us > e.begin_us;
                    });
                if (overlaps) {
                    ++attempted_during;
                    lat_during.push_back(lat);
                    if (!p.ok) ++failed_during;
                }
                if (!p.ok) {
                    ++failed;
                    const double t = p.begin_us / 1e6;
                    if (first_fail_sec < 0 || t < first_fail_sec) {
                        first_fail_sec = t;
                    }
                }
            }
        }
        auto pct = [](std::vector<uint64_t>& v, double f) -> uint64_t {
            if (v.empty()) return 0;
            std::sort(v.begin(), v.end());
            return v[std::min(
                v.size() - 1,
                static_cast<size_t>(std::ceil(f * v.size())) - 1)];
        };
        const double run_sec = FLAGS_baseline_sec + measured_sec;
        std::cout << "PUTS attempted=" << attempted
                  << " ok=" << attempted - failed << " failed=" << failed
                  << " rate_per_sec=" << std::fixed << std::setprecision(0)
                  << attempted / run_sec << " p50_us=" << pct(lat_all, 0.50)
                  << " p99_us=" << pct(lat_all, 0.99)
                  << " max_us=" << pct(lat_all, 1.0)
                  << " first_failure_sec=" << std::setprecision(1)
                  << first_fail_sec << std::endl;
        std::cout << "PUTS phase=overlapping_eviction attempted="
                  << attempted_during << " failed=" << failed_during
                  << " p50_us=" << pct(lat_during, 0.50)
                  << " p99_us=" << pct(lat_during, 0.99) << std::endl;
    }
    std::cout << "FINAL keys_written=" << written.load()
              << " live_keys=" << service.GetKeyCount()
              << " eviction_cycles=" << evictions.size() << std::endl;
    baseline.Print("baseline", FLAGS_baseline_sec);
    all.Print("periodic_eviction", measured_sec);
    during.Print("overlapping_eviction", eviction_us / 1e6);
    between.Print("between_evictions", measured_sec - eviction_us / 1e6);
    const bool lookups = FLAGS_lookup_threads > 0;
    if (FLAGS_skip_eviction) return baseline.errors == 0 && all.errors == 0;
    return baseline.errors == 0 && all.errors == 0 &&
           (!lookups ||
            (!baseline.latencies.empty() && !during.latencies.empty())) &&
           !evictions.empty() &&
           (FLAGS_put_rate > 0 || service.GetKeyCount() < FLAGS_num_objects);
}
}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging("ExistsEvictBench");
    FLAGS_logtostderr = true;
    FLAGS_minloglevel = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_put_rate < 0 || (FLAGS_put_rate > 0 && FLAGS_put_threads == 0) ||
        FLAGS_segment_headroom < 0 || FLAGS_eviction_worker_threads > 64) {
        LOG(ERROR)
            << "Invalid --put_rate, --put_threads, --segment_headroom or "
               "--eviction_worker_threads";
        return 1;
    }
    if (FLAGS_lookup_subset != "prefix" &&
        FLAGS_lookup_subset != "interleaved") {
        LOG(ERROR) << "--lookup_subset must be prefix or interleaved";
        return 1;
    }
    if (!(FLAGS_lookup_fraction > 0 && FLAGS_lookup_fraction <= 1)) {
        LOG(ERROR) << "--lookup_fraction must be in (0, 1]";
        return 1;
    }
    if (FLAGS_num_objects == 0 || FLAGS_prefill_threads == 0 ||
        (FLAGS_lookup_mode != "exist" && FLAGS_lookup_mode != "probe") ||
        FLAGS_batch_size == 0 || FLAGS_baseline_sec == 0 ||
        FLAGS_eviction_interval_sec == 0 ||
        FLAGS_duration_sec <= FLAGS_eviction_interval_sec ||
        FLAGS_num_objects >
            std::numeric_limits<uint64_t>::max() / 1152 - 1024 ||
        !(FLAGS_eviction_lowerbound > 0 &&
          FLAGS_eviction_lowerbound <= FLAGS_eviction_ratio &&
          FLAGS_eviction_ratio <= 1)) {
        LOG(ERROR) << "Invalid benchmark dimensions, durations, or ratios";
        return 1;
    }
    const bool ok = Run();
    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
