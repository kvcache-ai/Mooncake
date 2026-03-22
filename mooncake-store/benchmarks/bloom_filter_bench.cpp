// Benchmark for Bloom Filter effect on ExistKey and GetReplicaList
// Tests with varying ratios of existing vs non-existing keys to measure
// the bloom filter's impact on negative lookups.

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <latch>
#include <random>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

static constexpr size_t KiB = 1024;
static constexpr size_t MiB = 1024 * KiB;
static constexpr size_t GiB = 1024 * MiB;
static constexpr uintptr_t kSegmentBase = 0x100000000ULL;

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_uint64(num_segments, 16, "Number of segments to mount");
DEFINE_uint64(segment_size, 64 * GiB, "Size of each segment");
DEFINE_uint64(num_threads, 8, "Number of threads");
DEFINE_uint64(num_prefill_keys, 50000, "Keys to prefill");
DEFINE_uint64(duration, 10, "Test duration in seconds");
DEFINE_uint64(batch_size, 64, "Batch size for operations");
DEFINE_uint64(value_size, 4096, "Size of object values");
DEFINE_double(miss_ratio, 0.5,
              "Ratio of lookups for non-existing keys (0.0-1.0)");

static std::atomic<uint64_t> gHits{0};
static std::atomic<uint64_t> gMisses{0};
static std::atomic<bool> gRunning{true};

class SegmentClient {
   public:
    SegmentClient(const std::string& name, const std::string& master_server,
                  uintptr_t segment_base, uint64_t segment_size)
        : master_client_(mooncake::generate_uuid()) {
        auto ec = master_client_.Connect(master_server);
        CHECK(ec == mooncake::ErrorCode::OK) << "Connect failed";

        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = segment_base;
        segment_.size = segment_size;
        segment_.te_endpoint = name;
        auto mount_ec = master_client_.MountSegment(segment_);
        CHECK(mount_ec.has_value()) << "Mount failed";
    }

    ~SegmentClient() { master_client_.UnmountSegment(segment_.id); }

    void Ping() { master_client_.Ping(); }

   private:
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
};

class BenchWorker {
   public:
    BenchWorker(const std::string& master_server, uint64_t num_prefill_keys,
                double miss_ratio, uint64_t batch_size, uint64_t value_size)
        : master_client_(mooncake::generate_uuid()),
          num_prefill_keys_(num_prefill_keys),
          miss_ratio_(miss_ratio),
          batch_size_(batch_size),
          value_size_(value_size) {
        auto ec = master_client_.Connect(master_server);
        CHECK(ec == mooncake::ErrorCode::OK) << "Connect failed";
    }

    void Run() {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_real_distribution<double> miss_dist(0.0, 1.0);
        std::uniform_int_distribution<uint64_t> key_dist(0,
                                                          num_prefill_keys_ - 1);

        while (gRunning.load(std::memory_order_relaxed)) {
            std::vector<std::string> keys;
            keys.reserve(batch_size_);

            for (uint64_t i = 0; i < batch_size_; i++) {
                if (miss_dist(rng) < miss_ratio_) {
                    // Non-existing key — bloom filter should short-circuit
                    keys.push_back("nonexist_" +
                                   std::to_string(rng() % 100000000));
                } else {
                    // Existing key
                    keys.push_back("bench_key_" +
                                   std::to_string(key_dist(rng)));
                }
            }

            auto results = master_client_.BatchGetReplicaList(keys);
            for (size_t i = 0; i < results.size(); i++) {
                if (results[i].has_value()) {
                    gHits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    gMisses.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

   private:
    mooncake::MasterClient master_client_;
    uint64_t num_prefill_keys_;
    double miss_ratio_;
    uint64_t batch_size_;
    uint64_t value_size_;
};

int main(int argc, char** argv) {
    google::InitGoogleLogging("BloomFilterBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    // Mount segments
    LOG(INFO) << "Mounting " << FLAGS_num_segments << " segments...";
    std::vector<std::unique_ptr<SegmentClient>> segment_clients;
    for (size_t i = 0; i < FLAGS_num_segments; i++) {
        segment_clients.push_back(std::make_unique<SegmentClient>(
            "seg_" + std::to_string(i), FLAGS_master_server,
            kSegmentBase + i * FLAGS_segment_size, FLAGS_segment_size));
    }

    // Ping thread
    auto ping_thread = std::jthread([&](std::stop_token st) {
        while (!st.stop_requested()) {
            for (auto& sc : segment_clients) sc->Ping();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    // Prefill keys
    LOG(INFO) << "Prefilling " << FLAGS_num_prefill_keys << " keys...";
    {
        mooncake::MasterClient prefill_client(mooncake::generate_uuid());
        prefill_client.Connect(FLAGS_master_server);

        mooncake::ReplicateConfig config;
        config.replica_num = 1;
        for (uint64_t i = 0; i < FLAGS_num_prefill_keys;) {
            std::vector<std::string> keys;
            std::vector<std::vector<uint64_t>> slice_lengths;
            uint64_t batch = std::min(FLAGS_batch_size,
                                       FLAGS_num_prefill_keys - i);
            for (uint64_t j = 0; j < batch; j++) {
                keys.push_back("bench_key_" + std::to_string(i + j));
                slice_lengths.push_back({FLAGS_value_size});
            }
            auto put_results =
                prefill_client.BatchPutStart(keys, slice_lengths, config);
            std::vector<std::string> started_keys;
            for (size_t k = 0; k < put_results.size(); k++) {
                if (put_results[k].has_value()) {
                    started_keys.push_back(keys[k]);
                }
            }
            prefill_client.BatchPutEnd(started_keys);
            i += batch;
        }
    }
    LOG(INFO) << "Prefill done";

    // Run benchmark workers
    LOG(INFO) << "Starting " << FLAGS_num_threads << " workers, miss_ratio="
              << FLAGS_miss_ratio << ", duration=" << FLAGS_duration << "s";

    std::vector<std::thread> workers;
    for (size_t i = 0; i < FLAGS_num_threads; i++) {
        auto worker = std::make_unique<BenchWorker>(
            FLAGS_master_server, FLAGS_num_prefill_keys, FLAGS_miss_ratio,
            FLAGS_batch_size, FLAGS_value_size);
        workers.emplace_back([w = std::move(worker)]() { w->Run(); });
    }

    // Collect stats per second
    uint64_t last_total = 0;
    for (size_t sec = 0; sec < FLAGS_duration; sec++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        uint64_t curr_total = gHits.load() + gMisses.load();
        LOG(INFO) << "ops/s: " << (curr_total - last_total)
                  << "  hits=" << gHits.load() << "  misses=" << gMisses.load();
        last_total = curr_total;
    }

    gRunning.store(false);
    for (auto& w : workers) w.join();

    ping_thread.request_stop();
    ping_thread.join();
    segment_clients.clear();

    uint64_t total = gHits.load() + gMisses.load();
    double ops_per_sec = total / static_cast<double>(FLAGS_duration);

    std::cout << "\n=== Bloom Filter Benchmark Results ===\n"
              << "Miss ratio: " << FLAGS_miss_ratio << "\n"
              << "Threads: " << FLAGS_num_threads << "\n"
              << "Total ops: " << total << "\n"
              << "Ops/sec: " << std::fixed << std::setprecision(0) << ops_per_sec
              << "\n"
              << "Hits: " << gHits.load() << " Misses: " << gMisses.load()
              << "\n";

    return 0;
}
