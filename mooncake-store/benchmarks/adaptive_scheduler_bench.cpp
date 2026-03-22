// Adaptive Cache Scheduler Benchmark
// Simulates realistic LLM inference workload phases and measures how the
// adaptive scheduler detects and responds to workload changes.
//
// Three phases:
//   Phase 1 (PREFIX_HEAVY): 90% requests hit system prompts → high hit rate
//   Phase 2 (SCAN_HEAVY): batch of unique queries → low hit rate
//   Phase 3 (MIXED): interactive inference → moderate hit rate
//
// Measures: throughput per phase, scheduler mode transitions, parameter changes

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
DEFINE_uint64(num_threads, 4, "Number of worker threads");
DEFINE_uint64(num_system_prompts, 100, "Number of system prompt prefixes");
DEFINE_uint64(phase_duration, 15, "Duration of each phase in seconds");
DEFINE_uint64(batch_size, 32, "Batch size for operations");
DEFINE_uint64(value_size, 4096, "Size of object values");

static std::atomic<uint64_t> gHits{0};
static std::atomic<uint64_t> gMisses{0};
static std::atomic<bool> gRunning{true};

enum class WorkloadPhase {
    PREFIX_HEAVY,  // 90% system prompt hits
    SCAN_HEAVY,    // 90% unique queries (misses)
    MIXED,         // 50/50
};

static std::atomic<WorkloadPhase> gCurrentPhase{WorkloadPhase::PREFIX_HEAVY};

class SegmentClient {
   public:
    SegmentClient(const std::string& name, const std::string& master_server,
                  uintptr_t base, uint64_t size)
        : master_client_(mooncake::generate_uuid()) {
        auto ec = master_client_.Connect(master_server);
        CHECK(ec == mooncake::ErrorCode::OK);
        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = base;
        segment_.size = size;
        segment_.te_endpoint = name;
        auto r = master_client_.MountSegment(segment_);
        CHECK(r.has_value());
    }
    ~SegmentClient() { master_client_.UnmountSegment(segment_.id); }
    void Ping() { master_client_.Ping(); }

   private:
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
};

class WorkloadGenerator {
   public:
    WorkloadGenerator(const std::string& master_server,
                      uint64_t num_system_prompts, uint64_t batch_size)
        : master_client_(mooncake::generate_uuid()),
          num_system_prompts_(num_system_prompts),
          batch_size_(batch_size),
          scan_counter_(0) {
        auto ec = master_client_.Connect(master_server);
        CHECK(ec == mooncake::ErrorCode::OK);
    }

    void Run() {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<uint64_t> prompt_dist(
            0, num_system_prompts_ - 1);
        std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

        while (gRunning.load(std::memory_order_relaxed)) {
            auto phase = gCurrentPhase.load(std::memory_order_relaxed);
            std::vector<std::string> keys;
            keys.reserve(batch_size_);

            for (uint64_t i = 0; i < batch_size_; i++) {
                double r = ratio_dist(rng);
                switch (phase) {
                    case WorkloadPhase::PREFIX_HEAVY:
                        // 90% system prompt, 10% unique
                        if (r < 0.9) {
                            keys.push_back(
                                "sys_prompt_" +
                                std::to_string(prompt_dist(rng)));
                        } else {
                            keys.push_back(
                                "unique_" +
                                std::to_string(
                                    scan_counter_.fetch_add(1)));
                        }
                        break;
                    case WorkloadPhase::SCAN_HEAVY:
                        // 10% system prompt, 90% unique (will miss)
                        if (r < 0.1) {
                            keys.push_back(
                                "sys_prompt_" +
                                std::to_string(prompt_dist(rng)));
                        } else {
                            keys.push_back(
                                "scan_" +
                                std::to_string(
                                    scan_counter_.fetch_add(1)));
                        }
                        break;
                    case WorkloadPhase::MIXED:
                        // 50/50
                        if (r < 0.5) {
                            keys.push_back(
                                "sys_prompt_" +
                                std::to_string(prompt_dist(rng)));
                        } else {
                            keys.push_back(
                                "mixed_" +
                                std::to_string(
                                    scan_counter_.fetch_add(1)));
                        }
                        break;
                }
            }

            auto results = master_client_.BatchGetReplicaList(keys);
            for (auto& r : results) {
                if (r.has_value()) {
                    gHits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    gMisses.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

   private:
    mooncake::MasterClient master_client_;
    uint64_t num_system_prompts_;
    uint64_t batch_size_;
    std::atomic<uint64_t> scan_counter_;
};

static const char* PhaseToString(WorkloadPhase p) {
    switch (p) {
        case WorkloadPhase::PREFIX_HEAVY: return "PREFIX_HEAVY";
        case WorkloadPhase::SCAN_HEAVY: return "SCAN_HEAVY";
        case WorkloadPhase::MIXED: return "MIXED";
        default: return "UNKNOWN";
    }
}

int main(int argc, char** argv) {
    google::InitGoogleLogging("AdaptiveSchedulerBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    // Mount segments
    LOG(INFO) << "Mounting " << FLAGS_num_segments << " segments...";
    std::vector<std::unique_ptr<SegmentClient>> segments;
    for (size_t i = 0; i < FLAGS_num_segments; i++) {
        segments.push_back(std::make_unique<SegmentClient>(
            "seg_" + std::to_string(i), FLAGS_master_server,
            kSegmentBase + i * FLAGS_segment_size, FLAGS_segment_size));
    }

    // Ping thread
    auto ping_thread = std::jthread([&](std::stop_token st) {
        while (!st.stop_requested()) {
            for (auto& s : segments) s->Ping();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    // Prefill system prompts
    LOG(INFO) << "Prefilling " << FLAGS_num_system_prompts
              << " system prompt keys...";
    {
        mooncake::MasterClient prefill(mooncake::generate_uuid());
        prefill.Connect(FLAGS_master_server);
        mooncake::ReplicateConfig config;
        config.replica_num = 1;

        for (uint64_t i = 0; i < FLAGS_num_system_prompts;) {
            uint64_t batch =
                std::min(FLAGS_batch_size, FLAGS_num_system_prompts - i);
            std::vector<std::string> keys;
            std::vector<std::vector<uint64_t>> sizes;
            for (uint64_t j = 0; j < batch; j++) {
                keys.push_back("sys_prompt_" + std::to_string(i + j));
                sizes.push_back({FLAGS_value_size});
            }
            auto r = prefill.BatchPutStart(keys, sizes, config);
            std::vector<std::string> ok_keys;
            for (size_t k = 0; k < r.size(); k++) {
                if (r[k].has_value()) ok_keys.push_back(keys[k]);
            }
            prefill.BatchPutEnd(ok_keys);
            i += batch;
        }
    }
    LOG(INFO) << "Prefill done";

    // Start worker threads
    std::vector<std::thread> workers;
    for (size_t i = 0; i < FLAGS_num_threads; i++) {
        auto gen = std::make_unique<WorkloadGenerator>(
            FLAGS_master_server, FLAGS_num_system_prompts, FLAGS_batch_size);
        workers.emplace_back([g = std::move(gen)]() { g->Run(); });
    }

    // Run three phases
    WorkloadPhase phases[] = {WorkloadPhase::PREFIX_HEAVY,
                              WorkloadPhase::SCAN_HEAVY,
                              WorkloadPhase::MIXED};

    std::cout << "\n=== Adaptive Cache Scheduler Benchmark ===\n"
              << "Threads: " << FLAGS_num_threads
              << "  System prompts: " << FLAGS_num_system_prompts
              << "  Phase duration: " << FLAGS_phase_duration << "s\n\n";

    for (auto phase : phases) {
        gCurrentPhase.store(phase, std::memory_order_relaxed);
        gHits.store(0);
        gMisses.store(0);

        std::cout << "--- Phase: " << PhaseToString(phase) << " ---\n";

        uint64_t last_total = 0;
        for (size_t sec = 0; sec < FLAGS_phase_duration; sec++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t h = gHits.load();
            uint64_t m = gMisses.load();
            uint64_t total = h + m;
            double hit_rate = (total > 0) ? (double)h / total : 0;
            std::cout << "  [" << std::setw(2) << sec + 1 << "s] "
                      << std::setw(8) << (total - last_total) << " ops/s"
                      << "  hit_rate=" << std::fixed << std::setprecision(3)
                      << hit_rate << "\n";
            last_total = total;
        }

        uint64_t h = gHits.load();
        uint64_t m = gMisses.load();
        uint64_t total = h + m;
        double ops_per_sec = (double)total / FLAGS_phase_duration;
        double hit_rate = (total > 0) ? (double)h / total : 0;

        std::cout << "  RESULT: " << std::fixed << std::setprecision(0)
                  << ops_per_sec << " ops/s"
                  << "  hit_rate=" << std::setprecision(3) << hit_rate
                  << "  hits=" << h << "  misses=" << m << "\n\n";

        // Brief pause between phases for scheduler to react
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    gRunning.store(false);
    for (auto& w : workers) w.join();

    ping_thread.request_stop();
    ping_thread.join();
    segments.clear();

    std::cout << "=== Benchmark Complete ===\n"
              << "Check master log for [ADAPTIVE-SCHED] entries to verify\n"
              << "mode transitions: PREFIX_HEAVY → SCAN_HEAVY → MIXED\n";

    return 0;
}
