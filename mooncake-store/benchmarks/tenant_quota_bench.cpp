#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace {

constexpr uint64_t kAdmissionClosed = 1ULL << 63;
constexpr uint64_t kChargedBytesMask = kAdmissionClosed - 1;
constexpr size_t kCacheLineBytes = 64;

enum class Mode { kBoth, kMutex, kCas };
enum class Workload { kAbort, kCommit };
enum class TenantPattern { kSticky, kRoundRobin };

struct Config {
    Mode mode = Mode::kBoth;
    Workload workload = Workload::kCommit;
    TenantPattern tenant_pattern = TenantPattern::kSticky;
    size_t threads = std::max(1U, std::thread::hardware_concurrency());
    size_t tenants = 1;
    size_t mutex_shards = 1024;
    uint64_t iterations = 200000;
    uint64_t warmup_iterations = 10000;
    uint64_t quota_bytes = 1ULL << 50;
    uint64_t charge_bytes = 4096;
    uint32_t work_iterations = 0;
    size_t rounds = 3;
    bool pin_threads = false;
};

struct ThreadStats {
    uint64_t attempts = 0;
    uint64_t successes = 0;
    uint64_t rejected = 0;
    uint64_t accounting_errors = 0;
    uint64_t charge_cas_retries = 0;
    uint64_t release_cas_retries = 0;
    uint64_t checksum = 0;
    uint64_t elapsed_ns = 0;
};

struct BenchResult {
    std::string mode;
    uint64_t attempts = 0;
    uint64_t successes = 0;
    uint64_t rejected = 0;
    uint64_t accounting_errors = 0;
    uint64_t charge_cas_retries = 0;
    uint64_t release_cas_retries = 0;
    uint64_t final_charged_bytes = 0;
    uint64_t checksum = 0;
    double seconds = 0.0;
    double operations_per_second = 0.0;
    double nanoseconds_per_operation = 0.0;
};

[[noreturn]] void UsageError(const std::string& message);

std::string ModeName(Mode mode) {
    switch (mode) {
        case Mode::kBoth:
            return "both";
        case Mode::kMutex:
            return "mutex";
        case Mode::kCas:
            return "cas";
    }
    return "unknown";
}

std::string WorkloadName(Workload workload) {
    switch (workload) {
        case Workload::kAbort:
            return "abort";
        case Workload::kCommit:
            return "commit";
    }
    return "unknown";
}

std::string TenantPatternName(TenantPattern pattern) {
    switch (pattern) {
        case TenantPattern::kSticky:
            return "sticky";
        case TenantPattern::kRoundRobin:
            return "round_robin";
    }
    return "unknown";
}

void PrintUsage(std::ostream& output) {
    output
        << "Usage: tenant_quota_bench [options]\n"
        << "\n"
        << "Compare the current mutex-based tenant quota accounting with a\n"
        << "unified charged-bytes CAS prototype.\n"
        << "\n"
        << "Options:\n"
        << "  --mode=both|mutex|cas          Implementations to run (default: "
           "both)\n"
        << "  --workload=commit|abort        Successful lifecycle or abort "
           "path\n"
        << "                                   (default: commit)\n"
        << "  --threads=N                    Worker threads (default: hardware "
           "threads)\n"
        << "  --tenants=N                    Tenant accounts (default: 1)\n"
        << "  --tenant-pattern=sticky|round_robin\n"
        << "                                   Tenant selection (default: "
           "sticky)\n"
        << "  --mutex-shards=N               Baseline quota lock shards "
           "(default: 1024)\n"
        << "  --iterations=N                 Measured lifecycles per thread "
           "(default: 200000)\n"
        << "  --warmup=N                     Warmup lifecycles per thread "
           "(default: 10000)\n"
        << "  --rounds=N                     Repetitions per implementation "
           "(default: 3)\n"
        << "  --quota-bytes=N                Effective quota per tenant\n"
        << "  --charge-bytes=N               Bytes per admission (default: "
           "4096)\n"
        << "  --work=N                       CPU work iterations between quota "
           "calls\n"
        << "  --pin-threads                  Pin workers to allowed Linux "
           "CPUs\n"
        << "  --no-pin-threads               Disable worker pinning (default)\n"
        << "  --help                         Show this message\n"
        << "\n"
        << "The commit workload performs Reserve+Commit+Release for the mutex\n"
        << "baseline and TryCharge+no-op settlement+Release for CAS. The "
           "abort\n"
        << "workload performs Reserve+Abort versus TryCharge+Release.\n";
}

[[noreturn]] void UsageError(const std::string& message) {
    std::cerr << "error: " << message << "\n\n";
    PrintUsage(std::cerr);
    std::exit(2);
}

uint64_t ParseUnsigned(const std::string& name, const std::string& value) {
    if (value.empty() || value.front() == '-') {
        UsageError("invalid value for --" + name + ": " + value);
    }
    size_t parsed = 0;
    uint64_t result = 0;
    try {
        result = std::stoull(value, &parsed, 10);
    } catch (const std::exception&) {
        UsageError("invalid value for --" + name + ": " + value);
    }
    if (parsed != value.size()) {
        UsageError("invalid value for --" + name + ": " + value);
    }
    return result;
}

std::string ReadOptionValue(int& index, int argc, char** argv,
                            const std::string& argument,
                            const std::string& name) {
    const std::string prefix = "--" + name + "=";
    if (argument.rfind(prefix, 0) == 0) {
        return argument.substr(prefix.size());
    }
    if (argument == "--" + name) {
        if (++index >= argc) {
            UsageError("missing value for --" + name);
        }
        return argv[index];
    }
    return {};
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help" || argument == "-h") {
            PrintUsage(std::cout);
            std::exit(0);
        }
        if (argument == "--pin-threads") {
            config.pin_threads = true;
            continue;
        }
        if (argument == "--no-pin-threads") {
            config.pin_threads = false;
            continue;
        }

        std::string value;
        if (!(value = ReadOptionValue(i, argc, argv, argument, "mode"))
                 .empty()) {
            if (value == "both") {
                config.mode = Mode::kBoth;
            } else if (value == "mutex") {
                config.mode = Mode::kMutex;
            } else if (value == "cas") {
                config.mode = Mode::kCas;
            } else {
                UsageError("unknown mode: " + value);
            }
        } else if (!(value =
                         ReadOptionValue(i, argc, argv, argument, "workload"))
                        .empty()) {
            if (value == "commit") {
                config.workload = Workload::kCommit;
            } else if (value == "abort") {
                config.workload = Workload::kAbort;
            } else {
                UsageError("unknown workload: " + value);
            }
        } else if (!(value = ReadOptionValue(i, argc, argv, argument,
                                             "tenant-pattern"))
                        .empty()) {
            if (value == "sticky") {
                config.tenant_pattern = TenantPattern::kSticky;
            } else if (value == "round_robin") {
                config.tenant_pattern = TenantPattern::kRoundRobin;
            } else {
                UsageError("unknown tenant pattern: " + value);
            }
        } else if (!(value =
                         ReadOptionValue(i, argc, argv, argument, "threads"))
                        .empty()) {
            config.threads = ParseUnsigned("threads", value);
        } else if (!(value =
                         ReadOptionValue(i, argc, argv, argument, "tenants"))
                        .empty()) {
            config.tenants = ParseUnsigned("tenants", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument,
                                             "mutex-shards"))
                        .empty()) {
            config.mutex_shards = ParseUnsigned("mutex-shards", value);
        } else if (!(value =
                         ReadOptionValue(i, argc, argv, argument, "iterations"))
                        .empty()) {
            config.iterations = ParseUnsigned("iterations", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument, "warmup"))
                        .empty()) {
            config.warmup_iterations = ParseUnsigned("warmup", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument, "rounds"))
                        .empty()) {
            config.rounds = ParseUnsigned("rounds", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument,
                                             "quota-bytes"))
                        .empty()) {
            config.quota_bytes = ParseUnsigned("quota-bytes", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument,
                                             "charge-bytes"))
                        .empty()) {
            config.charge_bytes = ParseUnsigned("charge-bytes", value);
        } else if (!(value = ReadOptionValue(i, argc, argv, argument, "work"))
                        .empty()) {
            const uint64_t parsed = ParseUnsigned("work", value);
            if (parsed > std::numeric_limits<uint32_t>::max()) {
                UsageError("--work exceeds uint32_t range");
            }
            config.work_iterations = static_cast<uint32_t>(parsed);
        } else {
            UsageError("unknown option: " + argument);
        }
    }

    if (config.threads == 0 || config.tenants == 0 ||
        config.mutex_shards == 0 || config.iterations == 0 ||
        config.rounds == 0 || config.charge_bytes == 0) {
        UsageError(
            "threads, tenants, mutex-shards, iterations, rounds, and "
            "charge-bytes must be greater than zero");
    }
    if (config.quota_bytes > kChargedBytesMask ||
        config.charge_bytes > kChargedBytesMask) {
        UsageError("quota and charge bytes must be at most 2^63 - 1");
    }
    if (config.iterations >
        std::numeric_limits<uint64_t>::max() / config.threads) {
        UsageError("threads * iterations overflows uint64_t");
    }
    return config;
}

uint64_t DoWork(uint32_t iterations, uint64_t value) {
    for (uint32_t i = 0; i < iterations; ++i) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
    }
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return value;
}

std::vector<size_t> AllowedCpuIds() {
    std::vector<size_t> cpu_ids;
#ifdef __linux__
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) {
        for (size_t cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &allowed)) {
                cpu_ids.push_back(cpu);
            }
        }
    }
#endif
    return cpu_ids;
}

bool PinCurrentThread(size_t cpu_id) {
#ifdef __linux__
    if (cpu_id >= CPU_SETSIZE) {
        return false;
    }
    cpu_set_t target;
    CPU_ZERO(&target);
    CPU_SET(cpu_id, &target);
    return pthread_setaffinity_np(pthread_self(), sizeof(target), &target) == 0;
#else
    (void)cpu_id;
    return false;
#endif
}

struct alignas(kCacheLineBytes) MutexQuotaState {
    uint64_t used_bytes = 0;
    uint64_t reserved_bytes = 0;
    uint64_t effective_quota_bytes = 0;
};

struct alignas(kCacheLineBytes) MutexShard {
    std::mutex mutex;
};

class MutexQuotaBackend {
   public:
    explicit MutexQuotaBackend(const Config& config)
        : tenant_count_(config.tenants),
          shard_count_(config.mutex_shards),
          states_(std::make_unique<MutexQuotaState[]>(tenant_count_)),
          shards_(std::make_unique<MutexShard[]>(shard_count_)) {
        for (size_t i = 0; i < tenant_count_; ++i) {
            states_[i].effective_quota_bytes = config.quota_bytes;
        }
    }

    bool RunAbort(size_t tenant, uint64_t bytes, uint32_t work_iterations,
                  ThreadStats& stats) {
        if (!Reserve(tenant, bytes)) {
            return false;
        }
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 1);
        if (!Abort(tenant, bytes)) {
            ++stats.accounting_errors;
        }
        return true;
    }

    bool RunCommit(size_t tenant, uint64_t bytes, uint32_t work_iterations,
                   ThreadStats& stats) {
        if (!Reserve(tenant, bytes)) {
            return false;
        }
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 1);
        if (!Commit(tenant, bytes)) {
            ++stats.accounting_errors;
            return true;
        }
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 2);
        if (!Release(tenant, bytes)) {
            ++stats.accounting_errors;
        }
        return true;
    }

    uint64_t FinalChargedBytes() {
        uint64_t total = 0;
        for (size_t shard_index = 0; shard_index < shard_count_;
             ++shard_index) {
            std::lock_guard<std::mutex> lock(shards_[shard_index].mutex);
            for (size_t tenant = shard_index; tenant < tenant_count_;
                 tenant += shard_count_) {
                total += states_[tenant].used_bytes;
                total += states_[tenant].reserved_bytes;
            }
        }
        return total;
    }

   private:
    size_t ShardIndex(size_t tenant) const { return tenant % shard_count_; }

    bool Reserve(size_t tenant, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(shards_[ShardIndex(tenant)].mutex);
        auto& state = states_[tenant];
        const uint64_t charged = state.used_bytes + state.reserved_bytes;
        if (charged > state.effective_quota_bytes ||
            bytes > state.effective_quota_bytes - charged) {
            return false;
        }
        state.reserved_bytes += bytes;
        return true;
    }

    bool Commit(size_t tenant, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(shards_[ShardIndex(tenant)].mutex);
        auto& state = states_[tenant];
        if (state.reserved_bytes < bytes) {
            return false;
        }
        state.reserved_bytes -= bytes;
        state.used_bytes += bytes;
        return true;
    }

    bool Abort(size_t tenant, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(shards_[ShardIndex(tenant)].mutex);
        auto& state = states_[tenant];
        if (state.reserved_bytes < bytes) {
            return false;
        }
        state.reserved_bytes -= bytes;
        return true;
    }

    bool Release(size_t tenant, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(shards_[ShardIndex(tenant)].mutex);
        auto& state = states_[tenant];
        if (state.used_bytes < bytes) {
            return false;
        }
        state.used_bytes -= bytes;
        return true;
    }

    size_t tenant_count_;
    size_t shard_count_;
    std::unique_ptr<MutexQuotaState[]> states_;
    std::unique_ptr<MutexShard[]> shards_;
};

struct alignas(kCacheLineBytes) CasQuotaAccount {
    std::atomic<uint64_t> charged_state{0};
    alignas(kCacheLineBytes) std::atomic<uint64_t> effective_quota_bytes{0};
    std::atomic<uint64_t> policy_sequence{0};
};

class CasQuotaBackend {
   public:
    explicit CasQuotaBackend(const Config& config)
        : tenant_count_(config.tenants),
          accounts_(std::make_unique<CasQuotaAccount[]>(tenant_count_)) {
        for (size_t i = 0; i < tenant_count_; ++i) {
            accounts_[i].effective_quota_bytes.store(config.quota_bytes,
                                                     std::memory_order_relaxed);
        }
    }

    bool RunAbort(size_t tenant, uint64_t bytes, uint32_t work_iterations,
                  ThreadStats& stats) {
        if (!TryCharge(tenant, bytes, stats)) {
            return false;
        }
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 1);
        if (!Release(tenant, bytes, stats)) {
            ++stats.accounting_errors;
        }
        return true;
    }

    bool RunCommit(size_t tenant, uint64_t bytes, uint32_t work_iterations,
                   ThreadStats& stats) {
        if (!TryCharge(tenant, bytes, stats)) {
            return false;
        }
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 1);
        // Successful settlement transfers pending ownership to committed
        // ownership without changing the aggregate charged byte count.
        stats.checksum ^=
            DoWork(work_iterations, stats.checksum + tenant + bytes + 2);
        if (!Release(tenant, bytes, stats)) {
            ++stats.accounting_errors;
        }
        return true;
    }

    uint64_t FinalChargedBytes() const {
        uint64_t total = 0;
        for (size_t i = 0; i < tenant_count_; ++i) {
            total +=
                accounts_[i].charged_state.load(std::memory_order_relaxed) &
                kChargedBytesMask;
        }
        return total;
    }

   private:
    bool TryCharge(size_t tenant, uint64_t bytes, ThreadStats& stats) {
        auto& account = accounts_[tenant];
        for (;;) {
            const uint64_t sequence_before =
                account.policy_sequence.load(std::memory_order_acquire);
            if (sequence_before & 1) {
                ++stats.charge_cas_retries;
                continue;
            }

            uint64_t expected =
                account.charged_state.load(std::memory_order_acquire);
            if (expected & kAdmissionClosed) {
                return false;
            }
            const uint64_t charged = expected & kChargedBytesMask;
            const uint64_t limit =
                account.effective_quota_bytes.load(std::memory_order_acquire);
            if (charged > limit || bytes > limit - charged) {
                return false;
            }

            const uint64_t desired = charged + bytes;
            if (!account.charged_state.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                ++stats.charge_cas_retries;
                continue;
            }

            const uint64_t state_after =
                account.charged_state.load(std::memory_order_acquire);
            const uint64_t sequence_after =
                account.policy_sequence.load(std::memory_order_acquire);
            if (sequence_before == sequence_after && !(sequence_after & 1) &&
                !(state_after & kAdmissionClosed)) {
                return true;
            }

            if (!Release(tenant, bytes, stats)) {
                ++stats.accounting_errors;
                return false;
            }
        }
    }

    bool Release(size_t tenant, uint64_t bytes, ThreadStats& stats) {
        auto& charged_state = accounts_[tenant].charged_state;
        uint64_t expected = charged_state.load(std::memory_order_acquire);
        for (;;) {
            const uint64_t charged = expected & kChargedBytesMask;
            if (bytes > charged) {
                return false;
            }
            const uint64_t desired =
                (expected & kAdmissionClosed) | (charged - bytes);
            if (charged_state.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return true;
            }
            ++stats.release_cas_retries;
        }
    }

    size_t tenant_count_;
    std::unique_ptr<CasQuotaAccount[]> accounts_;
};

template <typename Backend>
BenchResult RunBenchmark(const Config& config, const std::string& mode_name) {
    Backend backend(config);
    const std::vector<size_t> allowed_cpus = AllowedCpuIds();
    if (config.pin_threads && allowed_cpus.empty()) {
        UsageError("--pin-threads requested but no allowed Linux CPUs found");
    }

    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<size_t> pin_failures{0};
    std::vector<ThreadStats> thread_stats(config.threads);
    std::vector<std::thread> workers;
    workers.reserve(config.threads);
    std::chrono::steady_clock::time_point start_time;

    for (size_t thread_index = 0; thread_index < config.threads;
         ++thread_index) {
        workers.emplace_back([&, thread_index] {
            if (config.pin_threads &&
                !PinCurrentThread(
                    allowed_cpus[thread_index % allowed_cpus.size()])) {
                pin_failures.fetch_add(1, std::memory_order_relaxed);
            }

            auto run_iterations = [&](uint64_t count, ThreadStats& stats) {
                const size_t sticky_tenant = thread_index % config.tenants;
                for (uint64_t i = 0; i < count; ++i) {
                    const size_t tenant =
                        config.tenant_pattern == TenantPattern::kSticky
                            ? sticky_tenant
                            : (thread_index + i) % config.tenants;
                    ++stats.attempts;
                    const bool admitted =
                        config.workload == Workload::kCommit
                            ? backend.RunCommit(tenant, config.charge_bytes,
                                                config.work_iterations, stats)
                            : backend.RunAbort(tenant, config.charge_bytes,
                                               config.work_iterations, stats);
                    if (admitted) {
                        ++stats.successes;
                    } else {
                        ++stats.rejected;
                    }
                }
            };

            ThreadStats warmup_stats;
            run_iterations(config.warmup_iterations, warmup_stats);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            ThreadStats measured;
            run_iterations(config.iterations, measured);
            const auto end_time = std::chrono::steady_clock::now();
            measured.elapsed_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                                     start_time)
                    .count();
            measured.accounting_errors += warmup_stats.accounting_errors;
            thread_stats[thread_index] = measured;
        });
    }

    while (ready.load(std::memory_order_acquire) != config.threads) {
        std::this_thread::yield();
    }
    start_time = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);

    for (auto& worker : workers) {
        worker.join();
    }

    if (pin_failures.load(std::memory_order_relaxed) != 0) {
        std::cerr << "warning: failed to pin "
                  << pin_failures.load(std::memory_order_relaxed)
                  << " worker thread(s)\n";
    }

    BenchResult result;
    result.mode = mode_name;
    uint64_t max_elapsed_ns = 0;
    for (const auto& stats : thread_stats) {
        result.attempts += stats.attempts;
        result.successes += stats.successes;
        result.rejected += stats.rejected;
        result.accounting_errors += stats.accounting_errors;
        result.charge_cas_retries += stats.charge_cas_retries;
        result.release_cas_retries += stats.release_cas_retries;
        result.checksum ^= stats.checksum;
        max_elapsed_ns = std::max(max_elapsed_ns, stats.elapsed_ns);
    }
    result.final_charged_bytes = backend.FinalChargedBytes();
    result.seconds = static_cast<double>(max_elapsed_ns) / 1e9;
    result.operations_per_second =
        static_cast<double>(result.attempts) / result.seconds;
    result.nanoseconds_per_operation =
        static_cast<double>(max_elapsed_ns) / result.attempts;
    return result;
}

void PrintResult(size_t round, const Config& config,
                 const BenchResult& result) {
    const uint64_t total_retries =
        result.charge_cas_retries + result.release_cas_retries;
    const double retries_per_operation =
        static_cast<double>(total_retries) / result.attempts;

    std::cout << std::fixed << std::setprecision(3) << "RESULT"
              << " round=" << round << " mode=" << result.mode
              << " workload=" << WorkloadName(config.workload)
              << " threads=" << config.threads << " tenants=" << config.tenants
              << " tenant_pattern=" << TenantPatternName(config.tenant_pattern)
              << " attempts=" << result.attempts
              << " successes=" << result.successes
              << " rejected=" << result.rejected
              << " seconds=" << result.seconds
              << " mops=" << result.operations_per_second / 1e6
              << " ns_per_op=" << result.nanoseconds_per_operation
              << " charge_cas_retries=" << result.charge_cas_retries
              << " release_cas_retries=" << result.release_cas_retries
              << " retries_per_op=" << retries_per_operation
              << " accounting_errors=" << result.accounting_errors
              << " final_charged_bytes=" << result.final_charged_bytes
              << " checksum=" << result.checksum << "\n";
}

double Median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const size_t middle = values.size() / 2;
    if (values.size() % 2 == 0) {
        return (values[middle - 1] + values[middle]) / 2.0;
    }
    return values[middle];
}

}  // namespace

int main(int argc, char** argv) {
    const Config config = ParseArguments(argc, argv);
    std::cout << "CONFIG"
              << " mode=" << ModeName(config.mode)
              << " workload=" << WorkloadName(config.workload)
              << " threads=" << config.threads
              << " hardware_threads=" << std::thread::hardware_concurrency()
              << " tenants=" << config.tenants
              << " tenant_pattern=" << TenantPatternName(config.tenant_pattern)
              << " mutex_shards=" << config.mutex_shards
              << " iterations=" << config.iterations
              << " warmup=" << config.warmup_iterations
              << " rounds=" << config.rounds
              << " quota_bytes=" << config.quota_bytes
              << " charge_bytes=" << config.charge_bytes
              << " work=" << config.work_iterations
              << " pin_threads=" << (config.pin_threads ? "true" : "false")
              << " compiler=\"" << __VERSION__ << "\"\n";

    std::vector<double> mutex_throughputs;
    std::vector<double> cas_throughputs;
    bool verification_failed = false;

    auto run_mutex = [&](size_t round) {
        const auto result = RunBenchmark<MutexQuotaBackend>(config, "mutex");
        PrintResult(round, config, result);
        mutex_throughputs.push_back(result.operations_per_second);
        verification_failed |=
            result.accounting_errors != 0 || result.final_charged_bytes != 0;
    };
    auto run_cas = [&](size_t round) {
        const auto result = RunBenchmark<CasQuotaBackend>(config, "cas");
        PrintResult(round, config, result);
        cas_throughputs.push_back(result.operations_per_second);
        verification_failed |=
            result.accounting_errors != 0 || result.final_charged_bytes != 0;
    };

    for (size_t round = 1; round <= config.rounds; ++round) {
        if (config.mode == Mode::kMutex) {
            run_mutex(round);
        } else if (config.mode == Mode::kCas) {
            run_cas(round);
        } else if (round % 2 == 1) {
            run_mutex(round);
            run_cas(round);
        } else {
            run_cas(round);
            run_mutex(round);
        }
    }

    if (!mutex_throughputs.empty() && !cas_throughputs.empty()) {
        const double mutex_median = Median(mutex_throughputs);
        const double cas_median = Median(cas_throughputs);
        std::cout << std::fixed << std::setprecision(3) << "SUMMARY"
                  << " mutex_median_mops=" << mutex_median / 1e6
                  << " cas_median_mops=" << cas_median / 1e6
                  << " cas_speedup=" << cas_median / mutex_median << "x\n";
    }

    if (verification_failed) {
        std::cerr << "verification failed: accounting error or non-zero final "
                     "charge\n";
        return 1;
    }
    return 0;
}
