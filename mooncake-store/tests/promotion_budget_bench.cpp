#include "master_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "types.h"

namespace mooncake::test {
namespace {

using Clock = std::chrono::steady_clock;

struct BenchCase {
    std::string name;
    size_t task_count;
    int64_t max_age_ms;
};

struct RunConfig {
    int runs = 3;
    int heartbeat_ms = 1;
    int base_ms = 25;
    int per_ki_token_ms = 15;
    std::vector<int> tokens{1024, 3072};
    std::vector<int> max_per_heartbeat{1, 2};
    std::string out_csv;
};

struct MountedSegmentContext {
    UUID segment_id;
    UUID client_id;
    std::string segment_name;
};

struct MetricsSnapshot {
    int64_t admitted;
    int64_t completed;
    int64_t expired;
    int64_t failed;
    int64_t cancelled;
    int64_t in_flight;
    int64_t within_budget;
    int64_t late;
};

struct RunResult {
    std::string scheduler;
    std::string backlog;
    int max_per_heartbeat;
    int budget_ms;
    int run;
    size_t task_count;
    double deadline_met_ratio;
    int completed;
    int expired;
    int wasted_ssd_reads;
    int within_budget_counter;
    int late_counter;
    double avg_in_flight;
    double p95_wait_ms;
    bool invariant_ok;
};

std::vector<BenchCase> BenchCases() {
    return {{"light", 30, 30}, {"medium", 48, 90}, {"heavy", 72, 180}};
}

int HiCacheBudgetMs(int base_ms, int per_ki_token_ms, int tokens) {
    return static_cast<int>(
        std::llround(base_ms + per_ki_token_ms * tokens / 1024.0));
}

double Median(std::vector<double> values) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

double Percentile(std::vector<double> values, double pct) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    auto idx = static_cast<size_t>(
        std::llround((values.size() - 1) * std::clamp(pct, 0.0, 1.0)));
    return values[std::min(idx, values.size() - 1)];
}

std::string CsvEscape(const std::string& value) {
    if (value.find_first_of(",\"\n") == std::string::npos) return value;
    std::string out = "\"";
    for (char ch : value) {
        if (ch == '"') out += '"';
        out += ch;
    }
    out += '"';
    return out;
}

Segment MakeSegment(std::string name, size_t base, size_t size) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = base;
    segment.size = size;
    segment.te_endpoint = segment.name;
    return segment;
}

MountedSegmentContext PrepareSegment(MasterService& service, std::string name,
                                     size_t base, size_t size) {
    Segment segment = MakeSegment(std::move(name), base, size);
    UUID client_id = generate_uuid();
    auto mount_result = service.MountSegment(segment, client_id);
    CHECK(mount_result.has_value()) << "MountSegment failed";
    auto mount_ld = service.MountLocalDiskSegment(client_id, true);
    CHECK(mount_ld.has_value()) << "MountLocalDiskSegment failed";
    return {.segment_id = segment.id,
            .client_id = client_id,
            .segment_name = segment.name};
}

bool InjectLocalDiskReplica(MasterService& service, const UUID& client_id,
                            const std::string& key, int64_t size,
                            const std::string& transport_endpoint) {
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "default", .key = key, .size = size}};
    StorageObjectMetadata sm;
    sm.bucket_id = 0;
    sm.offset = 0;
    sm.key_size = static_cast<int64_t>(key.size());
    sm.data_size = size;
    sm.transport_endpoint = transport_endpoint;
    std::vector<StorageObjectMetadata> metas{sm};
    auto res = service.NotifyOffloadSuccess(client_id, tasks, metas);
    return res.has_value();
}

MetricsSnapshot SnapshotMetrics() {
    auto& mm = MasterMetricManager::instance();
    return {.admitted = mm.get_promotion_admitted(),
            .completed = mm.get_promotion_completed(),
            .expired = mm.get_promotion_expired(),
            .failed = mm.get_promotion_failed(),
            .cancelled = mm.get_promotion_cancelled(),
            .in_flight = mm.get_promotion_in_flight(),
            .within_budget = mm.get_promotion_within_budget(),
            .late = mm.get_promotion_late()};
}

int64_t Delta(int64_t after, int64_t before) { return after - before; }

RunConfig ParseArgs(int argc, char** argv) {
    RunConfig config;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto parse_int = [&](const std::string& prefix, int* dst) {
            if (arg.rfind(prefix, 0) == 0) {
                *dst = std::stoi(arg.substr(prefix.size()));
                return true;
            }
            return false;
        };
        if (parse_int("--runs=", &config.runs) ||
            parse_int("--heartbeat_ms=", &config.heartbeat_ms) ||
            parse_int("--base_ms=", &config.base_ms) ||
            parse_int("--per_ki_token_ms=", &config.per_ki_token_ms)) {
            continue;
        }
        if (arg.rfind("--out_csv=", 0) == 0) {
            config.out_csv = arg.substr(std::string("--out_csv=").size());
            continue;
        }
        LOG(FATAL) << "unknown argument: " << arg;
    }
    return config;
}

void WriteRows(std::ostream& out, const std::vector<RunResult>& rows) {
    out << "scheduler,backlog,max_per_heartbeat,budget_ms,run,task_count,"
           "deadline_met_ratio,completed,expired,wasted_ssd_reads,"
           "within_budget_counter,late_counter,avg_in_flight,p95_wait_ms,"
           "invariant_ok\n";
    out << std::fixed << std::setprecision(4);
    for (const auto& row : rows) {
        out << CsvEscape(row.scheduler) << ',' << CsvEscape(row.backlog) << ','
            << row.max_per_heartbeat << ',' << row.budget_ms << ',' << row.run
            << ',' << row.task_count << ',' << row.deadline_met_ratio << ','
            << row.completed << ',' << row.expired << ','
            << row.wasted_ssd_reads << ',' << row.within_budget_counter << ','
            << row.late_counter << ',' << row.avg_in_flight << ','
            << row.p95_wait_ms << ',' << (row.invariant_ok ? "true" : "false")
            << '\n';
    }
}

}  // namespace

class PromotionBudgetBench {
   public:
    static bool SetQueuedTime(MasterService* service, const UUID& client_id,
                              const std::string& key,
                              Clock::time_point queued_time) {
        auto local_disk_segment_access =
            service->segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto holder_it = client_local_disk_segment.find(client_id);
        if (holder_it == client_local_disk_segment.end()) return false;
        MutexLocker locker(&holder_it->second->offloading_mutex_);
        auto task_it = holder_it->second->promotion_objects.find(
            MakeTenantScopedStorageKey("default", key));
        if (task_it == holder_it->second->promotion_objects.end()) return false;
        task_it->second.queued_time = queued_time;
        return true;
    }
};

RunResult RunOne(const std::string& scheduler, const BenchCase& bench_case,
                 int max_per_heartbeat, int budget_ms, int run,
                 int heartbeat_ms) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_max_per_heartbeat = max_per_heartbeat;
    config.promotion_max_budget_ms = scheduler == "budget" ? budget_ms : 0;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t kSegmentBase = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 128;
    auto segment = PrepareSegment(*service, "bench_segment_" + scheduler,
                                  kSegmentBase, kSegmentSize);
    const auto metrics_before = SnapshotMetrics();
    const auto now = Clock::now();

    for (size_t i = 0; i < bench_case.task_count; ++i) {
        const std::string key = bench_case.name + "_" + scheduler + "_" +
                                std::to_string(run) + "_" + std::to_string(i);
        CHECK(InjectLocalDiskReplica(*service, segment.client_id, key, 1024,
                                     segment.segment_name));
        auto replica_list = service->GetReplicaList(key, "default");
        CHECK(replica_list.has_value()) << "GetReplicaList failed for " << key;
        const double rank =
            bench_case.task_count <= 1
                ? 0.0
                : static_cast<double>(i) / (bench_case.task_count - 1);
        const int64_t age_ms = static_cast<int64_t>(
            std::llround(rank * static_cast<double>(bench_case.max_age_ms)));
        CHECK(PromotionBudgetBench::SetQueuedTime(
            service.get(), segment.client_id, key,
            now - std::chrono::milliseconds(age_ms)));
    }

    std::vector<double> wait_ms;
    std::vector<double> in_flight_samples;
    int selected = 0;
    int deadline_met = 0;
    int wasted = 0;
    const int max_ticks = static_cast<int>(bench_case.task_count) + 256;
    for (int tick = 0; tick < max_ticks; ++tick) {
        const auto before_tick = SnapshotMetrics();
        in_flight_samples.push_back(static_cast<double>(std::max<int64_t>(
            0, before_tick.in_flight - metrics_before.in_flight)));

        auto heartbeat = service->PromotionObjectHeartbeat(segment.client_id);
        CHECK(heartbeat.has_value()) << "PromotionObjectHeartbeat failed";

        const auto heartbeat_now = Clock::now();
        for (const auto& task : *heartbeat) {
            ++selected;
            const auto deadline =
                task.queued_time + std::chrono::milliseconds(budget_ms);
            const auto wait =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    heartbeat_now - task.queued_time)
                    .count() /
                1000.0;
            wait_ms.push_back(wait);
            if (heartbeat_now <= deadline) {
                ++deadline_met;
            } else {
                ++wasted;
            }

            auto alloc = service->PromotionAllocStart(
                segment.client_id, task.key, task.tenant_id, task.size, {});
            CHECK(alloc.has_value()) << "PromotionAllocStart failed for "
                                     << task.key << " err=" << alloc.error();
            auto success = service->NotifyPromotionSuccess(
                segment.client_id, task.key, task.tenant_id);
            CHECK(success.has_value())
                << "NotifyPromotionSuccess failed for " << task.key
                << " err=" << success.error();
        }

        const auto metrics_now = SnapshotMetrics();
        const int expired = static_cast<int>(
            Delta(metrics_now.expired, metrics_before.expired));
        if (selected + expired >= static_cast<int>(bench_case.task_count)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_ms));
    }

    const auto metrics_after = SnapshotMetrics();
    const int completed = static_cast<int>(
        Delta(metrics_after.completed, metrics_before.completed));
    const int expired =
        static_cast<int>(Delta(metrics_after.expired, metrics_before.expired));
    const int failed =
        static_cast<int>(Delta(metrics_after.failed, metrics_before.failed));
    const int cancelled = static_cast<int>(
        Delta(metrics_after.cancelled, metrics_before.cancelled));
    const int in_flight = static_cast<int>(
        Delta(metrics_after.in_flight, metrics_before.in_flight));
    const int admitted = static_cast<int>(
        Delta(metrics_after.admitted, metrics_before.admitted));
    const bool invariant_ok =
        admitted == completed + failed + expired + cancelled + in_flight;

    service->RemoveAll();

    const double avg_in_flight =
        in_flight_samples.empty()
            ? 0.0
            : std::accumulate(in_flight_samples.begin(),
                              in_flight_samples.end(), 0.0) /
                  in_flight_samples.size();
    return RunResult{
        .scheduler = scheduler,
        .backlog = bench_case.name,
        .max_per_heartbeat = max_per_heartbeat,
        .budget_ms = budget_ms,
        .run = run,
        .task_count = bench_case.task_count,
        .deadline_met_ratio =
            static_cast<double>(deadline_met) / bench_case.task_count,
        .completed = completed,
        .expired = expired,
        .wasted_ssd_reads = wasted,
        .within_budget_counter = static_cast<int>(
            Delta(metrics_after.within_budget, metrics_before.within_budget)),
        .late_counter =
            static_cast<int>(Delta(metrics_after.late, metrics_before.late)),
        .avg_in_flight = avg_in_flight,
        .p95_wait_ms = Percentile(wait_ms, 0.95),
        .invariant_ok = invariant_ok};
}

std::vector<RunResult> AggregateMedian(const std::vector<RunResult>& runs) {
    std::map<std::tuple<std::string, std::string, int, int>,
             std::vector<RunResult>>
        grouped;
    for (const auto& run : runs) {
        grouped[{run.scheduler, run.backlog, run.max_per_heartbeat,
                 run.budget_ms}]
            .push_back(run);
    }

    std::vector<RunResult> medians;
    for (const auto& [key, group] : grouped) {
        auto [scheduler, backlog, max_per_heartbeat, budget_ms] = key;
        auto median_metric = [&](auto getter) {
            std::vector<double> values;
            values.reserve(group.size());
            for (const auto& row : group) values.push_back(getter(row));
            return Median(values);
        };
        medians.push_back(RunResult{
            .scheduler = scheduler,
            .backlog = backlog,
            .max_per_heartbeat = max_per_heartbeat,
            .budget_ms = budget_ms,
            .run = -1,
            .task_count = group.front().task_count,
            .deadline_met_ratio = median_metric(
                [](const auto& row) { return row.deadline_met_ratio; }),
            .completed = static_cast<int>(
                median_metric([](const auto& row) { return row.completed; })),
            .expired = static_cast<int>(
                median_metric([](const auto& row) { return row.expired; })),
            .wasted_ssd_reads = static_cast<int>(median_metric(
                [](const auto& row) { return row.wasted_ssd_reads; })),
            .within_budget_counter = static_cast<int>(median_metric(
                [](const auto& row) { return row.within_budget_counter; })),
            .late_counter = static_cast<int>(median_metric(
                [](const auto& row) { return row.late_counter; })),
            .avg_in_flight = median_metric(
                [](const auto& row) { return row.avg_in_flight; }),
            .p95_wait_ms =
                median_metric([](const auto& row) { return row.p95_wait_ms; }),
            .invariant_ok =
                std::all_of(group.begin(), group.end(),
                            [](const auto& row) { return row.invariant_ok; })});
    }
    return medians;
}

int Main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    auto config = ParseArgs(argc, argv);

    std::vector<int> budgets;
    for (int tokens : config.tokens) {
        budgets.push_back(
            HiCacheBudgetMs(config.base_ms, config.per_ki_token_ms, tokens));
    }

    std::vector<RunResult> raw_runs;
    for (const auto& bench_case : BenchCases()) {
        for (int max_per : config.max_per_heartbeat) {
            for (int budget_ms : budgets) {
                for (int run = 0; run < config.runs; ++run) {
                    raw_runs.push_back(RunOne("fifo", bench_case, max_per,
                                              budget_ms, run,
                                              config.heartbeat_ms));
                    raw_runs.push_back(RunOne("budget", bench_case, max_per,
                                              budget_ms, run,
                                              config.heartbeat_ms));
                }
            }
        }
    }

    const auto medians = AggregateMedian(raw_runs);
    WriteRows(std::cout, medians);
    if (!config.out_csv.empty()) {
        std::filesystem::path out_path(config.out_csv);
        std::filesystem::create_directories(out_path.parent_path());
        std::ofstream out(out_path);
        WriteRows(out, medians);
        std::cerr << "csv=" << config.out_csv << "\n";
    }

    google::ShutdownGoogleLogging();
    return 0;
}

}  // namespace mooncake::test

int main(int argc, char** argv) { return mooncake::test::Main(argc, argv); }
