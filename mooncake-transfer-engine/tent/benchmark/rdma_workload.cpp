// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Native arrival-driven host-memory benchmark. No synthetic NIC counters.
#include "rdma_workload.h"
#include "tent/common/config.h"
#include "tent/runtime/topology.h"
#include "tent/transfer_engine.h"
#include <glog/logging.h>
#include <sys/resource.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

using namespace mooncake::tent;
namespace w = mooncake::tent::workload;
using Clock = std::chrono::steady_clock;
static int64_t nanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch())
        .count();
}
static void require(Status status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
static volatile std::sig_atomic_t stopped = 0;
static void stop(int) { stopped = 1; }

// The selector emits one record per sampled allocation, never per slice.
// Source addresses are unique registered slots, so delayed dispatch is also
// attributable without adding a production request ID or changing wire data.
class TraceSink : public google::LogSink {
   public:
    std::mutex mutex;
    std::vector<w::Json> traces;
    void send(google::LogSeverity, const char*, const char*, int,
              const std::tm*, const char* message, size_t length) override {
        std::string text(message, length);
        if (text.find("RDMA_BATCH ") != 0) return;
        auto item = w::parseTrace(text, nanos());
        std::lock_guard<std::mutex> lock(mutex);
        traces.push_back(std::move(item));
    }
};

static w::Json engineConfig(const w::Json& spec) {
    w::Json engine = spec.at("engine");
    const bool control = spec.value("control_tcp", false);
    const auto policy = spec.value("policy", std::string("inverse_score"));
    if (policy != "inverse_score" && policy != "virtual_load" &&
        policy != "static_capacity")
        throw std::invalid_argument("unknown batch policy");
    for (const auto* name :
         {"rdma", "tcp", "shm", "nvlink", "mnnvl", "gds", "io_uring"})
        engine["transports"][name]["enable"] = false;
    engine["transports"][control ? "tcp" : "rdma"]["enable"] = true;
    auto rails = spec.value("rails", std::vector<std::string>{});
    auto busy = spec.value("busy_rail", 0);
    if (!control &&
        (rails.size() != 2 || rails[0] == rails[1] || busy < 0 || busy > 1))
        throw std::invalid_argument(
            "two distinct rails and busy_rail=0 or 1 required");
    if (!control) {
        engine["topology"]["rdma_whitelist"] = rails;
        auto& rdma = engine["transports"]["rdma"];
        rdma["enable_smart_scheduling"] = true;
        rdma["workers"]["block_size"] = 2 * w::MiB;
        rdma["batch_allocation_policy"] = policy;
        rdma["batch_trace_interval"] = spec.value("diagnostic", false) ? 1 : 0;
        if (policy == "static_capacity") {
            if (!spec.contains("capacity_gbps"))
                throw std::invalid_argument(
                    "static_capacity requires frozen capacity_gbps by NIC ID");
            rdma["batch_capacity_gbps"] = spec.at("capacity_gbps");
        }
    }
    // Existing named-policy -> device_mask path; same priority for both flows.
    engine["policy"] = w::Json::array();
    for (const auto* name : {"background", "target"}) {
        w::Json p = {{"name", name},
                     {"segment_type", "memory"},
                     {"transports", {control ? "tcp" : "rdma"}}};
        if (!control)
            p["devices"] = std::string(name) == "background"
                               ? std::vector<std::string>{rails[busy]}
                               : rails;
        engine["policy"].push_back(p);
    }
    return engine;
}

static double cpuSeconds() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_utime.tv_sec + usage.ru_stime.tv_sec +
           (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}

static int execute(const w::Json& spec, bool dry) {
    const auto role = spec.at("role").get<std::string>();
    if (role != "server" && role != "client")
        throw std::invalid_argument("role must be server/client");
    const auto events = w::plan(spec);
    const auto bytes = w::memoryBytes(events);
    if (bytes > spec.value("max_memory_mib", uint64_t{4096}) * w::MiB)
        throw std::invalid_argument(
            "plan exceeds max_memory_mib; inspect dry-run layout");
    const auto timeout = spec.value("timeout_ms", int64_t{10000}) * 1000000;
    const auto drain = spec.value("drain_ms", int64_t{5000}) * 1000000;
    if (timeout <= 0 || drain <= 0)
        throw std::invalid_argument("positive timeout/drain required");
    auto engine_json = engineConfig(spec);
    w::Json layout = {{"evidence", "plan_only"},
                      {"registered_bytes", bytes},
                      {"engine", engine_json},
                      {"events", w::Json::array()}};
    for (const auto& e : events)
        layout["events"].push_back(w::recordJson(w::Record{e}));
    if (dry) {
        std::cout << layout.dump(2) << std::endl;
        return 0;
    }
    const auto output =
        std::filesystem::path(spec.at("output").get<std::string>());
    if (!std::filesystem::create_directory(output))
        throw std::runtime_error("output directory must be new: " +
                                 output.string());
    std::ofstream(output / "config.json") << spec.dump(2) << '\n';
    std::ofstream(output / "layout.json") << layout.dump(2) << '\n';
    auto config = std::make_shared<Config>();
    require(config->load(engine_json.dump()));
    // This executable's explicit spec owns the experiment. The general runner
    // also exports MC_TENT_CONF for tebench; it must not erase named policies
    // or diagnostic settings here when the engine reloads environment defaults.
    unsetenv("MC_TENT_CONF");
    TransferEngine engine(config);
    if (!engine.available()) throw std::runtime_error("engine unavailable");
    std::ofstream(output / "engine-effective.json") << config->dump() << '\n';
    std::ofstream(output / "topology.json")
        << engine.getLocalTopologyString() << '\n';
    const bool control = spec.value("control_tcp", false);
    const bool diagnostic = spec.value("diagnostic", false) && !control;
    if (!control) {
        auto topology = engine.getLocalTopology();
        std::vector<NicLoadStats> usable;
        require(engine.getNicLoadStats(usable));
        for (const auto& rail : spec.at("rails")) {
            auto name = rail.get<std::string>();
            if (!topology || topology->getNicId(name) < 0 ||
                topology->getNicId(name) >= 64 ||
                std::none_of(usable.begin(), usable.end(), [&](const auto& s) {
                    return s.device_name == name;
                }))
                throw std::runtime_error("requested rail is not usable: " +
                                         name);
        }
    }
    void* memory = nullptr;
    const auto location = "cpu:" + std::to_string(spec.value("numa", 0));
    require(engine.allocateLocalMemory(&memory, bytes, location));
    MemoryOptions options;
    options.location = location;
    options.type = control ? TCP : RDMA;
    require(engine.registerLocalMemory(memory, bytes, options));
    auto* buffer = static_cast<unsigned char*>(memory);
    // Initialization is outside the measured interval. Different request slots
    // carry different patterns; all bytes are checked after successful writes.
    std::memset(buffer, 0, bytes);
    for (const auto& e : events)
        if (role == "client")
            std::memset(buffer + e.offset, 1 + e.id % 251, e.bytes);
    if (role == "server") {
        std::ofstream(output / "ready.json")
            << w::Json({{"segment", engine.getSegmentName()}, {"bytes", bytes}})
                   .dump()
            << '\n';
        std::cout << "READY " << engine.getSegmentName() << std::endl;
        std::signal(SIGTERM, stop);
        std::signal(SIGINT, stop);
        while (!stopped)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        require(engine.unregisterLocalMemory(memory, bytes));
        require(engine.freeLocalMemory(memory));
        return 0;
    }
    SegmentID peer;
    require(engine.openSegment(peer, spec.at("peer").get<std::string>()));
    SegmentInfo info;
    require(engine.getSegmentInfo(peer, info));
    // One registered allocation per endpoint, internally partitioned into
    // disjoint slots. Reject ambiguous server layouts instead of guessing.
    if (info.buffers.size() != 1 || info.buffers[0].length < bytes)
        throw std::runtime_error(
            "server must expose one sufficiently large memory allocation");
    auto remote = info.buffers[0].base;
    auto makeRequest = [&](const w::Event& e, Request::OpCode op) {
        Request r{};
        r.opcode = op;
        r.source = buffer + e.offset;
        r.target_id = peer;
        r.target_offset = remote + e.offset;
        r.length = e.bytes;
        r.policy_name = e.group;
        r.transport_hint = control ? TCP : RDMA;
        return r;
    };
    // Warm the actual endpoints and normal feedback before the arrival epoch.
    // Also used for readback AFTER the measurement; no overlap with live
    // writes.
    bool auxiliary_unfinished = false;
    bool memory_quarantined = false;
    auto transferAndWait = [&](const w::Event& e, Request::OpCode op) {
        auto batch = engine.allocateBatch(1);
        if (!batch) return false;
        auto accepted = engine.submitTransfer(batch, {makeRequest(e, op)});
        auto start = nanos();
        bool canceled = false;
        for (;;) {
            TransferStatus status{};
            auto observed = engine.getTransferStatus(batch, status);
            if (observed.ok() && status.s != INITIAL && status.s != PENDING &&
                engine.freeBatch(batch).ok()) {
                memory_quarantined |= status.s != COMPLETED;
                return accepted.ok() && !canceled && status.s == COMPLETED &&
                       status.transferred_bytes == e.bytes;
            }
            if (nanos() - start > timeout && !canceled) {
                engine.cancelTransfer(batch, 0);
                canceled = true;
            }
            if (nanos() - start > timeout + drain) {
                auxiliary_unfinished = true;
                return false;  // Caller writes failure evidence before exit.
            }
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    };
    w::Event warm{events.size(), "target", 0, bytes - 64 * w::MiB, 64 * w::MiB};
    for (int i = 0; i < spec.value("warmup", 2); ++i)
        if (!transferAndWait(warm, Request::WRITE)) {
            if (auxiliary_unfinished || memory_quarantined) {
                std::ofstream(output / "setup-failure.json")
                    << w::Json({{"warmup_failed", true},
                                {"unfinished", auxiliary_unfinished},
                                {"memory_quarantined", memory_quarantined}})
                           .dump()
                    << '\n';
                std::_Exit(3);
            }
            throw std::runtime_error("warmup failed");
        }

    TraceSink sink;
    if (diagnostic) google::AddLogSink(&sink);
    const auto origin = nanos();
    const auto cpu_start = cpuSeconds();
    std::vector<BatchID> batches(events.size(), 0);
    std::vector<int64_t> last_pending(events.size(), -1);
    auto now = [&] { return nanos() - origin; };
    auto pending = [&](int id) {
        if (id < 0 || !batches[id]) return false;
        TransferStatus status{};
        auto ok = engine.getTransferStatus(batches[id], status);
        bool live = ok.ok() && (status.s == INITIAL || status.s == PENDING);
        if (live) last_pending[id] = now();
        return live;
    };
    w::Hooks hooks;
    hooks.now = now;
    hooks.wait_until = [&](int64_t deadline) {
        std::this_thread::sleep_until(
            Clock::time_point(std::chrono::nanoseconds(origin + deadline)));
    };
    hooks.submit = [&](w::Record& r) {
        if (r.event.background >= 0)
            r.detail["background_pending_before_submit"] =
                pending(r.event.background);
        auto& batch = batches[r.event.id];
        batch = engine.allocateBatch(1);
        if (!batch) return std::string("allocateBatch failed");
        r.detail["engine_submit_begin_ns"] = now();
        auto status = engine.submitTransfer(
            batch, {makeRequest(r.event, Request::WRITE)});
        r.detail["engine_submit_return_ns"] = now();
        if (r.event.background >= 0)
            r.detail["background_pending_after_submit"] =
                pending(r.event.background);
        return status.ok() ? std::string{} : status.ToString();
    };
    hooks.poll = [&](w::Record& r) -> w::Completion {
        auto& batch = batches[r.event.id];
        if (!batch) return {true, false, 0, "no batch"};
        TransferStatus status{};
        auto result = engine.getTransferStatus(batch, status);
        const auto visible = now();
        if (!result.ok()) {
            // A status API failure does not prove DMA stopped. Preserve the
            // batch and region, cancel/drain through the normal deadline path.
            r.error = result.ToString();
            return {};
        }
        if (status.s == INITIAL || status.s == PENDING) {
            last_pending[r.event.id] = now();
            return {};
        }
        r.detail["transport_status"] = int(status.s);
        // A failed terminal task can still have outstanding hardware WRs.
        // freeBatch preserves upstream orphan ownership, but does not promise
        // that its registered source/target memory is safe to reuse.
        memory_quarantined |= status.s != COMPLETED;
        auto freed = engine.freeBatch(batch);
        if (!freed.ok()) {
            r.error = freed.ToString();
            return {};
        }
        batch = 0;
        bool good =
            status.s == COMPLETED && status.transferred_bytes == r.event.bytes;
        return {true, good, status.transferred_bytes,
                good ? "" : "transfer_failed_or_short", visible};
    };
    hooks.cancel = [&](w::Record& r) {
        if (batches[r.event.id]) {
            auto status = engine.cancelTransfer(batches[r.event.id], 0);
            r.detail["cancel_result"] = status.ToString();
        }
    };
    auto records = w::run(events, timeout, drain, hooks);
    const auto elapsed = now();
    const auto cpu = cpuSeconds() - cpu_start;
    if (diagnostic) google::RemoveLogSink(&sink);
    bool unfinished = memory_quarantined;
    for (auto& r : records) {
        unfinished = unfinished || r.unfinished;
        w::Json traces = w::Json::array();
        for (auto trace : sink.traces) {
            if (trace.value("trace_id", uint64_t{0}) !=
                reinterpret_cast<uintptr_t>(buffer + r.event.offset))
                continue;
            trace["observed_ns"] =
                trace.at("observed_steady_ns").get<int64_t>() - origin;
            traces.push_back(trace);
        }
        r.detail["allocations"] = traces;
        if (r.event.background < 0) continue;
        std::string validity =
            control ? "control_only" : "unverified_trace_disabled";
        if (diagnostic) {
            validity = "expected_backlog_not_formed";
            auto busy =
                spec.at("rails")[spec.value("busy_rail", 0)].get<std::string>();
            if (w::validBacklog(traces, busy, r.event.bytes,
                                last_pending[r.event.background]))
                validity = "confirmed_live_asymmetric_backlog";
        }
        r.detail["backlog_validity"] = validity;
        r.detail["background_last_pending_ns"] =
            last_pending[r.event.background];
    }
    // Measure latency/goodput before readback, but count only fully verified
    // successful bytes. Never reuse memory while any request remains live.
    if (!unfinished) {
        for (auto& r : records) {
            if (!r.success) continue;
            std::memset(buffer + r.event.offset, 0, r.event.bytes);
            bool good = transferAndWait(r.event, Request::READ) &&
                        std::all_of(buffer + r.event.offset,
                                    buffer + r.event.offset + r.event.bytes,
                                    [&](unsigned char c) {
                                        return c == 1 + r.event.id % 251;
                                    });
            r.detail["data_verified"] = good;
            if (!good) {
                r.success = false;
                r.error = "readback_mismatch_or_failure";
            }
            if (auxiliary_unfinished || memory_quarantined) {
                unfinished = true;
                break;
            }
        }
    }
    if (unfinished) {
        for (auto& r : records) {
            if (r.detail.value("data_verified", false)) continue;
            r.detail["data_verified"] = false;
            if (r.success) {
                r.success = false;
                r.error = "verification_skipped_due_to_unsettled_work";
            }
        }
    }
    std::ofstream requests(output / "requests.jsonl");
    for (const auto& r : records) requests << w::recordJson(r).dump() << '\n';
    requests.close();
    auto stats = w::summary(records, elapsed);
    stats["evidence"] = control      ? "tcp_control_only"
                        : diagnostic ? "rdma_diagnostic"
                                     : "rdma_performance";
    stats["cpu_seconds"] = cpu;
    stats["cpu_percent_one_core"] = elapsed > 0 ? cpu * 1e11 / elapsed : 0.0;
    stats["policy"] = spec.value("policy", std::string("inverse_score"));
    stats["data_verification_complete"] = std::all_of(
        records.begin(), records.end(),
        [](const auto& r) { return r.detail.value("data_verified", false); });
    stats["memory_quarantined_after_failure"] = memory_quarantined;
    stats["verification_unfinished"] = auxiliary_unfinished;
    std::ofstream(output / "summary.json") << stats.dump(2) << '\n';
    std::cout << stats.dump(2) << std::endl;
    if (unfinished) std::_Exit(3);  // OS teardown, no premature MR/batch free
    require(engine.closeSegment(peer));
    require(engine.unregisterLocalMemory(memory, bytes));
    require(engine.freeLocalMemory(memory));
    return std::all_of(records.begin(), records.end(),
                       [](const auto& r) { return r.success; })
               ? 0
               : 2;
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        std::string path;
        bool dry = false;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--config" && i + 1 < argc)
                path = argv[++i];
            else if (arg == "--dry-run")
                dry = true;
            else if (arg == "--help") {
                std::cout << "tent_rdma_workload --config workload.json "
                             "[--dry-run]\n";
                return 0;
            } else
                throw std::invalid_argument("unknown/incomplete argument: " +
                                            arg);
        }
        if (path.empty()) throw std::invalid_argument("--config is required");
        std::ifstream input(path);
        w::Json spec;
        input >> spec;
        return execute(spec, dry);
    } catch (const std::exception& e) {
        std::cerr << "workload error: " << e.what() << std::endl;
        return 1;
    }
}
