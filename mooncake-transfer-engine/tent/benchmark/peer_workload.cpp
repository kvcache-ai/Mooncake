// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Native arrival-driven host-memory benchmark. No synthetic NIC counters.
#include "peer_workload.h"
#include "tent/common/config.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/slice.h"
#include "tent/transport/rdma/workers.h"
#include <glog/logging.h>
#include <sys/resource.h>
#include <chrono>
#include <csignal>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>

using namespace mooncake::tent;
namespace w = mooncake::tent::workload;
// Existing test friends keep experiment-only read access out of the engine API.
namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::RdmaTransport* rdma(tent::TransferEngine& engine) {
        return dynamic_cast<tent::RdmaTransport*>(
            engine.impl_->transport_list_[tent::RDMA].get());
    }
};
namespace tent {
class RdmaTransportTestPeer {
   public:
    static DeviceSelector* selector(RdmaTransport* transport) {
        return transport && transport->workers_
                   ? transport->workers_->getDeviceSelector()
                   : nullptr;
    }
};
}  // namespace tent
}  // namespace mooncake

static w::Json peerCounters(DeviceSelector* selector) {
    w::Json out = w::Json::object();
    if (!selector || !selector->getSchedulingParams().peer_accounting)
        return out;
    const auto d = selector->getDecisionStats();
    out = {{"decisions", d.nominal + d.nic + d.peer},
           {"nominal", d.nominal},
           {"nic", d.nic},
           {"peer", d.peer},
           {"cold_fallback", d.cold_fallback},
           {"expired_fallback", d.expired_fallback},
           {"aggregate", d.aggregate},
           {"probe", d.probe},
           {"cold_entries", 0ULL},
           {"expired", 0ULL},
           {"relearned", 0ULL},
           {"reclaimed", 0ULL},
           {"accounting_errors", 0ULL},
           {"relearn_delay_ns", 0ULL}};
    for (size_t dev = 0; dev < selector->getTopology()->getNicCount(); ++dev) {
        const auto s = selector->getPeerLedger(dev).stats;
        for (const auto& [key, value] :
             std::vector<std::pair<std::string, uint64_t>>{
                 {"cold_entries", s.cold},
                 {"expired", s.expired},
                 {"relearned", s.relearned},
                 {"reclaimed", s.reclaimed},
                 {"accounting_errors", s.accounting_errors},
                 {"relearn_delay_ns", s.relearn_delay_ns}})
            out[key] = out[key].get<uint64_t>() + value;
    }
    return out;
}

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

static w::Json engineConfig(const w::Json& spec) {
    w::Json engine = spec.at("engine");
    const bool control = spec.value("control_tcp", false);
    const auto variant = spec.value("variant", std::string("v1"));
    if (variant != "v0" && variant != "v1" && variant != "v2" &&
        variant != "v3" && variant != "baseline")
        throw std::invalid_argument(
            "variant must be baseline, v0, v1, v2 or v3");
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
        rdma["workers"]["block_size"] = spec.value("block_bytes", 2 * w::MiB);
        rdma["peer_accounting"] = variant != "baseline";
        rdma["peer_scoring"] = variant == "baseline" ? "v1" : variant;
        rdma["peer_trace"] = spec.value("diagnostic", false);
        rdma["peer_feedback_ttl_ns"] =
            spec.at("peer_feedback_ttl_ns").get<uint64_t>();
    }
    // Existing named-policy -> device_mask path; same priority for both flows.
    engine["policy"] = w::Json::array();
    for (const auto* name : {"background", "target"}) {
        w::Json p = {{"name", name},
                     {"segment_type", "memory"},
                     {"transports", {control ? "tcp" : "rdma"}}};
        if (!control)
            p["devices"] = std::string(name) == "background" &&
                                   spec.value("local_contention", false)
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
    if (spec.value("diagnostic", false) &&
        std::max(spec.at("target").value("count", 100),
                 spec.at("background").value("count", 100)) >
            spec.value("slots", 32))
        throw std::invalid_argument(
            "short diagnostics require count <= slots for unique request keys");
    auto engine_json = engineConfig(spec);
    w::Json layout = {{"evidence", "plan_only"},
                      {"registered_bytes", bytes},
                      {"engine", engine_json},
                      {"events", w::Json::array()}};
    for (const auto& e : events)
        layout["events"].push_back(w::recordJson(w::Record{e}));
    if (!spec.value("control_tcp", false)) {
        // The same production WRITE planner, including its 32-slice limit.
        const auto p = planRdmaSlices(
            events.front().bytes, spec.value("block_bytes", 2 * w::MiB), 32);
        layout["write_planner"] = {
            {"block_bytes", p.block_size},
            {"slices", p.count},
            {"last_slice_bytes",
             events.front().bytes - (p.count - 1) * p.block_size},
            {"evidence", "production_planner_function"}};
    }
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
            std::memset(buffer + e.offset, 1 + (e.offset / w::MiB) % 251,
                        e.bytes);
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
    std::map<std::string, SegmentID> peers;
    std::map<std::string, uint64_t> remote;
    for (const char* group : {"target", "background"}) {
        require(engine.openSegment(
            peers[group], spec.at(group).at("peer").get<std::string>()));
        SegmentInfo info;
        require(engine.getSegmentInfo(peers[group], info));
        if (info.buffers.size() != 1 || info.buffers[0].length < bytes)
            throw std::runtime_error(
                "peer must expose one sufficiently large allocation");
        remote[group] = info.buffers[0].base;
    }
    if (peers["target"] == peers["background"])
        throw std::runtime_error("two distinct peer segments required");
    auto makeRequest = [&](const w::Event& e, Request::OpCode op) {
        Request r{};
        r.opcode = op;
        r.source = buffer + e.offset;
        r.target_id = peers.at(e.group);
        r.target_offset = remote.at(e.group) + e.offset;
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
    w::Event warm{events.size(), "target", 0, bytes - 64 * w::MiB,
                  events.front().bytes};
    for (int i = 0; i < spec.value("warmup", 2) * 2; ++i) {
        warm.group = i % 2 ? "background" : "target";
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
    }

    auto* selector = RdmaTransportTestPeer::selector(
        mooncake::TransferEngineImplTestPeer::rdma(engine));
    // Both snapshots are outside latency/CPU measurement and exclude readback.
    const auto counters_before = peerCounters(selector);
    const auto origin = nanos();
    const auto cpu_start = cpuSeconds();
    std::vector<BatchID> batches(events.size(), 0);
    auto now = [&] { return nanos() - origin; };
    std::map<uint64_t, int> slot_owner;
    w::Hooks hooks;
    hooks.ready = [&](const w::Record& r) {
        if (memory_quarantined) return false;
        auto it = slot_owner.find(r.event.offset);
        return it == slot_owner.end() || batches[it->second] == 0;
    };
    hooks.now = now;
    hooks.wait_until = [&](int64_t deadline) {
        std::this_thread::sleep_until(
            Clock::time_point(std::chrono::nanoseconds(origin + deadline)));
    };
    hooks.submit = [&](w::Record& r) {
        slot_owner[r.event.offset] = r.event.id;
        auto& batch = batches[r.event.id];
        batch = engine.allocateBatch(1);
        if (!batch) return std::string("allocateBatch failed");
        r.detail["peer"] = peers.at(r.event.group);
        r.detail["request_key"] =
            reinterpret_cast<uint64_t>(buffer + r.event.offset);
        r.detail["engine_submit_begin_ns"] = now();
        auto status = engine.submitTransfer(
            batch, {makeRequest(r.event, Request::WRITE)});
        r.detail["engine_submit_return_ns"] = now();
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
    auto counters = peerCounters(selector);
    for (auto& field : counters.items())
        field.value() = field.value().get<uint64_t>() -
                        counters_before.at(field.key()).get<uint64_t>();
    bool unfinished = memory_quarantined;
    for (const auto& r : records) unfinished |= r.unfinished;
    // Measure latency/goodput before readback, but count only fully verified
    // successful bytes with final-slot validation (not every overwritten
    // write).
    std::map<uint64_t, bool> verified_slots;
    if (!unfinished) {
        for (auto& r : records) {
            if (!r.success) continue;
            if (verified_slots.count(r.event.offset)) {
                r.detail["data_verified"] = verified_slots[r.event.offset];
                r.detail["verification_scope"] = "final_slot_contents";
                if (!verified_slots[r.event.offset]) {
                    r.success = false;
                    r.error = "readback_mismatch_or_failure";
                }
                continue;
            }
            std::memset(buffer + r.event.offset, 0, r.event.bytes);
            bool good =
                transferAndWait(r.event, Request::READ) &&
                std::all_of(buffer + r.event.offset,
                            buffer + r.event.offset + r.event.bytes,
                            [&](unsigned char c) {
                                return c == 1 + (r.event.offset / w::MiB) % 251;
                            });
            verified_slots[r.event.offset] = good;
            r.detail["data_verified"] = good;
            r.detail["verification_scope"] = "final_slot_contents";
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
    stats["registered_bytes"] = bytes;
    stats["measurement_origin_ns"] = origin;
    stats["slots"] = spec.value("slots", 32);
    stats["verification_scope"] =
        "final_slot_contents; completion status and byte count checked for "
        "every request";
    stats["cpu_seconds"] = cpu;
    stats["cpu_percent_one_core"] = elapsed > 0 ? cpu * 1e11 / elapsed : 0.0;
    stats["variant"] = spec.value("variant", std::string("v1"));
    const auto variant = spec.value("variant", std::string("v1"));
    stats["peer_accounting"] = {
        {"enabled", !control && variant != "baseline"},
        {"scope", "measured_write_requests; excludes warmup and readback"},
        {"queue_scope", variant == "v0"   ? "none"
                        : variant == "v3" ? "peer"
                                          : "nic"},
        {"feedback_scope", "actual source counts: nominal / nic / peer"},
        {"counters", counters}};
    stats["data_verification_complete"] = std::all_of(
        records.begin(), records.end(),
        [](const auto& r) { return r.detail.value("data_verified", false); });
    stats["memory_quarantined_after_failure"] = memory_quarantined;
    stats["verification_unfinished"] = auxiliary_unfinished;
    std::ofstream(output / "summary.json") << stats.dump(2) << '\n';
    std::cout << stats.dump(2) << std::endl;
    if (unfinished) std::_Exit(3);  // OS teardown, no premature MR/batch free
    for (const auto& [group, peer] : peers) require(engine.closeSegment(peer));
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
                std::cout << "tent_peer_workload --config workload.json "
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
