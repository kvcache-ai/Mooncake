// Copyright 2026 Alibaba Cloud and its affiliates
// Licensed under the Apache License, Version 2.0.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <istream>
#include <latch>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "master_client.h"

DEFINE_string(trace, "", "Master RPC JSONL trace to replay");
DEFINE_string(prefill_trace, "",
              "Optional initialization trace, outside measurement");
DEFINE_string(master_server, "127.0.0.1:50051",
              "Dedicated benchmark master address");
DEFINE_string(tenant, "default", "Tenant used for all trace clients");
DEFINE_uint32(workers, 8,
              "Maximum concurrent trace calls (not simulated clients)");
DEFINE_uint32(heartbeat_interval_ms, 1000,
              "Pause between heartbeat sweeps in each worker partition");
DEFINE_uint32(heartbeat_workers, 16,
              "Concurrent heartbeat workers, separate from replay workers");
DEFINE_double(speed, 1.0,
              "Arrival-time speedup; 1 preserves logical intervals");
DEFINE_uint64(segment_size, 64ULL * 1024 * 1024,
              "Fake segment capacity per logical client, in bytes");
DEFINE_string(output, "master_trace_result.json",
              "Summary output, written after replay");
DEFINE_string(samples, "master_trace_samples.jsonl",
              "Per-event timings, written after replay");
DEFINE_bool(validate_only, false,
            "Validate traces and exit without contacting master");

namespace {

// A trace contains client API calls, not captured wire packets or KV payloads.
struct TraceEvent {
    std::string id;
    std::string client_id;
    std::string op;
    std::string phase = "workload";
    std::string segment_id;
    uint64_t size_bytes = 0;
    uint64_t timestamp_us = 0;
    std::vector<std::string> keys;
    std::vector<uint64_t> value_sizes;
    std::vector<std::vector<uint64_t>> value_slices;
    uint64_t replica_num = 1;
    std::vector<size_t> dependencies;
    std::optional<size_t> put_start;
};

struct RpcTrace {
    unsigned version = 1;
    Json::Value metadata;
    std::vector<TraceEvent> events;
};

enum class KeyStatus { OK, MISS, NOT_READY, ALREADY_EXISTS, ERROR, SKIPPED };

struct RpcOutcome {
    // One status per ORIGINAL key, including keys skipped after a failed put.
    std::vector<KeyStatus> keys;
    bool rpc_sent = false;
    std::string error;
};

struct TraceSample {
    int64_t replay_origin_monotonic_us = 0;
    int64_t phase_origin_us = 0;
    int64_t scheduled_us = 0;
    int64_t start_us = 0;
    int64_t finish_us = 0;
    RpcOutcome outcome;
};

// The second argument is the completed BatchPutStart result for End/Revoke.
// Calls for the same logical client may execute concurrently.
using TraceExecutor =
    std::function<RpcOutcome(const TraceEvent&, const RpcOutcome* put_start)>;

void Require(bool condition, const std::string& message) {
    if (!condition) throw std::invalid_argument(message);
}

bool IsUInt64(const Json::Value& value) {
    return (value.type() == Json::intValue ||
            value.type() == Json::uintValue) &&
           value.isUInt64();
}

std::string StringField(const Json::Value& value, const char* field) {
    Require(value[field].isString() && !value[field].asString().empty(),
            std::string(field) + " must be a nonempty string");
    return value[field].asString();
}

void CheckFields(const Json::Value& value,
                 const std::set<std::string>& allowed) {
    Require(value.isObject(), "each row must be a JSON object");
    for (const auto& name : value.getMemberNames()) {
        Require(allowed.count(name), "unknown field: " + name);
    }
}

Json::Value ParseLine(const std::string& line) {
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    builder["allowComments"] = false;
    builder["failIfExtra"] = true;
    builder["rejectDupKeys"] = true;
    Json::Value value;
    std::string error;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    const bool parsed =
        reader->parse(line.data(), line.data() + line.size(), &value, &error);
    Require(parsed, "invalid JSON: " + error);
    return value;
}

Json::Value Percentiles(std::vector<int64_t> values) {
    Json::Value result(Json::objectValue);
    if (values.empty()) return result;
    std::sort(values.begin(), values.end());
    for (const auto& [name, quantile] :
         std::vector<std::pair<const char*, double>>{
             {"p50", .50}, {"p95", .95}, {"p99", .99}, {"max", 1.0}}) {
        const auto index = static_cast<size_t>(std::ceil(
                               quantile * static_cast<double>(values.size()))) -
                           1;
        result[name] = Json::Int64(values[index]);
    }
    return result;
}

Json::Value StatusCounts(const RpcOutcome& outcome) {
    Json::Value counts(Json::objectValue);
    for (const auto* name :
         {"ok", "miss", "not_ready", "already_exists", "error", "skipped"}) {
        counts[name] = Json::UInt64(0);
    }
    for (auto status : outcome.keys) {
        const auto* name = "error";
        switch (status) {
            case KeyStatus::OK:
                name = "ok";
                break;
            case KeyStatus::MISS:
                name = "miss";
                break;
            case KeyStatus::NOT_READY:
                name = "not_ready";
                break;
            case KeyStatus::ALREADY_EXISTS:
                name = "already_exists";
                break;
            case KeyStatus::ERROR:
                name = "error";
                break;
            case KeyStatus::SKIPPED:
                name = "skipped";
                break;
        }
        counts[name] = Json::UInt64(counts[name].asUInt64() + 1);
    }
    return counts;
}

// Validation happens before connecting to a master or starting measurement.
// Dependencies must reference earlier rows. Timestamps must be nondecreasing.
RpcTrace ReadTrace(std::istream& input) {
    RpcTrace trace;
    std::unordered_map<std::string, size_t> ids;
    std::set<size_t> finalized;
    std::unordered_map<std::string, size_t> registered;
    std::unordered_map<std::string, size_t> mounted;
    std::set<std::string> unmounted;
    const std::map<std::string, int> phases = {
        {"setup", 0}, {"workload", 1}, {"teardown", 2}};
    std::string line;
    size_t line_number = 0;
    bool header_seen = false;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.find_first_not_of(" \t\r") == std::string::npos) continue;
        try {
            const auto row = ParseLine(line);
            if (!header_seen) {
                CheckFields(row, {"type", "version", "time_unit", "metadata"});
                Require(
                    row["type"] == "master_rpc_trace" &&
                        IsUInt64(row["version"]) &&
                        (row["version"].asUInt64() == 1 ||
                         row["version"].asUInt64() == 2) &&
                        row["time_unit"] == "us",
                    "expected master_rpc_trace v1/v2 header with time_unit=us");
                Require(!row.isMember("metadata") || row["metadata"].isObject(),
                        "metadata must be an object");
                trace.metadata = row["metadata"];
                trace.version = row["version"].asUInt();
                header_seen = true;
                continue;
            }
            CheckFields(row, {"id", "timestamp_us", "client_id", "op", "keys",
                              "value_sizes", "value_slices", "replica_num",
                              "depends_on", "put_start", "phase", "segment_id",
                              "size_bytes", "segments"});
            TraceEvent event;
            event.id = StringField(row, "id");
            Require(!ids.count(event.id), "duplicate event id: " + event.id);
            event.client_id = StringField(row, "client_id");
            event.op = StringField(row, "op");
            if (trace.version == 2) {
                event.phase = StringField(row, "phase");
                Require(phases.count(event.phase), "invalid phase");
                Require(trace.events.empty() ||
                            phases.at(event.phase) >=
                                phases.at(trace.events.back().phase),
                        "phases must be setup, workload, teardown in order");
            } else {
                Require(!row.isMember("phase"), "phase requires v2");
            }
            const std::set<std::string> operations = {
                "BatchExistKey",  "BatchGetReplicaList", "BatchPutStart",
                "BatchPutEnd",    "BatchPutRevoke",      "BatchRemove",
                "ReMountSegment", "MountSegment",        "UnmountSegment"};
            Require(operations.count(event.op), "unsupported op: " + event.op);
            const bool lifecycle = event.op == "ReMountSegment" ||
                                   event.op == "MountSegment" ||
                                   event.op == "UnmountSegment";
            Require(!lifecycle || trace.version == 2,
                    "lifecycle operations require v2");
            Require(IsUInt64(row["timestamp_us"]),
                    "timestamp_us must be a nonnegative integer");
            event.timestamp_us = row["timestamp_us"].asUInt64();
            Require(trace.events.empty() ||
                        event.phase != trace.events.back().phase ||
                        event.timestamp_us >= trace.events.back().timestamp_us,
                    "timestamps must be nondecreasing within a phase");
            Require(lifecycle ? !row.isMember("keys")
                              : (row["keys"].isArray() && !row["keys"].empty()),
                    "keys must be a nonempty array");
            for (const auto& key : row["keys"]) {
                Require(key.isString() && !key.asString().empty(),
                        "keys must contain nonempty strings");
                event.keys.push_back(key.asString());
            }
            if (row.isMember("depends_on")) {
                Require(row["depends_on"].isArray(),
                        "depends_on must be an array");
                for (const auto& dependency : row["depends_on"]) {
                    Require(dependency.isString() &&
                                ids.count(dependency.asString()),
                            "dependencies must reference earlier event ids");
                    event.dependencies.push_back(ids.at(dependency.asString()));
                }
            }
            if (event.op == "ReMountSegment") {
                Require(event.phase == "setup" &&
                            !registered.count(event.client_id),
                        "each client must register once during setup");
                Require(
                    row["segments"].isArray() && row["segments"].empty(),
                    "ReMountSegment requires segments=[] (initial handshake)");
                registered[event.client_id] = trace.events.size();
            } else if (trace.version == 2) {
                Require(registered.count(event.client_id),
                        "client must register before use");
                event.dependencies.push_back(registered.at(event.client_id));
            }
            if (event.op == "MountSegment") {
                Require(event.phase == "setup",
                        "MountSegment is supported in setup only");
                event.segment_id = StringField(row, "segment_id");
                Require(!mounted.count(event.segment_id),
                        "duplicate segment id");
                Require(IsUInt64(row["size_bytes"]) &&
                            row["size_bytes"].asUInt64() > 0,
                        "size_bytes must be positive");
                event.size_bytes = row["size_bytes"].asUInt64();
                mounted[event.segment_id] = trace.events.size();
            } else if (event.op == "UnmountSegment") {
                Require(event.phase == "teardown",
                        "UnmountSegment is supported in teardown only");
                event.segment_id = StringField(row, "segment_id");
                Require(mounted.count(event.segment_id) &&
                            unmounted.insert(event.segment_id).second,
                        "segment must be mounted and unmounted exactly once");
                const auto mount = mounted.at(event.segment_id);
                Require(trace.events[mount].client_id == event.client_id,
                        "unmount must use segment owner");
                event.dependencies.push_back(mount);
            } else {
                Require(!row.isMember("segment_id"),
                        "segment_id is only valid for mount/unmount");
            }
            Require(event.op == "MountSegment" || !row.isMember("size_bytes"),
                    "size_bytes is only valid for mount");
            Require(event.op == "ReMountSegment" || !row.isMember("segments"),
                    "segments is only valid for remount");
            Require(lifecycle || event.phase == "workload",
                    "key operations must be in workload");
            if (event.op == "BatchPutStart") {
                Require(
                    row.isMember("value_sizes") != row.isMember("value_slices"),
                    "provide exactly one of value_sizes or value_slices");
                if (row.isMember("value_slices")) {
                    Require(row["value_slices"].isArray() &&
                                row["value_slices"].size() == event.keys.size(),
                            "value_slices must match keys for BatchPutStart");
                    for (const auto& value : row["value_slices"]) {
                        Require(value.isArray() && !value.empty(),
                                "each value must have at least one slice");
                        std::vector<uint64_t> slices;
                        uint64_t total = 0;
                        for (const auto& size : value) {
                            Require(IsUInt64(size) && size.asUInt64() > 0,
                                    "slice lengths must be positive integers");
                            Require(size.asUInt64() <=
                                        std::numeric_limits<uint64_t>::max() -
                                            total,
                                    "value slice total overflows uint64");
                            total += size.asUInt64();
                            slices.push_back(size.asUInt64());
                        }
                        event.value_slices.push_back(std::move(slices));
                    }
                } else {
                    Require(row["value_sizes"].isArray() &&
                                row["value_sizes"].size() == event.keys.size(),
                            "value_sizes must match keys for BatchPutStart");
                    for (const auto& size : row["value_sizes"]) {
                        Require(IsUInt64(size) && size.asUInt64() > 0,
                                "value sizes must be positive integers");
                        event.value_sizes.push_back(size.asUInt64());
                    }
                }
                Require(
                    std::set<std::string>(event.keys.begin(), event.keys.end())
                            .size() == event.keys.size(),
                    "duplicate write keys are not supported");
                if (row.isMember("replica_num")) {
                    Require(IsUInt64(row["replica_num"]) &&
                                row["replica_num"].asUInt64() > 0,
                            "replica_num must be a positive integer");
                    event.replica_num = row["replica_num"].asUInt64();
                }
            } else {
                Require(!row.isMember("value_sizes") &&
                            !row.isMember("value_slices") &&
                            !row.isMember("replica_num"),
                        "value_sizes/value_slices/replica_num are only valid "
                        "for BatchPutStart");
            }
            if (event.op == "BatchPutEnd" || event.op == "BatchPutRevoke") {
                const auto start_id = StringField(row, "put_start");
                Require(ids.count(start_id),
                        "put_start must reference an earlier id");
                const auto start_index = ids.at(start_id);
                const auto& start = trace.events[start_index];
                Require(
                    start.op == "BatchPutStart" &&
                        start.client_id == event.client_id &&
                        start.keys == event.keys,
                    "put_start must match operation, client and ordered keys");
                Require(finalized.insert(start_index).second,
                        "a put_start may have only one End or Revoke");
                event.put_start = start_index;
                event.dependencies.push_back(start_index);
            } else {
                Require(!row.isMember("put_start"),
                        "put_start is only valid for End/Revoke");
            }
            std::sort(event.dependencies.begin(), event.dependencies.end());
            event.dependencies.erase(std::unique(event.dependencies.begin(),
                                                 event.dependencies.end()),
                                     event.dependencies.end());
            ids.emplace(event.id, trace.events.size());
            trace.events.push_back(std::move(event));
        } catch (const std::exception& error) {
            throw std::invalid_argument("trace line " +
                                        std::to_string(line_number) + ": " +
                                        error.what());
        }
    }
    Require(!input.bad(), "failed to read trace");
    Require(header_seen && !trace.events.empty(), "trace must contain events");
    Require(trace.version == 1 ||
                (!mounted.empty() && mounted.size() == unmounted.size()),
            "v2 must mount storage and unmount every segment");
    for (size_t i = 0; i < trace.events.size(); ++i) {
        Require(trace.events[i].op != "BatchPutStart" || finalized.count(i),
                "BatchPutStart has no End/Revoke: " + trace.events[i].id);
    }
    return trace;
}

RpcTrace LoadTrace(const std::string& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open trace: " + path);
    return ReadTrace(input);
}

// A bounded worker pool dispatches due, dependency-ready events. Slow dependent
// calls do not block unrelated events. Worker saturation is visible as lag.
std::vector<TraceSample> ReplayTrace(const RpcTrace& trace, size_t workers,
                                     double speed,
                                     const TraceExecutor& execute) {
    Require(workers > 0, "workers must be positive");
    Require(std::isfinite(speed) && speed > 0,
            "speed must be finite and positive");
    Require(!trace.events.empty(), "trace must contain events");
    using Clock = std::chrono::steady_clock;
    using Micros = std::chrono::microseconds;
    std::vector<TraceSample> samples(trace.events.size());
    std::vector<size_t> pending(trace.events.size());
    std::vector<std::vector<size_t>> children(trace.events.size());
    std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>>
        ready;
    std::mutex mutex;
    std::condition_variable cv;
    size_t remaining = trace.events.size();
    std::exception_ptr scheduling_error;
    size_t phase_begin = 0, phase_end = 0, phase_remaining = 0;
    auto origin = Clock::now();
    const auto max_delay =
        std::chrono::duration_cast<Micros>(Clock::time_point::max() - origin)
            .count();
    for (size_t i = 0; i < trace.events.size(); ++i) {
        const auto scaled =
            static_cast<long double>(trace.events[i].timestamp_us) / speed;
        Require(scaled <= max_delay,
                "scaled timestamp exceeds steady-clock range");
        samples[i].scheduled_us = static_cast<int64_t>(scaled);
        pending[i] = trace.events[i].dependencies.size();
        for (auto dependency : trace.events[i].dependencies) {
            Require(dependency < i, "invalid dependency index");
            children[dependency].push_back(i);
        }
    }
    const auto elapsed = [&] {
        return std::chrono::duration_cast<Micros>(Clock::now() - origin)
            .count();
    };
    const auto activate_phase = [&](int64_t phase_origin) {
        phase_end = phase_begin;
        while (phase_end < trace.events.size() &&
               trace.events[phase_end].phase ==
                   trace.events[phase_begin].phase) {
            auto& sample = samples[phase_end];
            Require(sample.scheduled_us <=
                        std::numeric_limits<int64_t>::max() - phase_origin,
                    "phase timestamp overflow");
            sample.scheduled_us += phase_origin;
            sample.phase_origin_us = phase_origin;
            if (pending[phase_end] == 0) ready.push(phase_end);
            ++phase_end;
        }
        phase_remaining = phase_end - phase_begin;
    };
    activate_phase(0);
    const auto worker_count = std::min(workers, trace.events.size());
    std::latch initialized(worker_count), start(1);
    bool startup_cancelled = false;
    auto run = [&] {
        initialized.count_down();
        start.wait();
        if (startup_cancelled) return;
        while (true) {
            size_t index;
            {
                std::unique_lock lock(mutex);
                while (true) {
                    if (remaining == 0) return;
                    if (ready.empty()) {
                        cv.wait(lock);
                    } else {
                        index = ready.top();
                        const auto due =
                            origin + Micros(samples[index].scheduled_us);
                        if (Clock::now() < due) {
                            cv.wait_until(lock, due);
                        } else {
                            ready.pop();
                            break;
                        }
                    }
                }
            }
            const auto& event = trace.events[index];
            auto& sample = samples[index];
            sample.start_us = elapsed();
            try {
                sample.outcome = execute(
                    event, event.put_start ? &samples[*event.put_start].outcome
                                           : nullptr);
                if (sample.outcome.keys.size() != event.keys.size()) {
                    throw std::runtime_error(
                        "executor returned wrong status count");
                }
            } catch (const std::exception& error) {
                // Whether a throwing executor actually sent a request is
                // unknown.
                sample.outcome.keys.assign(event.keys.size(), KeyStatus::ERROR);
                sample.outcome.error = error.what();
            }
            sample.finish_us = elapsed();
            {
                std::lock_guard lock(mutex);
                for (auto child : children[index]) {
                    if (--pending[child] == 0 && child < phase_end)
                        ready.push(child);
                }
                --remaining;
                if (--phase_remaining == 0 && remaining) {
                    phase_begin = phase_end;
                    try {
                        activate_phase(elapsed());
                    } catch (...) {
                        scheduling_error = std::current_exception();
                        remaining = 0;
                    }
                }
            }
            cv.notify_all();
        }
    };
    std::vector<std::jthread> threads;
    try {
        for (size_t i = 0; i < worker_count; ++i) threads.emplace_back(run);
    } catch (...) {
        startup_cancelled = true;
        start.count_down();
        throw;
    }
    initialized.wait();
    origin = Clock::now();
    for (auto& sample : samples)
        sample.replay_origin_monotonic_us =
            std::chrono::duration_cast<Micros>(origin.time_since_epoch())
                .count();
    start.count_down();
    for (auto& thread : threads) thread.join();
    if (scheduling_error) std::rethrow_exception(scheduling_error);
    return samples;
}

static Json::Value SummarizeEvents(const RpcTrace& trace,
                                   const std::vector<TraceSample>& samples,
                                   const char* phase = nullptr) {
    Require(samples.size() == trace.events.size(), "sample count mismatch");
    Json::Value result(Json::objectValue);
    result["schema_version"] = 1;
    result["trace_metadata"] = phase ? Json::Value{} : trace.metadata;
    int64_t elapsed_us = 0;
    size_t event_count = 0;
    std::set<std::string> operations;
    for (size_t i = 0; i < samples.size(); ++i) {
        if (phase && trace.events[i].phase != phase) continue;
        if (event_count++ == 0)
            result["replay_origin_monotonic_us"] =
                Json::Int64(samples[i].replay_origin_monotonic_us);
        operations.insert(trace.events[i].op);
        const auto origin = phase ? samples[i].phase_origin_us : 0;
        elapsed_us = std::max(elapsed_us, samples[i].finish_us - origin);
    }
    result["events"] = Json::UInt64(event_count);
    result["elapsed_us"] = Json::Int64(elapsed_us);
    for (const auto& op : operations) {
        auto& stats = result["operations"][op];
        uint64_t calls = 0, keys = 0, planned = 0;
        uint64_t failed_calls = 0;
        RpcOutcome outcomes;
        std::vector<int64_t> lag, latency, end_to_end;
        for (size_t i = 0; i < samples.size(); ++i) {
            if (phase && trace.events[i].phase != phase) continue;
            if (trace.events[i].op != op) continue;
            const auto& sample = samples[i];
            ++planned;
            failed_calls += !sample.outcome.error.empty();
            outcomes.keys.insert(outcomes.keys.end(),
                                 sample.outcome.keys.begin(),
                                 sample.outcome.keys.end());
            lag.push_back(sample.start_us - sample.scheduled_us);
            if (sample.outcome.rpc_sent) {
                ++calls;
                keys += std::count_if(
                    sample.outcome.keys.begin(), sample.outcome.keys.end(),
                    [](auto status) { return status != KeyStatus::SKIPPED; });
                latency.push_back(sample.finish_us - sample.start_us);
                end_to_end.push_back(sample.finish_us - sample.scheduled_us);
            }
        }
        stats["planned_calls"] = Json::UInt64(planned);
        stats["failed_calls"] = Json::UInt64(failed_calls);
        stats["rpc_calls"] = Json::UInt64(calls);
        stats["issued_keys"] = Json::UInt64(keys);
        stats["key_status"] = StatusCounts(outcomes);
        stats["dispatch_lag_us"] = Percentiles(std::move(lag));
        stats["client_call_latency_us"] = Percentiles(std::move(latency));
        stats["scheduled_to_completion_us"] =
            Percentiles(std::move(end_to_end));
        if (elapsed_us > 0) {
            stats["rpc_per_second"] = calls * 1e6 / elapsed_us;
            stats["keys_per_second"] = keys * 1e6 / elapsed_us;
        }
    }
    return result;
}

// Both per-operation summaries and per-event timing use real steady-clock time.
Json::Value SummarizeTrace(const RpcTrace& trace,
                           const std::vector<TraceSample>& samples) {
    auto result = SummarizeEvents(trace, samples);
    result["schema_version"] = 2;
    for (const auto* phase : {"setup", "workload", "teardown"}) {
        result["phases"][phase] = SummarizeEvents(trace, samples, phase);
    }
    return result;
}

void WriteSamples(const std::string& path, const RpcTrace& trace,
                  const std::vector<TraceSample>& samples) {
    Require(samples.size() == trace.events.size(), "sample count mismatch");
    std::ofstream output(path);
    if (!output) throw std::runtime_error("cannot open samples: " + path);
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    for (size_t i = 0; i < samples.size(); ++i) {
        Json::Value row;
        row["id"] = trace.events[i].id;
        row["client_id"] = trace.events[i].client_id;
        row["op"] = trace.events[i].op;
        row["phase"] = trace.events[i].phase;
        row["phase_origin_us"] = Json::Int64(samples[i].phase_origin_us);
        row["scheduled_us"] = Json::Int64(samples[i].scheduled_us);
        row["start_us"] = Json::Int64(samples[i].start_us);
        row["finish_us"] = Json::Int64(samples[i].finish_us);
        row["planned_keys"] = Json::UInt64(trace.events[i].keys.size());
        row["rpc_sent"] = samples[i].outcome.rpc_sent;
        row["key_status"] = StatusCounts(samples[i].outcome);
        row["error"] = samples[i].outcome.error;
        output << Json::writeString(writer, row) << '\n';
    }
    output.flush();
    if (!output) throw std::runtime_error("failed to write samples: " + path);
}

KeyStatus Classify(mooncake::ErrorCode error) {
    if (error == mooncake::ErrorCode::OBJECT_NOT_FOUND) return KeyStatus::MISS;
    if (error == mooncake::ErrorCode::REPLICA_IS_NOT_READY)
        return KeyStatus::NOT_READY;
    if (error == mooncake::ErrorCode::OBJECT_ALREADY_EXISTS)
        return KeyStatus::ALREADY_EXISTS;
    return KeyStatus::ERROR;
}

// Fake segments are valid metadata allocations but have no accessible payload.
// This executable must only be used against a dedicated benchmark master.
class Session {
   public:
    explicit Session(bool legacy)
        : client(mooncake::generate_uuid(), nullptr, FLAGS_tenant) {
        const auto error = client.Connect(FLAGS_master_server);
        if (error != mooncake::ErrorCode::OK) {
            throw std::runtime_error("connect failed: " + toString(error));
        }
        if (!legacy) return;
        segment.id = mooncake::generate_uuid();
        segment.name = "trace_" + mooncake::UuidToString(segment.id);
        segment.base = 0x100000000ULL;
        segment.size = FLAGS_segment_size;
        segment.host_id = segment.name;
        segment.te_endpoint = segment.name + ":12345";
        segment.protocol = "tcp";
        // First registration must finish the remount handshake to mark the
        // client OK for Ping. All of this happens before replay measurement.
        const auto mounted = client.ReMountSegment({segment});
        if (!mounted)
            throw std::runtime_error("mount failed: " +
                                     toString(mounted.error()));
        registered.store(true);
        segments.emplace("legacy", segment);
    }

    ~Session() {
        for (const auto& [id, value] : segments) {
            const auto result = client.UnmountSegment(value.id);
            if (!result)
                LOG(ERROR) << "Trace segment cleanup failed: "
                           << toString(result.error());
        }
    }

    mooncake::MasterClient client;
    mooncake::Segment segment;
    std::map<std::string, mooncake::Segment> segments;
    std::mutex lifecycle_mutex;
    std::atomic<bool> registered{false};
    const std::string host_id =
        "trace_" + mooncake::UuidToString(mooncake::generate_uuid());
};

struct PreparedCall {
    mooncake::MasterClient* client;
    Session* session;
    std::vector<std::vector<uint64_t>> lengths;
    std::vector<mooncake::ObjectMeta> object_metas;
    mooncake::ReplicateConfig config;
};

class MasterReplay {
   public:
    MasterReplay() {
        try {
            for (size_t i = 0; i < FLAGS_heartbeat_workers; ++i)
                heartbeats_.emplace_back([this, i] { Heartbeats(i); });
        } catch (...) {
            stopping_.store(true);
            wake_.notify_all();
            for (auto& thread : heartbeats_) thread.join();
            throw;
        }
    }

    ~MasterReplay() {
        stopping_.store(true);
        wake_.notify_all();
        for (auto& thread : heartbeats_) thread.join();
        // Sessions are destroyed only after all heartbeat calls have stopped.
    }

    void Prepare(const RpcTrace& trace) {
        for (const auto& event : trace.events) {
            if (!sessions_.count(event.client_id)) {
                auto session = std::make_unique<Session>(trace.version == 1);
                std::lock_guard lock(mutex_);
                sessions_.emplace(event.client_id, std::move(session));
            }
        }
    }

    std::vector<TraceSample> Run(const RpcTrace& trace) {
        std::vector<PreparedCall> prepared;
        prepared.reserve(trace.events.size());
        for (const auto& event : trace.events) {
            PreparedCall call;
            call.client = &sessions_.at(event.client_id)->client;
            call.session = sessions_.at(event.client_id).get();
            call.config.replica_num = event.replica_num;
            for (auto size : event.value_sizes) call.lengths.push_back({size});
            if (!event.value_slices.empty()) call.lengths = event.value_slices;
            if (event.op == "BatchPutEnd") {
                for (const auto& key : event.keys)
                    call.object_metas.push_back({key, std::nullopt});
            }
            prepared.push_back(std::move(call));
        }
        return ReplayTrace(
            trace, FLAGS_workers, FLAGS_speed,
            [&](const TraceEvent& event, const RpcOutcome* start) {
                const auto index = &event - trace.events.data();
                if (heartbeat_failed_.load()) {
                    throw std::runtime_error(
                        "client heartbeat failed; run is invalid");
                }
                return Execute(event, prepared[index], start);
            });
    }

    bool healthy() const { return !heartbeat_failed_.load(); }
    uint64_t heartbeat_calls() const { return heartbeat_calls_.load(); }
    size_t client_count() const { return sessions_.size(); }

   private:
    template <typename Results>
    static void Collect(RpcOutcome& outcome, const Results& results,
                        const std::vector<size_t>& positions) {
        outcome.rpc_sent = true;
        if (results.size() != positions.size()) {
            for (auto index : positions) outcome.keys[index] = KeyStatus::ERROR;
            outcome.error = "RPC response size does not match submitted keys";
            return;
        }
        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].has_value()) {
                outcome.keys[positions[i]] = KeyStatus::OK;
            } else {
                outcome.keys[positions[i]] = Classify(results[i].error());
                if (outcome.keys[positions[i]] == KeyStatus::ERROR &&
                    outcome.error.empty()) {
                    outcome.error = toString(results[i].error());
                }
            }
        }
    }

    static RpcOutcome Execute(const TraceEvent& event, PreparedCall& call,
                              const RpcOutcome* start) {
        RpcOutcome result;
        result.keys.assign(event.keys.size(), KeyStatus::SKIPPED);
        auto& session = *call.session;
        auto& client = *call.client;
        if (event.op == "ReMountSegment" || event.op == "MountSegment" ||
            event.op == "UnmountSegment") {
            std::lock_guard lock(session.lifecycle_mutex);
            tl::expected<void, mooncake::ErrorCode> response;
            if (event.op == "ReMountSegment") {
                response = client.ReMountSegment({});
                if (response) session.registered.store(true);
            } else if (!session.registered.load()) {
                throw std::runtime_error("client registration failed");
            } else if (event.op == "MountSegment") {
                mooncake::Segment segment;
                segment.id = mooncake::generate_uuid();
                segment.name = "trace_" + mooncake::UuidToString(segment.id);
                segment.base = 0x100000000ULL;
                segment.size = event.size_bytes;
                segment.host_id = session.host_id;
                segment.te_endpoint = segment.host_id + ":12345";
                segment.protocol = "tcp";
                response = client.MountSegment(segment);
                if (response)
                    session.segments.emplace(event.segment_id,
                                             std::move(segment));
            } else {
                const auto found = session.segments.find(event.segment_id);
                if (found == session.segments.end())
                    throw std::runtime_error(
                        "segment was not successfully mounted");
                response = client.UnmountSegment(found->second.id);
                if (response) session.segments.erase(found);
            }
            result.rpc_sent = true;
            if (!response) result.error = toString(response.error());
            return result;
        }
        if (!session.registered.load())
            throw std::runtime_error("client registration failed");
        std::vector<size_t> positions;
        for (size_t i = 0; i < event.keys.size(); ++i) {
            if (!start || start->keys[i] == KeyStatus::OK)
                positions.push_back(i);
        }
        if (positions.empty()) return result;
        if (event.op == "BatchExistKey") {
            const auto response = client.BatchExistKey(event.keys);
            Collect(result, response, positions);
            if (response.size() == event.keys.size()) {
                for (size_t i = 0; i < response.size(); ++i) {
                    if (response[i] && !response[i].value())
                        result.keys[i] = KeyStatus::MISS;
                }
            }
        } else if (event.op == "BatchGetReplicaList") {
            Collect(result, client.BatchGetReplicaList(event.keys), positions);
        } else if (event.op == "BatchPutStart") {
            Collect(result,
                    client.BatchPutStart(event.keys, call.lengths, call.config),
                    positions);
        } else if (event.op == "BatchPutEnd") {
            std::vector<mooncake::ObjectMeta> metas;
            for (auto i : positions) metas.push_back(call.object_metas[i]);
            Collect(result,
                    client.BatchPutEnd(metas, mooncake::ReplicaType::MEMORY),
                    positions);
        } else if (event.op == "BatchPutRevoke") {
            std::vector<std::string> keys;
            for (auto i : positions) keys.push_back(event.keys[i]);
            Collect(result,
                    client.BatchPutRevoke(keys, mooncake::ReplicaType::MEMORY),
                    positions);
        } else if (event.op == "BatchRemove") {
            Collect(result, client.BatchRemove(event.keys, false), positions);
        }
        return result;
    }

    void Heartbeats(size_t partition) {
        std::unique_lock lock(mutex_);
        while (!stopping_.load()) {
            std::vector<std::pair<std::string, Session*>> clients;
            size_t index = 0;
            for (const auto& [id, session] : sessions_) {
                if (index++ % FLAGS_heartbeat_workers == partition &&
                    session->registered.load())
                    clients.emplace_back(id, session.get());
            }
            // Sessions remain alive until all heartbeat workers have joined.
            // Do not hold the session-map lock while waiting for network I/O.
            lock.unlock();
            for (const auto& [id, session] : clients) {
                if (stopping_.load()) break;
                ++heartbeat_calls_;
                try {
                    const auto result = session->client.Ping();
                    if (!result) {
                        heartbeat_failed_.store(true);
                        LOG(ERROR) << "Trace client heartbeat failed: " << id
                                   << ": " << toString(result.error());
                    } else if (result->client_status !=
                               mooncake::ClientStatus::OK) {
                        heartbeat_failed_.store(true);
                        LOG(ERROR) << "Trace client heartbeat failed: " << id
                                   << ": " << result->client_status;
                    }
                } catch (const std::exception& error) {
                    heartbeat_failed_.store(true);
                    LOG(ERROR) << "Trace client heartbeat failed: " << id
                               << ": " << error.what();
                }
            }
            lock.lock();
            wake_.wait_for(
                lock, std::chrono::milliseconds(FLAGS_heartbeat_interval_ms),
                [&] { return stopping_.load(); });
        }
    }

    std::map<std::string, std::unique_ptr<Session>> sessions_;
    std::atomic<bool> stopping_{false}, heartbeat_failed_{false};
    std::atomic<uint64_t> heartbeat_calls_{0};
    std::mutex mutex_;
    std::condition_variable wake_;
    std::vector<std::thread> heartbeats_;
};

bool HasErrors(const std::vector<TraceSample>& samples) {
    for (const auto& sample : samples) {
        if (!sample.outcome.error.empty()) return true;
        for (auto status : sample.outcome.keys) {
            if (status == KeyStatus::ERROR) return true;
        }
    }
    return false;
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    try {
        if (FLAGS_trace.empty() || FLAGS_workers == 0 ||
            FLAGS_heartbeat_interval_ms == 0 || FLAGS_heartbeat_workers == 0 ||
            FLAGS_segment_size == 0 || !std::isfinite(FLAGS_speed) ||
            FLAGS_speed <= 0) {
            throw std::invalid_argument(
                "trace, positive "
                "workers/heartbeat_interval_ms/heartbeat_workers/segment_size/"
                "speed are "
                "required");
        }
        const auto trace = LoadTrace(FLAGS_trace);
        std::optional<RpcTrace> prefill;
        if (!FLAGS_prefill_trace.empty())
            prefill = LoadTrace(FLAGS_prefill_trace);
        if (prefill && (trace.version != 1 || prefill->version != 1))
            throw std::invalid_argument(
                "prefill_trace is for v1 only; include initialization in v2 "
                "workload");
        if (FLAGS_validate_only) {
            std::cout << "Validated " << trace.events.size()
                      << " measured events\n";
            return 0;
        }
        MasterReplay runner;
        runner.Prepare(trace);
        if (prefill) {
            runner.Prepare(*prefill);
            const auto samples = runner.Run(*prefill);
            for (const auto& sample : samples) {
                for (auto status : sample.outcome.keys) {
                    if (status != KeyStatus::OK) {
                        throw std::runtime_error(
                            "prefill did not fully succeed");
                    }
                }
            }
        }
        const auto heartbeats_before = runner.heartbeat_calls();
        const auto samples = runner.Run(trace);
        const auto heartbeats_after = runner.heartbeat_calls();
        auto result = SummarizeTrace(trace, samples);
        result["master_server"] = FLAGS_master_server;
        result["tenant"] = FLAGS_tenant;
        result["workers"] = FLAGS_workers;
        result["heartbeat_interval_ms"] = FLAGS_heartbeat_interval_ms;
        result["heartbeat_workers"] = FLAGS_heartbeat_workers;
        result["logical_clients"] = Json::UInt64(runner.client_count());
        if (trace.version == 1)
            result["segment_size_bytes"] = Json::UInt64(FLAGS_segment_size);
        result["speed"] = FLAGS_speed;
        result["prefill_trace"] = FLAGS_prefill_trace;
        result["heartbeat_calls_during_replay"] =
            Json::UInt64(heartbeats_after - heartbeats_before);
        result["healthy_heartbeats"] = runner.healthy();
        result["has_errors"] = HasErrors(samples);
        WriteSamples(FLAGS_samples, trace, samples);
        std::ofstream output(FLAGS_output);
        output << result << '\n';
        output.flush();
        if (!output)
            throw std::runtime_error("cannot write summary: " + FLAGS_output);
        std::cout << result << '\n';
        return runner.healthy() && !HasErrors(samples) ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "master_rpc_trace_bench: " << error.what() << '\n';
        return 1;
    }
}
