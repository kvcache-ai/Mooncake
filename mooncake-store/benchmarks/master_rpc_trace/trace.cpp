// Copyright 2026 Alibaba Cloud and its affiliates
// Licensed under the Apache License, Version 2.0.
#include "trace.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <fstream>
#include <latch>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace mooncake::bench {
namespace {

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

}  // namespace

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

}  // namespace mooncake::bench
