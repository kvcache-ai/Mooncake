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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "master_client.h"

DEFINE_string(trace, "", "Master RPC JSONL trace to replay");
DEFINE_string(master_server, "127.0.0.1:50051",
              "Dedicated benchmark master address");
DEFINE_string(tenant, "default", "Tenant used for all trace clients");
DEFINE_string(output, "master_trace_result.json",
              "Summary output, written after replay");
DEFINE_string(samples, "master_trace_samples.jsonl",
              "Per-event timings, written after replay");
DEFINE_string(heartbeats, "master_trace_heartbeats.jsonl",
              "Background Ping timings, written after replay");

namespace {

// A trace contains client API calls, not captured wire packets or KV payloads.
struct TraceEvent {
    std::string id;
    std::string client_id;
    std::string op;
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
    int64_t scheduled_us = 0;
    int64_t start_us = 0;
    int64_t finish_us = 0;
    RpcOutcome outcome;
};

// The second argument is the completed BatchPutStart result for End/Revoke.
// One worker owns each logical client and executes its calls in file order.
using TraceExecutor =
    std::function<RpcOutcome(const TraceEvent&, const RpcOutcome* put_start)>;

void Require(bool condition, const std::string& message) {
    if (!condition) throw std::invalid_argument(message);
}

Json::Value ParseLine(const std::string& line) {
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
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

// Decode the producer's trace and resolve event IDs before connecting.
// The producer is responsible for satisfying the documented trace contract.
RpcTrace ReadTrace(std::istream& input) {
    RpcTrace trace;
    std::unordered_map<std::string, size_t> ids;
    std::unordered_map<std::string, size_t> registered;
    std::unordered_map<std::string, size_t> mounted;
    std::string line;
    size_t line_number = 0;
    bool header_seen = false;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.find_first_not_of(" \t\r") == std::string::npos) continue;
        try {
            const auto row = ParseLine(line);
            if (!header_seen) {
                Require(row["type"] == "master_rpc_trace" &&
                            row["time_unit"] == "us",
                        "expected master_rpc_trace header with time_unit=us");
                trace.metadata = row["metadata"];
                header_seen = true;
                continue;
            }
            TraceEvent event;
            event.id = row["id"].asString();
            event.client_id = row["client_id"].asString();
            event.op = row["op"].asString();
            Require(event.op != "Ping",
                    "Ping is generated by the replayer; remove it from the "
                    "trace and its dependency lists");
            event.timestamp_us = row["timestamp_us"].asUInt64();
            for (const auto& key : row["keys"]) {
                event.keys.push_back(key.asString());
            }
            for (const auto& dependency : row["depends_on"]) {
                event.dependencies.push_back(ids.at(dependency.asString()));
            }
            if (event.op == "ReMountSegment") {
                registered[event.client_id] = trace.events.size();
            } else {
                event.dependencies.push_back(registered.at(event.client_id));
            }
            if (event.op == "MountSegment") {
                event.segment_id = row["segment_id"].asString();
                event.size_bytes = row["size_bytes"].asUInt64();
                mounted[event.segment_id] = trace.events.size();
            } else if (event.op == "UnmountSegment") {
                event.segment_id = row["segment_id"].asString();
                event.dependencies.push_back(mounted.at(event.segment_id));
            }
            if (event.op == "BatchPutStart") {
                if (row.isMember("value_slices")) {
                    for (const auto& value : row["value_slices"]) {
                        std::vector<uint64_t> slices;
                        for (const auto& size : value) {
                            slices.push_back(size.asUInt64());
                        }
                        event.value_slices.push_back(std::move(slices));
                    }
                } else {
                    for (const auto& size : row["value_sizes"]) {
                        event.value_sizes.push_back(size.asUInt64());
                    }
                }
                if (row.isMember("replica_num")) {
                    event.replica_num = row["replica_num"].asUInt64();
                }
            }
            if (event.op == "BatchPutEnd" || event.op == "BatchPutRevoke") {
                const auto start_index = ids.at(row["put_start"].asString());
                event.put_start = start_index;
                event.dependencies.push_back(start_index);
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
    Require(header_seen, "expected master_rpc_trace header with time_unit=us");
    return trace;
}

RpcTrace LoadTrace(const std::string& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open trace: " + path);
    return ReadTrace(input);
}

// Each client has a private event sequence. Completion flags synchronize
// cross-client dependencies without a shared dispatch queue or scheduling lock.
std::vector<TraceSample> ReplayTrace(const RpcTrace& trace,
                                     const TraceExecutor& execute) {
    Require(!trace.events.empty(), "trace must contain events");
    using Clock = std::chrono::steady_clock;
    using Micros = std::chrono::microseconds;
    std::vector<TraceSample> samples(trace.events.size());
    std::vector<std::atomic<uint32_t>> completed(trace.events.size());
    std::map<std::string, std::vector<size_t>> clients;
    auto origin = Clock::now();
    const auto max_delay =
        std::chrono::duration_cast<Micros>(Clock::time_point::max() - origin)
            .count();
    for (size_t i = 0; i < trace.events.size(); ++i) {
        Require(
            trace.events[i].timestamp_us <= static_cast<uint64_t>(max_delay),
            "timestamp exceeds steady-clock range");
        samples[i].scheduled_us =
            static_cast<int64_t>(trace.events[i].timestamp_us);
        clients[trace.events[i].client_id].push_back(i);
        for (auto dependency : trace.events[i].dependencies) {
            Require(dependency < i, "invalid dependency index");
        }
    }
    const auto elapsed = [&] {
        return std::chrono::duration_cast<Micros>(Clock::now() - origin)
            .count();
    };
    std::latch initialized(clients.size()), start(1);
    bool startup_cancelled = false;
    auto run = [&](const std::vector<size_t>& events) {
        initialized.count_down();
        start.wait();
        if (startup_cancelled) return;
        for (auto index : events) {
            const auto& event = trace.events[index];
            auto& sample = samples[index];
            std::this_thread::sleep_until(origin + Micros(sample.scheduled_us));
            for (auto dependency : event.dependencies) {
                completed[dependency].wait(0, std::memory_order_acquire);
            }
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
            // Publish both timing and outcome before dependent clients proceed.
            completed[index].store(1, std::memory_order_release);
            completed[index].notify_all();
        }
    };
    std::vector<std::jthread> threads;
    try {
        for (const auto& [client, events] : clients)
            threads.emplace_back(run, std::cref(events));
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
    return samples;
}

// Both per-operation summaries and per-event timing use real steady-clock time.
Json::Value SummarizeTrace(const RpcTrace& trace,
                           const std::vector<TraceSample>& samples) {
    Require(samples.size() == trace.events.size(), "sample count mismatch");
    Json::Value result(Json::objectValue);
    result["trace_metadata"] = trace.metadata;
    int64_t elapsed_us = 0;
    size_t event_count = 0;
    std::set<std::string> operations;
    for (size_t i = 0; i < samples.size(); ++i) {
        if (event_count++ == 0)
            result["replay_origin_monotonic_us"] =
                Json::Int64(samples[i].replay_origin_monotonic_us);
        operations.insert(trace.events[i].op);
        elapsed_us = std::max(elapsed_us, samples[i].finish_us);
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
    Session() : client(mooncake::generate_uuid(), nullptr, FLAGS_tenant) {
        const auto error = client.Connect(FLAGS_master_server);
        if (error != mooncake::ErrorCode::OK) {
            throw std::runtime_error("connect failed: " + toString(error));
        }
    }

    struct HeartbeatSample {
        int64_t start_us;
        int64_t finish_us;
        std::string error;
    };

    void StartHeartbeat() {
        Require(!heartbeat.joinable(), "client already registered");
        heartbeat = std::jthread([this](std::stop_token stop) {
            const auto now_us = [] {
                return std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now().time_since_epoch())
                    .count();
            };
            std::mutex mutex;
            std::condition_variable_any wake;
            std::unique_lock lock(mutex);
            do {
                HeartbeatSample sample{now_us(), 0, {}};
                try {
                    const auto response = client.Ping();
                    if (!response) {
                        sample.error = toString(response.error());
                    } else if (response->client_status !=
                               mooncake::ClientStatus::OK) {
                        sample.error = "Ping returned a non-OK client status";
                    }
                } catch (const std::exception& error) {
                    sample.error = error.what();
                }
                sample.finish_us = now_us();
                heartbeat_samples.push_back(std::move(sample));
                // Match the storage client's cadence: wait one second after
                // each response. Stop wakes this wait without delaying shutdown.
                wake.wait_for(lock, stop, std::chrono::seconds(1),
                              [] { return false; });
            } while (!stop.stop_requested());
        });
    }

    mooncake::MasterClient client;
    std::map<std::string, mooncake::Segment> segments;
    bool registered = false;
    const std::string host_id =
        "trace_" + mooncake::UuidToString(mooncake::generate_uuid());
    // Only the heartbeat thread writes samples; read them after joining it.
    std::vector<HeartbeatSample> heartbeat_samples;
    // Destroy/join the thread before the samples and RPC client it references.
    std::jthread heartbeat;
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
    void Prepare(const RpcTrace& trace) {
        for (const auto& event : trace.events) {
            if (!sessions_.count(event.client_id)) {
                auto session = std::make_unique<Session>();
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
        try {
            auto samples = ReplayTrace(
                trace, [&](const TraceEvent& event, const RpcOutcome* start) {
                    const auto index = &event - trace.events.data();
                    return Execute(event, prepared[index], start);
                });
            StopHeartbeats();
            return samples;
        } catch (...) {
            StopHeartbeats();
            throw;
        }
    }

    size_t client_count() const { return sessions_.size(); }

    Json::Value WriteHeartbeats(const std::string& path, int64_t origin) const {
        std::ofstream output(path);
        if (!output) throw std::runtime_error("cannot open heartbeats: " + path);
        Json::StreamWriterBuilder writer;
        writer["indentation"] = "";
        uint64_t calls = 0, failures = 0, clients = 0;
        std::vector<int64_t> latency;
        for (const auto& [id, session] : sessions_) {
            clients += !session->heartbeat_samples.empty();
            for (const auto& sample : session->heartbeat_samples) {
                ++calls;
                failures += !sample.error.empty();
                latency.push_back(sample.finish_us - sample.start_us);
                Json::Value row;
                row["client_id"] = id;
                row["op"] = "Ping";
                row["start_us"] = Json::Int64(sample.start_us - origin);
                row["finish_us"] = Json::Int64(sample.finish_us - origin);
                row["error"] = sample.error;
                output << Json::writeString(writer, row) << '\n';
            }
        }
        output.flush();
        if (!output)
            throw std::runtime_error("failed to write heartbeats: " + path);
        Json::Value summary;
        summary["interval_after_response_ms"] = 1000;
        summary["clients"] = Json::UInt64(clients);
        summary["rpc_calls"] = Json::UInt64(calls);
        summary["failed_calls"] = Json::UInt64(failures);
        summary["client_call_latency_us"] = Percentiles(std::move(latency));
        return summary;
    }

   private:
    void StopHeartbeats() {
        // Keep every registered client alive until ALL trace workers finish,
        // including storage clients whose last recorded event was much earlier.
        for (auto& [id, session] : sessions_) session->heartbeat.request_stop();
        for (auto& [id, session] : sessions_)
            if (session->heartbeat.joinable()) session->heartbeat.join();
    }

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
            tl::expected<void, mooncake::ErrorCode> response;
            if (event.op == "ReMountSegment") {
                response = client.ReMountSegment({});
                if (response) {
                    session.registered = true;
                    session.StartHeartbeat();
                }
            } else if (!session.registered) {
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
        if (!session.registered)
            throw std::runtime_error("client registration failed");
        std::vector<size_t> positions;
        for (size_t i = 0; i < event.keys.size(); ++i) {
            if (!start || start->keys.at(i) == KeyStatus::OK)
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

    std::map<std::string, std::unique_ptr<Session>> sessions_;
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
        if (FLAGS_trace.empty()) {
            throw std::invalid_argument("trace is required");
        }
        const auto trace = LoadTrace(FLAGS_trace);
        MasterReplay runner;
        runner.Prepare(trace);
        const auto samples = runner.Run(trace);
        auto result = SummarizeTrace(trace, samples);
        result["master_server"] = FLAGS_master_server;
        result["tenant"] = FLAGS_tenant;
        result["workers"] = Json::UInt64(runner.client_count());
        result["replay_policy"] = "one_worker_per_client";
        result["logical_clients"] = Json::UInt64(runner.client_count());
        result["heartbeats"] = runner.WriteHeartbeats(
            FLAGS_heartbeats, result["replay_origin_monotonic_us"].asInt64());
        const bool has_errors =
            HasErrors(samples) || result["heartbeats"]["failed_calls"].asUInt64();
        result["has_errors"] = has_errors;
        WriteSamples(FLAGS_samples, trace, samples);
        std::ofstream output(FLAGS_output);
        output << result << '\n';
        output.flush();
        if (!output)
            throw std::runtime_error("cannot write summary: " + FLAGS_output);
        std::cout << result << '\n';
        return has_errors ? 1 : 0;
    } catch (const std::exception& error) {
        std::cerr << "master_rpc_trace_bench: " << error.what() << '\n';
        return 1;
    }
}
