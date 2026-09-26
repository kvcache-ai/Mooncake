// Copyright 2026 Alibaba Cloud and its affiliates
// Licensed under the Apache License, Version 2.0.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>

#include "master_client.h"
#include "master_rpc_trace/trace.h"

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
using mooncake::bench::KeyStatus;
using mooncake::bench::RpcOutcome;
using mooncake::bench::RpcTrace;
using mooncake::bench::TraceEvent;
using mooncake::bench::TraceSample;

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
        return mooncake::bench::ReplayTrace(
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
        const auto trace = mooncake::bench::LoadTrace(FLAGS_trace);
        std::optional<RpcTrace> prefill;
        if (!FLAGS_prefill_trace.empty())
            prefill = mooncake::bench::LoadTrace(FLAGS_prefill_trace);
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
        auto result = mooncake::bench::SummarizeTrace(trace, samples);
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
        mooncake::bench::WriteSamples(FLAGS_samples, trace, samples);
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
