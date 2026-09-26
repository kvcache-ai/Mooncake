// Copyright 2026 Alibaba Cloud and its affiliates
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <json/json.h>

#include <cstdint>
#include <functional>
#include <istream>
#include <optional>
#include <string>
#include <vector>

namespace mooncake::bench {

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
    uint64_t replica_num = 1;
    std::vector<size_t> dependencies;
    std::optional<size_t> put_start;
};

struct RpcTrace {
    unsigned version = 1;
    Json::Value metadata;
    std::vector<TraceEvent> events;
};

// Validation happens before connecting to a master or starting measurement.
// Dependencies must reference earlier rows. Timestamps must be nondecreasing.
RpcTrace ReadTrace(std::istream& input);
RpcTrace LoadTrace(const std::string& path);

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

// A bounded worker pool dispatches due, dependency-ready events. Slow dependent
// calls do not block unrelated events. Worker saturation is visible as lag.
std::vector<TraceSample> ReplayTrace(const RpcTrace& trace, size_t workers,
                                     double speed,
                                     const TraceExecutor& execute);

// Both per-operation summaries and per-event timing use real steady-clock time.
Json::Value SummarizeTrace(const RpcTrace& trace,
                           const std::vector<TraceSample>& samples);
void WriteSamples(const std::string& path, const RpcTrace& trace,
                  const std::vector<TraceSample>& samples);

}  // namespace mooncake::bench
