// Copyright 2025 Alibaba Cloud and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

#include <json/json.h>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

namespace {

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;
constexpr uint64_t kMaxAmplify = 256;
constexpr size_t kMaxProfileBytes = 1 * MiB;
constexpr uintptr_t kSegmentBase = 0x200000000ULL;

enum class OpKind : uint8_t { kPut, kGet, kExist };

struct WeightedSize {
    uint32_t size = 0;
    uint32_t weight = 0;
};

struct ArrivalProfile {
    uint64_t intra_burst_p50 = 0;
    uint64_t intra_burst_p99 = 0;
    double burst_gap_prob = 0.0;
    uint64_t burst_gap_us = 0;
};

struct ModelProfile {
    std::string format;
    std::string model;
    std::string source;
    std::string fingerprint;
    uint64_t key_bytes = 64;
    uint64_t object_bytes = 24576;
    uint64_t keys_per_clone = 0;
    double read_hit_ratio = 0.0;
    double prefix_probe_ratio = 0.0;
    double prefix_probe_hit_ratio = 0.0;
    double hot_key_fraction = 0.0;
    double hot_access_prob = 0.0;
    std::vector<WeightedSize> put_batch_pmf;
    std::vector<WeightedSize> get_batch_pmf;
    std::vector<WeightedSize> prefix_probe_pmf;
    ArrivalProfile arrival_us;
};

struct PlannedOp {
    int64_t t_us = 0;
    OpKind kind = OpKind::kPut;
    bool check_deadline = true;
    uint64_t start_idx = 0;
    uint64_t miss_start = 0;
    uint32_t hit_count = 0;
    uint32_t miss_count = 0;
};

struct WorkloadPlan {
    std::vector<PlannedOp> ops;
    uint64_t put_ops = 0;
    uint64_t get_ops = 0;
    uint64_t exist_ops = 0;
    uint64_t put_keys = 0;
    uint64_t get_keys = 0;
    uint64_t exist_keys = 0;
};

struct LatencySummary {
    uint64_t count = 0;
    double mean = 0.0;
    double p50 = 0.0;
    double p95 = 0.0;
    double p99 = 0.0;
};

double PercentileSorted(const std::vector<double>& values, double q) {
    if (values.empty()) {
        return 0.0;
    }
    const double index = q * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(index));
    const size_t upper = static_cast<size_t>(std::ceil(index));
    if (lower == upper) {
        return values[lower];
    }
    const double weight = index - static_cast<double>(lower);
    return values[lower] * (1.0 - weight) + values[upper] * weight;
}

LatencySummary Summarize(std::vector<double> values) {
    if (values.empty()) {
        return {};
    }
    const double sum = std::accumulate(values.begin(), values.end(), 0.0);
    std::sort(values.begin(), values.end());
    return LatencySummary{
        static_cast<uint64_t>(values.size()),
        sum / static_cast<double>(values.size()),
        PercentileSorted(values, 0.50),
        PercentileSorted(values, 0.95),
        PercentileSorted(values, 0.99),
    };
}

Json::Value LatencyJson(const LatencySummary& summary) {
    Json::Value value;
    value["count"] = Json::UInt64(summary.count);
    value["mean"] = summary.mean;
    value["p50"] = summary.p50;
    value["p95"] = summary.p95;
    value["p99"] = summary.p99;
    return value;
}

bool IsPlainFileName(const std::string& name) {
    if (name.empty() || name == "." || name == "..") {
        return false;
    }
    if (name.find('/') != std::string::npos ||
        name.find('\\') != std::string::npos ||
        name.find('\0') != std::string::npos) {
        return false;
    }
    const std::filesystem::path p(name);
    return !p.is_absolute() && !p.has_parent_path() && p.filename() == p;
}

// 64-bit FNV-1a hex fingerprint of the profile bytes for A/B plan parity.
std::string Fnv1aHex(std::string_view data) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char byte : data) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(16, '0');
    for (int i = 15; i >= 0; --i) {
        out[static_cast<size_t>(i)] = kHex[hash & 0x0F];
        hash >>= 4;
    }
    return out;
}

std::string ReadFileBounded(const std::filesystem::path& path,
                            size_t max_bytes) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open profile: " + path.string());
    }
    std::string data;
    char buffer[16 * 1024];
    while (input) {
        input.read(buffer, sizeof(buffer));
        const std::streamsize read = input.gcount();
        if (read > 0) {
            if (static_cast<size_t>(read) > max_bytes - data.size()) {
                throw std::runtime_error("profile exceeds size limit: " +
                                         path.string());
            }
            data.append(buffer, static_cast<size_t>(read));
        }
    }
    if (input.bad()) {
        throw std::runtime_error("I/O error while reading: " + path.string());
    }
    return data;
}

void FormatKey(std::string& out, const std::string& prefix, uint64_t key_bytes,
               uint64_t index) {
    out.assign(key_bytes, '0');
    std::copy(prefix.begin(), prefix.end(), out.begin());
    out[prefix.size()] = '/';
    char* p = out.data() + out.size();
    for (int d = 0; d < 20; ++d) {
        *--p = static_cast<char>('0' + (index % 10));
        index /= 10;
    }
}

std::string StringField(const Json::Value& value, const char* name) {
    if (!value.isObject() || !value.isMember(name) || !value[name].isString()) {
        throw std::runtime_error(std::string("profile is missing string ") +
                                 name);
    }
    return value[name].asString();
}

uint64_t UintField(const Json::Value& value, const char* name) {
    if (!value.isObject() || !value.isMember(name) || value[name].isBool() ||
        !value[name].isUInt64()) {
        throw std::runtime_error(std::string("profile is missing uint64 ") +
                                 name);
    }
    return value[name].asUInt64();
}

bool BoolField(const Json::Value& value, const char* name) {
    if (!value.isObject() || !value.isMember(name) || !value[name].isBool()) {
        throw std::runtime_error(std::string("profile is missing bool ") +
                                 name);
    }
    return value[name].asBool();
}

double ProbField(const Json::Value& value, const char* name) {
    if (!value.isObject() || !value.isMember(name) || value[name].isBool() ||
        !value[name].isNumeric()) {
        throw std::runtime_error(std::string("profile is missing number ") +
                                 name);
    }
    const double v = value[name].asDouble();
    if (!std::isfinite(v) || v < 0.0 || v > 1.0) {
        throw std::runtime_error(std::string("profile ") + name +
                                 " must be in [0, 1]");
    }
    return v;
}

std::vector<WeightedSize> PmfField(const Json::Value& value, const char* name) {
    if (!value.isObject() || !value.isMember(name) || !value[name].isArray() ||
        value[name].empty()) {
        throw std::runtime_error(std::string("profile is missing non-empty ") +
                                 name);
    }
    std::vector<WeightedSize> pmf;
    pmf.reserve(value[name].size());
    uint64_t total_weight = 0;
    for (const auto& entry : value[name]) {
        const uint64_t size = UintField(entry, "size");
        const uint64_t weight = UintField(entry, "weight");
        if (size == 0 || size > std::numeric_limits<uint32_t>::max() ||
            weight == 0 || weight > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error(std::string("invalid entry in ") + name);
        }
        total_weight += weight;
        pmf.push_back(WeightedSize{static_cast<uint32_t>(size),
                                   static_cast<uint32_t>(weight)});
    }
    if (total_weight == 0) {
        throw std::runtime_error(std::string("zero total weight in ") + name);
    }
    return pmf;
}

std::vector<std::string> ListAvailableModels(
    const std::filesystem::path& directory) {
    std::vector<std::string> models;
    std::error_code ec;
    if (!std::filesystem::is_directory(directory, ec)) {
        return models;
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(directory, ec)) {
        if (!entry.is_regular_file(ec)) {
            continue;
        }
        const auto& p = entry.path();
        if (p.extension() == ".json") {
            models.push_back(p.stem().string());
        }
    }
    std::sort(models.begin(), models.end());
    return models;
}

std::filesystem::path ResolveProfileDir(const std::string& requested) {
    if (!requested.empty()) {
        return std::filesystem::path(requested);
    }
    std::error_code ec;
    const std::filesystem::path rel("mooncake-store/benchmarks/profiles");
    if (std::filesystem::is_directory(rel, ec)) {
        return rel;
    }
#ifdef MOONCAKE_TRACE_BENCH_PROFILE_DIR
    const std::filesystem::path compiled(MOONCAKE_TRACE_BENCH_PROFILE_DIR);
    if (std::filesystem::is_directory(compiled, ec)) {
        return compiled;
    }
#endif
    return rel;
}

ModelProfile LoadProfile(const std::filesystem::path& directory,
                         const std::string& model) {
    if (!IsPlainFileName(model)) {
        throw std::runtime_error("--model must be a plain file name component");
    }
    const auto path = directory / (model + ".json");
    if (!std::filesystem::is_regular_file(path)) {
        const auto models = ListAvailableModels(directory);
        std::string hint;
        if (!models.empty()) {
            hint = "; available models in " + directory.string() + ": ";
            for (size_t i = 0; i < models.size(); ++i) {
                if (i > 0) {
                    hint += ", ";
                }
                hint += models[i];
            }
        }
        throw std::runtime_error("cannot open profile: " + path.string() +
                                 hint);
    }
    const std::string raw = ReadFileBounded(path, kMaxProfileBytes);

    Json::CharReaderBuilder builder;
    Json::Value value;
    std::string error;
    std::istringstream stream(raw);
    if (!Json::parseFromStream(builder, stream, &value, &error)) {
        throw std::runtime_error("invalid profile JSON: " + error);
    }

    ModelProfile profile;
    profile.format = StringField(value, "format");
    profile.model = StringField(value, "model");
    profile.source = StringField(value, "source");
    profile.fingerprint = Fnv1aHex(raw);
    if (!BoolField(value, "desensitized")) {
        throw std::runtime_error("profile is not marked desensitized");
    }
    if (profile.format != "mooncake-master-profile-v1") {
        throw std::runtime_error("unsupported profile format: " +
                                 profile.format);
    }
    if (profile.model != model) {
        throw std::runtime_error("profile model does not match --model");
    }

    profile.key_bytes = UintField(value, "key_bytes");
    profile.object_bytes = UintField(value, "object_bytes");
    profile.keys_per_clone = UintField(value, "keys_per_clone");
    if (profile.key_bytes < 24 || profile.object_bytes == 0 ||
        profile.keys_per_clone == 0) {
        throw std::runtime_error(
            "profile key_bytes must be >= 24; object_bytes and keys_per_clone "
            "must be > 0");
    }
    profile.read_hit_ratio = ProbField(value, "read_hit_ratio");
    profile.prefix_probe_ratio = ProbField(value, "prefix_probe_ratio");
    profile.prefix_probe_hit_ratio = ProbField(value, "prefix_probe_hit_ratio");
    profile.hot_key_fraction = ProbField(value, "hot_key_fraction");
    profile.hot_access_prob = ProbField(value, "hot_access_prob");
    if (profile.read_hit_ratio + profile.prefix_probe_ratio >= 0.95) {
        throw std::runtime_error(
            "read_hit_ratio + prefix_probe_ratio must be < 0.95");
    }
    if (profile.hot_key_fraction <= 0.0) {
        throw std::runtime_error("hot_key_fraction must be > 0");
    }

    profile.put_batch_pmf = PmfField(value, "put_batch_pmf");
    profile.get_batch_pmf = PmfField(value, "get_batch_pmf");
    profile.prefix_probe_pmf = PmfField(value, "prefix_probe_pmf");

    if (!value.isMember("arrival_us") || !value["arrival_us"].isObject()) {
        throw std::runtime_error("profile is missing arrival_us object");
    }
    const auto& arrival = value["arrival_us"];
    profile.arrival_us.intra_burst_p50 = UintField(arrival, "intra_burst_p50");
    profile.arrival_us.intra_burst_p99 = UintField(arrival, "intra_burst_p99");
    profile.arrival_us.burst_gap_prob = ProbField(arrival, "burst_gap_prob");
    profile.arrival_us.burst_gap_us = UintField(arrival, "burst_gap_us");
    if (profile.arrival_us.intra_burst_p99 <
        profile.arrival_us.intra_burst_p50) {
        throw std::runtime_error(
            "arrival_us.intra_burst_p99 must be >= intra_burst_p50");
    }
    return profile;
}

class WeightedSampler {
   public:
    explicit WeightedSampler(const std::vector<WeightedSize>& pmf) : pmf_(pmf) {
        std::vector<uint32_t> weights;
        weights.reserve(pmf.size());
        for (const auto& entry : pmf) {
            weights.push_back(entry.weight);
        }
        dist_ =
            std::discrete_distribution<size_t>(weights.begin(), weights.end());
    }

    uint32_t Sample(std::mt19937_64& rng) { return pmf_[dist_(rng)].size; }

   private:
    const std::vector<WeightedSize>& pmf_;
    std::discrete_distribution<size_t> dist_;
};

uint64_t DeriveCloneSeed(uint64_t base_seed, uint64_t clone) {
    if (clone == 0) {
        return base_seed;
    }
    // SplitMix64 mixing so adjacent clones get independent deterministic
    // streams without marching in lockstep on large prefix-probe batches.
    uint64_t z = base_seed + clone * 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

WorkloadPlan GeneratePlan(const ModelProfile& profile, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    WeightedSampler put_sampler(profile.put_batch_pmf);
    WeightedSampler get_sampler(profile.get_batch_pmf);
    WeightedSampler probe_sampler(profile.prefix_probe_pmf);

    const auto next_delta_us = [&]() -> int64_t {
        const auto& a = profile.arrival_us;
        uint64_t d = 0;
        if (unit(rng) < a.burst_gap_prob) {
            d = a.burst_gap_us;
        } else if (unit(rng) < 0.90) {
            const uint64_t hi = a.intra_burst_p50 * 2;
            d = hi == 0 ? 0
                        : std::uniform_int_distribution<uint64_t>(0, hi)(rng);
        } else {
            const uint64_t lo = a.intra_burst_p50;
            const uint64_t hi = std::max(lo, a.intra_burst_p99);
            d = std::uniform_int_distribution<uint64_t>(lo, hi)(rng);
        }
        return static_cast<int64_t>(std::min<uint64_t>(
            d, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
    };

    const auto pick_hot_start = [&](uint64_t live_keys,
                                    uint64_t span) -> uint64_t {
        if (live_keys <= span) {
            return 0;
        }
        const uint64_t max_start = live_keys - span;
        uint64_t hi = max_start;
        if (unit(rng) < profile.hot_access_prob) {
            const uint64_t hot_limit = static_cast<uint64_t>(std::ceil(
                static_cast<double>(live_keys) * profile.hot_key_fraction));
            hi = std::min(max_start, hot_limit);
        }
        return std::uniform_int_distribution<uint64_t>(0, hi)(rng);
    };

    WorkloadPlan plan;
    int64_t t_us = 0;
    uint64_t next_key = 0;
    const uint64_t miss_base = profile.keys_per_clone;

    const auto advance_time = [&]() {
        const int64_t delta = next_delta_us();
        if (delta > std::numeric_limits<int64_t>::max() - t_us) {
            throw std::runtime_error("generated timestamp overflow");
        }
        t_us += delta;
    };

    while (next_key < profile.keys_per_clone) {
        const double r = unit(rng);
        if (next_key > 0 && r < profile.prefix_probe_ratio) {
            advance_time();
            const uint32_t total = probe_sampler.Sample(rng);
            const uint32_t hits = static_cast<uint32_t>(std::min<uint64_t>(
                next_key, static_cast<uint64_t>(
                              std::llround(static_cast<double>(total) *
                                           profile.prefix_probe_hit_ratio))));
            const uint32_t misses = total - hits;
            const uint64_t start = pick_hot_start(next_key, hits);
            plan.ops.push_back(PlannedOp{t_us, OpKind::kExist, true, start,
                                         miss_base, hits, misses});
            ++plan.exist_ops;
            plan.exist_keys += total;
            continue;
        }

        if (next_key > 0 &&
            r < profile.prefix_probe_ratio + profile.read_hit_ratio) {
            advance_time();
            const uint32_t batch = static_cast<uint32_t>(
                std::min<uint64_t>(get_sampler.Sample(rng), next_key));
            const uint64_t start = pick_hot_start(next_key, batch);
            // Causal hit path: BatchExistKey (hit) -> BatchGetReplicaList.
            // Only the leading op is paced against t_us; the follow-up runs
            // immediately after BatchExistKey completes.
            plan.ops.push_back(
                PlannedOp{t_us, OpKind::kExist, true, start, 0, batch, 0});
            ++plan.exist_ops;
            plan.exist_keys += batch;

            plan.ops.push_back(
                PlannedOp{t_us, OpKind::kGet, false, start, 0, batch, 0});
            ++plan.get_ops;
            plan.get_keys += batch;
            continue;
        }

        advance_time();
        const uint32_t batch = static_cast<uint32_t>(std::min<uint64_t>(
            put_sampler.Sample(rng), profile.keys_per_clone - next_key));
        // Causal miss path: BatchExistKey (miss) -> BatchPutStart/End.
        plan.ops.push_back(
            PlannedOp{t_us, OpKind::kExist, true, 0, next_key, 0, batch});
        ++plan.exist_ops;
        plan.exist_keys += batch;

        plan.ops.push_back(
            PlannedOp{t_us, OpKind::kPut, false, next_key, 0, batch, 0});
        ++plan.put_ops;
        plan.put_keys += batch;
        next_key += batch;
    }
    return plan;
}

class Client {
   public:
    Client(const std::string& master, const std::string& name, uintptr_t base,
           uint64_t size)
        : client_(mooncake::generate_uuid()) {
        const auto ec = client_.Connect(master);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::runtime_error("connect failed: " +
                                     std::to_string(static_cast<int>(ec)));
        }
        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = base;
        segment_.size = size;
        segment_.te_endpoint = name;
        // ReMountSegment registers the segment and adds the client to
        // ok_client_, avoiding a redundant NEED_REMOUNT on the first Ping().
        const auto mounted = client_.ReMountSegment({segment_});
        if (!mounted.has_value()) {
            throw std::runtime_error(
                "mount failed: " +
                std::to_string(static_cast<int>(mounted.error())));
        }
    }

    ~Client() {
        if (remount_.valid()) {
            try {
                remount_.get();
            } catch (const std::exception& error) {
                LOG(WARNING) << "background remount failed on teardown: "
                             << error.what();
            } catch (...) {
                LOG(WARNING) << "background remount failed on teardown";
            }
        }
        (void)client_.UnmountSegment(segment_.id);
    }

    void Ping() {
        if (remount_.valid() && remount_.wait_for(std::chrono::seconds(0)) ==
                                    std::future_status::ready) {
            remount_.get();
            remount_ = std::future<void>();
        }
        const auto ping = client_.Ping();
        if (!ping.has_value()) {
            throw std::runtime_error(
                "ping failed: " +
                std::to_string(static_cast<int>(ping.error())));
        }
        if (ping->client_status == mooncake::ClientStatus::NEED_REMOUNT &&
            !remount_.valid()) {
            remount_ = std::async(std::launch::async, [this]() {
                const auto result = client_.ReMountSegment({segment_});
                if (!result.has_value()) {
                    throw std::runtime_error(
                        "remount failed: " +
                        std::to_string(static_cast<int>(result.error())));
                }
            });
        }
    }

    bool Put(const std::vector<std::string>& keys, uint64_t bytes,
             double* latency_us) {
        auto revoke = [this](const std::vector<std::string>& failed_keys) {
            if (failed_keys.empty()) {
                return;
            }
            try {
                const auto results = client_.BatchPutRevoke(
                    failed_keys, mooncake::ReplicaType::MEMORY);
                if (results.size() != failed_keys.size()) {
                    LOG_FIRST_N(WARNING, 20)
                        << "BatchPutRevoke returned " << results.size()
                        << " results for " << failed_keys.size() << " keys";
                    return;
                }
                for (const auto& result : results) {
                    if (!result.has_value()) {
                        LOG_FIRST_N(WARNING, 20)
                            << "BatchPutRevoke item failed: "
                            << static_cast<int>(result.error());
                    }
                }
            } catch (const std::exception& error) {
                LOG_FIRST_N(WARNING, 20)
                    << "BatchPutRevoke threw: " << error.what();
            } catch (...) {
                LOG_FIRST_N(WARNING, 20)
                    << "BatchPutRevoke threw unknown exception";
            }
        };

        std::vector<std::vector<uint64_t>> lengths(keys.size(), {bytes});
        std::vector<mooncake::ObjectMeta> metas;
        metas.reserve(keys.size());
        for (const auto& key : keys) {
            metas.emplace_back(key, std::nullopt);
        }
        mooncake::ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_.name};

        const auto start = std::chrono::steady_clock::now();
        std::vector<tl::expected<std::vector<mooncake::Replica::Descriptor>,
                                 mooncake::ErrorCode>>
            starts;
        try {
            starts = client_.BatchPutStart(keys, lengths, config);
        } catch (...) {
            revoke(keys);
            throw;
        }
        if (starts.size() != keys.size()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchPutStart size mismatch: got " << starts.size()
                << ", expected " << keys.size();
            revoke(keys);
            return false;
        }

        // Scan every item so keys allocated after the first failure are also
        // revoked rather than leaked on the Master.
        std::vector<std::string> allocated;
        allocated.reserve(keys.size());
        std::optional<mooncake::ErrorCode> first_start_error;
        for (size_t i = 0; i < keys.size(); ++i) {
            if (starts[i].has_value()) {
                allocated.push_back(keys[i]);
            } else if (!first_start_error.has_value()) {
                first_start_error = starts[i].error();
            }
        }
        if (allocated.size() != keys.size()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchPutStart failed (" << allocated.size() << "/"
                << keys.size() << " allocated), first error="
                << static_cast<int>(*first_start_error);
            revoke(allocated);
            return false;
        }

        std::vector<tl::expected<void, mooncake::ErrorCode>> ends;
        try {
            ends = client_.BatchPutEnd(metas, mooncake::ReplicaType::MEMORY);
        } catch (...) {
            revoke(allocated);
            throw;
        }
        *latency_us = std::chrono::duration<double, std::micro>(
                          std::chrono::steady_clock::now() - start)
                          .count();
        if (ends.size() != keys.size()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchPutEnd size mismatch: got " << ends.size()
                << ", expected " << keys.size();
            revoke(allocated);
            return false;
        }
        std::vector<std::string> failed;
        std::optional<mooncake::ErrorCode> first_end_error;
        for (size_t i = 0; i < ends.size(); ++i) {
            if (!ends[i].has_value()) {
                failed.push_back(keys[i]);
                if (!first_end_error.has_value()) {
                    first_end_error = ends[i].error();
                }
            }
        }
        if (!failed.empty()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchPutEnd failed for " << failed.size() << "/"
                << keys.size()
                << " keys, first error=" << static_cast<int>(*first_end_error);
            revoke(failed);
            return false;
        }
        return true;
    }

    bool Get(const std::vector<std::string>& keys, bool allow_eviction,
             double* latency_us, uint64_t* evicted_misses, uint64_t* found_keys,
             bool* transport_error) {
        const auto start = std::chrono::steady_clock::now();
        const auto results = client_.BatchGetReplicaList(keys);
        *latency_us = std::chrono::duration<double, std::micro>(
                          std::chrono::steady_clock::now() - start)
                          .count();
        *evicted_misses = 0;
        *found_keys = 0;
        *transport_error = false;
        if (results.size() != keys.size()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchGetReplicaList size mismatch: got " << results.size()
                << ", expected " << keys.size();
            *transport_error = true;
            return false;
        }
        for (const auto& r : results) {
            if (!r.has_value()) {
                if (allow_eviction &&
                    r.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND) {
                    ++*evicted_misses;
                    continue;
                }
                LOG_FIRST_N(WARNING, 20) << "BatchGetReplicaList item failed: "
                                         << static_cast<int>(r.error());
                *transport_error =
                    r.error() != mooncake::ErrorCode::OBJECT_NOT_FOUND;
                return false;
            }
            if (r->replicas.empty()) {
                if (allow_eviction) {
                    ++*evicted_misses;
                    continue;
                }
                LOG_FIRST_N(WARNING, 20)
                    << "BatchGetReplicaList returned empty replicas";
                return false;
            }
            ++*found_keys;
        }
        return true;
    }

    bool Exist(const std::vector<std::string>& keys, double* latency_us,
               uint64_t* not_found,
               std::vector<std::string>* found_keys = nullptr) {
        const auto start = std::chrono::steady_clock::now();
        const auto results = client_.BatchExistKey(keys);
        *latency_us = std::chrono::duration<double, std::micro>(
                          std::chrono::steady_clock::now() - start)
                          .count();
        if (results.size() != keys.size()) {
            LOG_FIRST_N(WARNING, 20)
                << "BatchExistKey size mismatch: got " << results.size()
                << ", expected " << keys.size();
            return false;
        }
        *not_found = 0;
        if (found_keys != nullptr) {
            found_keys->clear();
            found_keys->reserve(keys.size());
        }
        for (size_t i = 0; i < results.size(); ++i) {
            const auto& result = results[i];
            if (!result.has_value()) {
                LOG_FIRST_N(WARNING, 20) << "BatchExistKey item failed: "
                                         << static_cast<int>(result.error());
                return false;
            }
            if (!result.value()) {
                ++*not_found;
            } else if (found_keys != nullptr) {
                found_keys->push_back(keys[i]);
            }
        }
        return true;
    }

   private:
    mooncake::MasterClient client_;
    mooncake::Segment segment_;
    std::future<void> remount_;
};

}  // namespace

DEFINE_string(master, "127.0.0.1:50051", "Master server address");
DEFINE_string(model, "qwen3.5-9b", "Model name; selects profiles/<model>.json");
DEFINE_string(profile_dir, "",
              "Directory containing model profiles (auto-detected if empty)");
DEFINE_bool(list_models, false, "List available model profiles and exit");
DEFINE_uint64(seed, 42, "Deterministic PRNG seed for workload synthesis");
DEFINE_uint64(amplify, 1,
              "Number of independent clone streams; 94 is about 100M keys");
DEFINE_uint64(object_bytes, 0,
              "Allocated bytes per object; 0 uses the profile default");
DEFINE_uint64(rpc_timeout_ms, 60000,
              "Per-request Master RPC timeout; 0 leaves the environment value");
DEFINE_uint64(segment_bytes, 64 * GiB, "Capacity of each clone segment");
DEFINE_string(key_prefix, "tracebench", "Object key prefix");
DEFINE_string(segment_prefix, "tracebench", "Segment name prefix");
DEFINE_uint64(speed, 1,
              "Playback speed multiplier; 0 replays unpaced at max throughput");
DEFINE_uint64(clone_spread_us, 0,
              "Deterministic start offset between adjacent clones");
DEFINE_bool(
    allow_eviction, false,
    "Allow evicted keys on causal Exist->Get reads (for eviction-churn runs)");
DEFINE_uint64(progress_interval_sec, 30,
              "Progress log interval in seconds; 0 disables progress logs");
DEFINE_string(result_file, "", "Write the result JSON to this path");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    const auto profile_dir = ResolveProfileDir(FLAGS_profile_dir);
    if (FLAGS_list_models) {
        const auto models = ListAvailableModels(profile_dir);
        if (models.empty()) {
            LOG(ERROR) << "no model profiles found in " << profile_dir;
            return 2;
        }
        for (const auto& m : models) {
            std::cout << m << "\n";
        }
        return 0;
    }

    if (FLAGS_amplify == 0 || FLAGS_amplify > kMaxAmplify ||
        FLAGS_segment_bytes == 0 || FLAGS_key_prefix.empty() ||
        FLAGS_segment_prefix.empty()) {
        LOG(ERROR) << "amplify must be in [1, " << kMaxAmplify
                   << "]; segment_bytes and prefixes must be non-empty";
        return 2;
    }

    if (FLAGS_rpc_timeout_ms > 0) {
        setenv("MC_RPC_TIMEOUT_MS",
               std::to_string(FLAGS_rpc_timeout_ms).c_str(), 1);
    }

    ModelProfile profile;
    std::vector<WorkloadPlan> plans;
    try {
        profile = LoadProfile(profile_dir, FLAGS_model);
        const std::string max_prefix =
            FLAGS_key_prefix + "/" + std::to_string(FLAGS_amplify - 1);
        if (profile.key_bytes < max_prefix.size() + 21) {
            throw std::runtime_error("profile key_bytes (" +
                                     std::to_string(profile.key_bytes) +
                                     ") is too small for key_prefix");
        }
        plans.reserve(FLAGS_amplify);
        for (uint64_t clone = 0; clone < FLAGS_amplify; ++clone) {
            plans.push_back(
                GeneratePlan(profile, DeriveCloneSeed(FLAGS_seed, clone)));
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << error.what();
        return 2;
    }
    const uint64_t object_bytes =
        FLAGS_object_bytes > 0 ? FLAGS_object_bytes : profile.object_bytes;

    const uint64_t clone_keys = profile.keys_per_clone;
    const uint64_t max_u64 = std::numeric_limits<uint64_t>::max();
    if (clone_keys > max_u64 / FLAGS_amplify) {
        LOG(ERROR) << "amplified key count overflows uint64";
        return 2;
    }
    uint64_t planned_ops = 0;
    uint64_t planned_put_ops = 0;
    uint64_t planned_get_ops = 0;
    uint64_t planned_exist_ops = 0;
    for (const auto& plan : plans) {
        if (plan.ops.size() > max_u64 - planned_ops) {
            LOG(ERROR) << "amplified op count overflows uint64";
            return 2;
        }
        planned_ops += plan.ops.size();
        planned_put_ops += plan.put_ops;
        planned_get_ops += plan.get_ops;
        planned_exist_ops += plan.exist_ops;
    }
    const uint64_t keys_total = clone_keys * FLAGS_amplify;
    LOG(INFO) << "profile=" << profile.model << " (" << profile.fingerprint
              << "), seed=" << FLAGS_seed << ", planned_ops=" << planned_ops
              << " (put=" << planned_put_ops << ", get=" << planned_get_ops
              << ", exist=" << planned_exist_ops
              << "), keys/clone=" << clone_keys
              << ", key_bytes=" << profile.key_bytes
              << ", object_bytes=" << object_bytes
              << ", clones=" << FLAGS_amplify << ", total_keys=" << keys_total
              << ", speed=" << FLAGS_speed;

    if (FLAGS_amplify > (std::numeric_limits<uintptr_t>::max() - kSegmentBase) /
                            FLAGS_segment_bytes) {
        LOG(ERROR) << "segment address range overflows uintptr_t";
        return 2;
    }
    if (FLAGS_amplify > 1 &&
        FLAGS_clone_spread_us >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                (FLAGS_amplify - 1)) {
        LOG(ERROR) << "clone_spread_us overflows int64 microseconds";
        return 2;
    }
    std::vector<std::unique_ptr<Client>> clients;
    clients.reserve(FLAGS_amplify);
    try {
        for (uint64_t i = 0; i < FLAGS_amplify; ++i) {
            clients.push_back(std::make_unique<Client>(
                FLAGS_master,
                FLAGS_segment_prefix + "-" + std::to_string(getpid()) + "-" +
                    std::to_string(i),
                kSegmentBase + i * FLAGS_segment_bytes, FLAGS_segment_bytes));
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "cannot mount segments: " << error.what();
        return 1;
    }

    std::atomic<bool> stop{false};
    std::atomic<bool> heartbeat_failed{false};
    std::mutex hb_mutex;
    std::condition_variable hb_cv;
    std::atomic<uint64_t> put_ops{0}, get_ops{0}, exist_ops{0};
    std::atomic<uint64_t> put_keys{0}, get_keys{0}, exist_keys{0};
    std::atomic<uint64_t> exist_not_found{0}, evicted_misses{0}, skipped_ops{0};
    std::atomic<uint64_t> errors{0}, missing{0}, missed{0};

    const auto request_stop = [&]() {
        {
            std::lock_guard<std::mutex> lock(hb_mutex);
            stop.store(true);
        }
        hb_cv.notify_all();
    };

    constexpr size_t kMaxHeartbeatThreads = 8;
    const size_t hb_threads =
        std::min<size_t>(clients.size(), kMaxHeartbeatThreads);
    std::vector<std::thread> heartbeats;
    heartbeats.reserve(hb_threads);
    for (size_t shard = 0; shard < hb_threads; ++shard) {
        heartbeats.emplace_back([&, shard]() {
            auto last_progress = std::chrono::steady_clock::now();
            try {
                while (!stop.load(std::memory_order_relaxed)) {
                    for (size_t i = shard; i < clients.size();
                         i += hb_threads) {
                        if (stop.load(std::memory_order_relaxed)) {
                            return;
                        }
                        clients[i]->Ping();
                    }
                    if (shard == 0 && FLAGS_progress_interval_sec > 0) {
                        const auto now = std::chrono::steady_clock::now();
                        if (now - last_progress >=
                            std::chrono::seconds(FLAGS_progress_interval_sec)) {
                            last_progress = now;
                            const uint64_t pk =
                                put_keys.load(std::memory_order_relaxed);
                            const uint64_t done =
                                put_ops.load(std::memory_order_relaxed) +
                                get_ops.load(std::memory_order_relaxed) +
                                exist_ops.load(std::memory_order_relaxed) +
                                skipped_ops.load(std::memory_order_relaxed);
                            const double pct =
                                keys_total == 0
                                    ? 100.0
                                    : 100.0 * static_cast<double>(pk) /
                                          static_cast<double>(keys_total);
                            LOG(INFO)
                                << "progress: put_keys=" << pk << "/"
                                << keys_total << " (" << static_cast<int>(pct)
                                << "%), ops=" << done << "/" << planned_ops
                                << ", errors="
                                << errors.load(std::memory_order_relaxed);
                        }
                    }
                    std::unique_lock<std::mutex> lock(hb_mutex);
                    hb_cv.wait_for(lock, std::chrono::seconds(1),
                                   [&]() { return stop.load(); });
                }
            } catch (const std::exception& error) {
                LOG(ERROR) << "heartbeat failed: " << error.what();
                heartbeat_failed.store(true);
                request_stop();
            } catch (...) {
                LOG(ERROR) << "heartbeat failed with unknown exception";
                heartbeat_failed.store(true);
                request_stop();
            }
        });
    }

    std::vector<std::vector<double>> put_latencies(FLAGS_amplify);
    std::vector<std::vector<double>> get_latencies(FLAGS_amplify);
    std::vector<std::vector<double>> exist_latencies(FLAGS_amplify);
    for (uint64_t i = 0; i < FLAGS_amplify; ++i) {
        put_latencies[i].reserve(plans[i].put_ops);
        get_latencies[i].reserve(plans[i].get_ops);
        exist_latencies[i].reserve(plans[i].exist_ops);
    }

    // Lock-free interruptible sleep avoids contending all clones on hb_mutex
    // during paced (--speed >= 1) playback while still reacting within 50 ms.
    const auto sleep_until_or_stop =
        [&](std::chrono::steady_clock::time_point due) {
            constexpr auto kSlice = std::chrono::milliseconds(50);
            while (!stop.load(std::memory_order_relaxed)) {
                const auto now = std::chrono::steady_clock::now();
                if (now >= due) {
                    return;
                }
                std::this_thread::sleep_until(std::min(due, now + kSlice));
            }
        };

    const auto started = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_amplify);
    for (uint64_t clone = 0; clone < FLAGS_amplify; ++clone) {
        workers.emplace_back([&, clone]() {
            try {
                const std::string prefix =
                    FLAGS_key_prefix + "/" + std::to_string(clone);
                const auto clone_start =
                    started +
                    std::chrono::microseconds(clone * FLAGS_clone_spread_us);
                if (FLAGS_clone_spread_us > 0 && clone > 0) {
                    sleep_until_or_stop(clone_start);
                }
                const auto& clone_plan = plans[clone];
                std::vector<std::string> keys;
                std::vector<std::string> causal_found_keys;
                bool has_causal_found = false;
                for (size_t idx = 0; idx < clone_plan.ops.size(); ++idx) {
                    const auto& op = clone_plan.ops[idx];
                    if (stop.load()) {
                        break;
                    }
                    if (FLAGS_speed > 0 && op.check_deadline) {
                        const auto due =
                            clone_start +
                            std::chrono::microseconds(op.t_us / FLAGS_speed);
                        if (std::chrono::steady_clock::now() > due) {
                            missed.fetch_add(1, std::memory_order_relaxed);
                        } else {
                            sleep_until_or_stop(due);
                            if (stop.load()) {
                                break;
                            }
                        }
                    }

                    const uint64_t count =
                        static_cast<uint64_t>(op.hit_count) + op.miss_count;
                    double us = 0.0;
                    if (op.kind == OpKind::kPut) {
                        has_causal_found = false;
                        keys.resize(count);
                        for (uint32_t i = 0; i < op.hit_count; ++i) {
                            FormatKey(keys[i], prefix, profile.key_bytes,
                                      op.start_idx + i);
                        }
                        if (!clients[clone]->Put(keys, object_bytes, &us)) {
                            errors.fetch_add(1, std::memory_order_relaxed);
                            LOG(ERROR) << "clone " << clone
                                       << " put failed; stopping benchmark";
                            request_stop();
                            break;
                        } else {
                            put_ops.fetch_add(1, std::memory_order_relaxed);
                            put_keys.fetch_add(count,
                                               std::memory_order_relaxed);
                            put_latencies[clone].push_back(us);
                        }
                    } else if (op.kind == OpKind::kGet) {
                        const std::vector<std::string>* target_keys = &keys;
                        if (FLAGS_allow_eviction && has_causal_found) {
                            if (causal_found_keys.empty()) {
                                skipped_ops.fetch_add(
                                    1, std::memory_order_relaxed);
                                has_causal_found = false;
                                continue;
                            }
                            target_keys = &causal_found_keys;
                        } else {
                            keys.resize(count);
                            for (uint32_t i = 0; i < op.hit_count; ++i) {
                                FormatKey(keys[i], prefix, profile.key_bytes,
                                          op.start_idx + i);
                            }
                        }
                        has_causal_found = false;
                        uint64_t ev_miss = 0;
                        uint64_t got_keys = 0;
                        bool transport_error = false;
                        if (!clients[clone]->Get(
                                *target_keys, FLAGS_allow_eviction, &us,
                                &ev_miss, &got_keys, &transport_error)) {
                            if (transport_error) {
                                errors.fetch_add(1, std::memory_order_relaxed);
                                LOG(ERROR) << "clone " << clone
                                           << " get failed; stopping benchmark";
                                request_stop();
                                break;
                            } else {
                                missing.fetch_add(1, std::memory_order_relaxed);
                            }
                        } else {
                            get_ops.fetch_add(1, std::memory_order_relaxed);
                            get_keys.fetch_add(got_keys,
                                               std::memory_order_relaxed);
                            evicted_misses.fetch_add(ev_miss,
                                                     std::memory_order_relaxed);
                            get_latencies[clone].push_back(us);
                        }
                    } else {
                        keys.resize(count);
                        for (uint32_t i = 0; i < op.hit_count; ++i) {
                            FormatKey(keys[i], prefix, profile.key_bytes,
                                      op.start_idx + i);
                        }
                        for (uint32_t i = 0; i < op.miss_count; ++i) {
                            FormatKey(keys[op.hit_count + i], prefix,
                                      profile.key_bytes, op.miss_start + i);
                        }
                        const bool followed_by_get =
                            FLAGS_allow_eviction &&
                            idx + 1 < clone_plan.ops.size() &&
                            clone_plan.ops[idx + 1].kind == OpKind::kGet;
                        uint64_t not_found = 0;
                        if (!clients[clone]->Exist(keys, &us, &not_found,
                                                   followed_by_get
                                                       ? &causal_found_keys
                                                       : nullptr)) {
                            errors.fetch_add(1, std::memory_order_relaxed);
                            has_causal_found = false;
                            LOG(ERROR) << "clone " << clone
                                       << " exist failed; stopping benchmark";
                            request_stop();
                            break;
                        } else {
                            exist_ops.fetch_add(1, std::memory_order_relaxed);
                            exist_keys.fetch_add(count,
                                                 std::memory_order_relaxed);
                            exist_not_found.fetch_add(
                                not_found, std::memory_order_relaxed);
                            exist_latencies[clone].push_back(us);
                            has_causal_found = followed_by_get;
                        }
                    }
                }
            } catch (const std::exception& error) {
                LOG(ERROR) << "clone " << clone << " threw: " << error.what();
                errors.fetch_add(1, std::memory_order_relaxed);
                request_stop();
            } catch (...) {
                LOG(ERROR) << "clone " << clone << " threw unknown exception";
                errors.fetch_add(1, std::memory_order_relaxed);
                request_stop();
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    // Capture elapsed time immediately after replay completes so heartbeat
    // teardown never adds jitter to seconds or ops_per_second.
    const auto finished = std::chrono::steady_clock::now();
    request_stop();
    for (auto& hb : heartbeats) {
        hb.join();
    }
    if (heartbeat_failed.load()) {
        LOG(ERROR) << "heartbeat failed; benchmark result is incomplete";
        return 1;
    }

    const double seconds =
        std::chrono::duration<double>(finished - started).count();
    std::vector<double> put_samples;
    std::vector<double> get_samples;
    std::vector<double> exist_samples;
    put_samples.reserve(put_ops.load());
    get_samples.reserve(get_ops.load());
    exist_samples.reserve(exist_ops.load());
    for (const auto& samples : put_latencies) {
        put_samples.insert(put_samples.end(), samples.begin(), samples.end());
    }
    for (const auto& samples : get_latencies) {
        get_samples.insert(get_samples.end(), samples.begin(), samples.end());
    }
    for (const auto& samples : exist_latencies) {
        exist_samples.insert(exist_samples.end(), samples.begin(),
                             samples.end());
    }
    const auto put_summary = Summarize(std::move(put_samples));
    const auto get_summary = Summarize(std::move(get_samples));
    const auto exist_summary = Summarize(std::move(exist_samples));
    const uint64_t completed_ops =
        put_ops.load() + get_ops.load() + exist_ops.load();
    const uint64_t failed_ops = errors.load() + missing.load();

    Json::Value result;
    result["benchmark"] = "master_trace";
    result["model"] = profile.model;
    result["profile_hash"] = profile.fingerprint;
    result["seed"] = Json::UInt64(FLAGS_seed);
    result["master"] = FLAGS_master;
    result["amplify"] = Json::UInt64(FLAGS_amplify);
    result["object_bytes"] = Json::UInt64(object_bytes);
    result["key_bytes"] = Json::UInt64(profile.key_bytes);
    result["speed"] = Json::UInt64(FLAGS_speed);
    result["clone_spread_us"] = Json::UInt64(FLAGS_clone_spread_us);
    result["segment_bytes"] = Json::UInt64(FLAGS_segment_bytes);
    result["allow_eviction"] = FLAGS_allow_eviction;
    result["keys_per_clone"] = Json::UInt64(clone_keys);
    result["keys_planned"] = Json::UInt64(keys_total);
    result["keys_total"] = Json::UInt64(put_keys.load());
    result["planned_ops"] = Json::UInt64(planned_ops);
    result["completed_ops"] = Json::UInt64(completed_ops);
    result["skipped_ops"] = Json::UInt64(skipped_ops.load());
    result["failed_ops"] = Json::UInt64(failed_ops);
    result["seconds"] = seconds;
    result["ops_per_second"] =
        seconds == 0.0 ? 0.0 : static_cast<double>(completed_ops) / seconds;
    result["keys_per_second"] =
        seconds == 0.0 ? 0.0 : static_cast<double>(put_keys.load()) / seconds;
    result["put_ops"] = Json::UInt64(put_ops.load());
    result["get_ops"] = Json::UInt64(get_ops.load());
    result["exist_ops"] = Json::UInt64(exist_ops.load());
    result["put_keys"] = Json::UInt64(put_keys.load());
    result["get_keys"] = Json::UInt64(get_keys.load());
    result["exist_keys"] = Json::UInt64(exist_keys.load());
    result["exist_not_found"] = Json::UInt64(exist_not_found.load());
    result["evicted_misses"] = Json::UInt64(evicted_misses.load());
    result["errors"] = Json::UInt64(errors.load());
    result["missing"] = Json::UInt64(missing.load());
    result["missed_deadlines"] = Json::UInt64(missed.load());
    result["put_latency_us"] = LatencyJson(put_summary);
    result["get_latency_us"] = LatencyJson(get_summary);
    result["exist_latency_us"] = LatencyJson(exist_summary);

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    builder["precision"] = 6;
    const std::string body = Json::writeString(builder, result) + "\n";
    std::cout << body;
    if (!FLAGS_result_file.empty()) {
        std::ofstream output(FLAGS_result_file);
        output << body;
        output.flush();
        if (!output) {
            LOG(ERROR) << "cannot write " << FLAGS_result_file;
            return 1;
        }
    }
    return failed_ops == 0 && completed_ops + skipped_ops.load() == planned_ops
               ? 0
               : 1;
}
