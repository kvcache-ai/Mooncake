#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <json/json.h>

#include "session_reader.h"

namespace {

using mooncake::traffic::Record;
using mooncake::traffic::RedactionName;
using mooncake::traffic::SessionReader;

struct UsageError {
    std::string message;
};

std::uint64_t Percentile(std::vector<std::uint64_t> values, double q) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t index =
        static_cast<std::size_t>(q * static_cast<double>(values.size() - 1));
    return values[index];
}

std::string JsonString(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (const char c : value) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

constexpr std::uint64_t kPayloadBuckets[] = {
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
constexpr std::size_t kBucketCount =
    sizeof(kPayloadBuckets) / sizeof(kPayloadBuckets[0]) + 1;

// routes.json maps a route key to the method name the server registered.
std::map<std::uint32_t, std::string> LoadRouteNames(const std::string& dir) {
    std::map<std::uint32_t, std::string> names;
    const std::string path = dir + "/routes.json";
    std::ifstream in(path);
    if (!in) {
        return names;
    }
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    if (!Json::parseFromStream(builder, in, &root, &errors)) {
        std::fprintf(stderr, "warning: cannot parse %s: %s\n", path.c_str(),
                     errors.c_str());
        return names;
    }
    for (const auto& key : root.getMemberNames()) {
        names.emplace(static_cast<std::uint32_t>(std::stoul(key)),
                      root[key].asString());
    }
    return names;
}

}  // namespace

int main(int argc, char** argv) {
    std::string dir;
    std::string out_path;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto value = [&](std::string* out) {
            if (i + 1 >= argc) {
                return false;
            }
            *out = argv[++i];
            return true;
        };
        if (arg == "--dir") {
            if (!value(&dir)) {
                std::fprintf(stderr, "--dir needs a value\n");
                return 2;
            }
        } else if (arg == "--out") {
            if (!value(&out_path)) {
                std::fprintf(stderr, "--out needs a value\n");
                return 2;
            }
        } else if (arg == "--help" || arg == "-h") {
            std::printf(
                "usage: traffic_profile --dir <session-dir> [--out <file>]\n"
                "\n"
                "Reads the request headers a traffic session recorded and "
                "prints the workload it contains: the route mix, the payload "
                "sizes, the arrival spacing and the connection count. No "
                "request payload is decoded, so one profile works for every "
                "route and every redaction mode.\n");
            return 0;
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            return 2;
        }
    }
    if (dir.empty()) {
        std::fprintf(stderr, "usage: traffic_profile --dir <session-dir>\n");
        return 2;
    }

    SessionReader reader;
    std::string error;
    if (!reader.Open(dir, &error)) {
        std::fprintf(stderr, "%s\n", error.c_str());
        return 1;
    }

    std::unordered_map<std::uint32_t, std::uint64_t> route_records;
    std::unordered_map<std::uint32_t, std::uint64_t> route_bytes;
    std::unordered_set<std::uint64_t> connections;
    std::vector<std::uint64_t> payload_sizes;
    std::vector<std::uint64_t> arrival_ms;
    std::uint64_t buckets[kBucketCount] = {0};

    std::uint64_t records = 0;
    std::uint64_t bytes = 0;
    // The recorder drains shards in turn, so the order inside the segments is
    // not the arrival order; every timestamp is kept and sorted below.
    std::vector<std::uint64_t> timestamps;

    Record record;
    while (true) {
        const auto next = reader.Next(&record, &error);
        if (next == SessionReader::NextResult::kError) {
            std::fprintf(stderr, "%s\n", error.c_str());
            return 1;
        }
        if (next == SessionReader::NextResult::kDone) {
            break;
        }
        ++records;
        bytes += record.payload.size();
        ++route_records[record.header.route_key];
        route_bytes[record.header.route_key] += record.payload.size();
        connections.insert(record.header.conn_id);
        payload_sizes.push_back(record.payload.size());

        std::size_t bucket = 0;
        while (bucket < kBucketCount - 1 &&
               record.payload.size() > kPayloadBuckets[bucket]) {
            ++bucket;
        }
        ++buckets[bucket];

        timestamps.push_back(record.header.timestamp_ms);
    }

    if (records == 0) {
        std::fprintf(stderr, "no records in %s\n", dir.c_str());
        return 1;
    }

    std::sort(timestamps.begin(), timestamps.end());
    for (std::size_t index = 1; index < timestamps.size(); ++index) {
        const std::uint64_t step = timestamps[index] - timestamps[index - 1];
        if (step > 0) {
            arrival_ms.push_back(step);
        }
    }
    const auto duration_ms = timestamps.back() - timestamps.front();
    const auto route_names = LoadRouteNames(dir);

    std::vector<std::pair<std::uint64_t, std::uint32_t>> ordered;
    ordered.reserve(route_records.size());
    for (const auto& [key, count] : route_records) {
        ordered.emplace_back(count, key);
    }
    std::sort(ordered.begin(), ordered.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::FILE* out = stdout;
    std::FILE* close_out = nullptr;
    if (!out_path.empty()) {
        out = std::fopen(out_path.c_str(), "w");
        if (out == nullptr) {
            std::fprintf(stderr, "cannot open %s: %s\n", out_path.c_str(),
                         std::strerror(errno));
            return 1;
        }
        close_out = out;
    }

    std::fprintf(out, "{\n");
    std::fprintf(out, "  \"session\": %s,\n", JsonString(dir).c_str());
    std::fprintf(out, "  \"redaction\": \"%s\",\n",
                 RedactionName(reader.redaction()));
    std::fprintf(out, "  \"incomplete\": %s,\n",
                 reader.incomplete() ? "true" : "false");
    std::fprintf(out, "  \"segments\": %llu,\n",
                 static_cast<unsigned long long>(reader.segment_count()));
    std::fprintf(out, "  \"blocks\": %llu,\n",
                 static_cast<unsigned long long>(reader.block_count()));
    std::fprintf(out, "  \"records\": %llu,\n",
                 static_cast<unsigned long long>(records));
    std::fprintf(out, "  \"request_bytes\": %llu,\n",
                 static_cast<unsigned long long>(bytes));
    std::fprintf(out, "  \"duration_ms\": %llu,\n",
                 static_cast<unsigned long long>(duration_ms));
    std::fprintf(out, "  \"records_per_second\": %.1f,\n",
                 duration_ms == 0 ? 0.0
                                  : 1000.0 * static_cast<double>(records) /
                                        static_cast<double>(duration_ms));
    std::fprintf(out, "  \"connections\": %zu,\n", connections.size());
    // A trace that exercises five routes out of the master's full set is a
    // canary, not a profile of the workload; the count makes that visible.
    std::fprintf(out, "  \"route_count\": %zu,\n", route_records.size());
    std::fprintf(out,
                 "  \"scope\": \"master control plane only; KV data does not "
                 "reach the master\",\n");
    std::fprintf(out,
                 "  \"arrival_ms\": {\"p50\": %llu, \"p90\": %llu, "
                 "\"p99\": %llu, \"max\": %llu},\n",
                 static_cast<unsigned long long>(Percentile(arrival_ms, 0.50)),
                 static_cast<unsigned long long>(Percentile(arrival_ms, 0.90)),
                 static_cast<unsigned long long>(Percentile(arrival_ms, 0.99)),
                 static_cast<unsigned long long>(
                     arrival_ms.empty() ? 0
                                        : *std::max_element(arrival_ms.begin(),
                                                            arrival_ms.end())));
    std::fprintf(
        out,
        "  \"payload_bytes\": {\"p50\": %llu, \"p90\": %llu, "
        "\"p99\": %llu, \"max\": %llu},\n",
        static_cast<unsigned long long>(Percentile(payload_sizes, 0.50)),
        static_cast<unsigned long long>(Percentile(payload_sizes, 0.90)),
        static_cast<unsigned long long>(Percentile(payload_sizes, 0.99)),
        static_cast<unsigned long long>(
            *std::max_element(payload_sizes.begin(), payload_sizes.end())));
    std::fprintf(out, "  \"payload_histogram\": {");
    for (std::size_t i = 0; i < kBucketCount; ++i) {
        if (i > 0) {
            std::fprintf(out, ", ");
        }
        if (i + 1 == kBucketCount) {
            std::fprintf(out, "\">%llu\": %llu",
                         static_cast<unsigned long long>(
                             kPayloadBuckets[kBucketCount - 2]),
                         static_cast<unsigned long long>(buckets[i]));
        } else {
            std::fprintf(out, "\"<=%llu\": %llu",
                         static_cast<unsigned long long>(kPayloadBuckets[i]),
                         static_cast<unsigned long long>(buckets[i]));
        }
    }
    std::fprintf(out, "},\n");
    std::fprintf(out, "  \"routes\": [");
    bool first = true;
    for (const auto& [count, key] : ordered) {
        const auto it = route_names.find(key);
        std::fprintf(out,
                     "%s\n    {\"key\": %u, \"name\": %s, "
                     "\"records\": %llu, \"bytes\": %llu, "
                     "\"share\": %.4f}",
                     first ? "" : ",", key,
                     it == route_names.end() ? std::string("null").c_str()
                                             : JsonString(it->second).c_str(),
                     static_cast<unsigned long long>(count),
                     static_cast<unsigned long long>(route_bytes[key]),
                     static_cast<double>(count) / static_cast<double>(records));
        first = false;
    }
    std::fprintf(out, "\n  ]\n}\n");

    if (close_out != nullptr) {
        std::fclose(close_out);
    }
    return 0;
}
