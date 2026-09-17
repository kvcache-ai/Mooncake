// P02 reader benchmark: measures the real DecodeOpLogBatchRecord entry point,
// including format dispatch, bounded validation, and the canonical logical
// checksum. It is a test-only target; it does not alter production code.
//
// Usage:
//   oplog_batch_reader_bench --format=json|binary --corpus=small|key|binary
//                             [--iterations=N] [--warmup=N] [--repeats=N]
//                             [--csv=PATH] [--label=TAG]
//
// Output: one CSV row per repeat plus a summary line per (format, corpus).

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include <xxhash.h>

#include "ha/oplog/oplog_batch_binary_codec.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"

namespace {

// main() lives outside this anonymous namespace, so the logical types it uses
// are brought in explicitly.
using mooncake::OpLogBatchRecord;
using mooncake::OpLogEntry;
using mooncake::OpType;

struct Options {
    std::string format{"json"};
    std::string corpus{"small"};
    int iterations{2000};
    int warmup{200};
    int repeats{5};
    std::string csv_path;
    std::string label;
};

struct CorpusStats {
    std::string name;
    size_t wire_bytes{0};
    size_t entry_count{0};
    size_t payload_bytes{0};
    size_t key_bytes{0};
};

OpLogEntry MakeEntry(uint64_t seq, OpType type, std::string key,
                     std::string payload) {
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 0;
    entry.op_type = type;
    entry.tenant_id = "default";
    entry.object_key = std::move(key);
    entry.payload = std::move(payload);
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash = 0;
    return entry;
}

std::string RandomBytes(std::mt19937_64& rng, size_t size) {
    std::uniform_int_distribution<int> dist(0, 255);
    std::string out(size, '\0');
    for (auto& ch : out) {
        ch = static_cast<char>(dist(rng));
    }
    return out;
}

std::vector<OpLogBatchRecord> BuildCorpus(const std::string& name) {
    std::mt19937_64 rng(0xb0a4d1e5ULL);
    std::vector<OpLogBatchRecord> batches;

    if (name == "small") {
        // Producer metadata: a few small entries per batch.
        for (uint64_t batch_id = 1; batch_id <= 64; ++batch_id) {
            std::vector<OpLogEntry> entries;
            const uint64_t first = (batch_id - 1) * 4 + 1;
            for (uint64_t i = 0; i < 4; ++i) {
                entries.push_back(MakeEntry(
                    first + i, OpType::PUT_END,
                    "obj/" + std::to_string(batch_id) + "/" + std::to_string(i),
                    "v" + std::to_string(batch_id * 100 + i)));
            }
            OpLogBatchRecord batch;
            batch.batch_id = batch_id;
            batch.first_seq = entries.front().sequence_id;
            batch.last_seq = entries.back().sequence_id;
            batch.entries = std::move(entries);
            batches.push_back(std::move(batch));
        }
        return batches;
    }

    if (name == "key") {
        // Key-heavy: long keys, small payloads.
        for (uint64_t batch_id = 1; batch_id <= 32; ++batch_id) {
            std::vector<OpLogEntry> entries;
            const uint64_t first = (batch_id - 1) * 4 + 1;
            for (uint64_t i = 0; i < 4; ++i) {
                std::string key =
                    "objects/tenant-a/prefix-" + std::to_string(batch_id) +
                    "/" + std::to_string(i) + "/" + std::string(3800, 'k');
                entries.push_back(
                    MakeEntry(first + i, OpType::PUT_END, std::move(key), "v"));
            }
            OpLogBatchRecord batch;
            batch.batch_id = batch_id;
            batch.first_seq = entries.front().sequence_id;
            batch.last_seq = entries.back().sequence_id;
            batch.entries = std::move(entries);
            batches.push_back(std::move(batch));
        }
        return batches;
    }

    if (name == "binary") {
        // Payload-heavy: opaque bytes resembling serialized resources.
        for (uint64_t batch_id = 1; batch_id <= 8; ++batch_id) {
            std::vector<OpLogEntry> entries;
            const uint64_t first = (batch_id - 1) * 4 + 1;
            for (uint64_t i = 0; i < 4; ++i) {
                entries.push_back(MakeEntry(
                    first + i, OpType::PUT_END,
                    "seg/" + std::to_string(batch_id) + "/" + std::to_string(i),
                    RandomBytes(rng, 64 * 1024)));
            }
            OpLogBatchRecord batch;
            batch.batch_id = batch_id;
            batch.first_seq = entries.front().sequence_id;
            batch.last_seq = entries.back().sequence_id;
            batch.entries = std::move(entries);
            batches.push_back(std::move(batch));
        }
        return batches;
    }

    std::fprintf(stderr, "unknown corpus: %s\n", name.c_str());
    std::exit(2);
}

double Median(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const size_t mid = values.size() / 2;
    if (values.size() % 2 == 1) {
        return values[mid];
    }
    return (values[mid - 1] + values[mid]) / 2.0;
}

double Percentile(std::vector<double> values, double fraction) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double index = fraction * static_cast<double>(values.size() - 1);
    const size_t lo = static_cast<size_t>(index);
    const size_t hi = std::min(lo + 1, values.size() - 1);
    const double weight = index - static_cast<double>(lo);
    return values[lo] * (1.0 - weight) + values[hi] * weight;
}

Options ParseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto value_of = [&arg](const char* name) -> std::string {
            const std::string prefix = std::string("--") + name + "=";
            if (arg.rfind(prefix, 0) == 0) {
                return arg.substr(prefix.size());
            }
            return {};
        };
        if (arg.rfind("--format=", 0) == 0) {
            options.format = value_of("format");
        } else if (arg.rfind("--corpus=", 0) == 0) {
            options.corpus = value_of("corpus");
        } else if (arg.rfind("--iterations=", 0) == 0) {
            options.iterations = std::atoi(value_of("iterations").c_str());
        } else if (arg.rfind("--warmup=", 0) == 0) {
            options.warmup = std::atoi(value_of("warmup").c_str());
        } else if (arg.rfind("--repeats=", 0) == 0) {
            options.repeats = std::atoi(value_of("repeats").c_str());
        } else if (arg.rfind("--csv=", 0) == 0) {
            options.csv_path = value_of("csv");
        } else if (arg.rfind("--label=", 0) == 0) {
            options.label = value_of("label");
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            std::exit(2);
        }
    }
    return options;
}

}  // namespace

int main(int argc, char** argv) {
    const Options options = ParseOptions(argc, argv);
    const auto batches = BuildCorpus(options.corpus);

    std::vector<std::string> wires;
    wires.reserve(batches.size());
    CorpusStats stats;
    stats.name = options.corpus;
    for (const auto& batch : batches) {
        const bool binary = options.format == "binary";
        wires.push_back(
            binary ? mooncake::EncodeOpLogBatchRecordBinaryForTest(batch)
                   : mooncake::EncodeOpLogBatchRecord(batch));
        stats.entry_count += batch.entries.size();
        for (const auto& entry : batch.entries) {
            stats.payload_bytes += entry.payload.size();
            stats.key_bytes += entry.object_key.size();
        }
    }
    for (const auto& wire : wires) {
        stats.wire_bytes += wire.size();
    }

    // Warm up and confirm every sample decodes; a decode failure would make the
    // timing meaningless.
    for (int i = 0; i < options.warmup; ++i) {
        const auto& wire = wires[static_cast<size_t>(i) % wires.size()];
        OpLogBatchRecord record;
        std::string reason;
        if (!mooncake::DecodeOpLogBatchRecord(wire, &record, &reason)) {
            std::fprintf(stderr, "warmup decode failed: %s\n", reason.c_str());
            return 1;
        }
    }

    FILE* csv = nullptr;
    if (!options.csv_path.empty()) {
        csv = std::fopen(options.csv_path.c_str(), "a");
        if (csv == nullptr) {
            std::fprintf(stderr, "cannot open csv: %s\n",
                         options.csv_path.c_str());
            return 1;
        }
    }

    const std::string label = options.label.empty()
                                  ? (options.format + "-" + options.corpus)
                                  : options.label;
    std::vector<double> mb_per_s;
    std::vector<double> ns_per_batch;
    std::vector<double> ns_per_byte;

    for (int repeat = 0; repeat < options.repeats; ++repeat) {
        uint64_t checksum = 0;
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < options.iterations; ++i) {
            const auto& wire = wires[static_cast<size_t>(i) % wires.size()];
            OpLogBatchRecord record;
            std::string reason;
            if (!mooncake::DecodeOpLogBatchRecord(wire, &record, &reason)) {
                std::fprintf(stderr, "decode failed: %s\n", reason.c_str());
                if (csv != nullptr) {
                    std::fclose(csv);
                }
                return 1;
            }
            checksum += record.checksum + record.entries.front().sequence_id;
        }
        const auto end = std::chrono::steady_clock::now();
        const double seconds =
            std::chrono::duration<double>(end - start).count();
        const size_t decoded_batches = static_cast<size_t>(options.iterations);
        const double batches_per_second = decoded_batches / seconds;
        const double bytes_per_second = batches_per_second *
                                        static_cast<double>(stats.wire_bytes) /
                                        static_cast<double>(wires.size());
        const double repeat_mb_per_s = bytes_per_second / (1024.0 * 1024.0);
        const double repeat_ns_per_batch = seconds * 1e9 / decoded_batches;
        const double repeat_ns_per_byte =
            seconds * 1e9 / (bytes_per_second * seconds);

        mb_per_s.push_back(repeat_mb_per_s);
        ns_per_batch.push_back(repeat_ns_per_batch);
        ns_per_byte.push_back(repeat_ns_per_byte);

        if (csv != nullptr) {
            std::fprintf(
                csv,
                "%s,%s,%s,%d,%d,%.6f,%.3f,%.3f,%.6f,%zu,%zu,%zu,%zu,%llu\n",
                label.c_str(), options.format.c_str(), options.corpus.c_str(),
                repeat, options.iterations, seconds, repeat_mb_per_s,
                repeat_ns_per_batch, repeat_ns_per_byte, stats.wire_bytes,
                stats.entry_count, stats.payload_bytes, stats.key_bytes,
                static_cast<unsigned long long>(checksum));
        }
    }
    if (csv != nullptr) {
        std::fclose(csv);
    }

    std::printf(
        "label=%s format=%s corpus=%s repeats=%d iterations=%d "
        "batches=%zu wire_bytes=%zu entries=%zu payload_bytes=%zu "
        "key_bytes=%zu mb_s_median=%.3f mb_s_p10=%.3f mb_s_p90=%.3f "
        "ns_batch_median=%.3f\n",
        label.c_str(), options.format.c_str(), options.corpus.c_str(),
        options.repeats, options.iterations, wires.size(), stats.wire_bytes,
        stats.entry_count, stats.payload_bytes, stats.key_bytes,
        Median(mb_per_s), Percentile(mb_per_s, 0.10),
        Percentile(mb_per_s, 0.90), Median(ns_per_batch));
    return 0;
}
