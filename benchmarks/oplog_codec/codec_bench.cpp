#include "candidate_codecs.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <stdexcept>

#include <nlohmann/json.hpp>
#include <xxhash.h>

using namespace mooncake;
using namespace mooncake::codec_bench;

namespace {
void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void Validate() {
    size_t checks = 0;
    for (const auto& workload : Workloads()) {
        Require(Encode(Format::JsonControl, workload.batch) ==
                    Encode(Format::Json, workload.batch),
                "JSON control wire differs from production JSON");
        ++checks;
        for (auto format : kFormats) {
            const auto wire = Encode(format, workload.batch);
            OpLogBatchRecord decoded;
            Require(Decode(format, wire, &decoded), "roundtrip decode");
            Require(Equivalent(workload.batch, decoded), "roundtrip equality");
            if (workload.name == "typed_replay_trace") {
                VerifyReplay(decoded);
                ++checks;
            }
            Require(!Decode(format, wire, nullptr), "null destination");
            Require(!Decode(format, wire.substr(0, wire.size() - 1), &decoded),
                    "truncated record accepted");
            auto corrupt = wire;
            corrupt.back() ^= 1;
            Require(!Decode(format, corrupt, &decoded), "corruption accepted");
            checks += 5;
        }
    }
    // Exercise wire integers beyond signed 64-bit and preserve NUL/high bytes.
    auto batch = Workloads().front().batch;
    batch.batch_id = UINT64_MAX;
    batch.first_seq = UINT64_MAX;
    batch.last_seq = UINT64_MAX;
    batch.entries.front().sequence_id = UINT64_MAX;
    batch.entries.front().payload = std::string("\0\xff\"\\", 4);
    // An empty tenant is accepted as the default tenant by production
    // validation.
    batch.entries.front().tenant_id = "";
    batch.entries.front().object_key = "quote\"-slash\\-\xe4\xb8\xad";
    Require(Encode(Format::JsonControl, batch) == Encode(Format::Json, batch),
            "JSON control escaping/checksum differs");
    ++checks;
    for (auto format : kFormats) {
        OpLogBatchRecord decoded;
        Require(Decode(format, Encode(format, batch), &decoded),
                "uint64 decode");
        Require(Equivalent(batch, decoded), "uint64 or binary payload changed");
        checks += 2;
    }
    // Reject well-formed candidate envelopes with wrong types, enum, version,
    // range, tenant, and checksum. Recompute the checksum for shape tests so
    // they cannot pass merely by rejecting stale checksums.
    using Json = nlohmann::json;
    for (auto format : kFormats) {
        const auto wire = Encode(format, Workloads().front().batch);
        auto root = format == Format::Cbor          ? Json::from_cbor(wire)
                    : format == Format::MessagePack ? Json::from_msgpack(wire)
                                                    : Json::parse(wire);
        root[5] = root[5].get<uint32_t>() ^ uint32_t { 1 };
        std::string corrupt;
        if (format == Format::Json || format == Format::JsonControl) {
            corrupt = root.dump(-1, ' ', true);
        } else {
            const auto bytes = format == Format::Cbor ? Json::to_cbor(root)
                                                      : Json::to_msgpack(root);
            corrupt.assign(bytes.begin(), bytes.end());
        }
        OpLogBatchRecord decoded;
        Require(!Decode(format, corrupt, &decoded),
                "well-formed record with wrong checksum accepted");
        ++checks;
    }
    for (auto format :
         {Format::JsonControl, Format::Cbor, Format::MessagePack}) {
        const auto wire = Encode(format, Workloads().front().batch);
        auto original = format == Format::JsonControl ? Json::parse(wire)
                        : format == Format::Cbor      ? Json::from_cbor(wire)
                                                 : Json::from_msgpack(wire);
        auto pack = [format](const Json& root) {
            if (format == Format::JsonControl) return root.dump(-1, ' ', true);
            const auto bytes = format == Format::Cbor ? Json::to_cbor(root)
                                                      : Json::to_msgpack(root);
            return std::string(bytes.begin(), bytes.end());
        };
        for (int mutation = 0; mutation < 7; ++mutation) {
            auto root = original;
            switch (mutation) {
                case 0:
                    root[0] = uint64_t{2};
                    break;
                case 1:
                    root[4][0][0] = uint64_t{256};
                    break;
                case 2:
                    root[3] = uint64_t{2};
                    break;
                case 3:
                    root[4][0][1] = std::string("bad\x01tenant");
                    break;
                case 4:
                    if (format == Format::JsonControl) {
                        root[4][0][3] = "invalid base64!";
                    } else {
                        root[4][0][3] = "not binary";
                    }
                    break;
                case 5:
                    root[1] = -1;
                    break;
                case 6:
                    root[1] = uint64_t{0};
                    break;
            }
            root.erase(root.end() - 1);
            const auto body = pack(root);
            root.push_back(XXH32(body.data(), body.size(), 0));
            const auto bytes = pack(root);
            OpLogBatchRecord decoded;
            if (Decode(format, bytes, &decoded)) {
                throw std::runtime_error(std::string(Name(format)) +
                                         " accepted invalid mutation " +
                                         std::to_string(mutation));
            }
            ++checks;
        }
    }
    std::cerr << "PASS: " << checks << " codec validation checks\n";
}
}  // namespace

int main(int argc, char** argv) {
    try {
        Validate();
        if (argc > 1 && std::string(argv[1]) == "--validate-only") return 0;
        const size_t iterations = argc > 1 ? std::stoul(argv[1]) : 100;
        const size_t repeats = argc > 2 ? std::stoul(argv[2]) : 5;
        Require(iterations > 0 && repeats > 0,
                "iterations/repeats must be positive");
        std::cout
            << "workload,format,repeat,entries,payload_bytes,wire_bytes,"
               "encode_cpu_us,decode_cpu_us,encode_wall_us,decode_wall_us,"
               "encode_batches_s,decode_batches_s\n";
        const auto workloads = Workloads();
        const auto formats = kFormats;
        uint64_t sink = 0;
        for (size_t repeat = 0; repeat < repeats; ++repeat) {
            for (const auto& workload : workloads) {
                for (size_t f = 0; f < formats.size(); ++f) {
                    auto format = formats[(f + repeat) % formats.size()];
                    const auto wire = Encode(format, workload.batch);
                    size_t payload = 0;
                    for (const auto& entry : workload.batch.entries) {
                        payload += entry.payload.size();
                    }
                    for (int warmup = 0; warmup < 5; ++warmup) {
                        OpLogBatchRecord decoded;
                        sink += Encode(format, workload.batch).size();
                        Require(Decode(format, wire, &decoded),
                                "warmup decode");
                    }
                    auto wall_begin = std::chrono::steady_clock::now();
                    auto cpu_begin = std::clock();
                    for (size_t i = 0; i < iterations; ++i) {
                        sink += Encode(format, workload.batch).size();
                    }
                    const double encode_cpu = 1e6 * (std::clock() - cpu_begin) /
                                              CLOCKS_PER_SEC / iterations;
                    const double encode_wall =
                        std::chrono::duration<double, std::micro>(
                            std::chrono::steady_clock::now() - wall_begin)
                            .count() /
                        iterations;
                    wall_begin = std::chrono::steady_clock::now();
                    cpu_begin = std::clock();
                    for (size_t i = 0; i < iterations; ++i) {
                        OpLogBatchRecord decoded;
                        Require(Decode(format, wire, &decoded), "timed decode");
                        sink += decoded.last_seq;
                    }
                    const double decode_cpu = 1e6 * (std::clock() - cpu_begin) /
                                              CLOCKS_PER_SEC / iterations;
                    const double decode_wall =
                        std::chrono::duration<double, std::micro>(
                            std::chrono::steady_clock::now() - wall_begin)
                            .count() /
                        iterations;
                    std::cout << workload.name << ',' << Name(format) << ','
                              << repeat << ',' << workload.batch.entries.size()
                              << ',' << payload << ',' << wire.size() << ','
                              << encode_cpu << ',' << decode_cpu << ','
                              << encode_wall << ',' << decode_wall << ','
                              << 1e6 / encode_wall << ',' << 1e6 / decode_wall
                              << '\n';
                }
            }
        }
        std::cerr << "sink=" << sink << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
