#include "candidate_codecs.h"

#include <limits>
#include <stdexcept>

#include <nlohmann/json.hpp>
#include <xxhash.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "common/base64.h"

namespace mooncake::codec_bench {
namespace {
using Json = nlohmann::json;
static_assert(NLOHMANN_JSON_VERSION_MAJOR == 3 &&
                  NLOHMANN_JSON_VERSION_MINOR == 12 &&
                  NLOHMANN_JSON_VERSION_PATCH == 0,
              "Reproduce P01 with nlohmann/json 3.12.0");

std::vector<uint8_t> Pack(Format format, const Json& root) {
    if (format == Format::JsonControl) {
        const auto text = root.dump(-1, ' ', true);
        return std::vector<uint8_t>(text.begin(), text.end());
    }
    return format == Format::Cbor ? Json::to_cbor(root)
                                  : Json::to_msgpack(root);
}

uint32_t Checksum(Format format, const Json& root) {
    auto bytes = Pack(format, root);
    return XXH32(bytes.data(), bytes.size(), 0);
}

Json Body(Format format, const OpLogBatchRecord& batch) {
    Json entries = Json::array();
    for (const auto& entry : batch.entries) {
        Json payload = format == Format::JsonControl
                           ? Json(base64::Encode(entry.payload))
                           : Json::binary(std::vector<uint8_t>(
                                 entry.payload.begin(), entry.payload.end()));
        entries.push_back(
            Json::array({static_cast<uint64_t>(entry.op_type), entry.tenant_id,
                         entry.object_key, std::move(payload)}));
    }
    return Json::array({batch.schema_version, batch.batch_id, batch.first_seq,
                        batch.last_seq, std::move(entries)});
}

// Fixed arithmetic byte pattern: reproducible across standard-library versions.
std::string Payload(size_t bytes, size_t index, bool binary) {
    std::string result(bytes, 'x');
    for (size_t i = 0; i < bytes; ++i) {
        result[i] = static_cast<char>(binary ? (i * 73 + index * 19) % 256
                                             : 'a' + (i + index) % 26);
    }
    return result;
}
}  // namespace

const char* Name(Format format) {
    switch (format) {
        case Format::Json:
            return "json";
        case Format::JsonControl:
            return "json-control";
        case Format::Cbor:
            return "cbor";
        case Format::MessagePack:
            return "msgpack";
    }
    throw std::invalid_argument("unknown format");
}

Format ParseFormat(const std::string& name) {
    for (auto format : kFormats) {
        if (name == Name(format)) return format;
    }
    throw std::invalid_argument("unknown format: " + name);
}

std::string Encode(Format format, const OpLogBatchRecord& batch) {
    if (format == Format::Json) return EncodeOpLogBatchRecord(batch);
    std::string reason;
    if (!ValidateOpLogBatchRecordShape(batch, &reason)) {
        throw std::invalid_argument(reason);
    }
    auto root = Body(format, batch);
    root.push_back(Checksum(format, root));
    auto bytes = Pack(format, root);
    return std::string(bytes.begin(), bytes.end());
}

bool Decode(Format format, const std::string& wire, OpLogBatchRecord* batch) {
    if (format == Format::Json) return DecodeOpLogBatchRecord(wire, batch);
    if (batch == nullptr) return false;
    try {
        auto root = format == Format::JsonControl ? Json::parse(wire)
                    : format == Format::Cbor
                        ? Json::from_cbor(wire.begin(), wire.end())
                        : Json::from_msgpack(wire.begin(), wire.end());
        if (!root.is_array() || root.size() != 6 || !root[4].is_array()) {
            return false;
        }
        for (size_t i : {0, 1, 2, 3, 5}) {
            if (!root[i].is_number_unsigned()) return false;
        }
        if (root[0].get<uint64_t>() != kOpLogBatchRecordSchemaVersion ||
            root[5].get<uint64_t>() > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        OpLogBatchRecord decoded;
        decoded.batch_id = root[1].get<uint64_t>();
        decoded.first_seq = root[2].get<uint64_t>();
        decoded.last_seq = root[3].get<uint64_t>();
        const auto& entries = root[4];
        if (entries.empty() || decoded.first_seq == 0 ||
            decoded.last_seq < decoded.first_seq ||
            decoded.last_seq - decoded.first_seq != entries.size() - 1) {
            return false;
        }
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto& item = entries[i];
            if (!item.is_array() || item.size() != 4 ||
                !item[0].is_number_unsigned() || !item[1].is_string() ||
                !item[2].is_string() ||
                (format == Format::JsonControl ? !item[3].is_string()
                                               : !item[3].is_binary())) {
                return false;
            }
            const auto op = item[0].get<uint64_t>();
            if (op == 0 || op >= static_cast<uint64_t>(OpType::OP_TYPE_MAX)) {
                return false;
            }
            OpLogEntry entry;
            entry.sequence_id = decoded.first_seq + i;
            entry.op_type = static_cast<OpType>(op);
            entry.tenant_id = item[1].get<std::string>();
            entry.object_key = item[2].get<std::string>();
            if (format == Format::JsonControl) {
                const auto encoded = item[3].get<std::string>();
                entry.payload = base64::Decode(encoded);
                if (base64::Encode(entry.payload) != encoded) return false;
            } else {
                const auto& payload = item[3].get_binary();
                entry.payload.assign(payload.begin(), payload.end());
            }
            entry.checksum = ComputeOpLogChecksum(entry.payload);
            if (!ValidateOpLogBatchEntry(entry)) return false;
            decoded.entries.push_back(std::move(entry));
        }
        decoded.checksum = root[5].get<uint32_t>();
        root.erase(root.end() - 1);
        if (decoded.checksum != Checksum(format, root) ||
            !ValidateOpLogBatchRecordShape(decoded)) {
            return false;
        }
        *batch = std::move(decoded);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool Equivalent(const OpLogBatchRecord& lhs, const OpLogBatchRecord& rhs) {
    // Production JSON projection: sequence is implicit; runtime timestamps and
    // prefix hashes are intentionally not persisted by any of these formats.
    return EncodeOpLogBatchRecord(lhs) == EncodeOpLogBatchRecord(rhs);
}

std::vector<Workload> Workloads() {
    struct Spec {
        const char* name;
        size_t entries;
        size_t payload;
        size_t key;
        bool binary;
    };
    std::vector<Workload> workloads;
    for (const auto& spec : {Spec{"single_metadata", 1, 128, 32, false},
                             Spec{"metadata_batch", 32, 1024, 64, false},
                             Spec{"full_batch", 1024, 256, 64, false},
                             Spec{"key_heavy", 256, 128, 512, false},
                             Spec{"binary_payload", 64, 4096, 64, true},
                             Spec{"large_payload", 8, 65536, 64, true}}) {
        OpLogBatchRecord batch;
        batch.batch_id = 1;
        batch.first_seq = 1;
        batch.last_seq = spec.entries;
        for (size_t i = 0; i < spec.entries; ++i) {
            OpLogEntry entry;
            entry.sequence_id = i + 1;
            entry.op_type = static_cast<OpType>(i % 7 + 1);
            entry.tenant_id = "p01-tenant-" + std::to_string(i % 8);
            entry.object_key = "object-" + std::to_string(i) + "-";
            entry.object_key.resize(spec.key, 'k');
            entry.payload = Payload(spec.payload, i, spec.binary);
            entry.checksum = ComputeOpLogChecksum(entry.payload);
            batch.entries.push_back(std::move(entry));
        }
        workloads.push_back({spec.name, std::move(batch)});
    }
    for (auto& workload : TypedWorkloads())
        workloads.push_back(std::move(workload));
    return workloads;
}
}  // namespace mooncake::codec_bench
