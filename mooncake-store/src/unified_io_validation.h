#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_service.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

// Pure request/response validation helpers for the unified Client Put/Get
// entry points. Internal to Client; kept in a header so validation rules stay
// decoupled from RPC/transfer orchestration and can be unit tested directly.
namespace mooncake::detail {

struct UnifiedPutCapabilities {
    ErrorCode whole{ErrorCode::OK};
    ErrorCode range{ErrorCode::OK};
};

inline bool IsLegacyTcpProtoForced() {
    const char* tcp_proto = std::getenv("MC_TCP_PROTO");
    return tcp_proto != nullptr && std::strcmp(tcp_proto, "1") == 0;
}

inline UnifiedPutCapabilities ResolveUnifiedPutCapabilities(
    bool has_transfer_engine, std::string_view protocol, bool is_using_tent,
    bool object_checksum_enabled, bool legacy_tcp_proto_forced,
    const ReplicateConfig& config) {
    UnifiedPutCapabilities caps;
    if (!has_transfer_engine || protocol == "rpc_only" || protocol == "cxl" ||
        config.nof_replica_num != 0 || config.dfs_replica_num != 0) {
        caps.whole = ErrorCode::NOT_SUPPORTED;
    }
    if (caps.whole != ErrorCode::OK || object_checksum_enabled ||
        is_using_tent || (protocol == "tcp" && legacy_tcp_proto_forced)) {
        caps.range = ErrorCode::NOT_SUPPORTED;
    }
    return caps;
}

inline std::optional<uint64_t> ComputeSlicesLength(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }
    uint64_t length = 0;
    for (const auto& slice : slices) {
        if (!slice.ptr || slice.size == 0 ||
            slice.size > std::numeric_limits<int64_t>::max() - length) {
            return std::nullopt;
        }
        length += slice.size;
    }
    return length;
}

inline std::vector<uint64_t> BuildZeroBasedSliceOffsets(
    std::span<const Slice> slices) {
    std::vector<uint64_t> offsets;
    offsets.reserve(slices.size());
    uint64_t offset = 0;
    for (const auto& slice : slices) {
        offsets.push_back(offset);
        offset += slice.size;
    }
    return offsets;
}

inline tl::expected<uint64_t, ErrorCode> ValidatePutItem(
    const Client::PutItem& item, const UnifiedPutCapabilities& caps) {
    if (item.key.empty()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto length = ComputeSlicesLength(item.slices);
    if (!length) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const ErrorCode err = std::visit(
        [&](const auto& target) -> ErrorCode {
            using T = std::decay_t<decltype(target)>;
            if constexpr (std::is_same_v<T, WholeObject>) {
                return caps.whole;
            } else if constexpr (std::is_same_v<T, PartTarget>) {
                if (const auto part_err = ValidateSinglePartTarget(target);
                    part_err != ErrorCode::OK) {
                    return part_err;
                }
                return caps.whole;
            } else {
                static_assert(std::is_same_v<T, RangeTarget>);
                if (const auto bounds_err =
                        ValidateRangeBounds(target, *length);
                    bounds_err != ErrorCode::OK) {
                    return bounds_err;
                }
                return caps.range;
            }
        },
        item.target);
    if (err != ErrorCode::OK) {
        return tl::unexpected(err);
    }
    return *length;
}

inline ErrorCode ValidateGetItem(const Client::GetItem& item) {
    if (item.key.empty() || !ComputeSlicesLength(item.slices)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

// Extracts the sub-slices covering `[byte_offset, byte_offset + byte_len)`
// from a scatter-gather slice vector.
inline std::vector<Slice> ExtractSliceRange(std::span<const Slice> slices,
                                            uint64_t byte_offset,
                                            uint64_t byte_len) {
    std::vector<Slice> out;
    uint64_t cursor = 0;
    uint64_t remaining = byte_len;
    for (const auto& slice : slices) {
        if (remaining == 0) {
            break;
        }
        const uint64_t slice_end = cursor + slice.size;
        if (slice_end <= byte_offset) {
            cursor = slice_end;
            continue;
        }
        const uint64_t start_in_slice =
            byte_offset > cursor ? byte_offset - cursor : 0;
        const uint64_t take =
            std::min<uint64_t>(slice.size - start_in_slice, remaining);
        out.push_back(Slice{static_cast<char*>(slice.ptr) + start_in_slice,
                            static_cast<size_t>(take)});
        remaining -= take;
        cursor = slice_end;
    }
    return out;
}

struct PartReadSlicePlan {
    size_t part_index{0};
    uint64_t part_src_offset{0};
    std::vector<Slice> slices;
    bool full_part{false};
};

// Maps a caller's destination slices and object-level `src_offset` onto an
// ordered list of `PartReplicaInfo` entries returned by Master.
inline tl::expected<std::vector<PartReadSlicePlan>, ErrorCode>
PlanMultiPartReadSlices(std::span<const PartReplicaInfo> parts,
                        std::span<const Slice> slices, uint64_t src_offset) {
    if (parts.empty()) {
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }
    const auto req_len_opt = ComputeSlicesLength(slices);
    if (!req_len_opt) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t req_len = *req_len_opt;

    uint64_t total_size = 0;
    for (const auto& part : parts) {
        if (part.length == 0 ||
            part.length > std::numeric_limits<uint64_t>::max() - total_size) {
            return tl::unexpected(ErrorCode::INVALID_REPLICA);
        }
        total_size += part.length;
    }

    if (src_offset == 0) {
        if (req_len != total_size) {
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
    } else if (src_offset > total_size || req_len > total_size - src_offset) {
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }

    const uint64_t read_end = src_offset + req_len;
    std::vector<PartReadSlicePlan> plans;
    plans.reserve(parts.size());
    uint64_t part_start = 0;
    uint64_t dst_cursor = 0;
    for (size_t p = 0; p < parts.size(); ++p) {
        const uint64_t part_len = parts[p].length;
        const uint64_t part_end = part_start + part_len;
        const uint64_t lo = std::max(part_start, src_offset);
        const uint64_t hi = std::min(part_end, read_end);
        if (lo < hi) {
            const uint64_t seg_len = hi - lo;
            plans.push_back(PartReadSlicePlan{
                .part_index = p,
                .part_src_offset = lo - part_start,
                .slices = ExtractSliceRange(slices, dst_cursor, seg_len),
                .full_part = (lo == part_start && seg_len == part_len),
            });
            dst_cursor += seg_len;
        }
        part_start = part_end;
    }
    return plans;
}

// Validates Master-allocated replicas before issuing data transfers.
// RangeTarget over TCP requires TCP protocol v2 on the target segment so
// pre-sliced writes have completion visibility semantics.
template <typename LookupTcpProtoVersion>
inline ErrorCode ValidateAllocatedWriteReplicas(
    const std::vector<Replica::Descriptor>& replicas, uint64_t expected_length,
    bool is_range, LookupTcpProtoVersion&& lookup_tcp_proto_version) {
    if (replicas.empty()) {
        return ErrorCode::INVALID_REPLICA;
    }
    for (const auto& replica : replicas) {
        if (!replica.is_memory_replica()) {
            return is_range ? ErrorCode::INVALID_REPLICA
                            : ErrorCode::NOT_SUPPORTED;
        }
        const auto& buffer = replica.get_memory_descriptor().buffer_descriptor;
        if (buffer.size_ != expected_length) {
            return ErrorCode::INVALID_REPLICA;
        }
        if (is_range && buffer.protocol_ == "tcp") {
            const std::optional<int> proto_version =
                lookup_tcp_proto_version(buffer.transport_endpoint_);
            if (!proto_version || *proto_version < 2) {
                return ErrorCode::NOT_SUPPORTED;
            }
        }
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::detail
