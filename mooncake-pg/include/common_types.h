// Common types shared by host and device code.
// Keep this header free of host-only facilities such as STL containers,
// exceptions, and PGResult so device translation units can include it directly.

#ifndef MOONCAKE_PG_COMMON_TYPES_H
#define MOONCAKE_PG_COMMON_TYPES_H

#include <cstddef>
#include <cstdint>

namespace mooncake {

// There are two rank namespaces that are easy to confuse:
//
//   * GlobalRank  - process-wide identifier, range 0 .. max_world_size-1.
//   * InGroupRank - group-local identifier, range 0 .. group_size-1. A
//                   GroupView maps it to GlobalRank through rank_order.
using GlobalRank = int32_t;
using InGroupRank = int32_t;

inline constexpr GlobalRank kInvalidGlobalRank = -1;
inline constexpr InGroupRank kInvalidInGroupRank = -1;

enum class OpType : uint8_t {
    Unknown = 0,
    Broadcast,
    AllReduce,
    AllGather,
    ReduceScatter,
    AllToAll,
    Barrier,
    Reduce,
    Gather,
    Scatter,
    Send,
    Recv,
};

enum class DataType : uint8_t {
    Int8 = 0,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float16,
    Float32,
    Float64,
    Bfloat16,
    Bool,
    Float8e4m3fn,
    Float8e5m2,
    Float8e4m3fnuz,
    Float8e5m2fnuz,
    Float8e8m0fnu,
};

// Returns zero for an invalid enum value. Public API validation is responsible
// for rejecting such values before using the result in size arithmetic.
inline constexpr std::size_t elementSize(DataType datatype) noexcept {
    switch (datatype) {
        case DataType::Int8:
        case DataType::Uint8:
        case DataType::Bool:
        case DataType::Float8e4m3fn:
        case DataType::Float8e5m2:
        case DataType::Float8e4m3fnuz:
        case DataType::Float8e5m2fnuz:
        case DataType::Float8e8m0fnu:
            return 1;
        case DataType::Int16:
        case DataType::Uint16:
        case DataType::Float16:
        case DataType::Bfloat16:
            return 2;
        case DataType::Int32:
        case DataType::Uint32:
        case DataType::Float32:
            return 4;
        case DataType::Int64:
        case DataType::Uint64:
        case DataType::Float64:
            return 8;
    }
    return 0;
}

enum class ReduceOp : uint8_t {
    Sum = 0,
    Avg = 1,
    Product = 2,
    Min = 3,
    Max = 4,
};

// Concrete GPU collective implementations.
enum class GpuCollectiveBackend : uint8_t {
    Legacy = 0,
    New = 1,
};

inline constexpr const char* gpuCollectiveBackendName(
    GpuCollectiveBackend backend) noexcept {
    switch (backend) {
        case GpuCollectiveBackend::Legacy:
            return "legacy";
        case GpuCollectiveBackend::New:
            return "new";
    }
    return "unknown";
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_COMMON_TYPES_H
