#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

// Capsule contract shared by two private Python extensions; does not depend
// on libTorch.
inline constexpr const char* kEngramMappedTableCapsule =
    "mooncake.engram.mapped_table.v1";
struct EngramMappedTable {
    const uint8_t* rows;
    int64_t count;
    size_t width;
    int device;
    std::shared_ptr<void> owner;
};
