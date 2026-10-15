#pragma once

// Writer of the shutdown handoff snapshot.

#include <cstddef>
#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "handoff/handoff_format.h"

namespace mooncake {

class MasterService;

struct HandoffWriteOptions {
    // Directory to write into. Created if missing; must not already hold a
    // handoff manifest.
    std::string dir;
    // Shard-parallel export + encode + write workers.
    size_t workers{4};
    // Durable writes: sync_file_range per chunk, fdatasync, directory fsync,
    // manifest via tmp + fsync + rename. Tests may turn this off.
    bool durable{true};
};

struct HandoffWriteResult {
    handoff::WriterStats stats;
    uint64_t segments_ns{0};
    uint64_t chunks_wall_ns{0};
    uint64_t export_cpu_ns{0};
    uint64_t encode_cpu_ns{0};
    uint64_t write_cpu_ns{0};
    uint64_t tail_ns{0};  // segments, weights, quarantine, manifests
    uint64_t total_ns{0};
    uint64_t chunks{0};
    std::string ToJson() const;
};

// Exports `service` into a handoff snapshot. The caller must have stopped the
// RPC server and quiesced the background work, so that no allocation or free
// happens during the export.
tl::expected<HandoffWriteResult, std::string> WriteHandoffSnapshot(
    MasterService& service, const HandoffWriteOptions& options);

}  // namespace mooncake
