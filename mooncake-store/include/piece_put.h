#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

// One producer's contiguous contribution. Source slices are concatenated in
// order at offset; they must remain registered and alive until PiecePut
// returns.
struct PiecePutItem {
    ObjectKey key;
    uint64_t total_size{0};
    uint64_t offset{0};
    std::string write_id;
    std::vector<Slice> slices;
};

// Contains no source pointers. A pending commit retries metadata only, never
// resubmits a write. Valid only for this Client and this Master incarnation.
struct PiecePutCommitHandle {
    std::string key;
    std::string tenant_id;
    UUID master_epoch{};
    uint64_t generation{0};
    uint64_t write_start_ticks{0};
    std::string write_id;
    UUID client_id{};
    uint64_t op_id{0};
    uint64_t offset{0};
    uint64_t length{0};
};
YLT_REFL(PiecePutCommitHandle, key, tenant_id, master_epoch, generation,
         write_start_ticks, write_id, client_id, op_id, offset, length);

struct PiecePutItemResult {
    bool piece_completed{false};
    bool object_published{false};
    std::optional<PiecePutCommitHandle> pending_commit;
};

struct PiecePutStartRequest {
    std::string key;
    uint64_t total_size{0};
    uint64_t offset{0};
    uint64_t length{0};
    std::string write_id;
    uint64_t op_id{0};
    ReplicateConfig config;
};
YLT_REFL(PiecePutStartRequest, key, total_size, offset, length, write_id, op_id,
         config);

struct PiecePutStartResponse {
    PiecePutCommitHandle handle;
    std::vector<Replica::Descriptor> replicas;
    bool piece_completed{false};
    uint64_t remaining_ms{0};
};
YLT_REFL(PiecePutStartResponse, handle, replicas, piece_completed,
         remaining_ms);

struct PiecePutFinishRequest {
    PiecePutCommitHandle handle;
    bool applied{false};
};
YLT_REFL(PiecePutFinishRequest, handle, applied);

struct PiecePutFinishResponse {
    bool piece_completed{false};
    bool object_published{false};
};
YLT_REFL(PiecePutFinishResponse, piece_completed, object_published);

}  // namespace mooncake
