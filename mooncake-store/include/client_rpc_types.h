#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include "types.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {

/**
 * @struct RemoteBufferDesc
 * @brief Describes a remote buffer location for RDMA transfer
 */
struct RemoteBufferDesc {
    std::string segment_endpoint;  // Target segment endpoint
    uintptr_t addr;                // Buffer address
    uint64_t size;                 // Buffer size in bytes
};

YLT_REFL(RemoteBufferDesc, segment_endpoint, addr, size);

/**
 * @struct RemoteReadRequest
 * @brief RPC request for reading remote data
 *
 * LIFETIME: `key` is a non-owning view. The caller must guarantee that the
 * string outlives tnis request AND all async tasks dispatched from it.
 */
struct RemoteReadRequest {
    std::string_view key;
    std::vector<RemoteBufferDesc>
        dest_buffers;  // Destination buffers on remote client
    std::optional<UUID> target_tier_id;
    std::optional<std::string> target_segment_group_id;
};

YLT_REFL(RemoteReadRequest, key, dest_buffers, target_tier_id,
         target_segment_group_id);

/**
 * @struct RemoteWriteRequest
 * @brief RPC request for writing remote data
 *
 * LIFETIME: `key` is a non-owning view. The caller must guarantee that the
 * string outlives tnis request AND all async tasks dispatched from it.
 */
struct RemoteWriteRequest {
    std::string_view key;
    std::vector<RemoteBufferDesc> src_buffers;
    std::optional<UUID> target_tier_id;
};

YLT_REFL(RemoteWriteRequest, key, src_buffers, target_tier_id);

/**
 * @struct BatchRemoteReadRequest
 * @brief Batch RPC request for reading multiple remote data objects
 *
 * LIFETIME: each element of `keys` is a non-owning view. The caller must
 * guarantee that all strings outlive this request and the RPC call.
 */
struct BatchRemoteReadRequest {
    std::vector<std::string_view> keys;
    std::vector<std::vector<RemoteBufferDesc>>
        dest_buffers_list;  // Destination buffers for each key
};

YLT_REFL(BatchRemoteReadRequest, keys, dest_buffers_list);

/**
 * @struct BatchRemoteWriteRequest
 * @brief Batch RPC request for writing multiple remote data objects
 *
 * LIFETIME: each element of `keys` is a non-owning view. The caller must
 * guarantee that all strings outlive this request and the RPC call.
 */
struct BatchRemoteWriteRequest {
    std::vector<std::string_view> keys;
    std::vector<std::vector<RemoteBufferDesc>>
        src_buffers_list;  // Source buffers for each key
    std::vector<std::optional<UUID>>
        target_tier_ids;  // Target tier IDs for each key
};

YLT_REFL(BatchRemoteWriteRequest, keys, src_buffers_list, target_tier_ids);

}  // namespace mooncake
