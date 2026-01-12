#pragma once

#include <string>
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
    std::string segment_name;  // Target segment name
    uintptr_t addr;            // Buffer address
    uint64_t size;             // Buffer size in bytes
};

YLT_REFL(RemoteBufferDesc, segment_name, addr, size);

/**
 * @struct RemoteReadRequest
 * @brief RPC request for reading remote data
 */
struct RemoteReadRequest {
    std::string key;  // Object key to read
    std::vector<RemoteBufferDesc>
        dest_buffers;  // Destination buffers on remote client
};

YLT_REFL(RemoteReadRequest, key, dest_buffers);

/**
 * @struct RemoteWriteRequest
 * @brief RPC request for writing remote data
 */
struct RemoteWriteRequest {
    std::string key;
    std::vector<RemoteBufferDesc> src_buffers;
    std::optional<UUID> target_tier_id;
};

YLT_REFL(RemoteWriteRequest, key, src_buffers, target_tier_id);

/**
 * @struct BatchRemoteReadRequest
 * @brief Batch RPC request for reading multiple remote data objects
 */
struct BatchRemoteReadRequest {
    std::vector<std::string> keys;  // Object keys to read
    std::vector<std::vector<RemoteBufferDesc>>
        dest_buffers_list;  // Destination buffers for each key
};

YLT_REFL(BatchRemoteReadRequest, keys, dest_buffers_list);

/**
 * @struct BatchRemoteWriteRequest
 * @brief Batch RPC request for writing multiple remote data objects
 */
struct BatchRemoteWriteRequest {
    std::vector<std::string> keys;  // Object keys to write
    std::vector<std::vector<RemoteBufferDesc>>
        src_buffers_list;  // Source buffers for each key
    std::vector<std::optional<UUID>>
        target_tier_ids;  // Target tier IDs for each key
};

YLT_REFL(BatchRemoteWriteRequest, keys, src_buffers_list, target_tier_ids);

}  // namespace mooncake
