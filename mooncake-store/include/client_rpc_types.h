#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <optional>
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
};

YLT_REFL(RemoteReadRequest, key, dest_buffers);

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

struct PreWriteRequest {
    std::string_view key;
    uint64_t size_bytes = 0;
    std::optional<UUID> target_tier_id;
};

YLT_REFL(PreWriteRequest, key, size_bytes, target_tier_id);

struct PreWriteResponse {
    RemoteBufferDesc remote_buffer;
    UUID pending_write_token;
};

YLT_REFL(PreWriteResponse, remote_buffer, pending_write_token);

struct WriteCommitRequest {
    std::string_view key;
    UUID pending_write_token;
};

YLT_REFL(WriteCommitRequest, key, pending_write_token);

struct PinKeyRequest {
    std::string_view key;
    std::optional<UUID> target_tier_id;
};

YLT_REFL(PinKeyRequest, key, target_tier_id);

struct PinKeyResponse {
    RemoteBufferDesc remote_buffer;
    UUID pin_token;
};

YLT_REFL(PinKeyResponse, remote_buffer, pin_token);

struct UnPinKeyRequest {
    std::string_view key;
    UUID pin_token;
};

YLT_REFL(UnPinKeyRequest, key, pin_token);

}  // namespace mooncake
