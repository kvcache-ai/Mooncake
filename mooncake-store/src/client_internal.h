// Copyright 2024 KVCache.AI

#pragma once

#include <functional>

#include "client_service.h"

namespace mooncake::internal {

struct SegmentMountOperations {
    std::function<int(void*, size_t, const std::string&, bool, bool)>
        register_memory;
    std::function<tl::expected<void, ErrorCode>(const Segment&)> mount_master;
    std::function<tl::expected<void, ErrorCode>(const UUID&)> unmount_master;
    std::function<int(void*)> unregister_memory;
};

class ClientAccess {
   public:
    static tl::expected<void, ErrorCode> MountSegmentWithId(
        Client& client, const UUID& segment_id, const void* buffer, size_t size,
        const std::string& protocol,
        const std::string& location = kWildcardLocation,
        const SegmentMountOperations* operations = nullptr) {
        return client.MountSegmentWithId(segment_id, buffer, size, protocol,
                                         location, operations);
    }

    static tl::expected<void, ErrorCode> CleanupSegmentByIdIfPresent(
        Client& client, const UUID& segment_id,
        const SegmentMountOperations* operations = nullptr) {
        return client.CleanupSegmentByIdIfPresent(segment_id, operations);
    }
};

}  // namespace mooncake::internal
