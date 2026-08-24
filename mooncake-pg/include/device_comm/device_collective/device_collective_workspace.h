#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_arena.h"
#include "error_types.h"

namespace mooncake {

// Context-wide payload storage shared by device collective protocols. The
// buffer is published once as the rank-level DeviceCollectiveEndpoint during
// registerAgent, alongside DeviceTransferEndpoint. Staging is local-only and
// allocated lazily when a route first needs it.
//
// StrongStream serializes protocol kernels that borrow these buffers, so one
// pair can be shared by every communicator in the context.
class DeviceCollectiveWorkspace {
   public:
    static PGResult<std::unique_ptr<DeviceCollectiveWorkspace>> create(
        DeviceArena& arena, GlobalRank self_rank, uint32_t max_world_size,
        size_t buffer_size, size_t alignment);

    ~DeviceCollectiveWorkspace() noexcept = default;

    DeviceCollectiveWorkspace(const DeviceCollectiveWorkspace&) = delete;
    DeviceCollectiveWorkspace& operator=(const DeviceCollectiveWorkspace&) =
        delete;

    [[nodiscard]] const DeviceArenaSlice& buffer() const noexcept;
    [[nodiscard]] const DeviceCollectiveEndpoint& localEndpoint()
        const noexcept;

    PGResult<void> installPeerEndpoint(
        GlobalRank rank, const DeviceCollectiveEndpoint& endpoint);
    PGResult<DeviceCollectiveEndpoint> endpoint(GlobalRank rank) const;

    // Allocates at most once. The returned slice has the same size and
    // alignment as buffer() and is never published in an endpoint.
    PGResult<const DeviceArenaSlice*> ensureStaging();

   private:
    DeviceCollectiveWorkspace(
        DeviceArena& arena, DeviceArenaSlice buffer, size_t alignment,
        GlobalRank self_rank, uint32_t max_world_size,
        DeviceCollectiveEndpoint local_endpoint);

    DeviceArena& arena_;
    DeviceArenaSlice buffer_;
    size_t alignment_ = 0;
    GlobalRank self_rank_ = kInvalidGlobalRank;
    DeviceCollectiveEndpoint local_endpoint_;
    std::vector<std::optional<DeviceCollectiveEndpoint>> endpoints_;
    std::optional<DeviceArenaSlice> staging_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H
