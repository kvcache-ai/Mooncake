#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"

namespace mooncake {

class TransferLane;

// Each lane owns one fixed submission slot.
// The caller decides how lanes map to its work.
inline constexpr uint32_t kTransferLaneCount = 32;

enum class DeviceRouteKind : uint32_t {
    Unreachable = 0,
    P2p = 1,
    HostProxy = 2,
};

// Terminal outcomes visible to a transfer-service caller.
enum class TransferResult : uint32_t {
    Succeeded = 0,
    RouteUnavailable = 1,
    TimedOut = 2,
    Failed = 3,
};

struct DeviceP2pRoute {
    // Local CUDA address produced by importing the peer's memory mapping.
    uint64_t mapped_region_address;
};

struct DeviceHostProxyRoute {
    // Address published by the peer and consumed by the host proxy through TE.
    uint64_t remote_region_address;
};

// One entry in the device-resident route table indexed by GlobalRank.
struct DeviceTransferRoute {
    DeviceRouteKind kind = DeviceRouteKind::Unreachable;
    uint64_t region_size = 0;
    union {
        DeviceP2pRoute p2p = {};
        DeviceHostProxyRoute host_proxy;
    };
};

struct HostProxyCommandSlot;

// Non-owning device view of one DTS-managed local allocation.
struct DeviceLocalRegion {
    void* addr = nullptr;
    uint64_t size = 0;

    [[nodiscard]] __device__ __forceinline__ bool contains(
        const void* ptr, uint64_t bytes) const {
        if (!addr || !ptr) return false;
        const auto begin = reinterpret_cast<uintptr_t>(addr);
        const auto address = reinterpret_cast<uintptr_t>(ptr);
        if (address < begin) return false;
        const uint64_t bytes_from_begin = address - begin;
        return bytes_from_begin <= size && bytes <= size - bytes_from_begin;
    }
};

// Stable device-resident state owned by DeviceTransferService. It contains
// only device-wide resources; a caller supplies its own peer selection,
// buffers, signals, and algorithm state.
struct DeviceTransferHandle {
    DeviceLocalRegion peer_accessible_region;
    DeviceLocalRegion local_staging_region;
    const DeviceTransferRoute* routes = nullptr;
    uint64_t* lane_results = nullptr;
    HostProxyCommandSlot* host_proxy_command_slots = nullptr;

    uint32_t max_world_size = 0;

    // Return a directly addressable pointer into a peer's registered region.
    // A null result means the selected route is not directly addressable. This
    // capability query never allocates or falls back to staging.
    [[nodiscard]] __device__ __forceinline__ void* remotePtr(
        GlobalRank rank, uint64_t remote_offset) const {
        const auto& route = routes[rank];
        if (route.kind != DeviceRouteKind::P2p) return nullptr;
        return reinterpret_cast<char*>(
                   static_cast<uintptr_t>(route.p2p.mapped_region_address)) +
               remote_offset;
    }

    // Return the route currently selected for a peer.
    [[nodiscard]] __device__ __forceinline__ DeviceRouteKind
    routeKind(GlobalRank rank) const {
        return routes[rank].kind;
    }

    // Return a lightweight view of one fixed service lane.
    __device__ __forceinline__ TransferLane lane(uint32_t lane_index) const;
};

// A single-publisher update to one 64-bit notification counter in the peer's
// registered region. `delta` must be nonzero and less than half the uint64_t
// rolling range. Multiple publishers require a separate atomic primitive.
struct SignalAdd {
    uint64_t remote_offset = 0;
    uint64_t delta = 1;
};

struct SignalAction {
    enum class Kind : uint32_t {
        None = 0,
        Add = 1,
    };

    Kind kind = Kind::None;
    // Meaningful only when kind is Add.
    SignalAdd add;
};

// One CTA-collective remote notification operation. Before updating the
// counter, signal() releases the calling CTA's preceding direct memory
// accesses. It does not order independently submitted transfer operations.
struct SignalRequest {
    SignalAction signal;
    uint64_t timeout_ticks = 0;
};

// Transfer one payload from DTS-prepared local memory into the peer's
// peer-accessible region. local_ptr must be fully contained in either the
// peer-accessible region or the local staging allocation.
// When signal is not None, observing the attached notification implies this
// payload is visible, but says nothing about unrelated transfer operations.
struct PutRequest {
    const void* local_ptr = nullptr;
    uint64_t remote_offset = 0;
    uint64_t size = 0;
    SignalAction signal;
    uint64_t timeout_ticks = 0;
};

// Acquire-wait for a notification counter in the local peer-accessible region
// to reach `least` under uint64_t rolling comparison. local_ptr must be a
// valid, naturally aligned counter for the lifetime of the wait.
struct SignalWaitRequest {
    const uint64_t* local_ptr = nullptr;
    uint64_t least = 0;
    uint64_t timeout_ticks = 0;
};

enum class SignalWaitStatus : uint32_t {
    Reached = 0,
    TimedOut = 1,
};

struct SignalWaitResult {
    SignalWaitStatus status = SignalWaitStatus::TimedOut;
    uint64_t observed = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_TYPES_CUH
