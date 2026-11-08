#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_utils/d2h_request_slot_types.h"
#include "device_comm/device_utils/h2d_request_slot_types.h"

namespace mooncake {

class TransferLane;

// Each lane owns one fixed submission slot.
// The caller decides how lanes map to its work.
inline constexpr uint32_t kTransferLaneCount = 32;

// Drain timeout for device recovery and host-side shutdown.
inline constexpr uint64_t kTransferDrainTimeoutMs = 5000;

enum class DeviceRouteType : uint32_t {
    Unreachable = 0,
    P2p = 1,
    HostProxy = 2,
    Rdma = 3,
    NcclDevice = 4,
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

struct DeviceRdmaRoute {
    uint64_t remote_region_address;
    uint32_t remote_key;
    uint32_t qp_offset;
};

struct DeviceNcclRoute {
    // Rank within the NCCL communicator, not GlobalRank or InGroupRank.
    int32_t nccl_rank;
    // LSA mapping owned by the nccl communicator, or zero for a GIN peer.
    uint64_t mapped_region_address;
};

struct DeviceHostProxyRoute {
    // Address published by the peer and consumed by the host proxy through TE.
    uint64_t remote_region_address;
};

// One entry in the device-resident route table indexed by GlobalRank.
struct DeviceTransferRoute {
    DeviceRouteType type = DeviceRouteType::Unreachable;
    uint64_t region_size = 0;
    union {
        DeviceP2pRoute p2p = {};
        DeviceRdmaRoute rdma;
        DeviceNcclRoute nccl;
        DeviceHostProxyRoute host_proxy;
    };

    // Local GPU address of the peer region, or zero without a direct mapping.
    [[nodiscard]] __host__ __device__ __forceinline__ uint64_t
    mappedRegionAddress() const {
        switch (type) {
            case DeviceRouteType::P2p:
                return p2p.mapped_region_address;
            case DeviceRouteType::NcclDevice:
                return nccl.mapped_region_address;
            default:
                return 0;
        }
    }
};

struct HostProxyCommand;
enum class HostProxyCommandResult : uint32_t;
// Each lane owns one command/reply slot serviced by the host proxy.
using HostProxyCommandSlot =
    D2HRequestSlot<HostProxyCommand, HostProxyCommandResult>;

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

struct DeviceRdmaContext {
    void* qp_devctxs = nullptr;
    DeviceLocalRegion peer_accessible_region;
    DeviceLocalRegion local_staging_region;
    uint32_t peer_accessible_lkey = 0;
    uint32_t local_staging_lkey = 0;
    uint32_t qps_per_rank = 0;
    // Registered sink for the discarded RDMA fetch-and-add result.
    uint64_t* atomic_sink = nullptr;
    uint32_t atomic_sink_lkey = 0;
};

namespace device {
class NcclDeviceContext;
}

struct DeviceNcclContext {
    const device::NcclDeviceContext* peer_accessible_ctx = nullptr;
    const device::NcclDeviceContext* local_staging_ctx = nullptr;
    DeviceLocalRegion peer_accessible_region;
};

struct DeviceHostProxyContext {
    HostProxyCommandSlot* command_slots = nullptr;
};

// Non-owning route-specific device state shared by all peer routes.
struct DeviceRouteContext {
    DeviceRdmaContext rdma;
    DeviceNcclContext nccl;
    DeviceHostProxyContext host_proxy;
};

// Stable device-resident state owned by DeviceTransferService. It contains
// only device-wide resources; a caller supplies its own peer selection,
// buffers, signals, and algorithm state.
struct DeviceTransferHandle {
    // Host pins a pause request. The device replies at a safe point, then
    // requests resumption and waits for the host's reply.
    struct alignas(64) PauseMailbox {
        using PauseSlot = H2DRequestSlot<>;
        using ResumeSlot = D2HRequestSlot<>;

        PauseSlot pause;
        ResumeSlot resume;
    };

    DeviceLocalRegion peer_accessible_region;
    DeviceLocalRegion local_staging_region;
    DeviceRouteContext route_context;             // Per device.
    const DeviceTransferRoute* routes = nullptr;  // One entry per peer.
    uint64_t drain_timeout_ticks = 0;

    uint32_t max_world_size = 0;
    PauseMailbox* pause_mailbox = nullptr;

    // Return a directly addressable pointer into a peer's registered region.
    // A null result means the selected route is not directly addressable. This
    // capability query never allocates or falls back to staging.
    [[nodiscard]] __device__ __forceinline__ void* remotePtr(
        GlobalRank rank, uint64_t remote_offset) const {
        const auto address = routes[rank].mappedRegionAddress();
        if (address == 0) return nullptr;
        return reinterpret_cast<char*>(static_cast<uintptr_t>(address)) +
               remote_offset;
    }

    // Return a lightweight view of one fixed service lane.
    __device__ __forceinline__ TransferLane lane(uint32_t lane_index) const;

    // Drain outstanding transfers on a best-effort basis after all producers
    // stop submitting. Called by one thread.
    __device__ void drain(const GlobalRank* peers, uint32_t peer_count) const;

    // Acknowledge a pause request and wait for the host to resume execution.
    // Called by one thread.
    __device__ __forceinline__ void pauseIfRequested() const;
};

// A single-publisher update to one 64-bit notification counter in the peer's
// registered region. `delta` must be nonzero and less than half the uint64_t
// rolling range. Multiple publishers require a separate atomic primitive.
struct SignalAdd {
    uint64_t delta = 1;
};

// A single-publisher absolute update. This is used for incarnation values
// whose producer and consumer may not share a rolling-counter history.
struct SignalSet {
    uint64_t value = 0;
};

struct SignalAction {
    enum class Kind : uint32_t {
        None = 0,
        Add = 1,
        Set = 2,
    };

    Kind kind = Kind::None;
    uint64_t remote_offset = 0;
    union {
        SignalAdd add = {};
        SignalSet set;
    };

    __host__ __device__ constexpr SignalAction() : add{} {}
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
