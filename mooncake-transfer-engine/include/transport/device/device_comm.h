#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake::device {

// One remote backend per communicator. Direct peer mappings coexist with it.
enum class DeviceBackend : uint32_t { kIbgda, kNccl };

// Opaque, naturally aligned completion slot. Values are accumulated modulo
// 2^32; the upper word is reserved for transports with 64-bit VA signals.
struct alignas(8) DeviceSignal {
    uint64_t storage;
};

struct DeviceComm;
enum class DeviceTeam : uint32_t { kWorld, kScaleup, kScaleout };

enum DeviceElasticCapability : uint32_t {
    kElasticTmaOrdering = 1u << 0,
    kElasticAggregateScaleupSignal = 1u << 1,
    kElasticGinBarrier = 1u << 2,
};

struct DeviceChannel {
    int index;
    const void* atomic_source;
    const void* atomic_target;
    int sharing_mode = 0;
};

// Elastic EP needs team-aware scalar publication, packed tail updates and GIN
// control signals in addition to the legacy payload/completion surface.  Keep
// this as a separately versioned extension so a legacy-only consumer neither
// pays for nor needs to understand elastic's algorithm-specific operations.
struct DeviceCommElasticOps {
    uint32_t abi_version;
    uint32_t struct_size;
    void* (*resolve)(const DeviceComm&, DeviceTeam, int peer, const void*);
    void (*put)(const DeviceComm&, DeviceChannel, DeviceTeam, int peer,
                void* dst, const void* src, uint32_t bytes, uint32_t flags);
    void (*put_value32)(const DeviceComm&, DeviceChannel, DeviceTeam, int peer,
                        int32_t* dst, int32_t value, uint32_t flags);
    void (*put_value64)(const DeviceComm&, DeviceChannel, DeviceTeam, int peer,
                        int64_t* dst, int64_t value, uint32_t flags);
    void (*red_add32)(const DeviceComm&, DeviceChannel, DeviceTeam, int peer,
                      int32_t* dst, int32_t value, uint32_t flags);
    void (*red_add64)(const DeviceComm&, DeviceChannel, DeviceTeam, int peer,
                      int64_t* dst, int64_t value, uint32_t flags);
    void (*flush_channel)(const DeviceComm&, DeviceChannel);
    void (*flush)(const DeviceComm&);
    void (*barrier_signal_inc)(const DeviceComm&, DeviceChannel, DeviceTeam,
                               int peer, int signal_id);
    uint64_t (*barrier_advance_shadow)(const DeviceComm&, int signal_id);
    uint64_t (*barrier_read)(const DeviceComm&, int signal_id);
};

struct DeviceCommOps {
    uint32_t abi_version;
    uint32_t struct_size;
    void* (*resolve)(const DeviceComm&, int peer, const void*);
    // Exactly one elected lane calls these operations. Staged source data
    // must be visible before put; all pointers belong to the bound window.
    void (*put)(const DeviceComm&, DeviceChannel, int peer, void* dst,
                const void* src, uint32_t bytes);
    // Release publication: preceding puts to peer on this channel settle
    // before the receiver observes the increment. Reset slots between epochs.
    void (*signal_add)(const DeviceComm&, DeviceChannel, int peer,
                       DeviceSignal*, int32_t delta);
    int32_t (*signal_read)(const DeviceComm&, const DeviceSignal*);
    const DeviceCommElasticOps* elastic_ops;
};

inline constexpr uint32_t kDeviceCommAbi = 3;
inline constexpr uint32_t kDeviceCommElasticAbi = 1;

// Host-prepared kernel argument, identical for IBGDA and NCCL. All pointers
// refer to device storage and stay live until every consuming kernel finishes.
// Ops and consuming kernels must belong to the same device-linked CUDA image.
// JIT consumers link the backend device archive into their image and bind that
// image's table; ABI compatibility alone does not make module addresses
// portable.
struct DeviceComm {
    const DeviceCommOps* ops = nullptr;
    const void* state = nullptr;
    void* local_base = nullptr;
    const int32_t* peer_available = nullptr;
    void* const* peer_bases = nullptr;
    int rank = 0;
    int channel_count = 1;
    bool enable_p2p = true;
    DeviceBackend backend = DeviceBackend::kIbgda;
    // Team-local to world-rank mapping.  It is data rather than a backend
    // policy: the same communicator can use P2P for a local destination and
    // the selected remote backend for another destination in one kernel.
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    int num_scaleup_ranks = 1;
    const void* elastic_atomic_source = nullptr;
    const void* elastic_atomic_target = nullptr;
    uint32_t aggregate_requests = 0;
    uint32_t elastic_capabilities = 0;
};

struct IbgdaCommBinding {
    const uint64_t* raddrs;
    const uint32_t* rkeys;
    void* qp_contexts;
};

// These functions are compiled in a CUDA translation unit. The host receives
// device function addresses, never host function addresses. The caller owns
// state and supplies device-resident backend storage of deviceCommStateSize().
size_t deviceCommStateSize(DeviceBackend backend);
DeviceComm bindDeviceComm(DeviceBackend backend, void* device_state,
                          const void* host_backend_state, void* local_base,
                          int rank, const int32_t* peer_available = nullptr,
                          void* const* peer_bases = nullptr);

}  // namespace mooncake::device
