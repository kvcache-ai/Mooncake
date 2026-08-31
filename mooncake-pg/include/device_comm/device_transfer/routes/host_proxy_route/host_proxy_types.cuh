#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_TYPES_CUH

#include <cstdint>

#include "common_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

enum class HostProxyCommandResult : uint32_t {
    Pending = 0,
    Succeeded = 1,
    Failed = 2,
};

struct HostProxyCommand {
    uint64_t local_addr = 0;
    uint64_t remote_region_addr = 0;
    uint64_t remote_offset = 0;
    uint64_t size = 0;
    SignalAction signal;
    GlobalRank target_rank = kInvalidGlobalRank;
};

// One mapped SPSC command slot belongs to each device lane:
//
//   GPU producer                         Host proxy consumer
//   ------------                         -------------------
//   acquire completed_sequence == N - 1
//   write command
//   system fence
//   release submitted_sequence = N  -->  acquire submitted_sequence == N
//                                        snapshot command
//                                        execute payload, then signal action
//                                        write result
//   acquire completed_sequence = N  <--  release completed_sequence = N
//   read result
//
// The service contract permits only one producer per lane at a time.
struct HostProxyCommandSlot {
    uint64_t submitted_sequence = 0;
    uint64_t completed_sequence = 0;

    HostProxyCommandResult result = HostProxyCommandResult::Succeeded;

    HostProxyCommand command;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_TYPES_CUH
