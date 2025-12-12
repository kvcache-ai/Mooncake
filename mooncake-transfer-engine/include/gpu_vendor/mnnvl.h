#pragma once

#ifdef USE_HIP
#include <transport/hip_transport/hip_transport.h>
#define allocateFabricMemory(size) \
    mooncake::HipTransport::allocatePinnedLocalMemory(size)
#define freeFabricMemory(addr) \
    mooncake::HipTransport::freePinnedLocalMemory(addr)
#else
#include <transport/nvlink_transport/nvlink_transport.h>
#define allocateFabricMemory(size) \
    mooncake::NvlinkTransport::allocatePinnedLocalMemory(size)
#define freeFabricMemory(addr) \
    mooncake::NvlinkTransport::freePinnedLocalMemory(addr)
#endif
