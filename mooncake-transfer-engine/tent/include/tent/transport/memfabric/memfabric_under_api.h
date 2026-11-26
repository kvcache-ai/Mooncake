#ifndef MEMFABRIC_UNDER_API_H_
#define MEMFABRIC_UNDER_API_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "memfabric_types.h"

namespace mooncake {
namespace tent {

using smemTransInitFunc = int32_t (*)(uint32_t, uint32_t);
using smemTransUninitFunc = void (*)(uint32_t);
using smemTransConfigInitFunc = int32_t (*)(smem_trans_config_t*);
using smemTransCreateFunc = smem_trans_t (*)(const smem_trans_config_t*,
                                             uint32_t);
using smemTransDestroyFunc = void (*)(smem_trans_t, uint32_t);
using smemTransGetRpcPortFunc = int32_t (*)(smem_trans_t, uint16_t*);
using smemTransRegisterMemFunc = int32_t (*)(smem_trans_t, void*, size_t,
                                             uint32_t);
using smemTransBatchRegisterMemFunc = int32_t (*)(smem_trans_t, void*[],
                                                  size_t[], uint32_t, uint32_t);
using smemTransDeregisterMemFunc = int32_t (*)(smem_trans_t, void*);
using smemTransBatchCopyFunc = int32_t (*)(smem_trans_t,
                                           smem_trans_batch_params_t*,
                                           uint64_t*);
using smemTransBatchStatusQueryFunc = int32_t (*)(smem_trans_t, uint64_t,
                                                  int32_t*, uint32_t);
using smemSetLogLevelFunc = int32_t (*)(int);

using smemMemAllocFunc = int32_t (*)(uint16_t, uint64_t, uint32_t, void**);
using smemMemFreeFunc = int32_t (*)(void*);

class MemFabricUnderApi {
    friend class MemFabricTransportTest;

   public:
    static bool LoadLibrary();
    static void CleanupLibrary();
    static bool IsLoaded() { return gLoaded; }
    static int32_t SmemMemAlloc(uint16_t deviceId, uint64_t size,
                                uint32_t flags, void** address);
    // managed distinguishes fabric allocations from ACL Host buffers.
    static int32_t SmemMemFree(void* address, bool& managed);

    static inline int32_t SmemTransInit(uint32_t deviceId, uint32_t flags) {
        if (pSmemTransInit == nullptr) return -1;
        return pSmemTransInit(deviceId, flags);
    }
    static inline void SmemTransUninit(uint32_t flags) {
        if (pSmemTransUninit) pSmemTransUninit(flags);
    }
    static inline int32_t SmemTransConfigInit(smem_trans_config_t* config) {
        if (pSmemTransConfigInit == nullptr) return -1;
        return pSmemTransConfigInit(config);
    }
    static inline smem_trans_t SmemTransCreate(
        const smem_trans_config_t* config, uint32_t flags) {
        if (pSmemTransCreate == nullptr) return nullptr;
        return pSmemTransCreate(config, flags);
    }
    static inline void SmemTransDestroy(smem_trans_t handle, uint32_t flags) {
        if (pSmemTransDestroy) pSmemTransDestroy(handle, flags);
    }
    static inline int32_t SmemTransGetRpcPort(smem_trans_t handle,
                                              uint16_t* port) {
        if (pSmemTransGetRpcPort == nullptr) return -1;
        return pSmemTransGetRpcPort(handle, port);
    }
    static inline int32_t SmemTransRegisterMem(smem_trans_t handle,
                                               void* address, size_t capacity,
                                               uint32_t flags) {
        if (pSmemTransRegisterMem == nullptr) return -1;
        return pSmemTransRegisterMem(handle, address, capacity, flags);
    }
    static inline int32_t SmemTransBatchRegisterMem(smem_trans_t handle,
                                                    void* addresses[],
                                                    size_t capacities[],
                                                    uint32_t count,
                                                    uint32_t flags) {
        if (pSmemTransBatchRegisterMem == nullptr) return -1;
        return pSmemTransBatchRegisterMem(handle, addresses, capacities, count,
                                          flags);
    }
    static inline int32_t SmemTransDeregisterMem(smem_trans_t handle,
                                                 void* address) {
        if (pSmemTransDeregisterMem == nullptr) return -1;
        return pSmemTransDeregisterMem(handle, address);
    }
    static inline int32_t SmemTransBatchCopy(smem_trans_t handle,
                                             smem_trans_batch_params_t* param,
                                             uint64_t* batchId) {
        if (pSmemTransBatchCopy == nullptr) return -1;
        return pSmemTransBatchCopy(handle, param, batchId);
    }
    static inline int32_t SmemTransBatchStatusQuery(smem_trans_t handle,
                                                    uint64_t batchId,
                                                    int32_t* result,
                                                    uint32_t flags) {
        if (pSmemTransBatchStatusQuery == nullptr) return -1;
        return pSmemTransBatchStatusQuery(handle, batchId, result, flags);
    }
    static inline int32_t SmemSetLogLevel(int level) {
        if (pSmemSetLogLevel == nullptr) return -1;
        return pSmemSetLogLevel(level);
    }

   private:
    // Keep allocation functions and their library alive with outstanding
    // buffers.
    struct MemoryApi {
        std::shared_ptr<void> library;
        smemMemAllocFunc allocate = nullptr;
        smemMemFreeFunc free = nullptr;
    };

    static bool LoadLibraryLocked();
    static void ResetSymbols();
    static std::mutex gMutex;
    static bool gLoaded;
    static void* gLibHandle;
    static std::shared_ptr<const MemoryApi> gMemoryApi;
    static std::weak_ptr<const MemoryApi> gMemoryApiWeak;
    static std::unordered_map<void*, std::shared_ptr<const MemoryApi>>
        gAllocations;

    static smemTransInitFunc pSmemTransInit;
    static smemTransUninitFunc pSmemTransUninit;
    static smemTransConfigInitFunc pSmemTransConfigInit;
    static smemTransCreateFunc pSmemTransCreate;
    static smemTransDestroyFunc pSmemTransDestroy;
    static smemTransGetRpcPortFunc pSmemTransGetRpcPort;
    static smemTransRegisterMemFunc pSmemTransRegisterMem;
    static smemTransBatchRegisterMemFunc pSmemTransBatchRegisterMem;
    static smemTransDeregisterMemFunc pSmemTransDeregisterMem;
    static smemTransBatchCopyFunc pSmemTransBatchCopy;
    static smemTransBatchStatusQueryFunc pSmemTransBatchStatusQuery;
    static smemSetLogLevelFunc pSmemSetLogLevel;
};

// HYBM GVA API: libmf_hybm_core.so.
constexpr uint32_t kGvaMemoryTypeDevice = 1;
constexpr uint32_t kGvaMemoryTypeHost = 2;
// hybm_data_copy_direction is a bitmask: the source type occupies bit 2
// (host 0x4) or bit 3 (device 0x8), and the destination type occupies bit 0
// (host 0x0, device 0x1). For example, device-to-device is 0x9.
constexpr uint32_t kGvaCopySourceHost = 0x4;
constexpr uint32_t kGvaCopySourceDevice = 0x8;
constexpr uint32_t kGvaCopyDestinationHost = 0x0;
constexpr uint32_t kGvaCopyDestinationDevice = 0x1;

struct MemFabricGvaConfig {
    bool enabled = false;
    std::string protocol = "device_sdma";
    uint32_t rank_count = 0, rank = 0;
    uint16_t device = 0, entity = 64;
    uint64_t hbm_window = 0, dram_window = 0, hbm_size = 0, dram_size = 0;
};
struct MemFabricGvaCopy {
    uint64_t source, destination;
    size_t size;
};
// This adapter uses official HYBM headers in its implementation. No native ABI
// structures or implementation-dependent handles appear in wire metadata.
class MemFabricGvaApi {
   public:
    // Symbols are shared for process lifetime, including failed teardown.
    // No CleanupLibrary: unloading could invalidate live entities or workers.
    static bool LoadLibrary();
    static int batchCopy(void*, const std::vector<MemFabricGvaCopy>&, uint32_t);
    static int copy(void* entity, uint64_t source, uint64_t destination,
                    size_t size, uint32_t direction);
    static int wait(void* entity);
    static int bind(uint16_t device);
    static int initialize(uint16_t device);
    static void uninitialize();
    static void* create(const MemFabricGvaConfig& config);
    static void destroy(void* entity);
    static int reserve(void* entity);
    static int unreserve(void* entity);
    static void* allocate(void* entity, uint32_t memory_type, uint64_t size);
    static int free(void* entity, void* slice);
    static uint64_t address(void* entity, void* slice);
    static uint64_t size(void* entity, void* slice);
    static uint64_t base(void* entity, uint32_t memory_type);
    static int translate(uint64_t address, uint32_t memory_type, uint64_t& out);
    static int exportBlob(void* entity, void* slice,
                          std::vector<uint8_t>& blob);
    static int importBlob(void* entity, bool entity_info,
                          const std::vector<uint8_t>& blob);
    static int map(void* entity);
    static int connect(void* entity, uint32_t rank);
    static int reach(void* entity, uint32_t rank, uint32_t& out);
    static int remove(void* entity, uint32_t rank);
    static int unmap(void* entity, uint32_t rank);

   private:
    MemFabricGvaApi() = delete;
};

}  // namespace tent
}  // namespace mooncake

#endif  // MEMFABRIC_UNDER_API_H_
