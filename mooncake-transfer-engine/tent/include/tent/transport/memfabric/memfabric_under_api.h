#ifndef MEMFABRIC_UNDER_API_H_
#define MEMFABRIC_UNDER_API_H_

#include <cstdint>
#include <mutex>

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

class MemFabricUnderApi {
    friend class MemFabricTransportTest;

   public:
    static bool LoadLibrary();
    static void CleanupLibrary();
    static bool IsLoaded() { return gLoaded; }

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
    static std::mutex gMutex;
    static bool gLoaded;
    static void* gLibHandle;

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

}  // namespace tent
}  // namespace mooncake

#endif  // MEMFABRIC_UNDER_API_H_
