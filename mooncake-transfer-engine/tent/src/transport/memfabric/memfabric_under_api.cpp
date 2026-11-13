#include "tent/transport/memfabric/memfabric_under_api.h"

#include <dlfcn.h>

#include <glog/logging.h>

namespace mooncake {
namespace tent {

std::mutex MemFabricUnderApi::gMutex;
bool MemFabricUnderApi::gLoaded = false;
void* MemFabricUnderApi::gLibHandle = nullptr;
std::shared_ptr<const MemFabricUnderApi::MemoryApi>
    MemFabricUnderApi::gMemoryApi;
std::weak_ptr<const MemFabricUnderApi::MemoryApi>
    MemFabricUnderApi::gMemoryApiWeak;
std::unordered_map<void*, std::shared_ptr<const MemFabricUnderApi::MemoryApi>>
    MemFabricUnderApi::gAllocations;

smemTransInitFunc MemFabricUnderApi::pSmemTransInit = nullptr;
smemTransUninitFunc MemFabricUnderApi::pSmemTransUninit = nullptr;
smemTransConfigInitFunc MemFabricUnderApi::pSmemTransConfigInit = nullptr;
smemTransCreateFunc MemFabricUnderApi::pSmemTransCreate = nullptr;
smemTransDestroyFunc MemFabricUnderApi::pSmemTransDestroy = nullptr;
smemTransGetRpcPortFunc MemFabricUnderApi::pSmemTransGetRpcPort = nullptr;
smemTransRegisterMemFunc MemFabricUnderApi::pSmemTransRegisterMem = nullptr;
smemTransBatchRegisterMemFunc MemFabricUnderApi::pSmemTransBatchRegisterMem =
    nullptr;
smemTransDeregisterMemFunc MemFabricUnderApi::pSmemTransDeregisterMem = nullptr;
smemTransBatchCopyFunc MemFabricUnderApi::pSmemTransBatchCopy = nullptr;
smemTransBatchStatusQueryFunc MemFabricUnderApi::pSmemTransBatchStatusQuery =
    nullptr;
smemSetLogLevelFunc MemFabricUnderApi::pSmemSetLogLevel = nullptr;

#define DL_LOAD_SYM(field, name)                                           \
    do {                                                                   \
        field = (decltype(field))dlsym(gLibHandle, name);                  \
        if (!field) {                                                      \
            LOG(ERROR) << "Failed to dlsym " << name << ": " << dlerror(); \
            ResetSymbols();                                                \
            return false;                                                  \
        }                                                                  \
    } while (0)

bool MemFabricUnderApi::LoadLibrary() {
    std::lock_guard<std::mutex> guard(gMutex);
    return LoadLibraryLocked();
}

bool MemFabricUnderApi::LoadLibraryLocked() {
    if (gLoaded) return true;

    auto api = gMemoryApiWeak.lock();
    if (!api) {
        auto loaded = std::make_shared<MemoryApi>();
        void* library = dlopen("libmf_smem.so", RTLD_NOW | RTLD_GLOBAL);
        if (!library) {
            LOG(ERROR) << "Failed to dlopen libmf_smem.so: " << dlerror();
            return false;
        }
        loaded->library = std::shared_ptr<void>(
            library, [](void* handle) { (void)dlclose(handle); });
        gLibHandle = library;
        DL_LOAD_SYM(loaded->allocate, "smem_mem_alloc");
        DL_LOAD_SYM(loaded->free, "smem_mem_free");
        api = std::move(loaded);
    }
    gLibHandle = api->library.get();

    DL_LOAD_SYM(pSmemTransInit, "smem_trans_init");
    DL_LOAD_SYM(pSmemTransUninit, "smem_trans_uninit");
    DL_LOAD_SYM(pSmemTransConfigInit, "smem_trans_config_init");
    DL_LOAD_SYM(pSmemTransCreate, "smem_trans_create");
    DL_LOAD_SYM(pSmemTransDestroy, "smem_trans_destroy");
    DL_LOAD_SYM(pSmemTransGetRpcPort, "smem_trans_get_rpc_port");
    DL_LOAD_SYM(pSmemTransRegisterMem, "smem_trans_register_mem");
    DL_LOAD_SYM(pSmemTransBatchRegisterMem, "smem_trans_batch_register_mem");
    DL_LOAD_SYM(pSmemTransDeregisterMem, "smem_trans_deregister_mem");
    DL_LOAD_SYM(pSmemTransBatchCopy, "smem_trans_batch_copy");
    DL_LOAD_SYM(pSmemTransBatchStatusQuery, "smem_trans_batch_status_query");
    DL_LOAD_SYM(pSmemSetLogLevel, "smem_set_log_level");

    gMemoryApi = std::move(api);
    gMemoryApiWeak = gMemoryApi;
    gLoaded = true;
    return true;
}

int32_t MemFabricUnderApi::SmemMemAlloc(uint16_t deviceId, uint64_t size,
                                        uint32_t flags, void** address) {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        if (!LoadLibraryLocked()) return SM_ERROR;
        api = gMemoryApi;
    }
    const int32_t ret = api->allocate(deviceId, size, flags, address);
    if (ret != 0 || !address || !*address) return ret;
    try {
        std::lock_guard<std::mutex> guard(gMutex);
        gAllocations.emplace(*address, api);
    } catch (...) {
        (void)api->free(*address);
        throw;
    }
    return ret;
}

int32_t MemFabricUnderApi::SmemMemFree(void* address, bool& managed) {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        auto it = gAllocations.find(address);
        managed = it != gAllocations.end();
        if (!managed) return SM_OK;
        api = std::move(it->second);
        gAllocations.erase(it);
    }
    const int32_t ret = api->free(address);
    if (ret != 0) {
        std::lock_guard<std::mutex> guard(gMutex);
        gAllocations.emplace(address, std::move(api));
    }
    return ret;
}

void MemFabricUnderApi::CleanupLibrary() {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        ResetSymbols();
        api = std::move(gMemoryApi);
    }
}

void MemFabricUnderApi::ResetSymbols() {
    pSmemTransInit = nullptr;
    pSmemTransUninit = nullptr;
    pSmemTransConfigInit = nullptr;
    pSmemTransCreate = nullptr;
    pSmemTransDestroy = nullptr;
    pSmemTransGetRpcPort = nullptr;
    pSmemTransRegisterMem = nullptr;
    pSmemTransBatchRegisterMem = nullptr;
    pSmemTransDeregisterMem = nullptr;
    pSmemTransBatchCopy = nullptr;
    pSmemTransBatchStatusQuery = nullptr;
    pSmemSetLogLevel = nullptr;

    gLibHandle = nullptr;
    gLoaded = false;
}

}  // namespace tent
}  // namespace mooncake
