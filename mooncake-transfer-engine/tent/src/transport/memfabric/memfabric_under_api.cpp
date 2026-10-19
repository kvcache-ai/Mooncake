#include "tent/transport/memfabric/memfabric_under_api.h"

#include <dlfcn.h>

#include <glog/logging.h>

namespace mooncake {
namespace tent {

std::mutex MemFabricUnderApi::gMutex;
bool MemFabricUnderApi::gLoaded = false;
void* MemFabricUnderApi::gLibHandle = nullptr;

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
            dlclose(gLibHandle);                                           \
            gLibHandle = nullptr;                                          \
            return false;                                                  \
        }                                                                  \
    } while (0)

bool MemFabricUnderApi::LoadLibrary() {
    std::lock_guard<std::mutex> guard(gMutex);
    if (gLoaded) return true;

    gLibHandle = dlopen("libmf_smem.so", RTLD_NOW | RTLD_GLOBAL);
    if (!gLibHandle) {
        LOG(ERROR) << "Failed to dlopen libmf_smem.so: " << dlerror();
        return false;
    }

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

    gLoaded = true;
    return true;
}

void MemFabricUnderApi::CleanupLibrary() {
    std::lock_guard<std::mutex> guard(gMutex);
    if (!gLoaded) return;

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

    if (gLibHandle) {
        dlclose(gLibHandle);
        gLibHandle = nullptr;
    }
    gLoaded = false;
}

}  // namespace tent
}  // namespace mooncake
