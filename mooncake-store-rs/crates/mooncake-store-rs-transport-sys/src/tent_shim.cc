#include "tent/runtime/platform.h"
#include "tent/transport/rdma/ibv_loader.h"

// CANN/ACL and the shared Ascend host allocator are optional. Builds without
// the Ascend transports (for example the classic-TE CI gate, or any host
// without CANN) still compile and load this shim; the affected entry points
// below then report "unavailable" instead of referencing missing symbols.
#if defined(MOONCAKE_STORE_RS_TENT_ACL)
#include <acl/acl.h>
#endif
#if defined(MOONCAKE_STORE_RS_TENT_ASCEND_ALLOCATOR)
#include "ascend_allocator.h"
#endif

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>

extern "C" int mooncake_tent_platform_free_memory(void* addr, size_t size) {
    if (addr == nullptr) {
        return -1;
    }
    auto status = mooncake::tent::Platform::getLoader().free(addr, size);
    return status.ok() ? 0 : -1;
}

extern "C" uint64_t mooncake_tent_probe_rdma_max_mr_size() {
    auto& loader = mooncake::tent::IbvLoader::Instance();
    if (!loader.available()) {
        return 0;
    }

    const auto& verbs = loader.sym();
    int device_count = 0;
    ibv_device** devices = verbs.ibv_get_device_list(&device_count);
    if (devices == nullptr || device_count <= 0) {
        if (devices != nullptr) {
            verbs.ibv_free_device_list(devices);
        }
        return 0;
    }

    uint64_t limit = std::numeric_limits<uint64_t>::max();
    bool found = false;
    for (int index = 0; index < device_count; ++index) {
        ibv_context* context = verbs.ibv_open_device(devices[index]);
        if (context == nullptr) {
            continue;
        }
        ibv_device_attr attr = {};
        if (verbs.ibv_query_device(context, &attr) == 0 &&
            attr.max_mr_size > 0) {
            limit = std::min(limit, static_cast<uint64_t>(attr.max_mr_size));
            found = true;
        }
        verbs.ibv_close_device(context);
    }
    verbs.ibv_free_device_list(devices);
    return found ? limit : 0;
}

// Bring up ACL for a plain Rust process before a TENT engine is created.
//
// TENT's ascend_direct transport installs one HIXL engine per device and its
// initEngines() starts with aclrtGetCurrentContext/aclrtGetDevice
// (tent/src/transport/ascend/ascend_direct_transport.cpp:313-350). Store-RS
// creates the TENT engine from a process where nothing has called
// aclInit/aclrtSetDevice, so the transport is skipped with
// "Get device context failed, device may be not set." and later host-memory
// allocation/registration fails. aclInit is process-wide (once); the device
// binding is per-thread, so callers must run this on the thread that creates
// the engine.
#if defined(MOONCAKE_STORE_RS_TENT_ACL)
extern "C" int mooncake_tent_prepare_ascend(int device_id) {
    static std::once_flag acl_once;
    static aclError acl_init_status = ACL_ERROR_NONE;
    std::call_once(acl_once, []() {
        aclError ret = aclInit(nullptr);
        if (ret != ACL_ERROR_NONE) {
            // Another component may have initialised ACL already; that is
            // fine as long as the runtime is usable.
            uint32_t device_count = 0;
            if (aclrtGetDeviceCount(&device_count) != ACL_ERROR_NONE) {
                acl_init_status = ret;
                LOG(ERROR) << "aclInit failed, ret=" << ret
                           << ", errmsg: " << aclGetRecentErrMsg();
            }
        }
    });
    if (acl_init_status != ACL_ERROR_NONE) {
        return -1;
    }
    if (aclrtSetDevice(device_id) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtSetDevice(" << device_id
                   << ") failed, errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    return 0;
}
#else
// No ACL in this build: TENT's ascend_direct transport cannot be initialised.
extern "C" int mooncake_tent_prepare_ascend(int /*device_id*/) { return -1; }
#endif

// Ascend host allocator (memfd-backed when ASCEND_USE_SHMEM=1). A peer on the
// same host can import the memfd and serve transfers as local copies, matching
// the C++ Store TENT path. Falls back to nullptr and lets the caller allocate.
#if defined(MOONCAKE_STORE_RS_TENT_ASCEND_ALLOCATOR)
extern "C" void* mooncake_tent_ascend_host_alloc(size_t size) {
    return mooncake::ascend_allocate_memory(size, "");
}

extern "C" void mooncake_tent_ascend_host_free(void* ptr) {
    if (ptr != nullptr) {
        mooncake::ascend_free_memory("", ptr);
    }
}
#else
// No shared Ascend allocator in this build: the caller falls back to its own
// 2 MiB-aligned host allocation.
extern "C" void* mooncake_tent_ascend_host_alloc(size_t /*size*/) {
    return nullptr;
}

extern "C" void mooncake_tent_ascend_host_free(void* /*ptr*/) {}
#endif

// --- ACL device-memory helpers for Store-RS ---------------------------------
// Store-RS runs its CPU copies in the client dispatcher: when a put_from source
// or a get_into destination is NPU device memory, the payload has to be staged
// through host memory with a device copy. Device id comes from
// MC_STORE_RS_ASCEND_DEVICE.
#if defined(MOONCAKE_STORE_RS_TENT_ACL)
namespace {
int BindAscendDeviceFromEnv() {
    const char* env = std::getenv("MC_STORE_RS_ASCEND_DEVICE");
    int device_id = 0;
    if (env != nullptr && env[0] != '\0') {
        device_id = std::atoi(env);
    }
    return aclrtSetDevice(device_id) == ACL_ERROR_NONE ? 0 : -1;
}
}  // namespace

extern "C" int mooncake_tent_acl_pointer_is_device(void* addr) {
    if (addr == nullptr) {
        return -1;
    }
    aclrtPtrAttributes attributes{};
    if (aclrtPointerGetAttributes(addr, &attributes) != ACL_ERROR_NONE) {
        return -1;
    }
    return attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE ? 1 : 0;
}

extern "C" int mooncake_tent_acl_memcpy_d2h(void* dst, const void* src,
                                            size_t len) {
    if (BindAscendDeviceFromEnv() != 0) {
        return -1;
    }
    return aclrtMemcpy(dst, len, src, len, ACL_MEMCPY_DEVICE_TO_HOST) ==
                   ACL_ERROR_NONE
               ? 0
               : -1;
}

extern "C" int mooncake_tent_acl_memcpy_h2d(void* dst, const void* src,
                                            size_t len) {
    if (BindAscendDeviceFromEnv() != 0) {
        return -1;
    }
    return aclrtMemcpy(dst, len, src, len, ACL_MEMCPY_HOST_TO_DEVICE) ==
                   ACL_ERROR_NONE
               ? 0
               : -1;
}
#else
// No ACL in this build: every pointer looks like host memory and staging
// copies stay unavailable, so callers keep the non-device behaviour.
extern "C" int mooncake_tent_acl_pointer_is_device(void* /*addr*/) {
    return -1;
}

extern "C" int mooncake_tent_acl_memcpy_d2h(void* /*dst*/, const void* /*src*/,
                                            size_t /*len*/) {
    return -1;
}

extern "C" int mooncake_tent_acl_memcpy_h2d(void* /*dst*/, const void* /*src*/,
                                            size_t /*len*/) {
    return -1;
}
#endif
