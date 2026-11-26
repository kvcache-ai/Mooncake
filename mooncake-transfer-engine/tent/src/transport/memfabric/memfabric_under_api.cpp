// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_under_api.h"

#include <dlfcn.h>

#include <glog/logging.h>

#if defined(TENT_WITH_HYBM_GVA) && !defined(TENT_MEMFABRIC_TEST_GVA_API)
#include <acl/acl.h>
#include <hybm_big_mem.h>
#include <hybm_data_op.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <list>
#include <thread>
#endif

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

#undef DL_LOAD_SYM

#if !defined(TENT_MEMFABRIC_TEST_GVA_API)
#if defined(TENT_WITH_HYBM_GVA)
// HYBM GVA API. Keep its symbols and lifecycle independent of trans.
namespace {

// clang-format off
#define HYBM_GVA_SYMBOLS(name)     \
    name(hybm_init)                \
    name(hybm_uninit)              \
    name(hybm_create_entity)       \
    name(hybm_destroy_entity)      \
    name(hybm_reserve_mem_space)   \
    name(hybm_unreserve_mem_space) \
    name(hybm_alloc_local_memory)  \
    name(hybm_free_local_memory)   \
    name(hybm_get_slice_va)        \
    name(hybm_get_memory_ptr)      \
    name(hybm_get_slice_size)      \
    name(hybm_gva_to_va)           \
    name(hybm_export)              \
    name(hybm_export_info_free)    \
    name(hybm_import)              \
    name(hybm_mmap)                \
    name(hybm_entity_reach_types)  \
    name(hybm_remove_imported)     \
    name(hybm_unmap_rank)          \
    name(hybm_data_copy)           \
    name(hybm_data_batch_copy)     \
    name(hybm_transport_connect)   \
    name(hybm_wait)
// clang-format on

struct NativeHybm {
    bool loadSymbols() {
        library_.reset(dlopen("libmf_hybm_core.so", RTLD_NOW | RTLD_LOCAL));
        if (!library_) return false;
#define LOAD_REQUIRED(name)                                    \
    name##_ = (decltype(name##_))dlsym(library_.get(), #name); \
    if (!name##_) return false;
        HYBM_GVA_SYMBOLS(LOAD_REQUIRED)
#undef LOAD_REQUIRED
        return true;
    }
    struct BatchBuffers {
        void* entity = nullptr;
        const std::thread::id submitter = std::this_thread::get_id();
        bool failed = false;
        std::vector<void*> sources, destinations;
        std::vector<uint64_t> sizes;
    };
    std::mutex batch_mutex_;
    std::list<std::shared_ptr<BatchBuffers>> pending_batches_;
    struct CloseLibrary {
        void operator()(void* handle) const { (void)dlclose(handle); }
    };
    std::unique_ptr<void, CloseLibrary> library_;
#define DECLARE(name) decltype(&name) name##_ = nullptr;
    HYBM_GVA_SYMBOLS(DECLARE)
#undef DECLARE
};
#undef HYBM_GVA_SYMBOLS
// Publish only complete tables. Failed loads leave no state and can retry.
std::atomic<NativeHybm*>& nativeApiSlot() {
    static std::atomic<NativeHybm*> api{nullptr};
    return api;
}
NativeHybm* nativeApi() {
    return nativeApiSlot().load(std::memory_order_acquire);
}
}  // namespace
bool MemFabricGvaApi::LoadLibrary() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    if (nativeApi()) return true;
    auto api = std::make_unique<NativeHybm>();
    if (!api->loadSymbols()) return false;
    // Intentionally resident: entities can survive failed cleanup.
    nativeApiSlot().store(api.release(), std::memory_order_release);
    return true;
}
int MemFabricGvaApi::copy(void* entity, uint64_t source, uint64_t destination,
                          size_t size, uint32_t direction) {
    auto* api = nativeApi();
    if (!api) return -1;
    hybm_copy_params params{reinterpret_cast<void*>(source),
                            reinterpret_cast<void*>(destination), size};
    return api->hybm_data_copy_(
        entity, &params, static_cast<hybm_data_copy_direction>(direction),
        nullptr, 0);
}
int MemFabricGvaApi::batchCopy(void* entity,
                               const std::vector<MemFabricGvaCopy>& copies,
                               uint32_t direction) {
    auto* api = nativeApi();
    if (!api || copies.empty() || copies.size() > UINT32_MAX) return -1;
    auto buffers = std::make_shared<NativeHybm::BatchBuffers>();
    buffers->entity = entity;
    buffers->sources.reserve(copies.size());
    buffers->destinations.reserve(copies.size());
    buffers->sizes.reserve(copies.size());
    for (const auto& copy : copies) {
        buffers->sources.push_back(reinterpret_cast<void*>(copy.source));
        buffers->destinations.push_back(
            reinterpret_cast<void*>(copy.destination));
        buffers->sizes.push_back(copy.size);
    }
    // HYBM can rewrite the pointer arrays. Own them independently of the
    // validated requests, and retain them if native completion is unknown.
    std::list<std::shared_ptr<NativeHybm::BatchBuffers>>::iterator retained;
    {
        std::lock_guard<std::mutex> lock(api->batch_mutex_);
        api->pending_batches_.push_front(buffers);
        retained = api->pending_batches_.begin();
    }
    hybm_batch_copy_params params{
        buffers->sources.data(), buffers->destinations.data(),
        buffers->sizes.data(), static_cast<uint32_t>(copies.size())};
    const auto ret = api->hybm_data_batch_copy_(
        entity, &params, static_cast<hybm_data_copy_direction>(direction),
        nullptr, 0);
    {
        std::lock_guard<std::mutex> lock(api->batch_mutex_);
        if (!ret) {
            api->pending_batches_.erase(retained);
        } else {
            buffers->failed = true;
        }
    }
    return ret;
}
int MemFabricGvaApi::wait(void* entity) {
    auto* api = nativeApi();
    if (!api) return -1;
    std::vector<std::shared_ptr<NativeHybm::BatchBuffers>> failed;
    const auto submitter = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> lock(api->batch_mutex_);
        for (const auto& batch : api->pending_batches_) {
            if (batch->failed && batch->entity == entity &&
                batch->submitter == submitter) {
                failed.push_back(batch);
            }
        }
    }
    const auto ret = api->hybm_wait_(entity);
    if (!ret) {
        std::lock_guard<std::mutex> lock(api->batch_mutex_);
        // HYBM synchronizes the current thread's SDMA stream. Other workers'
        // failures must retain their own arrays until their streams drain.
        api->pending_batches_.remove_if([&failed](const auto& batch) {
            return std::find(failed.begin(), failed.end(), batch) !=
                   failed.end();
        });
    }
    return ret;
}
int MemFabricGvaApi::bind(uint16_t device) {
    return nativeApi() ? aclrtSetDevice(device) : -1;
}
int MemFabricGvaApi::initialize(uint16_t device) {
    auto* api = nativeApi();
    return api ? api->hybm_init_(device, 0) : -1;
}
void MemFabricGvaApi::uninitialize() {
    if (auto* api = nativeApi()) api->hybm_uninit_();
}
void* MemFabricGvaApi::create(const MemFabricGvaConfig& config) {
    auto* api = nativeApi();
    if (!api) return nullptr;
    hybm_options options{};
    options.bmType = HYBM_TYPE_HOST_INITIATE;
    options.memType = static_cast<hybm_mem_type>(
        (config.hbm_window ? HYBM_MEM_TYPE_DEVICE : 0) |
        (config.dram_window ? HYBM_MEM_TYPE_HOST : 0));
    options.bmDataOpType =
        config.protocol == "device_rdma"   ? HYBM_DOP_TYPE_DEVICE_RDMA
        : config.protocol == "device_urma" ? HYBM_DOP_TYPE_DEVICE_URMA
                                           : HYBM_DOP_TYPE_SDMA;
    if (config.protocol == "device_rdma") {
        // Match SmemTransEntry's default HYBM endpoint. HYBM discovers the
        // bound NPU's IP; this URL supplies a nonzero device port.
        const auto trans_url =
            "tcp://127.0.0.1:" + std::to_string(11000U + config.entity);
        std::memcpy(options.transUrl, trans_url.c_str(), trans_url.size() + 1);
    }
    options.rankCount = config.rank_count;
    options.rankId = config.rank;
    options.devId = config.device;
    options.maxHBMSize = config.hbm_window;
    options.maxDRAMSize = config.dram_window;
    options.deviceVASpace = config.hbm_size;
    options.hostVASpace = config.dram_size;
    options.scene = HYBM_SCENE_DEFAULT;
    options.enable56BitsGva = false;
    options.dramShmFd = -1;
    return api->hybm_create_entity_(config.entity, &options, 0);
}
void MemFabricGvaApi::destroy(void* entity) {
    if (auto* api = nativeApi()) api->hybm_destroy_entity_(entity, 0);
}
int MemFabricGvaApi::reserve(void* entity) {
    auto* api = nativeApi();
    return api ? api->hybm_reserve_mem_space_(entity, 0) : -1;
}
int MemFabricGvaApi::unreserve(void* entity) {
    auto* api = nativeApi();
    return api ? api->hybm_unreserve_mem_space_(entity, 0) : -1;
}
void* MemFabricGvaApi::allocate(void* entity_handle, uint32_t memory_type,
                                uint64_t size_bytes) {
    auto* api = nativeApi();
    if (!api) return nullptr;
    return api->hybm_alloc_local_memory_(
        entity_handle, static_cast<hybm_mem_type>(memory_type), size_bytes, 0);
}
int MemFabricGvaApi::free(void* entity, void* slice) {
    auto* api = nativeApi();
    return api ? api->hybm_free_local_memory_(entity, slice, 1, 0) : -1;
}
uint64_t MemFabricGvaApi::address(void* entity_handle, void* slice) {
    auto* api = nativeApi();
    if (!api) return 0;
    return reinterpret_cast<uint64_t>(
        api->hybm_get_slice_va_(entity_handle, slice, HYBM_GET_SLICE_GVA));
}
uint64_t MemFabricGvaApi::size(void* entity, void* slice) {
    auto* api = nativeApi();
    return api ? api->hybm_get_slice_size_(entity, slice) : 0;
}
uint64_t MemFabricGvaApi::base(void* entity_handle, uint32_t memory_type) {
    auto* api = nativeApi();
    if (!api) return 0;
    return reinterpret_cast<uint64_t>(api->hybm_get_memory_ptr_(
        entity_handle, static_cast<hybm_mem_type>(memory_type)));
}
int MemFabricGvaApi::translate(uint64_t address, uint32_t memory_type,
                               uint64_t& out) {
    auto* api = nativeApi();
    if (!api) return -1;
    return api->hybm_gva_to_va_(address,
                                static_cast<hybm_mem_type>(memory_type), &out);
}
int MemFabricGvaApi::exportBlob(void* entity_handle, void* slice,
                                std::vector<uint8_t>& blob) {
    auto* api = nativeApi();
    if (!api) return -1;
    hybm_exchange_info exchange_info{};
    int result =
        api->hybm_export_(entity_handle, slice,
                          slice ? 0 : HYBM_FLAG_EXPORT_ENTITY, &exchange_info);
    if (result == 0 && exchange_info.desc && exchange_info.descLen &&
        exchange_info.descLen <= (1U << 20)) {
        blob.assign(exchange_info.desc,
                    exchange_info.desc + exchange_info.descLen);
    } else if (result == 0) {
        result = -1;
    }
    api->hybm_export_info_free_(&exchange_info);
    return result;
}
int MemFabricGvaApi::importBlob(void* entity_handle, bool entity,
                                const std::vector<uint8_t>& blob) {
    auto* api = nativeApi();
    if (!api) return -1;
    hybm_exchange_info exchange_info{const_cast<uint8_t*>(blob.data()),
                                     static_cast<uint32_t>(blob.size())};
    return api->hybm_import_(entity_handle, &exchange_info, 1, nullptr,
                             entity ? HYBM_FLAG_EXPORT_ENTITY : 0);
}
int MemFabricGvaApi::map(void* entity) {
    auto* api = nativeApi();
    return api ? api->hybm_mmap_(entity, 0) : -1;
}
int MemFabricGvaApi::connect(void* entity, uint32_t rank) {
    auto* api = nativeApi();
    return api ? api->hybm_transport_connect_(entity, &rank, 1, 0) : -1;
}
int MemFabricGvaApi::reach(void* entity_handle, uint32_t rank, uint32_t& out) {
    auto* api = nativeApi();
    if (!api) return -1;
    hybm_data_op_type capabilities = HYBM_DOP_TYPE_DEFAULT;
    auto result =
        api->hybm_entity_reach_types_(entity_handle, rank, capabilities, 0);
    out = capabilities;
    return result;
}
int MemFabricGvaApi::remove(void* entity, uint32_t rank) {
    auto* api = nativeApi();
    return api ? api->hybm_remove_imported_(entity, rank, 0) : -1;
}
int MemFabricGvaApi::unmap(void* entity, uint32_t rank) {
    auto* api = nativeApi();
    return api ? api->hybm_unmap_rank_(entity, rank) : -1;
}

#else

// Builds without HYBM headers can use MemFabric trans. GVA initialization
// reports the native API as unavailable.

bool MemFabricGvaApi::LoadLibrary() { return false; }
int MemFabricGvaApi::batchCopy(void*, const std::vector<MemFabricGvaCopy>&,
                               uint32_t) {
    return -1;
}
int MemFabricGvaApi::copy(void*, uint64_t, uint64_t, size_t, uint32_t) {
    return -1;
}
int MemFabricGvaApi::wait(void*) { return -1; }
int MemFabricGvaApi::bind(uint16_t) { return -1; }
int MemFabricGvaApi::initialize(uint16_t) { return -1; }
void MemFabricGvaApi::uninitialize() {}
void* MemFabricGvaApi::create(const MemFabricGvaConfig&) { return nullptr; }
void MemFabricGvaApi::destroy(void*) {}
int MemFabricGvaApi::reserve(void*) { return -1; }
int MemFabricGvaApi::unreserve(void*) { return -1; }
void* MemFabricGvaApi::allocate(void*, uint32_t, uint64_t) { return nullptr; }
int MemFabricGvaApi::free(void*, void*) { return -1; }
uint64_t MemFabricGvaApi::address(void*, void*) { return 0; }
uint64_t MemFabricGvaApi::size(void*, void*) { return 0; }
uint64_t MemFabricGvaApi::base(void*, uint32_t) { return 0; }
int MemFabricGvaApi::translate(uint64_t, uint32_t, uint64_t&) { return -1; }
int MemFabricGvaApi::exportBlob(void*, void*, std::vector<uint8_t>&) {
    return -1;
}
int MemFabricGvaApi::importBlob(void*, bool, const std::vector<uint8_t>&) {
    return -1;
}
int MemFabricGvaApi::map(void*) { return -1; }
int MemFabricGvaApi::connect(void*, uint32_t) { return -1; }
int MemFabricGvaApi::reach(void*, uint32_t, uint32_t&) { return -1; }
int MemFabricGvaApi::remove(void*, uint32_t) { return -1; }
int MemFabricGvaApi::unmap(void*, uint32_t) { return -1; }

#endif  // TENT_WITH_HYBM_GVA
#endif  // TENT_MEMFABRIC_TEST_GVA_API

}  // namespace tent
}  // namespace mooncake
