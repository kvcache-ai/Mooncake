// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_MEMORY_ENGINE_H
#define TENT_MEMORY_ENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define TENT_MEMORY_ENGINE_MEM_HOST (0)
#define TENT_MEMORY_ENGINE_MEM_DEVICE (1)

/* Every C ABI entrypoint returns zero on success or one of the negative
 * values below. Per-entry batchCopy results use the same values. */
typedef enum tent_memory_engine_error {
    MEMORY_ENGINE_SUCCESS = 0,
    MEMORY_ENGINE_E_INVALID_ARG = -1,
    MEMORY_ENGINE_E_NOT_JOINED = -2,
    MEMORY_ENGINE_E_ALREADY_JOINED = -3,
    MEMORY_ENGINE_E_NO_SPACE = -4,
    MEMORY_ENGINE_E_TIMEOUT = -5,
    MEMORY_ENGINE_E_UNSUPPORTED = -6,
    MEMORY_ENGINE_E_BACKEND = -7,
    MEMORY_ENGINE_E_DISABLED = -8,
    MEMORY_ENGINE_E_INTERNAL = -9,
} tent_memory_engine_error_t;

/* Mirrors tent::MemoryEngineConfig. Call tent_memory_engine_config_init()
 * first, then override the fields you need. A raw zero-initialized struct
 * disables the engine and forces auto_ranking off, which is rarely intended. */
typedef struct tent_memory_engine_config {
    uint32_t world_size;
    uint32_t local_rank; /* only used when auto_ranking == 0 */
    uint64_t host_mem_max;
    uint64_t device_mem_max;
    uint32_t join_timeout_s;
    int enabled_server;
    int auto_ranking;
    char store_url[256]; /* empty: derive from metadata_* via fromConfig */
} tent_memory_engine_config_t;

/* Mirrors tent::MemoryEngineCopyEntry. Either side may be a local VA or a
 * GVA returned by tent_memory_engine_allocate(). */
typedef struct tent_memory_engine_copy_entry {
    const void* src;
    void* dst;
    uint64_t size;
} tent_memory_engine_copy_entry_t;

typedef void* tent_memory_engine_t;

/* Fill config with the same defaults as tent::MemoryEngineConfig. Callers
 * using the C ABI should call this before overriding individual fields. */
void tent_memory_engine_config_init(tent_memory_engine_config_t* config);

/* On success stores the new engine in *engine_out. On failure *engine_out is
 * set to NULL and a MEMORY_ENGINE_E_* value is returned. When store_url is
 * empty, the engine derives it from the TENT metadata config
 * (MC_TENT_CONF/environment, metadata_type/metadata_servers). */
int tent_memory_engine_create(const tent_memory_engine_config_t* config,
                              tent_memory_engine_t* engine_out);

void tent_memory_engine_destroy(tent_memory_engine_t engine);

int tent_memory_engine_join(tent_memory_engine_t engine);

int tent_memory_engine_leave(tent_memory_engine_t engine);

int tent_memory_engine_get_local_rank(tent_memory_engine_t engine,
                                      uint32_t* rank_out);

int tent_memory_engine_gva_base(tent_memory_engine_t engine, int mem_type,
                                uint32_t rank, void** gva_out);

int tent_memory_engine_rank_of_gva(tent_memory_engine_t engine, const void* gva,
                                   uint32_t* rank_out);

int tent_memory_engine_allocate(tent_memory_engine_t engine, uint64_t size,
                                int mem_type, void** gva_out);

int tent_memory_engine_free(tent_memory_engine_t engine, void* gva);

/* results must have room for count entries. It is always filled, including
 * when the overall call fails, so callers can report per-entry outcomes. */
int tent_memory_engine_batch_copy(
    tent_memory_engine_t engine, const tent_memory_engine_copy_entry_t* entries,
    size_t count, int32_t* results);

/* Reserved for a later phase: always returns MEMORY_ENGINE_E_UNSUPPORTED. */
int tent_memory_engine_register_memory(tent_memory_engine_t engine, void* va,
                                       uint64_t size, void** gva_out);

/* Reserved for a later phase: always returns MEMORY_ENGINE_E_UNSUPPORTED. */
int tent_memory_engine_unregister_memory(tent_memory_engine_t engine,
                                         void* gva);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#ifdef __cplusplus

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class Config;
class MemfabricBackend;

enum class MemoryEngineMemType { Host = 0, Device = 1 };

// Mirrors tent_memory_engine_config_t. The memory_engine/* config subtree
// populates this struct through fromConfig().
struct MemoryEngineConfig {
    uint32_t world_size{0};
    uint32_t local_rank{0};  // required when auto_ranking == false
    uint64_t host_mem_max{0};
    uint64_t device_mem_max{0};
    uint32_t join_timeout_s{120};  // seconds, passed to smem_bm as-is
    bool enabled_server{true};
    bool auto_ranking{true};
    // Filled by fromConfig() from memory_engine/store_url, or derived from
    // metadata_type/metadata_servers when that key is absent.
    std::string store_url;

    static Status fromConfig(const Config& config, MemoryEngineConfig* out);
};

// Mirrors tent_memory_engine_copy_entry_t. src and dst may each be a local VA
// or a GVA returned by allocate().
struct MemoryEngineCopyEntry {
    const void* src{nullptr};
    void* dst{nullptr};
    uint64_t size{0};
};

// Convert an internal Status into the C ABI error code space.
inline int32_t memoryEngineErrorCode(const Status& status) {
    if (status.ok()) return MEMORY_ENGINE_SUCCESS;
    switch (status.code()) {
        case Status::Code::kInvalidArgument:
            return MEMORY_ENGINE_E_INVALID_ARG;
        case Status::Code::kMemoryEngineNotJoined:
            return MEMORY_ENGINE_E_NOT_JOINED;
        case Status::Code::kMemoryEngineAlreadyJoined:
            return MEMORY_ENGINE_E_ALREADY_JOINED;
        case Status::Code::kMemoryEngineNoSpace:
            return MEMORY_ENGINE_E_NO_SPACE;
        case Status::Code::kMemoryEngineTimeout:
            return MEMORY_ENGINE_E_TIMEOUT;
        case Status::Code::kMemoryEngineUnsupported:
        case Status::Code::kNotImplemented:
            return MEMORY_ENGINE_E_UNSUPPORTED;
        case Status::Code::kMemoryEngineDisabled:
            return MEMORY_ENGINE_E_DISABLED;
        case Status::Code::kMemoryEngineBackendError:
        case Status::Code::kDeviceNotFound:
        case Status::Code::kInternalError:
            return MEMORY_ENGINE_E_BACKEND;
        default:
            return MEMORY_ENGINE_E_INTERNAL;
    }
}

// A peer of TransferEngine: it owns the GVA address space and pool lifecycle,
// not the data path. The default backend is MemFabric smem_bm, loaded from
// libmf_smem.so at create() time.
//
// Backend conventions (not part of MemoryEngineConfig):
//   metadata_type/metadata_servers: select the smem_bm config store. An
//     explicit URL scheme wins; etcd maps to etcd://, everything else to
//     tcp://.
//   MC_MEMORY_ENGINE_LIB: override the libmf_smem.so path.
//   MC_MEMORY_ENGINE_DATA_OP: sdma | host_rdma | host_tcp | device_rdma |
//     host_urma | host_shm | device_urma | device_uboe (default device_rdma).
//   Device id is passed to smem_bm_init as 0xffff; smem_bm resolves it.
//
// Lifecycle: create() -> join() -> allocate()/batchCopy() -> free() -> leave().
// join()/leave() may be called repeatedly; the backend re-creates its local
// segment on the next join after leave.
class MemoryEngine {
   public:
    // config.store_url must be set, normally through
    // MemoryEngineConfig::fromConfig() (memory_engine/store_url or the TENT
    // metadata settings). create() does not read the environment directly.
    static Status create(const MemoryEngineConfig& config,
                         std::unique_ptr<MemoryEngine>* out);

    ~MemoryEngine();

    MemoryEngine(const MemoryEngine&) = delete;
    MemoryEngine& operator=(const MemoryEngine&) = delete;

    Status join();
    Status leave();

    // Address queries. They only inspect the local layout and do not touch the
    // network; getLocalRank() is available as soon as create() returns.
    Status getLocalRank(uint32_t* rank_out) const;
    Status gvaBase(MemoryEngineMemType type, uint32_t rank,
                   void** gva_out) const;
    Status rankOfGva(const void* gva, uint32_t* rank_out) const;

    // Pool memory: both the returned and accepted addresses are GVAs.
    // allocate() passes size through to smem_bm, which expects its native
    // allocation granularity (currently 2 MB). free() is a no-op; the slice is
    // reclaimed when leave() destroys the BM.
    Status allocate(uint64_t size, MemoryEngineMemType type, void** gva_out);
    Status free(void* gva);

    // Executes each entry independently. results[i] is 0 on success or a
    // MEMORY_ENGINE_E_* value; the overall Status only reports argument,
    // lifecycle, or backend errors.
    Status batchCopy(const std::vector<MemoryEngineCopyEntry>& entries,
                     std::vector<int32_t>* results);

    // Reserved for a later phase; always MEMORY_ENGINE_E_UNSUPPORTED.
    Status registerMemory(void* va, uint64_t size, void** gva_out);
    Status unregisterMemory(void* gva);

   private:
    explicit MemoryEngine(std::unique_ptr<MemfabricBackend> backend);

    std::unique_ptr<MemfabricBackend> backend_;
    bool joined_{false};
    mutable std::shared_mutex mutex_;
};

}  // namespace tent
}  // namespace mooncake
#endif  // __cplusplus

#endif  // TENT_MEMORY_ENGINE_H
