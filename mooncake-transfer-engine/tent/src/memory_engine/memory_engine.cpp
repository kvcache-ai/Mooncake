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

#include "tent/memory_engine.h"

#include <glog/logging.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <string>

#include "tent/common/config.h"
#include "tent/memory_engine/backend/memfabric_backend.h"

namespace mooncake {
namespace tent {
namespace {

// smem_bm keeps its control plane in the TENT metadata configuration. An
// explicit URL scheme wins; otherwise etcd maps to etcd:// and every other
// TENT metadata type maps to the TCP config store.
std::string DeriveStoreUrl(const Config& config) {
    const std::string servers = config.get("metadata_servers", std::string());
    if (servers.empty()) return {};
    if (servers.find("://") != std::string::npos) return servers;
    const std::string type = config.get("metadata_type", std::string("p2p"));
    if (type == "etcd") return "etcd://" + servers;
    return "tcp://" + servers;
}

}  // namespace

Status MemoryEngineConfig::fromConfig(const Config& config,
                                      MemoryEngineConfig* out) {
    if (!out) {
        return Status::InvalidArgument("out is null" LOC_MARK);
    }
    MemoryEngineConfig result;
    result.enabled_server = config.get("memory_engine/enabled_server", true);
    result.world_size = config.get<uint32_t>("memory_engine/world_size", 0);
    result.auto_ranking = config.get("memory_engine/auto_ranking", true);
    result.local_rank = config.get<uint32_t>("memory_engine/local_rank", 0);
    result.host_mem_max = config.get<uint64_t>("memory_engine/host_mem_max", 0);
    result.device_mem_max =
        config.get<uint64_t>("memory_engine/device_mem_max", 0);
    result.join_timeout_s =
        config.get<uint32_t>("memory_engine/join_timeout_s", 120);
    result.store_url = config.get("memory_engine/store_url", std::string());
    if (result.store_url.empty()) result.store_url = DeriveStoreUrl(config);
    *out = result;
    return Status::OK();
}

MemoryEngine::MemoryEngine(std::unique_ptr<MemfabricBackend> backend)
    : backend_(std::move(backend)) {}

MemoryEngine::~MemoryEngine() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (joined_) {
        const Status status = backend_->leave();
        if (!status.ok()) {
            LOG(WARNING) << "MemoryEngine::leave during destruction: "
                         << status.ToString();
        }
        joined_ = false;
    }
    backend_->shutdown();
}

Status MemoryEngine::create(const MemoryEngineConfig& config,
                            std::unique_ptr<MemoryEngine>* out) {
    if (!out) {
        return Status::InvalidArgument("out is null" LOC_MARK);
    }
    *out = nullptr;
    if (!config.enabled_server) {
        return Status::MemoryEngineDisabled(
            "memory_engine/enabled_server is false" LOC_MARK);
    }
    if (config.store_url.empty()) {
        return Status::InvalidArgument(
            "memory_engine/store_url or metadata_servers is required" LOC_MARK);
    }

    auto backend = std::make_unique<MemfabricBackend>();
    CHECK_STATUS(backend->create(config));
    *out = std::unique_ptr<MemoryEngine>(new MemoryEngine(std::move(backend)));
    return Status::OK();
}

Status MemoryEngine::join() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (joined_) {
        return Status::MemoryEngineAlreadyJoined(
            "MemoryEngine is already joined" LOC_MARK);
    }
    CHECK_STATUS(backend_->join());
    joined_ = true;
    return Status::OK();
}

Status MemoryEngine::leave() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!joined_) {
        return Status::MemoryEngineNotJoined(
            "MemoryEngine is not joined" LOC_MARK);
    }
    const Status status = backend_->leave();
    joined_ = false;
    return status;
}

Status MemoryEngine::getLocalRank(uint32_t* rank_out) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return backend_->getLocalRank(rank_out);
}

Status MemoryEngine::gvaBase(MemoryEngineMemType type, uint32_t rank,
                             void** gva_out) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return backend_->gvaBase(type, rank, gva_out);
}

Status MemoryEngine::rankOfGva(const void* gva, uint32_t* rank_out) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return backend_->rankOfGva(gva, rank_out);
}

Status MemoryEngine::allocate(uint64_t size, MemoryEngineMemType type,
                              void** gva_out) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return backend_->allocate(size, type, gva_out);
}

Status MemoryEngine::free(void* gva) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return backend_->free(gva);
}

Status MemoryEngine::batchCopy(
    const std::vector<MemoryEngineCopyEntry>& entries,
    std::vector<int32_t>* results) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return backend_->batchCopy(entries, results);
}

Status MemoryEngine::registerMemory(void* va, uint64_t size, void** gva_out) {
    (void)va;
    (void)size;
    (void)gva_out;
    return Status::MemoryEngineUnsupported(
        "registerMemory is reserved for a later phase" LOC_MARK);
}

Status MemoryEngine::unregisterMemory(void* gva) {
    (void)gva;
    return Status::MemoryEngineUnsupported(
        "unregisterMemory is reserved for a later phase" LOC_MARK);
}

}  // namespace tent
}  // namespace mooncake

namespace {

mooncake::tent::MemoryEngine* Cast(tent_memory_engine_t engine) {
    return static_cast<mooncake::tent::MemoryEngine*>(engine);
}

int MemoryEngineStatus(const mooncake::tent::Status& status,
                       const char* operation) {
    if (status.ok()) return MEMORY_ENGINE_SUCCESS;
    LOG(ERROR) << operation << ": " << status.ToString();
    return mooncake::tent::memoryEngineErrorCode(status);
}

}  // namespace

extern "C" {

void tent_memory_engine_config_init(tent_memory_engine_config_t* config) {
    if (!config) return;
    config->world_size = 0;
    config->local_rank = 0;
    config->host_mem_max = 0;
    config->device_mem_max = 0;
    config->join_timeout_s = 120;
    config->enabled_server = 1;
    config->auto_ranking = 1;
    config->store_url[0] = '\0';
}

int tent_memory_engine_create(const tent_memory_engine_config_t* config,
                              tent_memory_engine_t* engine_out) {
    if (!engine_out) return MEMORY_ENGINE_E_INVALID_ARG;
    *engine_out = nullptr;

    mooncake::tent::MemoryEngineConfig engine_config;
    std::unique_ptr<mooncake::tent::MemoryEngine> engine;
    mooncake::tent::Status status;
    if (config) {
        engine_config.world_size = config->world_size;
        engine_config.local_rank = config->local_rank;
        engine_config.host_mem_max = config->host_mem_max;
        engine_config.device_mem_max = config->device_mem_max;
        engine_config.join_timeout_s = config->join_timeout_s;
        engine_config.enabled_server = config->enabled_server != 0;
        engine_config.auto_ranking = config->auto_ranking != 0;
        engine_config.store_url.assign(
            config->store_url,
            strnlen(config->store_url, sizeof(config->store_url)));
        if (engine_config.store_url.empty()) {
            auto tent_config = std::make_shared<mooncake::tent::Config>();
            status = mooncake::tent::ConfigHelper().loadFromEnv(*tent_config);
            if (status.ok()) {
                mooncake::tent::MemoryEngineConfig derived;
                status = mooncake::tent::MemoryEngineConfig::fromConfig(
                    *tent_config, &derived);
                if (status.ok()) {
                    engine_config.store_url = std::move(derived.store_url);
                }
            }
        }
        if (status.ok()) {
            status =
                mooncake::tent::MemoryEngine::create(engine_config, &engine);
        }
    } else {
        auto tent_config = std::make_shared<mooncake::tent::Config>();
        status = mooncake::tent::ConfigHelper().loadFromEnv(*tent_config);
        if (status.ok()) {
            status = mooncake::tent::MemoryEngineConfig::fromConfig(
                *tent_config, &engine_config);
            if (status.ok()) {
                status = mooncake::tent::MemoryEngine::create(engine_config,
                                                              &engine);
            }
        }
    }
    if (!status.ok()) {
        return MemoryEngineStatus(status, "tent_memory_engine_create");
    }
    *engine_out = engine.release();
    return MEMORY_ENGINE_SUCCESS;
}

void tent_memory_engine_destroy(tent_memory_engine_t engine) {
    delete Cast(engine);
}

int tent_memory_engine_join(tent_memory_engine_t engine) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->join(), "tent_memory_engine_join");
}

int tent_memory_engine_leave(tent_memory_engine_t engine) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->leave(),
                              "tent_memory_engine_leave");
}

int tent_memory_engine_get_local_rank(tent_memory_engine_t engine,
                                      uint32_t* rank_out) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->getLocalRank(rank_out),
                              "tent_memory_engine_get_local_rank");
}

int tent_memory_engine_gva_base(tent_memory_engine_t engine, int mem_type,
                                uint32_t rank, void** gva_out) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    if (mem_type != TENT_MEMORY_ENGINE_MEM_HOST &&
        mem_type != TENT_MEMORY_ENGINE_MEM_DEVICE)
        return MEMORY_ENGINE_E_INVALID_ARG;
    using mooncake::tent::MemoryEngineMemType;
    const MemoryEngineMemType type = mem_type == TENT_MEMORY_ENGINE_MEM_DEVICE
                                         ? MemoryEngineMemType::Device
                                         : MemoryEngineMemType::Host;
    return MemoryEngineStatus(Cast(engine)->gvaBase(type, rank, gva_out),
                              "tent_memory_engine_gva_base");
}

int tent_memory_engine_rank_of_gva(tent_memory_engine_t engine, const void* gva,
                                   uint32_t* rank_out) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->rankOfGva(gva, rank_out),
                              "tent_memory_engine_rank_of_gva");
}

int tent_memory_engine_allocate(tent_memory_engine_t engine, uint64_t size,
                                int mem_type, void** gva_out) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    using mooncake::tent::MemoryEngineMemType;
    const MemoryEngineMemType type = mem_type == TENT_MEMORY_ENGINE_MEM_DEVICE
                                         ? MemoryEngineMemType::Device
                                         : MemoryEngineMemType::Host;
    return MemoryEngineStatus(Cast(engine)->allocate(size, type, gva_out),
                              "tent_memory_engine_allocate");
}

int tent_memory_engine_free(tent_memory_engine_t engine, void* gva) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->free(gva),
                              "tent_memory_engine_free");
}

int tent_memory_engine_batch_copy(
    tent_memory_engine_t engine, const tent_memory_engine_copy_entry_t* entries,
    size_t count, int32_t* results) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    if (count > 0 && (!entries || !results)) return MEMORY_ENGINE_E_INVALID_ARG;

    std::vector<mooncake::tent::MemoryEngineCopyEntry> copy_entries;
    copy_entries.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        copy_entries.push_back(mooncake::tent::MemoryEngineCopyEntry{
            entries[i].src, entries[i].dst, entries[i].size});
    }
    std::vector<int32_t> copy_results;
    const mooncake::tent::Status status =
        Cast(engine)->batchCopy(copy_entries, &copy_results);
    for (size_t i = 0; i < count; ++i) results[i] = copy_results[i];
    return MemoryEngineStatus(status, "tent_memory_engine_batch_copy");
}

int tent_memory_engine_register_memory(tent_memory_engine_t engine, void* va,
                                       uint64_t size, void** gva_out) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->registerMemory(va, size, gva_out),
                              "tent_memory_engine_register_memory");
}

int tent_memory_engine_unregister_memory(tent_memory_engine_t engine,
                                         void* gva) {
    if (!engine) return MEMORY_ENGINE_E_INVALID_ARG;
    return MemoryEngineStatus(Cast(engine)->unregisterMemory(gva),
                              "tent_memory_engine_unregister_memory");
}

}  // extern "C"
