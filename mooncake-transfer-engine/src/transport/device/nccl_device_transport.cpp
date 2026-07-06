// Copyright 2024 KVCache.AI
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

#include "transport/device/device_transport.h"

#include <cuda_runtime.h>
#include <glog/logging.h>

#include <cstring>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace mooncake {
namespace device {
namespace {

int reportNcclError(ncclResult_t result, const char* operation) {
    if (result == ncclSuccess) return 0;
    LOG(ERROR) << "[Device NCCL] " << operation
               << " failed: " << ncclGetErrorString(result);
    return -1;
}

int reportCudaError(cudaError_t result, const char* operation) {
    if (result == cudaSuccess) return 0;
    LOG(ERROR) << "[Device NCCL] " << operation
               << " failed: " << cudaGetErrorString(result);
    return -1;
}

const char* ginTypeName(ncclGinType_t type) {
    switch (type) {
        case NCCL_GIN_TYPE_NONE:
            return "none";
        case NCCL_GIN_TYPE_PROXY:
            return "proxy";
        case NCCL_GIN_TYPE_GDAKI:
            return "gdaki";
        default:
            return "unknown";
    }
}

bool decodeUniqueId(const std::vector<int32_t>& encoded, ncclUniqueId* id) {
    const size_t words =
        (sizeof(ncclUniqueId) + sizeof(int32_t) - 1) / sizeof(int32_t);
    if (!id || encoded.size() != words) return false;
    std::memcpy(id, encoded.data(), sizeof(*id));
    return true;
}

}  // namespace

class NcclDeviceTransportImpl final : public NcclTransport {
   public:
    ~NcclDeviceTransportImpl() override { shutdown(); }

    std::vector<int32_t> createUniqueId() override {
        ncclUniqueId id{};
        if (reportNcclError(ncclGetUniqueId(&id), "ncclGetUniqueId") != 0) {
            return {};
        }

        const size_t words =
            (sizeof(id) + sizeof(int32_t) - 1) / sizeof(int32_t);
        std::vector<int32_t> encoded(words, 0);
        std::memcpy(encoded.data(), &id, sizeof(id));
        return encoded;
    }

    int initialize(const NcclTransportConfig& config,
                   const std::vector<int32_t>& unique_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (initialized_) {
            LOG(ERROR) << "[Device NCCL] transport is already initialized";
            return -1;
        }
        if (config.num_ranks <= 0 || config.rank < 0 ||
            config.rank >= config.num_ranks || config.gin_context_count <= 0 ||
            config.gin_context_count > std::numeric_limits<int>::max() / 2 ||
            config.gin_counter_count < 0 ||
            config.world_gin_barrier_count < 0) {
            LOG(ERROR) << "[Device NCCL] invalid communicator configuration";
            return -1;
        }

        const bool gin_enabled =
            config.gin_connection_type != NCCL_GIN_CONNECTION_NONE;
        int signal_count = config.gin_signal_count;
        if (signal_count < 0) {
            signal_count = gin_enabled ? 2 * config.gin_context_count : 0;
        }
        if (signal_count < 0) {
            LOG(ERROR) << "[Device NCCL] invalid GIN signal count";
            return -1;
        }
        if (!gin_enabled &&
            (signal_count != 0 || config.gin_counter_count != 0 ||
             config.world_gin_barrier_count != 0)) {
            LOG(ERROR) << "[Device NCCL] GIN resources require a GIN "
                          "connection";
            return -1;
        }

        ncclUniqueId id{};
        if (!decodeUniqueId(unique_id, &id)) {
            LOG(ERROR) << "[Device NCCL] invalid NCCL unique ID size";
            return -1;
        }

        int runtime_version = 0;
        if (reportNcclError(ncclGetVersion(&runtime_version),
                            "ncclGetVersion") != 0) {
            return -1;
        }
        if (runtime_version != NCCL_VERSION_CODE) {
            LOG(ERROR) << "[Device NCCL] Device API requires matching compile-time "
                          "and runtime NCCL versions: compiled="
                       << NCCL_VERSION_CODE << " runtime=" << runtime_version;
            return -1;
        }

        int cuda_device = -1;
        if (reportCudaError(cudaGetDevice(&cuda_device), "cudaGetDevice") !=
            0) {
            return -1;
        }

        if (reportNcclError(
                ncclCommInitRank(&comm_, config.num_ranks, id, config.rank),
                "ncclCommInitRank") != 0) {
            comm_ = nullptr;
            return -1;
        }

        ncclCommProperties_t comm_properties =
            NCCL_COMM_PROPERTIES_INITIALIZER;
        if (reportNcclError(
                ncclCommQueryProperties(comm_, &comm_properties),
                "ncclCommQueryProperties") != 0) {
            abortPartialInitialization();
            return -1;
        }
        if (!comm_properties.deviceApiSupport) {
            LOG(ERROR) << "[Device NCCL] NCCL communicator does not support "
                          "the device API";
            abortPartialInitialization();
            return -1;
        }
        if (config.gin_connection_type != NCCL_GIN_CONNECTION_NONE &&
            comm_properties.ginType == NCCL_GIN_TYPE_NONE) {
            LOG(ERROR) << "[Device NCCL] GIN is unavailable on this "
                          "communicator";
            abortPartialInitialization();
            return -1;
        }

        ncclDevCommRequirements_t requirements =
            NCCL_DEV_COMM_REQUIREMENTS_INITIALIZER;
        requirements.lsaMultimem = config.lsa_multimem;
        requirements.ginContextCount = config.gin_context_count;
        requirements.ginSignalCount = signal_count;
        requirements.ginCounterCount = config.gin_counter_count;
        requirements.ginConnectionType = config.gin_connection_type;
        requirements.ginExclusiveContexts = config.gin_exclusive_contexts;
        requirements.worldGinBarrierCount =
            config.world_gin_barrier_count;

        ncclResult_t start_result = ncclGroupStart();
        ncclResult_t create_result = ncclSuccess;
        ncclResult_t end_result = ncclSuccess;
        if (start_result == ncclSuccess) {
            create_result =
                ncclDevCommCreate(comm_, &requirements, &dev_comm_);
            end_result = ncclGroupEnd();
        }
        if (reportNcclError(start_result, "ncclGroupStart") != 0 ||
            reportNcclError(create_result, "ncclDevCommCreate") != 0 ||
            reportNcclError(end_result, "ncclGroupEnd") != 0) {
            abortPartialInitialization();
            return -1;
        }
        dev_comm_created_ = true;

        properties_.runtime_version = runtime_version;
        properties_.rank = comm_properties.rank;
        properties_.num_ranks = comm_properties.nRanks;
        properties_.cuda_device = comm_properties.cudaDev;
        properties_.device_api_supported =
            comm_properties.deviceApiSupport;
        properties_.gin_type = comm_properties.ginType;
        properties_.gin_connection_count = dev_comm_.ginConnectionCount;
        properties_.gin_context_count =
            static_cast<int>(dev_comm_.ginContextCount);
        properties_.gin_signal_count = dev_comm_.ginSignalCount;
        properties_.gin_counter_count = dev_comm_.ginCounterCount;
        initialized_ = true;

        LOG(INFO) << "[Device NCCL] initialized rank=" << properties_.rank
                  << "/" << properties_.num_ranks
                  << " cuda_device=" << properties_.cuda_device
                  << " gin_type=" << ginTypeName(properties_.gin_type)
                  << " gin_connections="
                  << properties_.gin_connection_count
                  << " gin_contexts=" << properties_.gin_context_count
                  << " gin_signals=" << properties_.gin_signal_count;
        return 0;
    }

    void* allocateBuffer(size_t bytes) override {
        if (bytes == 0) {
            LOG(ERROR) << "[Device NCCL] cannot allocate a zero-sized buffer";
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        void* ptr = nullptr;
        if (reportNcclError(ncclMemAlloc(&ptr, bytes), "ncclMemAlloc") != 0) {
            return nullptr;
        }

        allocations_.insert(ptr);
        return ptr;
    }

    int freeBuffer(void* ptr) override {
        if (!ptr) return 0;

        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : windows_) {
            if (entry.second.ptr == ptr) {
                LOG(ERROR) << "[Device NCCL] deregister the window before "
                              "freeing its buffer";
                return -1;
            }
        }

        auto it = allocations_.find(ptr);
        if (it == allocations_.end()) {
            LOG(ERROR) << "[Device NCCL] buffer was not allocated by this "
                          "transport";
            return -1;
        }
        if (reportNcclError(ncclMemFree(ptr), "ncclMemFree") != 0) {
            return -1;
        }
        allocations_.erase(it);
        return 0;
    }

    int registerWindow(void* ptr, size_t bytes, ncclWindow_t* window,
                       int flags) override {
        if (!ptr || bytes == 0 || !window) {
            LOG(ERROR) << "[Device NCCL] invalid window registration";
            return -1;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (!initialized_) {
            LOG(ERROR) << "[Device NCCL] initialize before registering a "
                          "window";
            return -1;
        }

        ncclWindow_t registered = nullptr;
        if (reportNcclError(
                ncclCommWindowRegister(comm_, ptr, bytes, &registered, flags),
                "ncclCommWindowRegister") != 0) {
            return -1;
        }
        windows_.emplace(registered, WindowRecord{ptr});
        *window = registered;
        return 0;
    }

    int deregisterWindow(ncclWindow_t window) override {
        if (!window) return 0;

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = windows_.find(window);
        if (it == windows_.end()) {
            LOG(ERROR) << "[Device NCCL] unknown window";
            return -1;
        }
        if (reportNcclError(ncclCommWindowDeregister(comm_, window),
                            "ncclCommWindowDeregister") != 0) {
            return -1;
        }
        windows_.erase(it);
        return 0;
    }

    NcclDeviceContext deviceContext() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        NcclDeviceContext context{};
        if (initialized_) context.comm = dev_comm_;
        return context;
    }

    ncclComm_t communicator() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return comm_;
    }

    const NcclTransportProperties& properties() const override {
        return properties_;
    }

    bool initialized() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return initialized_;
    }

    int shutdown() override {
        std::lock_guard<std::mutex> lock(mutex_);
        int status = 0;

        if (comm_) {
            for (const auto& entry : windows_) {
                if (reportNcclError(
                        ncclCommWindowDeregister(comm_, entry.first),
                        "ncclCommWindowDeregister") != 0) {
                    status = -1;
                }
            }
        }
        windows_.clear();

        for (void* ptr : allocations_) {
            if (reportNcclError(ncclMemFree(ptr), "ncclMemFree") != 0) {
                status = -1;
            }
        }
        allocations_.clear();

        if (dev_comm_created_) {
            if (reportNcclError(ncclDevCommDestroy(comm_, &dev_comm_),
                                "ncclDevCommDestroy") != 0) {
                status = -1;
            }
            dev_comm_created_ = false;
            dev_comm_ = {};
        }
        if (comm_) {
            if (reportNcclError(ncclCommDestroy(comm_), "ncclCommDestroy") !=
                0) {
                status = -1;
            }
            comm_ = nullptr;
        }

        initialized_ = false;
        properties_ = {};
        return status;
    }

   private:
    struct WindowRecord {
        void* ptr;
    };

    void abortPartialInitialization() {
        if (dev_comm_created_) {
            ncclDevCommDestroy(comm_, &dev_comm_);
            dev_comm_created_ = false;
            dev_comm_ = {};
        }
        if (comm_) {
            ncclCommAbort(comm_);
            comm_ = nullptr;
        }
    }

    mutable std::mutex mutex_;
    ncclComm_t comm_ = nullptr;
    ncclDevComm_t dev_comm_{};
    bool dev_comm_created_ = false;
    bool initialized_ = false;
    std::unordered_map<ncclWindow_t, WindowRecord> windows_;
    std::unordered_set<void*> allocations_;
    NcclTransportProperties properties_;
};

std::unique_ptr<NcclTransport> createNcclDeviceTransport() {
    return std::make_unique<NcclDeviceTransportImpl>();
}

}  // namespace device
}  // namespace mooncake
