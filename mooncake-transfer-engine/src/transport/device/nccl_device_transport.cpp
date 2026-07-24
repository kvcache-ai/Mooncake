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

#include "transport/device/nccl_device_transport.h"

#include <cuda_runtime.h>
#include <glog/logging.h>
#include <nccl.h>
#include <nccl_device.h>

#include <cstring>
#include <limits>
#include <unordered_map>
#include <unordered_set>

#if NCCL_VERSION_CODE < 23004
#error "Mooncake NCCL DeviceTransport requires NCCL 2.30.4 or newer"
#endif

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

NcclGinBackend toGinBackend(ncclGinType_t type) {
    switch (type) {
        case NCCL_GIN_TYPE_PROXY:
            return NcclGinBackend::kProxy;
        case NCCL_GIN_TYPE_GDAKI:
            return NcclGinBackend::kGdaki;
#if NCCL_VERSION_CODE >= NCCL_VERSION(2, 30, 6)
        case NCCL_GIN_TYPE_GPI:
            return NcclGinBackend::kGpi;
#endif
        case NCCL_GIN_TYPE_NONE:
        default:
            return NcclGinBackend::kNone;
    }
}

const char* ginBackendName(NcclGinBackend backend) {
    switch (backend) {
        case NcclGinBackend::kProxy:
            return "proxy";
        case NcclGinBackend::kGdaki:
            return "gdaki";
        case NcclGinBackend::kGpi:
            return "gpi";
        case NcclGinBackend::kNone:
        default:
            return "none";
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
        if (initialized_) {
            LOG(ERROR) << "[Device NCCL] transport is already initialized";
            return -1;
        }
        if (config.num_ranks <= 0 || config.rank < 0 ||
            config.rank >= config.num_ranks || config.gin_context_count < 0 ||
            (config.enable_gin && config.gin_context_count == 0) ||
            config.gin_context_count > std::numeric_limits<int>::max() / 2 ||
            config.lsa_barrier_count < 0) {
            LOG(ERROR) << "[Device NCCL] invalid communicator configuration";
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
            LOG(ERROR)
                << "[Device NCCL] Device API requires matching compile-time "
                   "and runtime NCCL versions: compiled="
                << NCCL_VERSION_CODE << " runtime=" << runtime_version
                << ". Rebuild Mooncake against the runtime NCCL installation, "
                   "and rebuild AOT NCCL device kernels or invalidate and "
                   "re-JIT cached NCCL Device API kernels";
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

        ncclCommProperties_t comm_properties = NCCL_COMM_PROPERTIES_INITIALIZER;
        if (reportNcclError(ncclCommQueryProperties(comm_, &comm_properties),
                            "ncclCommQueryProperties") != 0) {
            abortPartialInitialization();
            return -1;
        }
        if (!comm_properties.deviceApiSupport) {
            LOG(ERROR) << "[Device NCCL] communicator does not support the "
                          "device API";
            abortPartialInitialization();
            return -1;
        }
        if (config.require_lsa_multimem && !comm_properties.multimemSupport) {
            LOG(ERROR) << "[Device NCCL] LSA multimem was required but is "
                          "not supported";
            abortPartialInitialization();
            return -1;
        }
        if (config.enable_gin &&
            comm_properties.ginType == NCCL_GIN_TYPE_NONE) {
            LOG(ERROR) << "[Device NCCL] full GIN connectivity was requested "
                          "but GIN is unavailable";
            abortPartialInitialization();
            return -1;
        }

        ncclDevCommRequirements_t requirements =
            NCCL_DEV_COMM_REQUIREMENTS_INITIALIZER;
        requirements.lsaMultimem = config.require_lsa_multimem;
        requirements.lsaBarrierCount = config.lsa_barrier_count;
        requirements.ginContextCount =
            config.enable_gin ? config.gin_context_count : 0;
        requirements.ginConnectionType = config.enable_gin
                                             ? NCCL_GIN_CONNECTION_FULL
                                             : NCCL_GIN_CONNECTION_NONE;
        requirements.ginExclusiveContexts =
            config.enable_gin && config.gin_exclusive_contexts;

        ncclResult_t start_result = ncclGroupStart();
        ncclResult_t create_result = ncclSuccess;
        ncclResult_t end_result = ncclSuccess;
        if (start_result == ncclSuccess) {
            create_result = ncclDevCommCreate(comm_, &requirements, &dev_comm_);
            end_result = ncclGroupEnd();
        }
        if (reportNcclError(start_result, "ncclGroupStart") != 0 ||
            reportNcclError(create_result, "ncclDevCommCreate") != 0 ||
            reportNcclError(end_result, "ncclGroupEnd") != 0) {
            abortPartialInitialization();
            return -1;
        }
        dev_comm_created_ = true;

        if (config.enable_gin && (dev_comm_.ginConnectionCount == 0 ||
                                  dev_comm_.ginContextCount == 0)) {
            LOG(ERROR) << "[Device NCCL] communicator did not provide the "
                          "requested full GIN resources";
            abortPartialInitialization();
            return -1;
        }

        if (reportCudaError(cudaMalloc(reinterpret_cast<void**>(&device_comm_),
                                       sizeof(dev_comm_)),
                            "cudaMalloc(device communicator)") != 0 ||
            reportCudaError(
                cudaMemcpy(device_comm_, &dev_comm_, sizeof(dev_comm_),
                           cudaMemcpyHostToDevice),
                "cudaMemcpy(device communicator)") != 0 ||
            reportCudaError(cudaStreamCreateWithFlags(&control_stream_,
                                                      cudaStreamNonBlocking),
                            "cudaStreamCreateWithFlags(control)") != 0 ||
            reportCudaError(
                cudaMalloc(reinterpret_cast<void**>(&collective_status_),
                           sizeof(int)),
                "cudaMalloc(collective status)") != 0) {
            abortPartialInitialization();
            return -1;
        }

        properties_.runtime_version = runtime_version;
        properties_.rank = comm_properties.rank;
        properties_.num_ranks = comm_properties.nRanks;
        properties_.cuda_device = comm_properties.cudaDev;
        properties_.device_api_supported = comm_properties.deviceApiSupport;
        properties_.multimem_supported = comm_properties.multimemSupport;
        properties_.lsa_multimem_enabled = config.require_lsa_multimem;
        properties_.lsa_team_count = comm_properties.nLsaTeams;
        properties_.lsa_barrier_count = config.lsa_barrier_count;
        properties_.gin_enabled = config.enable_gin;
        properties_.gin_backend = toGinBackend(comm_properties.ginType);
        properties_.gin_connection_count = dev_comm_.ginConnectionCount;
        properties_.gin_context_count =
            static_cast<int>(dev_comm_.ginContextCount);
        initialized_ = true;

        LOG(INFO) << "[Device NCCL] initialized rank=" << properties_.rank
                  << "/" << properties_.num_ranks
                  << " cuda_device=" << properties_.cuda_device
                  << " lsa_teams=" << properties_.lsa_team_count
                  << " multimem=" << properties_.lsa_multimem_enabled
                  << " lsa_barriers=" << properties_.lsa_barrier_count
                  << " gin_backend=" << ginBackendName(properties_.gin_backend)
                  << " gin_connections=" << properties_.gin_connection_count
                  << " gin_contexts=" << properties_.gin_context_count;
        return 0;
    }

    void* allocateBuffer(size_t bytes) override {
        if (bytes == 0) {
            LOG(ERROR) << "[Device NCCL] cannot allocate a zero-sized buffer";
            return nullptr;
        }

        void* ptr = nullptr;
        if (reportNcclError(ncclMemAlloc(&ptr, bytes), "ncclMemAlloc") != 0) {
            return nullptr;
        }

        allocations_.insert(ptr);
        return ptr;
    }

    int freeBuffer(void* ptr) override {
        if (!ptr) return 0;

        for (const auto& entry : registrations_) {
            if (entry.second.ptr == ptr) {
                LOG(ERROR) << "[Device NCCL] deregister the buffer before "
                              "freeing its allocation";
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

    int registerBuffer(void* ptr, size_t bytes,
                       NcclBufferRegistration* registration) override {
        if (!ptr || bytes == 0 || !registration || registration->valid()) {
            LOG(ERROR) << "[Device NCCL] invalid buffer registration";
            return -1;
        }
        if (!initialized_) {
            LOG(ERROR) << "[Device NCCL] initialize before registering a "
                          "buffer";
            return -1;
        }

        ncclWindow_t window = nullptr;
        if (reportNcclError(ncclCommWindowRegister(comm_, ptr, bytes, &window,
                                                   NCCL_WIN_COLL_SYMMETRIC),
                            "ncclCommWindowRegister") != 0) {
            return -1;
        }

        if (next_registration_id_ == 0) {
            LOG(ERROR) << "[Device NCCL] registration ID space exhausted";
            ncclCommWindowDeregister(comm_, window);
            return -1;
        }
        const uint64_t id = next_registration_id_++;
        registrations_.emplace(id, WindowRecord{window, ptr, bytes});
        registration->id_ = id;
        return 0;
    }

    int deregisterBuffer(NcclBufferRegistration* registration) override {
        if (!registration || !registration->valid()) return 0;

        auto it = registrations_.find(registration->id_);
        if (it == registrations_.end()) {
            LOG(ERROR) << "[Device NCCL] unknown buffer registration";
            return -1;
        }
        if (reportNcclError(ncclCommWindowDeregister(comm_, it->second.window),
                            "ncclCommWindowDeregister") != 0) {
            return -1;
        }
        registrations_.erase(it);
        registration->id_ = 0;
        return 0;
    }

    int allocateAndRegisterBuffer(
        size_t bytes, void** ptr,
        NcclBufferRegistration* registration) override {
        if (!initialized_ || bytes == 0 || !ptr || !registration ||
            registration->valid()) {
            LOG(ERROR) << "[Device NCCL] invalid collective buffer request";
            return -1;
        }

        *ptr = nullptr;
        void* local_ptr = allocateBuffer(bytes);
        const bool all_allocated = collectiveAllSucceeded(local_ptr != nullptr);
        if (!all_allocated) {
            if (local_ptr) freeBuffer(local_ptr);
            return -1;
        }

        NcclBufferRegistration local_registration;
        const int register_status =
            registerBuffer(local_ptr, bytes, &local_registration);
        const bool all_registered =
            collectiveAllSucceeded(register_status == 0);
        if (!all_registered) {
            if (local_registration.valid())
                deregisterBuffer(&local_registration);
            freeBuffer(local_ptr);
            return -1;
        }

        *ptr = local_ptr;
        *registration = local_registration;
        return 0;
    }

    NcclDeviceContext deviceContext(
        const NcclBufferRegistration& registration) const override {
        NcclDeviceContext context;
        if (!initialized_ || !registration.valid()) return context;

        const auto it = registrations_.find(registration.id_);
        if (it == registrations_.end()) {
            LOG(ERROR) << "[Device NCCL] unknown buffer registration";
            return context;
        }

        context.native_comm_ = device_comm_;
        context.native_window_ = it->second.window;
        context.local_base_ = it->second.ptr;
        context.rank_ = properties_.rank;
        context.gin_context_count_ = properties_.gin_context_count;
        context.gin_enabled_ = properties_.gin_enabled;
        context.lsa_multimem_enabled_ = properties_.lsa_multimem_enabled;
        return context;
    }

    NcclTransportProperties properties() const override { return properties_; }

    bool initialized() const override { return initialized_; }

    int shutdown() override {
        int status = 0;

        if (comm_) {
            for (const auto& entry : registrations_) {
                if (reportNcclError(
                        ncclCommWindowDeregister(comm_, entry.second.window),
                        "ncclCommWindowDeregister") != 0) {
                    status = -1;
                }
            }
        }
        registrations_.clear();

        for (void* ptr : allocations_) {
            if (reportNcclError(ncclMemFree(ptr), "ncclMemFree") != 0) {
                status = -1;
            }
        }
        allocations_.clear();

        if (collective_status_) {
            if (reportCudaError(cudaFree(collective_status_),
                                "cudaFree(collective status)") != 0) {
                status = -1;
            }
            collective_status_ = nullptr;
        }
        if (control_stream_) {
            if (reportCudaError(cudaStreamDestroy(control_stream_),
                                "cudaStreamDestroy(control)") != 0) {
                status = -1;
            }
            control_stream_ = nullptr;
        }
        if (device_comm_) {
            if (reportCudaError(cudaFree(device_comm_),
                                "cudaFree(device communicator)") != 0) {
                status = -1;
            }
            device_comm_ = nullptr;
        }

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
        next_registration_id_ = 1;
        properties_ = {};
        return status;
    }

   private:
    struct WindowRecord {
        ncclWindow_t window;
        void* ptr;
        size_t bytes;
    };

    bool collectiveAllSucceeded(bool local_success) {
        int value = local_success ? 1 : 0;
        if (reportCudaError(
                cudaMemcpyAsync(collective_status_, &value, sizeof(value),
                                cudaMemcpyHostToDevice, control_stream_),
                "cudaMemcpyAsync(collective status H2D)") != 0 ||
            reportNcclError(
                ncclAllReduce(collective_status_, collective_status_, 1,
                              ncclInt, ncclMin, comm_, control_stream_),
                "ncclAllReduce(collective status)") != 0 ||
            reportCudaError(
                cudaMemcpyAsync(&value, collective_status_, sizeof(value),
                                cudaMemcpyDeviceToHost, control_stream_),
                "cudaMemcpyAsync(collective status D2H)") != 0 ||
            reportCudaError(cudaStreamSynchronize(control_stream_),
                            "cudaStreamSynchronize(collective status)") != 0) {
            return false;
        }
        return value != 0;
    }

    void abortPartialInitialization() {
        if (collective_status_) {
            cudaFree(collective_status_);
            collective_status_ = nullptr;
        }
        if (control_stream_) {
            cudaStreamDestroy(control_stream_);
            control_stream_ = nullptr;
        }
        if (device_comm_) {
            cudaFree(device_comm_);
            device_comm_ = nullptr;
        }
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

    ncclComm_t comm_ = nullptr;
    ncclDevComm_t dev_comm_{};
    ncclDevComm_t* device_comm_ = nullptr;
    bool dev_comm_created_ = false;
    bool initialized_ = false;

    cudaStream_t control_stream_ = nullptr;
    int* collective_status_ = nullptr;

    uint64_t next_registration_id_ = 1;
    std::unordered_map<uint64_t, WindowRecord> registrations_;
    std::unordered_set<void*> allocations_;
    NcclTransportProperties properties_;
};

std::unique_ptr<NcclTransport> createNcclDeviceTransport() {
    return std::make_unique<NcclDeviceTransportImpl>();
}

}  // namespace device
}  // namespace mooncake
