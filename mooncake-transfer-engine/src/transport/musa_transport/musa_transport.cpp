// Copyright 2025 KVCache.AI
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

#include "transport/musa_transport/musa_transport.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "cuda_alike.h"
#include "memory_location.h"

namespace mooncake {
namespace {

int deviceForPointer(const void* ptr) {
    cudaPointerAttributes attr;
    if (!ptr || cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return attr.type == cudaMemoryTypeDevice ? attr.device : -1;
}

int deviceFromLocation(const std::string& location) {
    if (location.rfind(GPU_PREFIX, 0) != 0) return -1;
    const char* begin = location.c_str() + GPU_PREFIX.size();
    if (*begin == '\0') return -1;

    errno = 0;
    char* end = nullptr;
    long value = std::strtol(begin, &end, 10);
    if (errno == ERANGE || end == begin || *end != '\0' || value < 0 ||
        value > std::numeric_limits<int>::max()) {
        return -1;
    }

    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || value >= count) return -1;
    return static_cast<int>(value);
}

bool openOnMetadataDevice() {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_MUSA_IPC_OPEN_DEVICE");
        if (!value || std::string(value) == "metadata") {
            const char* musa_visible = std::getenv("MUSA_VISIBLE_DEVICES");
            const char* mthreads_visible =
                std::getenv("MTHREADS_VISIBLE_DEVICES");
            LOG(INFO) << "MusaTransport: opening IPC handles on the metadata "
                         "device; peers must use identical ordered physical "
                         "device lists (MUSA_VISIBLE_DEVICES="
                      << (musa_visible ? musa_visible : "(not set)")
                      << ", MTHREADS_VISIBLE_DEVICES="
                      << (mthreads_visible ? mthreads_visible : "(not set)")
                      << ")";
            return true;
        }
        if (std::string(value) == "current") {
            LOG(INFO) << "MusaTransport: opening IPC handles on the current "
                         "device (A/B mode)";
            return false;
        }
        LOG(WARNING) << "MusaTransport: unknown MC_MUSA_IPC_OPEN_DEVICE="
                     << value << ", using metadata";
        return true;
    }();
    return enabled;
}

bool localVisibilityMappingIsConsistent() {
    const char* musa_visible = std::getenv("MUSA_VISIBLE_DEVICES");
    const char* mthreads_visible = std::getenv("MTHREADS_VISIBLE_DEVICES");
    if (!musa_visible || !mthreads_visible) return true;
    if (std::string(musa_visible) == std::string(mthreads_visible)) return true;
    LOG(ERROR) << "MusaTransport: MUSA_VISIBLE_DEVICES=" << musa_visible
               << " differs from MTHREADS_VISIBLE_DEVICES=" << mthreads_visible
               << "; refusing metadata-device IPC mapping";
    return false;
}

enum class MusaCopyApi { Auto, Default, TransferBatch };

MusaCopyApi copyApi() {
    static const MusaCopyApi api = [] {
        const char* value = std::getenv("MC_MUSA_COPY_API");
        if (!value || std::string(value) == "auto") return MusaCopyApi::Auto;
        if (std::string(value) == "default") return MusaCopyApi::Default;
        if (std::string(value) == "transfer_batch")
            return MusaCopyApi::TransferBatch;
        LOG(WARNING) << "MusaTransport: unknown MC_MUSA_COPY_API=" << value
                     << ", using auto";
        return MusaCopyApi::Auto;
    }();
    return api;
}

const char* copyApiName(MusaCopyApi api) {
    switch (api) {
        case MusaCopyApi::Auto:
            return "auto";
        case MusaCopyApi::Default:
            return "default";
        case MusaCopyApi::TransferBatch:
            return "transfer_batch";
    }
    return "unknown";
}

size_t transferBatchMinBytes() {
    static const size_t min_bytes = [] {
        constexpr size_t kDefaultMinBytes = 1024ULL * 1024ULL;
        const char* value = std::getenv("MC_MUSA_TRANSFER_BATCH_MIN_BYTES");
        if (!value) return kDefaultMinBytes;

        errno = 0;
        char* end = nullptr;
        unsigned long long parsed = std::strtoull(value, &end, 0);
        if (errno == ERANGE || end == value || *end != '\0' ||
            parsed > std::numeric_limits<size_t>::max()) {
            LOG(WARNING)
                << "MusaTransport: invalid MC_MUSA_TRANSFER_BATCH_MIN_BYTES="
                << value << ", using " << kDefaultMinBytes;
            return kDefaultMinBytes;
        }
        return static_cast<size_t>(parsed);
    }();
    return min_bytes;
}

bool shouldUseTransferBatch(MusaCopyApi api, const std::vector<size_t>& sizes) {
    if (api == MusaCopyApi::Default) return false;
    if (api == MusaCopyApi::TransferBatch) return true;
    const size_t min_bytes = transferBatchMinBytes();
    for (size_t size : sizes) {
        if (size < min_bytes) return false;
    }
    return true;
}

class MusaTransportPolicy final : public GpuIpcTransportPolicy {
   public:
    const char* protocol() const override { return "musa"; }
    const char* displayName() const override { return "MusaTransport"; }

    bool groupTransfersByDevice() const override { return true; }
    bool requiresStreamDeviceGuard() const override { return true; }
    bool preserveExistingMetadata() const override { return true; }

    std::string normalizeMemoryLocation(
        const void* addr, const std::string& location) const override {
        if (location.rfind(GPU_PREFIX, 0) == 0 ||
            location != kWildcardLocation) {
            return location;
        }

        cudaPointerAttributes attr{};
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err == cudaSuccess && attr.type == cudaMemoryTypeDevice &&
            attr.device >= 0) {
            return GPU_PREFIX + std::to_string(attr.device);
        }

        int current_device = -1;
        if (cudaGetDevice(&current_device) == cudaSuccess &&
            current_device >= 0) {
            LOG(WARNING)
                << "MusaTransport: could not query wildcard buffer owner; "
                   "using current device "
                << current_device;
            return GPU_PREFIX + std::to_string(current_device);
        }
        LOG(ERROR) << "MusaTransport: could not resolve wildcard buffer "
                      "location for IPC registration";
        return location;
    }

    int selectStreamDevice(const void* /*local_buffer*/, const void* /*source*/,
                           const void* destination) const override {
        // MUSA P2P IPC reaches the best path when the DMA stream belongs to
        // the copy destination: remote device for WRITE and local device for
        // READ.  The destination is already relocated by the shared core.
        int device_id = deviceForPointer(destination);
        if (device_id < 0) cudaGetDevice(&device_id);
        return device_id < 0 ? 0 : device_id;
    }

    bool submitBatchCopies(const std::vector<void*>& srcs,
                           const std::vector<void*>& dsts,
                           const std::vector<size_t>& sizes,
                           cudaStream_t stream,
                           size_t& fail_index) const override {
        const MusaCopyApi api = copyApi();
        static const bool logged_api = [api] {
            LOG(INFO) << "MusaTransport: copy API " << copyApiName(api)
                      << ", transfer-batch min bytes "
                      << transferBatchMinBytes();
            return true;
        }();
        (void)logged_api;
        if (!shouldUseTransferBatch(api, sizes)) return false;

        if (srcs.empty() || srcs.size() != dsts.size() ||
            srcs.size() != sizes.size()) {
            fail_index = std::numeric_limits<size_t>::max();
            LOG(ERROR) << "MusaTransport: invalid batch-copy vectors";
            return true;
        }

#if defined(MUSA_VERSION) && MUSA_VERSION >= 50200
        std::vector<MUdeviceptr> driver_srcs(srcs.size());
        std::vector<MUdeviceptr> driver_dsts(dsts.size());
        std::vector<size_t> driver_sizes(sizes.begin(), sizes.end());
        for (size_t i = 0; i < srcs.size(); ++i) {
            driver_srcs[i] = reinterpret_cast<MUdeviceptr>(srcs[i]);
            driver_dsts[i] = reinterpret_cast<MUdeviceptr>(dsts[i]);
        }

        MUmemcpyAttributes attributes{};
        attributes.srcAccessOrder = MU_MEMCPY_SRC_ACCESS_ORDER_STREAM;
        size_t attributes_index = 0;
        fail_index = std::numeric_limits<size_t>::max();
        MUresult result = muMemoryTransferBatchAsync(
            driver_dsts.data(), driver_srcs.data(), driver_sizes.data(),
            driver_srcs.size(), &attributes, &attributes_index, 1, &fail_index,
            reinterpret_cast<MUstream>(stream));
        if (result != MUSA_SUCCESS) {
            LOG(ERROR) << "MusaTransport: muMemoryTransferBatchAsync failed: "
                       << result;
            return true;
        }
        fail_index = srcs.size();
        return true;
#else
        static const bool warned_once = [] {
            LOG(WARNING)
                << "MusaTransport: muMemoryTransferBatchAsync requires MUSA "
                   "SDK 5.2 or newer; using per-slice copies";
            return true;
        }();
        (void)warned_once;
        return false;
#endif
    }

    cudaError_t openIpcMemHandle(void** address, cudaIpcMemHandle_t handle,
                                 const std::string& location,
                                 int& opened_device) const override {
        int saved_device = -1;
        cudaError_t saved_err = cudaGetDevice(&saved_device);
        if (saved_err != cudaSuccess) return saved_err;

        opened_device = saved_device;
        if (openOnMetadataDevice()) {
            const int metadata_device = deviceFromLocation(location);
            if (metadata_device >= 0) {
                if (!localVisibilityMappingIsConsistent())
                    return musaErrorInvalidDevice;
                opened_device = metadata_device;
            } else if (location == kWildcardLocation) {
                // Older peers may still advertise the wildcard location. A
                // current-device open is compatible, though it intentionally
                // gives up the metadata-device optimization for that buffer.
                LOG(WARNING)
                    << "MusaTransport: wildcard IPC location; opening on "
                       "the current device for compatibility";
            } else {
                LOG(ERROR) << "MusaTransport: invalid metadata device in "
                           << location;
                return musaErrorInvalidDevice;
            }
            if (opened_device != saved_device) {
                cudaError_t set_err = cudaSetDevice(opened_device);
                if (set_err != cudaSuccess) return set_err;
            }
        }

        cudaError_t open_err = cudaIpcOpenMemHandle(
            address, handle, cudaIpcMemLazyEnablePeerAccess);
        cudaError_t restore_err = cudaSuccess;
        if (opened_device != saved_device)
            restore_err = cudaSetDevice(saved_device);

        if (restore_err != cudaSuccess) {
            if (open_err == cudaSuccess) {
                cudaError_t close_err = cudaSetDevice(opened_device);
                if (close_err == cudaSuccess) {
                    cudaIpcCloseMemHandle(*address);
                    cudaSetDevice(saved_device);
                }
            }
            return restore_err;
        }
        return open_err;
    }

    cudaError_t closeIpcMemHandle(void* address,
                                  int opened_device) const override {
        int saved_device = -1;
        cudaError_t saved_err = cudaGetDevice(&saved_device);
        if (saved_err != cudaSuccess) return saved_err;

        if (opened_device >= 0 && opened_device != saved_device) {
            cudaError_t set_err = cudaSetDevice(opened_device);
            if (set_err != cudaSuccess) return set_err;
        }
        cudaError_t close_err = cudaIpcCloseMemHandle(address);
        if (opened_device >= 0 && opened_device != saved_device) {
            cudaError_t restore_err = cudaSetDevice(saved_device);
            if (close_err == cudaSuccess) close_err = restore_err;
        }
        return close_err;
    }
};

}  // namespace

MusaTransport::MusaTransport()
    : NvlinkTransport(std::make_shared<MusaTransportPolicy>(), false) {}

void* MusaTransport::allocatePinnedLocalMemory(size_t length) {
    void* ptr = nullptr;
    if (cudaMalloc(&ptr, length) != cudaSuccess) return nullptr;
    return ptr;
}

void MusaTransport::freePinnedLocalMemory(void* addr) { cudaFree(addr); }

}  // namespace mooncake
