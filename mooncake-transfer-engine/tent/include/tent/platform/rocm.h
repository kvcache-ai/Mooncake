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

#ifndef ROCM_H
#define ROCM_H

#include "tent/runtime/platform.h"
#include "tent/common/config.h"
#include "tent/common/concurrent/rw_spinlock.h"

#include <hip/hip_runtime.h>

namespace mooncake {
namespace tent {

class HIPStreamPool;

// RAII wrapper for hipStream_t.
// When this handle destructs, the stream is automatically returned to the pool.
class HIPStreamHandle {
   public:
    HIPStreamHandle() = default;

    HIPStreamHandle(hipStream_t stream, int deviceId, HIPStreamPool* pool)
        : stream_(stream), deviceId_(deviceId), pool_(pool) {}

    HIPStreamHandle(const HIPStreamHandle&) = delete;
    HIPStreamHandle& operator=(const HIPStreamHandle&) = delete;

    HIPStreamHandle(HIPStreamHandle&& other) noexcept
        : stream_(other.stream_),
          deviceId_(other.deviceId_),
          pool_(other.pool_) {
        other.stream_ = nullptr;
        other.pool_ = nullptr;
    }

    HIPStreamHandle& operator=(HIPStreamHandle&& other) noexcept;
    ~HIPStreamHandle();

    [[nodiscard]] hipStream_t get() const { return stream_; }

   private:
    void releaseToPool();

    hipStream_t stream_ = nullptr;
    int deviceId_ = -1;
    HIPStreamPool* pool_ = nullptr;
};

// HIP Stream Pool managing all devices.
class HIPStreamPool {
    friend class HIPStreamHandle;

   public:
    HIPStreamPool() = default;
    ~HIPStreamPool() = default;

    HIPStreamPool(const HIPStreamPool&) = delete;
    HIPStreamPool& operator=(const HIPStreamPool&) = delete;

    static constexpr int kCurrentDevice = -1;
    Status acquire(HIPStreamHandle& outHandle, int deviceId = kCurrentDevice);

   private:
    class DevicePool {
       public:
        explicit DevicePool(int deviceId);
        ~DevicePool();

        Status acquire(hipStream_t& outStream);
        void release(hipStream_t stream);

       private:
        int deviceId_;
        RWSpinlock dev_lock_;
        std::vector<hipStream_t> availableStreams_;
    };

    void release(int deviceId, hipStream_t stream);
    DevicePool* getDevicePool(int deviceId);

    RWSpinlock pools_lock_;
    std::vector<std::unique_ptr<DevicePool>> devicePools_;
};

class RocmPlatform : public Platform {
   public:
    RocmPlatform(std::shared_ptr<Config> config) : conf(std::move(config)) {}

    virtual ~RocmPlatform() {}

    virtual Status probe(std::vector<Topology::NicEntry>& nic_list,
                         std::vector<Topology::MemEntry>& mem_list);

    virtual Status allocate(void** pptr, size_t size, MemoryOptions& options);

    virtual Status free(void* ptr, size_t size);

    virtual Status copy(void* dst, void* src, size_t length);

    virtual MemoryType getMemoryType(void* addr);

    virtual const std::vector<RangeLocation> getLocation(
        void* start, size_t len, bool skip_prefault = false);

    virtual const std::string type() const { return "rocm"; }

    Status getStreamFromPool(HIPStreamHandle& outHandle,
                             int deviceId = HIPStreamPool::kCurrentDevice);

   private:
    std::shared_ptr<Config> conf;
    HIPStreamPool stream_pool_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // ROCM_H
