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

#ifndef CUDA_H
#define CUDA_H

#include "tent/runtime/platform.h"
#include "tent/common/config.h"
#include "tent/common/concurrent/rw_spinlock.h"

#include <cuda_runtime.h>

namespace mooncake {
namespace tent {

class CUDAStreamPool;

// RAII Wrapper for cudaStream_t.
// When this handle destructs, the stream is automatically returned to the pool.
class CUDAStreamHandle {
   public:
    CUDAStreamHandle() = default;

    CUDAStreamHandle(cudaStream_t stream, int deviceId, CUDAStreamPool* pool)
        : stream_(stream), deviceId_(deviceId), pool_(pool) {}

    // exclusive ownership
    CUDAStreamHandle(const CUDAStreamHandle&) = delete;
    CUDAStreamHandle& operator=(const CUDAStreamHandle&) = delete;

    CUDAStreamHandle(CUDAStreamHandle&& other) noexcept
        : stream_(other.stream_),
          deviceId_(other.deviceId_),
          pool_(other.pool_) {
        other.stream_ = nullptr;
        other.pool_ = nullptr;
    }

    CUDAStreamHandle& operator=(CUDAStreamHandle&& other) noexcept;
    ~CUDAStreamHandle();

    // Get the underlying CUDA stream
    [[nodiscard]] cudaStream_t get() const { return stream_; }

   private:
    void releaseToPool();

    cudaStream_t stream_ = nullptr;
    int deviceId_ = -1;
    CUDAStreamPool* pool_ = nullptr;
};

// CUDA Stream Pool managing all devices.
class CUDAStreamPool {
    friend class CUDAStreamHandle;

   public:
    CUDAStreamPool() = default;
    ~CUDAStreamPool() = default;

    // Non-copyable
    CUDAStreamPool(const CUDAStreamPool&) = delete;
    CUDAStreamPool& operator=(const CUDAStreamPool&) = delete;

    // Acquires a stream for the specified device.
    static constexpr int kCurrentDevice = -1;
    Status acquire(CUDAStreamHandle& outHandle, int deviceId = kCurrentDevice);

   private:
    class DevicePool {
       public:
        explicit DevicePool(int deviceId);
        ~DevicePool();

        Status acquire(cudaStream_t& outStream);
        void release(cudaStream_t stream);

       private:
        int deviceId_;
        RWSpinlock dev_lock_;
        std::vector<cudaStream_t> availableStreams_;
    };

    // called by CUDAStreamHandle::releaseToPool
    void release(int deviceId, cudaStream_t stream);
    DevicePool* getDevicePool(int deviceId);

    RWSpinlock pools_lock_;
    std::vector<std::unique_ptr<DevicePool>> devicePools_;
};

class CudaPlatform : public Platform {
   public:
    CudaPlatform(std::shared_ptr<Config> config) : conf(std::move(config)) {}

    virtual ~CudaPlatform() {}

    virtual Status probe(std::vector<Topology::NicEntry>& nic_list,
                         std::vector<Topology::MemEntry>& mem_list);

    virtual Status allocate(void** pptr, size_t size, MemoryOptions& options);

    virtual Status free(void* ptr, size_t size);

    virtual Status copy(void* dst, void* src, size_t length);

    virtual MemoryType getMemoryType(void* addr);

    virtual const std::vector<RangeLocation> getLocation(
        void* start, size_t len, bool skip_prefault = false);

    virtual const std::string type() const { return "cuda"; }

    Status getStreamFromPool(CUDAStreamHandle& outHandle,
                             int deviceId = CUDAStreamPool::kCurrentDevice);

   private:
    std::shared_ptr<Config> conf;
    CUDAStreamPool stream_pool;
};

}  // namespace tent
}  // namespace mooncake

#endif  // CUDA_H