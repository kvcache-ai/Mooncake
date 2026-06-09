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

#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {

CUDAStreamHandle& CUDAStreamHandle::operator=(
    CUDAStreamHandle&& other) noexcept {
    if (this != &other) {
        // Release current resource if any
        releaseToPool();
        stream_ = other.stream_;
        deviceId_ = other.deviceId_;
        pool_ = other.pool_;

        other.stream_ = nullptr;
        other.pool_ = nullptr;
    }
    return *this;
}

CUDAStreamHandle::~CUDAStreamHandle() { releaseToPool(); }

void CUDAStreamHandle::releaseToPool() {
    if (stream_ != nullptr && pool_ != nullptr) {
        pool_->release(deviceId_, stream_);
        stream_ = nullptr;
        pool_ = nullptr;
    }
}

CUDAStreamPool::DevicePool::DevicePool(int deviceId) : deviceId_(deviceId) {}

CUDAStreamPool::DevicePool::~DevicePool() {
    int currentDevice;
    if (cudaGetDevice(&currentDevice) == cudaSuccess) {
        cudaSetDevice(deviceId_);
        for (cudaStream_t stream : availableStreams_) {
            cudaStreamDestroy(stream);
        }
        cudaSetDevice(currentDevice);
    }
}

Status CUDAStreamPool::DevicePool::acquire(cudaStream_t& outStream) {
    {
        RWSpinlock::WriteGuard guard(dev_lock_);
        if (!availableStreams_.empty()) {
            outStream = availableStreams_.back();
            availableStreams_.pop_back();
            return Status::OK();
        }
    }

    // Lazy creation: avoid holding the lock so we don't block other threads
    // checking out streams.
    int currentDevice;
    CHECK_CUDA(cudaGetDevice(&currentDevice));

    if (currentDevice != deviceId_) {
        CHECK_CUDA(cudaSetDevice(deviceId_));
    }

    CHECK_CUDA(cudaStreamCreateWithFlags(&outStream, cudaStreamNonBlocking));

    // Restore previous device
    if (currentDevice != deviceId_) {
        CHECK_CUDA(cudaSetDevice(currentDevice));
    }

    return Status::OK();
}

void CUDAStreamPool::DevicePool::release(cudaStream_t stream) {
    RWSpinlock::WriteGuard guard(dev_lock_);
    availableStreams_.push_back(stream);
}

Status CUDAStreamPool::acquire(CUDAStreamHandle& outHandle, int deviceId) {
    if (deviceId == kCurrentDevice) {
        if (cudaGetDevice(&deviceId) != cudaSuccess) {
            return Status::InternalError("Failed to get current device ID");
        }
    } else if (deviceId < 0) {
        return Status::InternalError("Invalid device ID");
    }

    DevicePool* devicePool = getDevicePool(deviceId);

    if (!devicePool) {
        return Status::InternalError("Failed to get device pool");
    }

    cudaStream_t rawStream;
    CHECK_STATUS(devicePool->acquire(rawStream));

    outHandle = CUDAStreamHandle(rawStream, deviceId, this);
    return Status::OK();
}

void CUDAStreamPool::release(int deviceId, cudaStream_t stream) {
    DevicePool* devicePool = getDevicePool(deviceId);
    if (devicePool) devicePool->release(stream);
}

CUDAStreamPool::DevicePool* CUDAStreamPool::getDevicePool(int deviceId) {
    // check if the device pool already exists
    {
        RWSpinlock::ReadGuard readGuard(pools_lock_);
        if (static_cast<size_t>(deviceId) < devicePools_.size() &&
            devicePools_[deviceId]) {
            return devicePools_[deviceId].get();
        }
    }

    // write lock to resize vector and create DevicePool
    RWSpinlock::WriteGuard writeGuard(pools_lock_);

    // Double-check devicePools_[deviceId] after acquiring the lock,
    // since another thread may have created it in the meantime.
    if (static_cast<size_t>(deviceId) < devicePools_.size() &&
        devicePools_[deviceId]) {
        return devicePools_[deviceId].get();
    }

    // consistency check on deviceId
    int actualDeviceCount = 0;
    if (cudaGetDeviceCount(&actualDeviceCount) != cudaSuccess ||
        deviceId >= actualDeviceCount) {
        LOG(ERROR) << "Invalid cuda device id " << deviceId;
        return nullptr;
    }

    if (static_cast<size_t>(deviceId) >= devicePools_.size()) {
        devicePools_.resize(deviceId + 1);
    }

    devicePools_[deviceId] = std::make_unique<DevicePool>(deviceId);
    return devicePools_[deviceId].get();
}

Status CudaPlatform::getStreamFromPool(CUDAStreamHandle& outHandle,
                                       int deviceId) {
    return stream_pool.acquire(outHandle, deviceId);
}

}  // namespace tent
}  // namespace mooncake
