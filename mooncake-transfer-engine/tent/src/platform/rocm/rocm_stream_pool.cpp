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

#include "tent/platform/rocm.h"

namespace mooncake {
namespace tent {

HIPStreamHandle& HIPStreamHandle::operator=(HIPStreamHandle&& other) noexcept {
    if (this != &other) {
        releaseToPool();
        stream_ = other.stream_;
        deviceId_ = other.deviceId_;
        pool_ = other.pool_;

        other.stream_ = nullptr;
        other.pool_ = nullptr;
    }
    return *this;
}

HIPStreamHandle::~HIPStreamHandle() { releaseToPool(); }

void HIPStreamHandle::releaseToPool() {
    if (stream_ != nullptr && pool_ != nullptr) {
        pool_->release(deviceId_, stream_);
        stream_ = nullptr;
        pool_ = nullptr;
    }
}

HIPStreamPool::DevicePool::DevicePool(int deviceId) : deviceId_(deviceId) {}

HIPStreamPool::DevicePool::~DevicePool() {
    int currentDevice;
    if (hipGetDevice(&currentDevice) == hipSuccess) {
        hipSetDevice(deviceId_);
        for (hipStream_t stream : availableStreams_) {
            hipStreamDestroy(stream);
        }
        hipSetDevice(currentDevice);
    }
}

Status HIPStreamPool::DevicePool::acquire(hipStream_t& outStream) {
    {
        RWSpinlock::WriteGuard guard(dev_lock_);
        if (!availableStreams_.empty()) {
            outStream = availableStreams_.back();
            availableStreams_.pop_back();
            return Status::OK();
        }
    }

    int currentDevice;
    CHECK_HIP(hipGetDevice(&currentDevice));

    if (currentDevice != deviceId_) {
        CHECK_HIP(hipSetDevice(deviceId_));
    }

    CHECK_HIP(hipStreamCreateWithFlags(&outStream, hipStreamNonBlocking));

    if (currentDevice != deviceId_) {
        CHECK_HIP(hipSetDevice(currentDevice));
    }

    return Status::OK();
}

void HIPStreamPool::DevicePool::release(hipStream_t stream) {
    RWSpinlock::WriteGuard guard(dev_lock_);
    availableStreams_.push_back(stream);
}

Status HIPStreamPool::acquire(HIPStreamHandle& outHandle, int deviceId) {
    if (deviceId == kCurrentDevice) {
        if (hipGetDevice(&deviceId) != hipSuccess) {
            return Status::InternalError("Failed to get current HIP device ID");
        }
    } else if (deviceId < 0) {
        return Status::InternalError("Invalid HIP device ID");
    }

    DevicePool* devicePool = getDevicePool(deviceId);
    if (!devicePool) {
        return Status::InternalError("Failed to get HIP device pool");
    }

    hipStream_t rawStream;
    CHECK_STATUS(devicePool->acquire(rawStream));

    outHandle = HIPStreamHandle(rawStream, deviceId, this);
    return Status::OK();
}

void HIPStreamPool::release(int deviceId, hipStream_t stream) {
    DevicePool* devicePool = getDevicePool(deviceId);
    if (devicePool) devicePool->release(stream);
}

HIPStreamPool::DevicePool* HIPStreamPool::getDevicePool(int deviceId) {
    {
        RWSpinlock::ReadGuard readGuard(pools_lock_);
        if (static_cast<size_t>(deviceId) < devicePools_.size() &&
            devicePools_[deviceId]) {
            return devicePools_[deviceId].get();
        }
    }

    RWSpinlock::WriteGuard writeGuard(pools_lock_);

    if (static_cast<size_t>(deviceId) < devicePools_.size() &&
        devicePools_[deviceId]) {
        return devicePools_[deviceId].get();
    }

    int actualDeviceCount = 0;
    if (hipGetDeviceCount(&actualDeviceCount) != hipSuccess ||
        deviceId >= actualDeviceCount) {
        LOG(ERROR) << "Invalid HIP device id " << deviceId;
        return nullptr;
    }

    if (static_cast<size_t>(deviceId) >= devicePools_.size()) {
        devicePools_.resize(deviceId + 1);
    }

    devicePools_[deviceId] = std::make_unique<DevicePool>(deviceId);
    return devicePools_[deviceId].get();
}

Status RocmPlatform::getStreamFromPool(HIPStreamHandle& outHandle,
                                       int deviceId) {
    return stream_pool_.acquire(outHandle, deviceId);
}

}  // namespace tent
}  // namespace mooncake
