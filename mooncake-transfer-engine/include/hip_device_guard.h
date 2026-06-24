// Copyright 2025 Mooncake Authors
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

#pragma once

#if defined(USE_HIP) || defined(USE_HIP_DMABUF)

#include <hip/hip_runtime.h>

namespace mooncake {

// RAII: saves the active HIP device and restores it on scope exit, so a
// function that calls hipSetDevice() doesn't leave the caller on the wrong GPU
// (which would fail its next kernel launch with hipErrorInvalidDevice).
class HipDeviceGuard {
   public:
    HipDeviceGuard() { saved_ = (hipGetDevice(&prev_device_) == hipSuccess); }

    // Also switch to target_device; set_ok() reports whether that succeeded.
    explicit HipDeviceGuard(int target_device) : HipDeviceGuard() {
        set_ok_ = (hipSetDevice(target_device) == hipSuccess);
    }

    ~HipDeviceGuard() {
        if (saved_) (void)hipSetDevice(prev_device_);
    }

    bool set_ok() const { return set_ok_; }

    HipDeviceGuard(const HipDeviceGuard&) = delete;
    HipDeviceGuard& operator=(const HipDeviceGuard&) = delete;

   private:
    int prev_device_ = 0;
    bool saved_ = false;
    bool set_ok_ = true;
};

}  // namespace mooncake

#endif  // USE_HIP || USE_HIP_DMABUF
