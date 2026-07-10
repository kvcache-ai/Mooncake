// Copyright 2026 Huawei Technologies Co., Ltd
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

#ifndef ASCEND_CONTEXT_MANAGER_H
#define ASCEND_CONTEXT_MANAGER_H

#include <acl/acl.h>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace mooncake {

/**
 * @brief Singleton class to manage multiple Ascend device contexts.
 *
 * In dummy-real mode, one real client serves multiple dummy clients
 * that may be running on different devices. This manager initializes
 * all available devices and records logic <-> physical device id mapping.
 * Dummy RPC carries physical id; use setCurrentContextByPhysicalId on the
 * real side. Slice/engine paths continue to use logic indices.
 */
class ContextManager {
   public:
    // Get the singleton instance
    static ContextManager &getInstance();

    // Initialize the manager with all available devices
    // Must be called before using other methods
    bool initialize();

    // Check if the manager has been initialized
    bool isInitialized() const;

    // Get the number of devices
    uint32_t getDeviceCount() const;

    // Get context for a specific device
    // Returns nullptr if device_id is invalid
    aclrtContext getContext(int32_t device_id) const;

    // Set current context for a specific device
    // Returns false if device_id is invalid or acl call fails
    bool setCurrentContext(int32_t device_id) const;

    // Set context for Ascend physical device id (dummy-real RPC path).
    // Returns false if physical id is not managed by this process.
    bool setCurrentContextByPhysicalId(int32_t physical_dev_id) const;

    // Cleanup all contexts
    void finalize();

   private:
    ContextManager() = default;
    ~ContextManager();

    // Prevent copying
    ContextManager(const ContextManager &) = delete;
    ContextManager &operator=(const ContextManager &) = delete;

    std::vector<aclrtContext> contexts_;
    std::vector<int32_t> logic_to_physical_;
    std::unordered_map<int32_t, int32_t> physical_to_logic_;
    uint32_t device_count_ = 0;
    bool initialized_ = false;
    mutable std::mutex mutex_;
};

}  // namespace mooncake

#endif  // ASCEND_CONTEXT_MANAGER_H
