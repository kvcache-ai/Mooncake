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

#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"

#include <acl/acl_rt.h>

#include <glog/logging.h>

#include <mutex>

namespace mooncake {

namespace {
// Helper function to check ACL return code
bool checkAclError(aclError error, const char *message) {
    if (error != ACL_ERROR_NONE) {
        LOG(ERROR) << message << ", error: " << error << ", "
                   << aclGetRecentErrMsg();
        return false;
    }
    return true;
}
}  // namespace

ContextManager &ContextManager::getInstance() {
    static ContextManager instance;
    return instance;
}

bool ContextManager::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_) {
        LOG(INFO) << "ContextManager already initialized";
        return true;
    }

    contexts_.clear();
    logic_to_physical_.clear();
    physical_to_logic_.clear();
    device_count_ = 0;

    aclError ret = aclrtGetDeviceCount(&device_count_);
    if (!checkAclError(ret, "Failed to get device count")) {
        return false;
    }

    LOG(INFO) << "ContextManager: found " << device_count_ << " device(s)";

    int32_t saved_logic_device = 0;
    ret = aclrtGetDevice(&saved_logic_device);
    if (ret != ACL_ERROR_NONE) {
        saved_logic_device = 0;
    }

    contexts_.reserve(device_count_);
    logic_to_physical_.reserve(device_count_);
    for (uint32_t i = 0; i < device_count_; ++i) {
        const int32_t logic_id = static_cast<int32_t>(i);
        ret = aclrtSetDevice(logic_id);
        if (!checkAclError(ret, "Failed to set device")) {
            contexts_.clear();
            logic_to_physical_.clear();
            physical_to_logic_.clear();
            device_count_ = 0;
            return false;
        }

        aclrtContext context = nullptr;
        ret = aclrtGetCurrentContext(&context);
        if (!checkAclError(ret, "Failed to get current context")) {
            contexts_.clear();
            logic_to_physical_.clear();
            physical_to_logic_.clear();
            device_count_ = 0;
            return false;
        }

        int32_t physical_id = 0;
        ret = aclrtGetPhyDevIdByLogicDevId(logic_id, &physical_id);
        if (!checkAclError(ret, "Failed to get physical device id")) {
            contexts_.clear();
            logic_to_physical_.clear();
            physical_to_logic_.clear();
            device_count_ = 0;
            return false;
        }
        auto existing = physical_to_logic_.find(physical_id);
        if (existing != physical_to_logic_.end() &&
            existing->second != logic_id) {
            LOG(ERROR) << "Duplicate physical device id " << physical_id
                       << " for logic " << logic_id << " and "
                       << existing->second;
            contexts_.clear();
            logic_to_physical_.clear();
            physical_to_logic_.clear();
            device_count_ = 0;
            return false;
        }
        physical_to_logic_[physical_id] = logic_id;
        logic_to_physical_.push_back(physical_id);

        contexts_.push_back(context);
        LOG(INFO) << "ContextManager: logic " << logic_id << " -> physical "
                  << physical_id;
    }

    ret = aclrtSetDevice(saved_logic_device);
    if (!checkAclError(ret, "Failed to restore device after init")) {
        contexts_.clear();
        logic_to_physical_.clear();
        physical_to_logic_.clear();
        device_count_ = 0;
        return false;
    }

    initialized_ = true;
    return true;
}

bool ContextManager::isInitialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

uint32_t ContextManager::getDeviceCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return device_count_;
}

aclrtContext ContextManager::getContext(int32_t device_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || device_id < 0 ||
        static_cast<uint32_t>(device_id) >= device_count_) {
        return nullptr;
    }
    return contexts_[device_id];
}

bool ContextManager::setCurrentContext(int32_t device_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || device_id < 0 ||
        static_cast<uint32_t>(device_id) >= device_count_) {
        LOG(ERROR) << "Invalid device_id: " << device_id
                   << ", initialized: " << initialized_
                   << ", device_count: " << device_count_;
        return false;
    }

    aclError ret = aclrtSetCurrentContext(contexts_[device_id]);
    if (!checkAclError(ret, "Failed to set current context")) {
        return false;
    }
    return true;
}

bool ContextManager::setCurrentContextByPhysicalId(
    int32_t physical_dev_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) {
        LOG(ERROR) << "ContextManager not initialized";
        return false;
    }
    auto it = physical_to_logic_.find(physical_dev_id);
    if (it == physical_to_logic_.end()) {
        LOG(ERROR) << "Physical device id not managed by this process: "
                   << physical_dev_id;
        return false;
    }
    const int32_t logic_id = it->second;
    if (logic_id < 0 || static_cast<uint32_t>(logic_id) >= device_count_) {
        LOG(ERROR) << "Invalid logic id " << logic_id << " for physical "
                   << physical_dev_id;
        return false;
    }
    aclError ret = aclrtSetCurrentContext(contexts_[logic_id]);
    if (!checkAclError(ret, "Failed to set current context")) {
        return false;
    }
    return true;
}

void ContextManager::finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) {
        return;
    }

    LOG(INFO) << "ContextManager: finalizing " << device_count_ << " device(s)";
    contexts_.clear();
    logic_to_physical_.clear();
    physical_to_logic_.clear();
    device_count_ = 0;
    initialized_ = false;
}

ContextManager::~ContextManager() { finalize(); }

}  // namespace mooncake
