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

#include "tent/transport/ascend/hixl_engine.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>
#include <hixl/hixl.h>

#include "tent/common/status.h"
#include "tent/transport/ascend/resource_config.h"

namespace mooncake {
namespace tent {
namespace {

class RealHixlVendor final : public HixlEngine::Vendor {
   public:
    uint32_t Initialize(
        const std::string& local_name,
        const std::map<std::string, std::string>& options) override {
        std::map<hixl::AscendString, hixl::AscendString> hixl_options;
        for (const auto& [key, value] : options) {
            hixl_options[key.c_str()] = value.c_str();
        }
        return hixl_.Initialize(hixl::AscendString(local_name.c_str()),
                                hixl_options);
    }

    void Finalize() override { hixl_.Finalize(); }

    uint32_t RegisterMem(uint64_t addr, size_t len, bool host,
                         void*& handle) override {
        hixl::MemDesc mem_desc{};
        mem_desc.addr = addr;
        mem_desc.len = len;
        hixl::MemHandle mem_handle = nullptr;
        auto status = hixl_.RegisterMem(
            mem_desc, host ? hixl::MEM_HOST : hixl::MEM_DEVICE, mem_handle);
        handle = mem_handle;
        return status;
    }

    uint32_t DeregisterMem(void* handle) override {
        return hixl_.DeregisterMem(handle);
    }

    uint32_t TransferAsync(const std::string& remote, bool write,
                           const std::vector<HixlOpDesc>& descs,
                           void*& req) override {
        std::vector<hixl::TransferOpDesc> op_descs;
        op_descs.reserve(descs.size());
        for (const auto& desc : descs) {
            hixl::TransferOpDesc op{};
            op.local_addr = desc.local_addr;
            op.remote_addr = desc.remote_addr;
            op.len = desc.len;
            op_descs.push_back(op);
        }
        hixl::TransferReq handle = nullptr;
        auto status =
            hixl_.TransferAsync(hixl::AscendString(remote.c_str()),
                                write ? hixl::WRITE : hixl::READ, op_descs,
                                hixl::TransferArgs(), handle);
        req = handle;
        return status;
    }

    uint32_t GetTransferStatus(void* req, HixlXferState& status) override {
        hixl::TransferStatus xfer_status = hixl::TransferStatus::WAITING;
        auto err = hixl_.GetTransferStatus(req, xfer_status);
        if (err != hixl::SUCCESS) {
            status = HixlXferState::Failed;
            return err;
        }
        switch (xfer_status) {
            case hixl::TransferStatus::COMPLETED:
                status = HixlXferState::Completed;
                break;
            case hixl::TransferStatus::TIMEOUT:
                status = HixlXferState::Timeout;
                break;
            case hixl::TransferStatus::FAILED:
                status = HixlXferState::Failed;
                break;
            case hixl::TransferStatus::WAITING:
            default:
                status = HixlXferState::Waiting;
                break;
        }
        return hixl::SUCCESS;
    }

    uint32_t Disconnect(const std::string& remote,
                        int32_t timeout_ms) override {
        return hixl_.Disconnect(hixl::AscendString(remote.c_str()), timeout_ms);
    }

    uint32_t Connect(const std::string& remote, int32_t timeout_ms) override {
        return hixl_.Connect(hixl::AscendString(remote.c_str()), timeout_ms);
    }

   private:
    hixl::Hixl hixl_;
};

std::mutex g_vendor_factory_mu;
HixlEngine::VendorFactory g_vendor_factory =
    []() -> std::unique_ptr<HixlEngine::Vendor> {
    return std::make_unique<RealHixlVendor>();
};

void DisconnectRemotes(HixlEngine::Vendor& vendor,
                       std::vector<std::string>& remotes) {
    for (const auto& remote : remotes) {
        const auto status =
            vendor.Disconnect(remote, kAscendTimeoutDisconnectMs);
        if (status != kHixlOk && status != kHixlNotConnected) {
            LOG(WARNING) << "Disconnect " << remote
                         << " before teardown failed, status=" << status;
        }
    }
    remotes.clear();
}

}  // namespace

void HixlEngine::SetVendorFactory(VendorFactory factory) {
    std::lock_guard<std::mutex> lock(g_vendor_factory_mu);
    if (factory) {
        g_vendor_factory = std::move(factory);
    } else {
        g_vendor_factory = []() -> std::unique_ptr<Vendor> {
            return std::make_unique<RealHixlVendor>();
        };
    }
}

HixlEngine::VendorFactory HixlEngine::GetVendorFactory() {
    std::lock_guard<std::mutex> lock(g_vendor_factory_mu);
    return g_vendor_factory;
}

std::unique_ptr<HixlEngine::Vendor> HixlEngine::CreateVendor() {
    return GetVendorFactory()();
}

HixlEngine::ContextGuard::ContextGuard(aclrtContext target) {
    ok_ = (aclrtGetCurrentContext(&saved_) == ACL_ERROR_NONE);
    if (!ok_) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return;
    }
    if (target != nullptr && aclrtSetCurrentContext(target) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtSetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        ok_ = false;
    }
}

HixlEngine::ContextGuard::~ContextGuard() {
    if (ok_ && saved_ != nullptr) {
        (void)aclrtSetCurrentContext(saved_);
    }
}

HixlEngine::~HixlEngine() { finalize(); }

Status HixlEngine::initialize(
    const std::string& name, aclrtContext context, int32_t device_id,
    const std::map<std::string, std::string>& options) {
    std::lock_guard<std::mutex> lock(mutex_);
    name_ = name;
    context_ = context;
    device_id_ = device_id;
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    vendor_ = CreateVendor();
    if (!vendor_) {
        return Status::InternalError("Create HIXL vendor failed" LOC_MARK);
    }
    auto status = vendor_->Initialize(name_, options);
    if (status != kHixlOk) {
        LOG(ERROR) << "Failed to initialize HIXL engine " << name_
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        vendor_.reset();
        return Status::InternalError("Initialize hixl failed" LOC_MARK);
    }
    initialized_ = true;
    LOG(INFO) << "Initialized HIXL engine " << name_
              << " device_id=" << device_id_;
    return Status::OK();
}

void HixlEngine::finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!vendor_) {
        addr_to_handle_.clear();
        remotes_.clear();
        initialized_ = false;
        return;
    }
    ContextGuard guard(context_);
    // AutoConnect leaves sessions up on the success path. HIXL requires
    // those sessions to be gone before DeregisterMem / Finalize.
    DisconnectRemotes(*vendor_, remotes_);
    for (auto& [addr, handle] : addr_to_handle_) {
        (void)addr;
        (void)vendor_->DeregisterMem(handle);
    }
    addr_to_handle_.clear();
    vendor_->Finalize();
    vendor_.reset();
    initialized_ = false;
}

Status HixlEngine::registerMem(void* addr, size_t length, bool host) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!vendor_) {
        return Status::InternalError("HIXL engine is not initialized" LOC_MARK);
    }
    if (addr == nullptr || length == 0) {
        return Status::InvalidArgument(
            "Cannot register an empty HIXL memory range" LOC_MARK);
    }
    if (addr_to_handle_.find(addr) != addr_to_handle_.end()) {
        return Status::InvalidArgument(
            "HIXL memory range is already registered" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    void* handle = nullptr;
    auto status = vendor_->RegisterMem(reinterpret_cast<uint64_t>(addr), length,
                                       host, handle);
    if (status != kHixlOk) {
        LOG(ERROR) << "RegisterMem failed for " << addr
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Register failed" LOC_MARK);
    }
    addr_to_handle_[addr] = handle;
    return Status::OK();
}

Status HixlEngine::deregisterMem(void* addr) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = addr_to_handle_.find(addr);
    if (!vendor_ || it == addr_to_handle_.end()) {
        return Status::OK();
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    DisconnectRemotes(*vendor_, remotes_);
    auto status = vendor_->DeregisterMem(it->second);
    addr_to_handle_.erase(it);
    if (status != kHixlOk) {
        LOG(ERROR) << "DeregisterMem failed for " << addr
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Deregister failed" LOC_MARK);
    }
    return Status::OK();
}

void HixlEngine::rollbackMem(void* addr) { (void)deregisterMem(addr); }

Status HixlEngine::transferAsync(const std::string& remote, bool write,
                                 const std::vector<HixlOpDesc>& descs,
                                 void*& req) {
    std::lock_guard<std::mutex> lock(mutex_);
    req = nullptr;
    if (!vendor_) {
        return Status::InternalError("HIXL engine is not initialized" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    auto status = vendor_->TransferAsync(remote, write, descs, req);
    if (status != kHixlOk) {
        LOG(ERROR) << "TransferAsync failed to " << remote
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("TransferAsync failed" LOC_MARK);
    }
    if (std::find(remotes_.begin(), remotes_.end(), remote) == remotes_.end()) {
        remotes_.push_back(remote);
    }
    return Status::OK();
}

Status HixlEngine::getTransferStatus(void* req, HixlXferState& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!vendor_) {
        status = HixlXferState::Failed;
        return Status::InternalError("HIXL engine is not initialized" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        status = HixlXferState::Failed;
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    auto err = vendor_->GetTransferStatus(req, status);
    if (err != kHixlOk) {
        status = HixlXferState::Failed;
        return Status::InternalError("GetTransferStatus failed" LOC_MARK);
    }
    return Status::OK();
}

Status HixlEngine::disconnectOnTimeout(const std::string& remote,
                                       int32_t timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!vendor_) {
        return Status::OK();
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    auto status = vendor_->Disconnect(remote, timeout_ms);
    if (status != kHixlOk && status != kHixlNotConnected) {
        LOG(ERROR) << "Disconnect after timeout failed for " << remote
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Disconnect failed" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
