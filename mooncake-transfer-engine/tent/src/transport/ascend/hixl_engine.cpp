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

#include <mutex>
#include <type_traits>
#include <utility>

#include <glog/logging.h>
#include <hixl/hixl.h>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {
namespace {

// MemDesc::local_only requires CANN 9.3 or newer (hixl::MemDesc grew the
// field in that release). Older toolkits have no way to say that a range must
// not be exported, so a buffer the upper layer marked as local stays remotely
// reachable there, as it did before this flag existed. Warn once: the fallback
// is a toolkit limitation, not a per-registration event, and it would otherwise
// flood the log when a process registers many buffers.
template <typename T, typename = void>
struct HasLocalOnly : std::false_type {};
template <typename T>
struct HasLocalOnly<T, std::void_t<decltype(std::declval<T&>().local_only)>>
    : std::true_type {};

template <typename T>
void SetMemDescLocalOnly(T& mem_desc, bool local_only) {
    if constexpr (HasLocalOnly<T>::value) {
        mem_desc.local_only = local_only;
    } else {
        (void)mem_desc;
        if (local_only) {
            LOG_FIRST_N(WARNING, 1)
                << "This CANN release has no MemDesc::local_only (requires "
                   "CANN 9.3 or newer); registering a local-only buffer as "
                   "remotely accessible";
        }
    }
}

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

    uint32_t RegisterMem(uint64_t addr, size_t len, bool host, bool local_only,
                         void*& handle) override {
        hixl::MemDesc mem_desc{};
        mem_desc.addr = addr;
        mem_desc.len = len;
        SetMemDescLocalOnly(mem_desc, local_only);
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

   private:
    hixl::Hixl hixl_;
};

std::mutex g_vendor_factory_mu;
HixlEngine::VendorFactory g_vendor_factory =
    []() -> std::unique_ptr<HixlEngine::Vendor> {
    return std::make_unique<RealHixlVendor>();
};

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
    // A thread that never bound a context has nothing to restore, which is not
    // an error: teardown reaches this engine from the signal handling thread on
    // Ctrl-C, and the engine binds the context its own calls need below.
    if (aclrtGetCurrentContext(&saved_) != ACL_ERROR_NONE) {
        saved_ = nullptr;
    }
    if (target != nullptr && aclrtSetCurrentContext(target) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtSetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return;
    }
    // ok() still promises that some context is bound for the guarded calls.
    ok_ = target != nullptr || saved_ != nullptr;
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!vendor_) {
        addr_to_handle_.clear();
        initialized_ = false;
        return;
    }
    // TENT only uses AutoConnect, so HIXL owns the peer sessions and Finalize()
    // tears them down after the memory registrations are gone. A caller without
    // a bound context is tolerated: each registration is deregistered under the
    // context it was registered with, and there is nothing to restore.
    ContextGuard guard(context_);
    for (auto& [addr, handle] : addr_to_handle_) {
        (void)addr;
        (void)vendor_->DeregisterMem(handle);
    }
    addr_to_handle_.clear();
    vendor_->Finalize();
    vendor_.reset();
    initialized_ = false;
}

Status HixlEngine::registerMem(void* addr, size_t length, bool host,
                               bool local_only) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
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
                                       host, local_only, handle);
    if (status != kHixlOk) {
        LOG(ERROR) << "RegisterMem failed, addr: " << addr
                   << ", length: " << length
                   << ", mem type: " << (host ? "host" : "device")
                   << ", local_only: " << local_only
                   << ", device_id: " << device_id_ << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Register failed" LOC_MARK);
    }
    addr_to_handle_[addr] = handle;
    return Status::OK();
}

Status HixlEngine::deregisterMem(void* addr) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = addr_to_handle_.find(addr);
    if (!vendor_ || it == addr_to_handle_.end()) {
        return Status::OK();
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
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

Status HixlEngine::transferAsync(const std::string& remote, bool write,
                                 const std::vector<HixlOpDesc>& descs,
                                 void*& req) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    req = nullptr;
    if (!vendor_) {
        return Status::InternalError("HIXL engine is not initialized" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    // AutoConnect builds the session inside HIXL on first use. A failure is
    // reported to the caller and the next submission tries again; HIXL handles
    // its own reconnect/session state, so nothing is cached or forgotten here.
    auto status = vendor_->TransferAsync(remote, write, descs, req);
    if (status != kHixlOk) {
        LOG(ERROR) << "TransferAsync failed to " << remote
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("TransferAsync failed" LOC_MARK);
    }
    return Status::OK();
}

Status HixlEngine::getTransferStatus(void* req, HixlXferState& status) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
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

}  // namespace tent
}  // namespace mooncake
