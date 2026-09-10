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

#ifndef TENT_HIXL_ENGINE_H_
#define TENT_HIXL_ENGINE_H_

#include <acl/acl.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

constexpr uint32_t kHixlOk = 0;
constexpr uint32_t kHixlNotConnected = 103902;

enum class HixlXferState {
    Waiting,
    Completed,
    Timeout,
    Failed,
};

struct HixlOpDesc {
    uintptr_t local_addr = 0;
    uintptr_t remote_addr = 0;
    size_t len = 0;
};

// One HIXL instance per NPU: ACL context, registered handles, and a mutex so
// dummy-real workers cannot race the same engine. Vendor is the hixl::Hixl
// call surface so tests can inject a fake without a live CANN process.
class HixlEngine {
   public:
    class Vendor {
       public:
        virtual ~Vendor() = default;

        virtual uint32_t Initialize(
            const std::string& local_name,
            const std::map<std::string, std::string>& options) = 0;

        virtual void Finalize() = 0;

        virtual uint32_t RegisterMem(uint64_t addr, size_t len, bool host,
                                     void*& handle) = 0;

        virtual uint32_t DeregisterMem(void* handle) = 0;

        virtual uint32_t TransferAsync(const std::string& remote, bool write,
                                       const std::vector<HixlOpDesc>& descs,
                                       void*& req) = 0;

        virtual uint32_t GetTransferStatus(void* req,
                                           HixlXferState& status) = 0;

        virtual uint32_t Disconnect(const std::string& remote,
                                    int32_t timeout_ms) = 0;

        // Kept so tests can assert AutoConnect-only never calls Connect.
        virtual uint32_t Connect(const std::string& remote,
                                 int32_t timeout_ms) = 0;
    };

    using VendorFactory = std::function<std::unique_ptr<Vendor>()>;

    static void SetVendorFactory(VendorFactory factory);

    HixlEngine() = default;
    ~HixlEngine();

    HixlEngine(const HixlEngine&) = delete;
    HixlEngine& operator=(const HixlEngine&) = delete;

    Status initialize(const std::string& name, aclrtContext context,
                      int32_t device_id,
                      const std::map<std::string, std::string>& options);

    void finalize();

    Status registerMem(void* addr, size_t length, bool host);
    Status deregisterMem(void* addr);
    void rollbackMem(void* addr);

    Status transferAsync(const std::string& remote, bool write,
                         const std::vector<HixlOpDesc>& descs, void*& req);
    Status getTransferStatus(void* req, HixlXferState& status);
    Status disconnectOnTimeout(const std::string& remote, int32_t timeout_ms);

    const std::string& name() const { return name_; }
    int32_t deviceId() const { return device_id_; }
    aclrtContext context() const { return context_; }
    bool initialized() const { return initialized_; }

   private:
    class ContextGuard {
       public:
        explicit ContextGuard(aclrtContext target);
        ~ContextGuard();
        bool ok() const { return ok_; }

       private:
        aclrtContext saved_{nullptr};
        bool ok_{false};
    };

    static VendorFactory GetVendorFactory();
    static std::unique_ptr<Vendor> CreateVendor();

    std::mutex mutex_;
    std::unique_ptr<Vendor> vendor_;
    std::map<void*, void*> addr_to_handle_;
    std::vector<std::string> remotes_;
    std::string name_;
    aclrtContext context_{nullptr};
    int32_t device_id_{-1};
    bool initialized_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_HIXL_ENGINE_H_
