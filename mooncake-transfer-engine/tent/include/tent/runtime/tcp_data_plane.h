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

#ifndef TENT_RUNTIME_TCP_DATA_PLANE_H_
#define TENT_RUNTIME_TCP_DATA_PLANE_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake::tent {

class TcpBufferRegistryTestPeer;

struct TcpProcessIdentity {
    uint64_t high{0};
    uint64_t low{0};

    bool operator==(const TcpProcessIdentity&) const = default;
};

struct TcpDataPlaneIdentity {
    TcpProcessIdentity process;
    uint64_t registration_id{0};
};

class TcpBufferRegistry {
   private:
    struct Entry;

   public:
    class Lease {
       public:
        Lease() = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&& other) noexcept;
        ~Lease();

        void reset();

       private:
        friend class TcpBufferRegistry;
        explicit Lease(std::shared_ptr<Entry> entry);
        std::shared_ptr<Entry> entry_;
    };

    TcpBufferRegistry();

    Status add(uint64_t base, uint64_t length, Permission permission,
               uint64_t* registration_id);
    Status remove(uint64_t registration_id);
    Status acquire(uint64_t registration_id, uint64_t addr, uint64_t length,
                   bool write, Lease* lease);
    void close(bool discard_entries = false);
    Status reopen();
    bool empty();

   private:
    struct Entry {
        uint64_t base{0};
        uint64_t length{0};
        Permission permission{kLocalReadWrite};
        std::mutex mutex;
        std::condition_variable drained;
        uint64_t active_leases{0};
    };

    friend class TcpBufferRegistryTestPeer;
    void setAcquireHookForTest(std::function<void(uint64_t)> hook);

    std::shared_mutex mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Entry>> entries_;
    const uint64_t registration_namespace_;
    uint64_t next_registration_sequence_{1};
    bool closing_{false};
    std::atomic<bool> acquire_hook_active_{false};
    std::mutex acquire_hook_mutex_;
    std::function<void(uint64_t)> acquire_hook_for_test_;
};

TcpProcessIdentity makeTcpProcessIdentity();
std::string encodeTcpProcessAttr(const TcpProcessIdentity& identity);
std::string encodeTcpRegistrationAttr(uint64_t registration_id);
bool decodeTcpProcessAttr(std::string_view attr, TcpProcessIdentity* identity);
bool decodeTcpRegistrationAttr(std::string_view attr,
                               uint64_t* registration_id);

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_TCP_DATA_PLANE_H_
