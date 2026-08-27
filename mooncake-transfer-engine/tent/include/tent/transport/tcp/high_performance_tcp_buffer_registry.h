// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_BUFFER_REGISTRY_H_
#define TENT_HIGH_PERFORMANCE_TCP_BUFFER_REGISTRY_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"

namespace mooncake::tent {

class HighPerformanceTcpBufferRegistry {
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
        void* data() const;
        uint64_t base() const;

       private:
        friend class HighPerformanceTcpBufferRegistry;
        explicit Lease(std::shared_ptr<Entry> entry);
        std::shared_ptr<Entry> entry_;
    };

    Status add(uint64_t base, uint64_t length, Permission permission,
               uint64_t* registration_id);
    Status remove(uint64_t base, uint64_t length);

    // Prevent new registrations and leases during quiesce. Existing leases
    // remain valid and drain through normal unregister/session completion.
    void close();
    Status reopen();

    // Local access checks lifetime/range only. MemoryOptions::perm is a remote
    // authorization policy and must not reject the local side of a transfer.
    Status acquireLocalLease(uint64_t addr, uint64_t length, Lease* lease);

    Status acquireRemoteLease(uint64_t addr, uint64_t length,
                              uint64_t registration_id,
                              HighPerformanceTcpOpcode opcode, Lease* lease,
                              HighPerformanceTcpStatus* wire_status = nullptr);

    bool tracks(uint64_t base, uint64_t length) const;

   private:
    struct Entry {
        uint64_t base{0};
        uint64_t length{0};
        uint64_t registration_id{0};
        Permission permission{kLocalReadWrite};
        std::mutex mutex;
        std::condition_variable drained;
        bool closing{false};
        uint64_t active_leases{0};
    };

    Status acquire(uint64_t addr, uint64_t length, uint64_t registration_id,
                   HighPerformanceTcpOpcode opcode, bool remote, Lease* lease,
                   HighPerformanceTcpStatus* wire_status);

    mutable std::mutex registry_mutex_;
    std::map<uint64_t, std::shared_ptr<Entry>> entries_;
    uint64_t next_registration_id_{1};
    bool closing_{false};
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_BUFFER_REGISTRY_H_
