// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_BUFFER_REGISTRY_H_
#define TENT_HIGH_PERFORMANCE_TCP_BUFFER_REGISTRY_H_

#include <condition_variable>
#include <atomic>
#include <cstdint>
#include <memory>
#include <map>
#include <mutex>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"

namespace mooncake::tent {
class HighPerformanceTcpBufferRegistry {
 public:
  struct Entry { uint64_t base{}, length{}, registration_id{}; Permission permission{}; std::atomic<bool> closing{false}; std::atomic<uint64_t> active_leases{0}; std::condition_variable drained; };
  class Lease {
   public:
    Lease() = default; Lease(const Lease&) = delete; Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept { *this = std::move(other); }
    Lease& operator=(Lease&& other) noexcept;
    ~Lease();
    void* data() const; uint64_t length() const;
   private:
    friend class HighPerformanceTcpBufferRegistry;
    explicit Lease(std::shared_ptr<Entry> entry); void reset(); std::shared_ptr<Entry> entry_;
  };
  Status add(uint64_t base, uint64_t length, Permission permission, uint64_t* registration_id);
  Status remove(uint64_t base, uint64_t length);
  Status acquireLocalLease(uint64_t addr, uint64_t length, Lease* lease);
  Status acquireRemoteLease(uint64_t addr, uint64_t length, uint64_t registration_id,
                            HighPerformanceTcpOpcode opcode, Lease* lease);
  size_t size() const;
 private:
  Status acquire(uint64_t addr, uint64_t length, uint64_t registration_id,
                 HighPerformanceTcpOpcode opcode, bool remote, Lease* lease);
  mutable std::mutex mutex_; std::map<uint64_t, std::shared_ptr<Entry>> entries_; uint64_t next_registration_id_{1};
};
}  // namespace mooncake::tent
#endif
