#pragma once

#include <algorithm>
#include <atomic>
#include <future>
#include <memory>
#include <string>

#include "allocator.h"
#include "replica.h"

namespace mooncake::test {

class TestBufferAllocator final
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<TestBufferAllocator> {
   public:
    TestBufferAllocator(std::string name, std::string endpoint, size_t capacity,
                        uintptr_t base = 0x100000000ULL)
        : name_(std::move(name)),
          endpoint_(std::move(endpoint)),
          capacity_(capacity),
          base_(base),
          allow_allocation_(allow_allocation_promise_.get_future().share()) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override {
        allocation_calls_.fetch_add(1, std::memory_order_relaxed);
        if (block_next_.exchange(false, std::memory_order_acq_rel)) {
            allocation_started_promise_.set_value();
            allow_allocation_.wait();
        }
        if (always_fail_.load(std::memory_order_relaxed) ||
            fail_next_.exchange(false, std::memory_order_acq_rel)) {
            return nullptr;
        }
        size_t current = used_.load(std::memory_order_relaxed);
        while (size <= capacity_ - std::min(current, capacity_)) {
            if (used_.compare_exchange_weak(current, current + size,
                                            std::memory_order_relaxed)) {
                const uintptr_t address =
                    base_ +
                    next_offset_.fetch_add(size, std::memory_order_relaxed);
                return std::make_unique<AllocatedBuffer>(
                    shared_from_this(), reinterpret_cast<void*>(address), size);
            }
        }
        return nullptr;
    }

    void deallocate(AllocatedBuffer* handle) override {
        used_.fetch_sub(handle->size(), std::memory_order_relaxed);
    }
    size_t capacity() const override { return capacity_; }
    size_t size() const override {
        return used_.load(std::memory_order_relaxed);
    }
    uintptr_t base() const override { return base_; }
    std::string getSegmentName() const override { return name_; }
    std::string getTransportEndpoint() const override { return endpoint_; }
    size_t getLargestFreeRegion() const override {
        return capacity_ - std::min(size(), capacity_);
    }

    void SetUsed(size_t used) { used_.store(used, std::memory_order_relaxed); }
    void SetAlwaysFail(bool fail = true) {
        always_fail_.store(fail, std::memory_order_relaxed);
    }
    void FailNext() { fail_next_.store(true, std::memory_order_relaxed); }
    void BlockNext() { block_next_.store(true, std::memory_order_relaxed); }
    std::future<void> AllocationStarted() {
        return allocation_started_promise_.get_future();
    }
    void AllowAllocation() { allow_allocation_promise_.set_value(); }
    size_t allocation_calls() const {
        return allocation_calls_.load(std::memory_order_relaxed);
    }

   private:
    std::string name_;
    std::string endpoint_;
    size_t capacity_;
    uintptr_t base_;
    std::atomic<size_t> used_{0};
    std::atomic<uintptr_t> next_offset_{0};
    std::atomic<size_t> allocation_calls_{0};
    std::atomic<bool> always_fail_{false};
    std::atomic<bool> fail_next_{false};
    std::atomic<bool> block_next_{false};
    std::promise<void> allocation_started_promise_;
    std::promise<void> allow_allocation_promise_;
    std::shared_future<void> allow_allocation_;
};

inline std::string ReplicaEndpoint(const Replica& replica) {
    auto descriptor = replica.get_descriptor();
    if (descriptor.is_memory_replica()) {
        return descriptor.get_memory_descriptor()
            .buffer_descriptor.transport_endpoint_;
    }
    return descriptor.get_nof_descriptor()
        .buffer_descriptor.transport_endpoint_;
}

}  // namespace mooncake::test
