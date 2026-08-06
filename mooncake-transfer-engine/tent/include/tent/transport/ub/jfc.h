// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_JFC_H_
#define TENT_TRANSPORT_UB_JFC_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// TENT-facing completion queue.  Native JFC handles stay behind the adapter;
// this object only adds stable ownership and transport telemetry.
class UbJfc final {
   public:
    UbJfc(size_t index, std::shared_ptr<UrmaAdapter> adapter, JfcPtr handle)
        : index_(index),
          adapter_(std::move(adapter)),
          handle_(std::move(handle)) {}

    UbJfc(const UbJfc&) = delete;
    UbJfc& operator=(const UbJfc&) = delete;

    ~UbJfc();

    [[nodiscard]] size_t index() const noexcept { return index_; }
    [[nodiscard]] const JfcPtr& handle() const noexcept { return handle_; }
    [[nodiscard]] bool valid() const noexcept {
        return handle_ && handle_->valid();
    }

    Status poll(size_t max_completions, std::vector<Completion>& completions);
    Status close();

    [[nodiscard]] uint64_t completionCount() const noexcept {
        return completion_count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t pollErrorCount() const noexcept {
        return poll_error_count_.load(std::memory_order_relaxed);
    }

   private:
    const size_t index_;
    std::shared_ptr<UrmaAdapter> adapter_;
    JfcPtr handle_;
    std::atomic<uint64_t> completion_count_{0};
    std::atomic<uint64_t> poll_error_count_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JFC_H_
