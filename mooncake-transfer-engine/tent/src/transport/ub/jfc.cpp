// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/jfc.h"

namespace mooncake::tent::ub {

UbJfc::~UbJfc() { (void)close(); }

Status UbJfc::poll(size_t max_completions,
                   std::vector<Completion>& completions) {
    if (!valid()) {
        return Status::InvalidArgument("UB JFC is not active" LOC_MARK);
    }
    auto status = adapter_->poll(handle_, max_completions, completions);
    if (!status.ok()) {
        poll_error_count_.fetch_add(1, std::memory_order_relaxed);
        return status;
    }
    completion_count_.fetch_add(completions.size(), std::memory_order_relaxed);
    return Status::OK();
}

Status UbJfc::close() {
    if (!handle_) return Status::OK();
    return adapter_->deleteJfc(handle_);
}

}  // namespace mooncake::tent::ub
