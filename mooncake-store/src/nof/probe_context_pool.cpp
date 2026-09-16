#include "nof/probe_context_pool.h"

namespace mooncake {

void NofProbeContext::Reset() {
    std::lock_guard<std::mutex> lock(error_mutex);
    done.store(false, std::memory_order_release);
    success.store(false, std::memory_order_release);
    error_reason.clear();
    adaptor = {};
    seg = nullptr;
}

NofProbeContext* NofProbeContextPool::Acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    ReapQuarantineLocked();
    if (free_.empty()) {
        ReplenishLocked(8);
    }
    auto* ctx = free_.top();
    free_.pop();
    ctx->Reset();
    return ctx;
}

void NofProbeContextPool::Recycle(NofProbeContext* ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    free_.push(ctx);
}

void NofProbeContextPool::Quarantine(NofProbeContext* ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    quarantine_.push_back(ctx);
}

size_t NofProbeContextPool::QuarantinedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return quarantine_.size();
}

std::vector<NofProbeContext*> NofProbeContextPool::QuarantinedSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return quarantine_;
}

void NofProbeContextPool::ReplenishLocked(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        auto ctx = std::make_unique<NofProbeContext>();
        free_.push(ctx.get());
        contexts_.push_back(std::move(ctx));
    }
}

void NofProbeContextPool::ReapQuarantineLocked() {
    for (auto it = quarantine_.begin(); it != quarantine_.end();) {
        if ((*it)->done.load(std::memory_order_acquire)) {
            // The stale callback has run: the context is safe to reuse.
            free_.push(*it);
            it = quarantine_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace mooncake
