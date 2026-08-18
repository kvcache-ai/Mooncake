#include "remote_stage_operation.h"

#include <exception>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace internal {
namespace {

void runDeferredCleanup(RemoteStageOperation::DeferredCleanup cleanup) {
    if (!cleanup) return;
    try {
        cleanup();
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Failed to start deferred remote buffer cleanup: "
                     << ex.what();
    } catch (...) {
        LOG(WARNING)
            << "Failed to start deferred remote buffer cleanup: unknown error";
    }
}

}  // namespace

RemoteStageOperation::RemoteStageOperation(std::string server_addr,
                                           uint64_t remote_buffer,
                                           DeferredCleanup deferred_cleanup)
    : server_addr_(std::move(server_addr)),
      remote_buffer_(remote_buffer),
      deferred_cleanup_(std::move(deferred_cleanup)) {}

void RemoteStageOperation::complete(Status status) {
    DeferredCleanup cleanup;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = std::move(status);
        cleanup = takeCleanupIfReadyLocked();
    }
    runDeferredCleanup(std::move(cleanup));
}

std::optional<Status> RemoteStageOperation::tryTakeResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (abandoned_ || !result_) return std::nullopt;
    auto result = std::move(result_);
    result_.reset();
    return result;
}

void RemoteStageOperation::abandonForCleanup() {
    DeferredCleanup cleanup;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        abandoned_ = true;
        cleanup = takeCleanupIfReadyLocked();
    }
    runDeferredCleanup(std::move(cleanup));
}

bool RemoteStageOperation::ownsRemoteBuffer(const std::string& server_addr,
                                            uint64_t remote_buffer) const {
    return server_addr_ == server_addr && remote_buffer_ == remote_buffer;
}

RemoteStageOperation::DeferredCleanup
RemoteStageOperation::takeCleanupIfReadyLocked() {
    if (!abandoned_ || !result_ || cleanup_started_ || !deferred_cleanup_) {
        return {};
    }
    cleanup_started_ = true;
    return deferred_cleanup_;
}

bool pollRemoteOperations(
    std::vector<RemoteStageOperationPtr>& remote_operations) {
    for (auto it = remote_operations.begin(); it != remote_operations.end();) {
        if (!*it) {
            it = remote_operations.erase(it);
            continue;
        }
        auto result = (*it)->tryTakeResult();
        if (!result) {
            ++it;
            continue;
        }
        if (!result->ok()) {
            LOG(WARNING) << "Failed to drain remote staging request: "
                         << *result;
        }
        it = remote_operations.erase(it);
    }
    return remote_operations.empty();
}

}  // namespace internal
}  // namespace tent
}  // namespace mooncake
