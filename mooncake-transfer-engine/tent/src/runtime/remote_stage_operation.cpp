#include "remote_stage_operation.h"

#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace internal {

void RemoteStageOperation::complete(Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    result_ = std::move(status);
}

std::optional<Status> RemoteStageOperation::tryTakeResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!result_) return std::nullopt;
    auto result = std::move(result_);
    result_.reset();
    return result;
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
