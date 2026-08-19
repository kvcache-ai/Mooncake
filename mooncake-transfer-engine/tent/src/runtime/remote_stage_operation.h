#ifndef REMOTE_STAGE_OPERATION_H_
#define REMOTE_STAGE_OPERATION_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {
namespace internal {

struct RemoteStageResult {
    Status status;
    bool confirmed;
};

class RemoteStageOperation {
   public:
    using DeferredCleanup = std::function<void()>;

    RemoteStageOperation() = default;
    RemoteStageOperation(std::string server_addr, uint64_t remote_buffer,
                         DeferredCleanup deferred_cleanup);

    void complete(Status status, bool confirmed);

    std::optional<RemoteStageResult> tryTakeResult();

    void abandonForCleanup();

    bool ownsRemoteBuffer(const std::string& server_addr,
                          uint64_t remote_buffer) const;

   private:
    DeferredCleanup takeCleanupIfReadyLocked();

    std::string server_addr_;
    uint64_t remote_buffer_{0};
    DeferredCleanup deferred_cleanup_;
    std::mutex mutex_;
    std::optional<RemoteStageResult> result_;
    bool abandoned_{false};
    bool cleanup_started_{false};
};

using RemoteStageOperationPtr = std::shared_ptr<RemoteStageOperation>;

bool pollRemoteOperations(
    std::vector<RemoteStageOperationPtr>& remote_operations);

}  // namespace internal
}  // namespace tent
}  // namespace mooncake

#endif  // REMOTE_STAGE_OPERATION_H_
