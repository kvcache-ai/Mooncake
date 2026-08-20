#ifndef REMOTE_STAGE_OPERATION_H_
#define REMOTE_STAGE_OPERATION_H_

#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {
namespace internal {

class RemoteStageOperation {
   public:
    void complete(Status status);

    std::optional<Status> tryTakeResult();

   private:
    std::mutex mutex_;
    std::optional<Status> result_;
};

using RemoteStageOperationPtr = std::shared_ptr<RemoteStageOperation>;

bool pollRemoteOperations(
    std::vector<RemoteStageOperationPtr>& remote_operations);

}  // namespace internal
}  // namespace tent
}  // namespace mooncake

#endif  // REMOTE_STAGE_OPERATION_H_
