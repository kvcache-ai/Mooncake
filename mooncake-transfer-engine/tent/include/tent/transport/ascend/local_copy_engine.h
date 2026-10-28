// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_ASCEND_LOCAL_COPY_ENGINE_H_
#define TENT_ASCEND_LOCAL_COPY_ENGINE_H_

#include <acl/acl.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/transport/ascend/hixl_engine.h"

namespace mooncake {
namespace tent {

// Same-engine copies: one aclrtMemcpyAsync per slice spread over several
// streams, each closed by a D2H of a host completion flag. A group is done
// when every stream it used has set its flag, so nothing here waits on a
// stream. Any submit/timeout failure destroys the streams, recreates them,
// and fails every in-flight group on them.
class LocalCopyEngine {
   public:
    // Runs one stream's issue work. The transport supplies a pool so a group
    // spread over several streams does not issue one stream at a time.
    using TaskRunner = std::function<void(std::function<void()>)>;

    LocalCopyEngine() = default;
    ~LocalCopyEngine();

    LocalCopyEngine(const LocalCopyEngine&) = delete;
    LocalCopyEngine& operator=(const LocalCopyEngine&) = delete;

    Status initialize(aclrtContext context, int32_t device_id);
    void finalize();
    void setTaskRunner(TaskRunner runner);

    Status submit(bool write, const std::vector<HixlOpDesc>& ops, void*& req,
                  std::vector<void*>& failed_handles);

    TransferStatusEnum poll(void* req);
    void release(void* req);
    std::vector<void*> failAndRecreate();

   private:
    struct Group {
        // One flag per stream this group used, padded apart so two streams
        // never land their completion write in the same cache line.
        void* host_flags = nullptr;
        size_t stream_count = 0;
        bool failed = false;
    };

    class ContextGuard {
       public:
        explicit ContextGuard(aclrtContext target);
        ~ContextGuard();
        bool ok() const { return ok_; }

       private:
        aclrtContext saved_{nullptr};
        bool ok_{false};
    };

    Status createStreamsLocked();
    void destroyStreamsLocked();
    bool issueGroupLocked(size_t stream_count,
                          const std::function<bool(size_t)>& issue_stream);
    Status allocHostFlags(Group& group);
    void freeHostFlags(Group& group);
    static volatile uint32_t* flagAt(const Group& group, size_t index);
    std::vector<void*> failAndRecreateLocked();
    static aclrtMemcpyKind memcpyKind(void* src, void* dst);

    mutable std::mutex mutex_;
    aclrtContext context_{nullptr};
    int32_t device_id_{-1};
    std::vector<aclrtStream> streams_;
    void* device_one_{nullptr};
    std::unordered_map<void*, std::unique_ptr<Group>> groups_;
    TaskRunner task_runner_;
    bool initialized_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_ASCEND_LOCAL_COPY_ENGINE_H_
