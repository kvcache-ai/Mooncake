// Copyright 2025 KVCache.AI
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

#ifndef TENT_CQ_H
#define TENT_CQ_H

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class RdmaContext;

class RdmaCQ {
   public:
    RdmaCQ() : cq_(nullptr), cqe_now_(0), cqe_limit_(-1), context_(nullptr) {}

    ~RdmaCQ();

    int construct(RdmaContext *context, int cqe_limit, int index);

    // To prevent CQ overflow, the user should reserve CQE quota before posting
    // ibv_post_send to HW. if there are no available CQEs, the function returns
    // false.
    //
    // TODO handling work requests without IBV_SEND_SIGNALED
    bool reserveQuota(int num_entries = 1);

    void cancelQuota(int num_entries);

    int getQuota() const { return cqe_now_; }

    int maxCqe() const { return cqe_limit_; }

    bool empty() const { return cqe_now_ == 0; }

    int poll(int num_entries, ibv_wc *wc);

    RdmaContext &context() const { return *context_; }

    ibv_cq *cq() const { return cq_; }

   private:
    ibv_cq *cq_;
    volatile int cqe_now_;
    int cqe_limit_;
    RdmaContext *context_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CQ_H
