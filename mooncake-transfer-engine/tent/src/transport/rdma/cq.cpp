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

#include "tent/transport/rdma/cq.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "tent/transport/rdma/context.h"

namespace mooncake {
namespace tent {
RdmaCQ::~RdmaCQ() {
    if (cq_) {
        context_->verbs_.ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
}

int RdmaCQ::construct(RdmaContext* context, int cqe_limit, int index) {
    context_ = context;
    cqe_limit_ = cqe_limit;
    auto native_context = context_->nativeContext();
    ibv_comp_channel* comp_channel =
        (context_->num_comp_channel_)
            ? context_->comp_channel_[index % context_->num_comp_channel_]
            : nullptr;
    int comp_vector = (native_context->num_comp_vectors == 0)
                          ? 0
                          : index % native_context->num_comp_vectors;
    cq_ = context_->verbs_.ibv_create_cq(native_context, cqe_limit, nullptr,
                                         comp_channel, comp_vector);
    if (!cq_) {
        PLOG(ERROR) << "ibv_create_cq";
        return -1;
    }
    return 0;
}

bool RdmaCQ::reserveQuota(int num_entries) {
    int prev_cqe_now = __sync_fetch_and_add(&cqe_now_, num_entries);
    if (prev_cqe_now + num_entries > cqe_limit_) {
        cancelQuota(num_entries);
        return false;
    }
    return true;
}

void RdmaCQ::cancelQuota(int num_entries) {
    __sync_fetch_and_sub(&cqe_now_, num_entries);
}

int RdmaCQ::poll(int num_entries, ibv_wc* wc) {
    if (!cqe_now_) return 0;
    int rc = ibv_poll_cq(cq_, num_entries, wc);
    if (rc < 0) {
        PLOG(ERROR) << "ibv_poll_cq";
    }
    return rc;
}
}  // namespace tent
}  // namespace mooncake