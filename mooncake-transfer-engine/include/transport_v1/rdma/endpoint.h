// Copyright 2024 KVCache.AI
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

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include <queue>

#include "context.h"

namespace mooncake {
namespace v1 {
class RdmaEndPoint {
   public:
    RdmaEndPoint();

    ~RdmaEndPoint();

    struct EndPointParams {
        int qp_mul_factor = 1;
        int max_sge = 4;
        int max_qp_wr = 256;
        int max_inline_bytes = 64;

        // Advanced parameters, do not change unless you understand them
        // INIT State
        uint16_t pkey_index = 0;
        // RTR State
        uint8_t hop_limit = 16;
        uint32_t flow_label = 0;
        uint8_t traffic_class = 0;
        uint8_t service_level = 0;
        uint8_t src_path_bits = 0;
        uint8_t static_rate = 0;
        uint32_t rq_psn = 0;
        uint8_t max_dest_rd_atomic = 16;
        uint8_t min_rnr_timer = 12;
        // RTS State
        uint32_t sq_psn = 0;
        uint8_t send_timeout = 14;
        uint8_t send_retry_count = 7;
        uint8_t send_rnr_count = 7;
        uint8_t max_rd_atomic = 16;
    };

    int construct(CompletionQueue *cq, const EndPointParams &params);

   public:
    // Enable, disable, restart
    enum EndPointStatus {
        kEndPointUninitialized,
        kEndPointDisabled,
        kEndPointPreparing,
        kEndPointReady
    };

    int enable();

    int disable();

    int reset();

    int configurePeer(const std::string &peer_gid, uint16_t peer_lid,
                      std::vector<uint32_t> peer_qp_num_list,
                      std::string *reply_msg = nullptr);

    EndPointStatus status() const { return status_; }

    std::vector<uint32_t> qpNum();

    int outstandingSlices() const;

    RdmaContext &context() const { return *cq_->context; }

   public:
    struct Request {
        struct SglEntry {
            uint32_t length;
            uint64_t addr;
            uint32_t lkey;
        };
        ibv_wr_opcode opcode;
        std::vector<SglEntry> local;
        uint64_t remote_addr;
        uint32_t remote_key;
        uint32_t imm_data;
        void *user_context;
        // managed by endpoint
        volatile int *wr_depth_ptr;
        bool submitted;
        bool failed;
    };

    int submitGeneralRequests(std::vector<Request> &requests);

    int submitRecvImmDataRequest(int qp_index, uint64_t id);

   private:
    int setupSingleQueuePair(int qp_index, const std::string &peer_gid,
                             uint16_t peer_lid, uint32_t peer_qp_num,
                             std::string *reply_msg = nullptr);

   private:
    std::atomic<EndPointStatus> status_;
    RWSpinlock ep_lock_;

    CompletionQueue *cq_;
    volatile int *cqe_now_ptr_;
    EndPointParams params_;

    std::vector<ibv_qp *> qp_list_;
    volatile int *wr_depth_list_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_ENDPOINT_H