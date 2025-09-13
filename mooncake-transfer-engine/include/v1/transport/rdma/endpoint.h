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

#define ERR_ENDPOINT (-102)

namespace mooncake {
namespace v1 {
class RdmaEndPoint {
    struct WrDepthBlock {
        volatile int value;
        uint64_t padding[7];
    };

   public:
    RdmaEndPoint();

    ~RdmaEndPoint();

    int construct(RdmaContext *context, EndPointParams *params,
                  const std::string &endpoint_name);

    int deconstruct();

   public:
    enum EndPointStatus { EP_UNINIT, EP_HANDSHAKING, EP_READY, EP_RESET };

    int reset();

    int configurePeer(const std::string &peer_gid, uint16_t peer_lid,
                      std::vector<uint32_t> peer_qp_num_list,
                      std::string *reply_msg = nullptr);

    EndPointStatus status() const { return status_; }

    std::vector<uint32_t> qpNum();

    int getInflightSlices() const;

    RdmaContext &context() const { return *context_; }

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
        bool failed;
    };

    int submitSlices(std::vector<RdmaSlice *> &slice_list, int qp_index);

    int submitRecvImmDataRequest(int qp_index, uint64_t id);

    volatile int *getQuotaCounter(int qp_index) const {
        return &wr_depth_list_[qp_index].value;
    }

   private:
    int setupSingleQueuePair(int qp_index, const std::string &peer_gid,
                             uint16_t peer_lid, uint32_t peer_qp_num,
                             std::string *reply_msg = nullptr);

    bool reserveQuota(int qp_index, int num_entries);

    void cancelQuota(int qp_index, int num_entries);

    void waitForAllInflightSlices();

    int resetUnlocked();

   private:
    std::atomic<EndPointStatus> status_;
    RWSpinlock ep_lock_;

    RdmaContext *context_;
    EndPointParams *params_;
    std::string endpoint_name_;

    std::vector<ibv_qp *> qp_list_;
    WrDepthBlock *wr_depth_list_;
    volatile int inflight_slices_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_ENDPOINT_H