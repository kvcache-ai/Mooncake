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

#pragma once

#include <level_zero/ze_api.h>
#include <sycl/sycl.hpp>

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "xpu_ep_kernels.h"
#include "xpu_rdma.h"

namespace mooncake {
namespace xpu_ep {

// The host proxy of the hybrid path. After a send kernel completes, it
// writes what the kernel staged for ranks on other nodes into their EP
// buffers, then sets their ready flags. Each such rank has its own RC queue
// pair, so its flag lands after its data.
//
// A rank whose writes fail is skipped from then on, with an error logged:
// like a peer that stops posting flags, its receivers time out and drop it
// from active_ranks.
class XpuEpProxy {
   public:
    // Builds the puts of one call; runs on the proxy thread once the call's
    // send kernel has completed.
    using Plan = std::function<std::vector<ProxyPut>()>;

    // Registers [base, base + bytes) (a whole Level Zero device allocation
    // made exportable as a dma-buf) with the RDMA device nearest `device`,
    // and creates a queue pair for each rank r with remote[r] set.
    XpuEpProxy(const sycl::device& device, ze_context_handle_t ctx, void* base,
               size_t bytes, int rank, const std::vector<int>& remote);
    ~XpuEpProxy();
    XpuEpProxy(const XpuEpProxy&) = delete;
    XpuEpProxy& operator=(const XpuEpProxy&) = delete;

    // [base low, base high, rkey, 0], then for each rank r the queue pair
    // for r as [qpn, psn, lid, mtu, gid as 4 words] (zero if r is not
    // remote). Its length is info_ints(num_ranks).
    std::vector<int32_t> localInfo() const;
    static size_t info_ints(int num_ranks) { return 4 + 8 * num_ranks; }

    // Connects to each remote rank whose `active` entry is set, from every
    // rank's localInfo().
    void connect(const std::vector<std::vector<int32_t>>& infos,
                 const std::vector<int>& active);
    bool connected(int rank) const { return peers_[rank].connected; }

    // Runs `plan` once `after` has completed and posts its puts in order.
    void submit(sycl::event after, Plan plan);
    // Waits until every submitted plan's writes have completed.
    void drain();

   private:
    struct Peer {
        uint64_t base = 0;
        uint32_t rkey = 0;
        bool connected = false;
        bool failed = false;
    };

    void run();
    void execute(const std::vector<ProxyPut>& puts);
    void fail(int rank, const char* what);

    int rank_;
    uint64_t base_;
    std::unique_ptr<RdmaDevice> device_;
    RdmaRegion region_{};
    std::vector<std::unique_ptr<RdmaQueuePair>> qps_;  // null if not remote
    std::vector<Peer> peers_;
    std::vector<int> pending_;     // signaled requests not yet polled, by rank
    std::vector<int> unsignaled_;  // requests since the last signaled one

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::pair<sycl::event, Plan>> work_;
    bool busy_ = false;
    bool stop_ = false;
    std::thread thread_;
};

}  // namespace xpu_ep
}  // namespace mooncake
