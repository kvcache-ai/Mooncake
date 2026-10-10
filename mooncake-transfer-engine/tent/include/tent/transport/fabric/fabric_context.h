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

#ifndef TENT_FABRIC_CONTEXT_H
#define TENT_FABRIC_CONTEXT_H

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

// Everything that differs between libfabric providers. Addressing mode (VA
// or offset) and MR binding are derived from the mr_mode fi_getinfo returns,
// so a profile only carries hints.
struct FabricProfile {
    const char* name;       // value of transports/fabric/provider
    const char* prov_name;  // libfabric provider string
    uint32_t api_version;   // 0 = fi_version()
    uint64_t mr_mode_hints;
    const char* domain_suffix;  // only domains with this suffix are used
    bool limit_mr_by_pte;       // cap MR size by the device PTE budget
    // Request FI_DELIVERY_COMPLETE so a WRITE completion means the data is
    // visible at the target, for providers that complete on transmit.
    bool delivery_complete;
};

// Returns nullptr for an unknown name. "auto" is resolved by the caller.
const FabricProfile* findFabricProfile(const std::string& name);

// Profiles tried in order when provider is "auto".
std::vector<const FabricProfile*> autoFabricProfiles();

struct FabricParams {
    std::string provider = "auto";
    std::string fabric_name;           // optional fi_fabric_attr->name filter
    std::vector<std::string> devices;  // optional domain whitelist
    size_t slice_size = 512 * 1024;
    uint64_t idle_sleep_us = 50;
    // A queued op that could not be posted within this long (no credit, or
    // -FI_EAGAIN) fails without reaching the provider.
    uint64_t post_timeout_ms = 10000;
    uint64_t quiesce_timeout_ms = 10000;
    // A posted op without a completion after this long is overdue: new ops
    // to its peer fail without being posted until it returns. The overdue
    // op itself keeps its task pending and its buffers in use until the
    // provider returns it, since the NIC may still access them. Some
    // providers (EFA over the NIC) retry a dead peer for a long time.
    uint64_t op_timeout_ms = 10000;
    size_t max_posted_ops = 0;  // per context; 0 = provider tx queue size
    size_t max_pte_entries = 22 * 1024 * 1024;
    size_t max_register_threads = 8;
};

class FabricContext;
struct FabricTask;

// One RMA operation. ctx must stay first: libfabric hands back its address
// as op_context and FI_CONTEXT(2) providers own that storage while posted.
struct FabricOp {
    struct fi_context2 ctx;
    FabricTask* task = nullptr;
    void* local = nullptr;
    void* desc = nullptr;
    uint64_t remote_addr = 0;
    uint64_t key = 0;
    size_t length = 0;
    fi_addr_t peer = FI_ADDR_UNSPEC;
    bool is_write = true;
    uint64_t enqueue_ns = 0;
    uint64_t post_ns = 0;
    bool overdue = false;  // posted longer than op_timeout_ms
};

// Shared state of one transfer request, split into one or more FabricOps.
// Reference counted: the sub-batch holds one reference and every op not yet
// completed holds one, so freeing a batch never races with completions.
struct FabricTask {
    std::atomic<int> refs{1};
    std::atomic<TransferStatusEnum> status{TransferStatusEnum::PENDING};
    std::atomic<uint64_t> transferred{0};
    std::atomic<uint32_t> pending{0};
    std::atomic<bool> failed{false};
    uint64_t length = 0;
    BatchID progress_batch_id{0};
    std::function<void(BatchID)> notify_progress;
    std::shared_ptr<void> local_buffer;  // keeps local MRs alive
    // Tasks of the local buffer the provider may still access; decremented
    // once the last op has returned.
    std::atomic<uint64_t>* local_active = nullptr;

    void ref() { refs.fetch_add(1, std::memory_order_relaxed); }
    void unref();
    // Called once per op; the last one publishes the terminal status.
    void opDone(bool ok, size_t bytes);
};

FabricOp* allocateFabricOp();
void freeFabricOp(FabricOp* op);
FabricTask* allocateFabricTask();

struct FabricDomainInfo {
    std::string name;
    int numa_node = -1;
    struct fi_info* info = nullptr;  // owned, fi_freeinfo on destruction
};

// Lists the domains one profile can open, deduplicated and filtered by the
// whitelist. Fails with DeviceNotFound if none is usable.
Status discoverFabricDomains(const FabricProfile& profile,
                             const FabricParams& params,
                             std::vector<FabricDomainInfo>& domains);

void freeFabricDomains(std::vector<FabricDomainInfo>& domains);

// One libfabric domain + RDM endpoint + CQ + AV, driven by one worker
// thread that posts queued ops and polls the CQ.
class FabricContext {
   public:
    FabricContext() = default;
    ~FabricContext();
    FabricContext(const FabricContext&) = delete;
    FabricContext& operator=(const FabricContext&) = delete;

    // Takes ownership of domain.info.
    Status open(const FabricProfile& profile, FabricDomainInfo& domain,
                const FabricParams& params);

    // Joins the worker, closes the endpoint and fails every op that has not
    // completed. MRs stay valid and must be deregistered before close().
    void stop();

    // stop() plus releasing the AV, CQ, domain and fabric.
    void close();

    const std::string& name() const { return name_; }
    const std::string& address() const { return address_; }
    int numaNode() const { return numa_node_; }
    bool virtAddr() const { return virt_addr_; }
    uint64_t maxMrSize() const { return max_mr_size_; }
    size_t maxMsgSize() const { return max_msg_size_; }
    uint64_t inflight() const {
        return inflight_.load(std::memory_order_acquire);
    }

    Status registerMemory(void* addr, size_t length, struct fid_mr*& mr,
                          void*& desc, uint64_t& key);
    void deregisterMemory(struct fid_mr* mr);

    // Inserts a peer's fi_getname() blob into the AV once and caches it.
    Status resolvePeer(const std::string& address, fi_addr_t& peer);

    // Hands a batch of ops to the worker. Never fails; errors are reported
    // through the task.
    void submit(std::vector<FabricOp*>& ops);

   private:
    void workerLoop();
    bool postPending(uint64_t now_ns);
    bool pollCompletions();
    void completeOp(FabricOp* op, bool ok);
    void expirePosted(uint64_t now_ns);
    void expirePending(uint64_t now_ns);
    bool failUnposted(const FabricOp* op, uint64_t now_ns) const;

   private:
    std::string name_;
    std::string address_;
    int numa_node_ = -1;
    bool virt_addr_ = true;
    bool mr_bind_ep_ = false;
    bool prov_key_ = true;
    uint64_t max_mr_size_ = 0;
    size_t max_msg_size_ = 0;
    size_t credits_ = 0;
    FabricParams params_;

    struct fi_info* info_ = nullptr;
    struct fid_fabric* fabric_ = nullptr;
    struct fid_domain* domain_ = nullptr;
    struct fid_av* av_ = nullptr;
    struct fid_cq* cq_ = nullptr;
    struct fid_ep* ep_ = nullptr;

    std::mutex av_mutex_;
    std::unordered_map<std::string, fi_addr_t> peers_;

    std::atomic<uint64_t> next_key_{1};

    std::mutex queue_mutex_;
    std::vector<FabricOp*> queue_;
    std::atomic<bool> queue_nonempty_{false};
    std::condition_variable queue_cv_;
    bool worker_idle_ = false;  // guarded by queue_mutex_

    // Worker-thread only.
    std::vector<FabricOp*> pending_;
    size_t pending_head_ = 0;
    std::unordered_set<FabricOp*> posted_;
    std::unordered_map<fi_addr_t, size_t> overdue_peers_;  // peer -> ops

    std::atomic<uint64_t> inflight_{0};
    std::atomic<bool> running_{false};
    std::thread worker_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_FABRIC_CONTEXT_H
