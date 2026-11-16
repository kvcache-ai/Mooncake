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

#include "xpu_ep_proxy.h"

#include <glog/logging.h>

#include <cstring>
#include <exception>

#include "xpu_ep_common.h"

namespace mooncake {
namespace xpu_ep {

namespace {

// Signal one write in kSignalEvery, and the last of each rank's in a call,
// and keep at most kMaxPending signaled ones unpolled: a queue pair then
// holds fewer than kMaxPending * kSignalEvery + kSignalEvery requests, under
// its 256-entry send queue.
constexpr int kSignalEvery = 64;
constexpr int kMaxPending = 2;
constexpr int kPollTimeoutMs = 30000;

}  // namespace

XpuEpProxy::XpuEpProxy(const sycl::device& device, ze_context_handle_t ctx,
                       void* base, size_t bytes, int rank,
                       const std::vector<int>& remote)
    : rank_(rank),
      base_(reinterpret_cast<uint64_t>(base)),
      qps_(remote.size()),
      peers_(remote.size()),
      pending_(remote.size(), 0),
      unsignaled_(remote.size(), 0) {
    device_ = std::make_unique<RdmaDevice>(nearestRdmaDevice(device));
    int fd = -1;
    uint64_t offset = 0;
    exportDmabuf(ctx, base, &fd, &offset);
    region_ = device_->registerDmabuf(base, bytes, fd, offset);
    for (size_t r = 0; r < remote.size(); ++r)
        if (remote[r] && static_cast<int>(r) != rank)
            qps_[r] = std::make_unique<RdmaQueuePair>(*device_);
    LOG(INFO) << "[EP XPU] rank " << rank << " reaches other nodes through "
              << device_->name() << " (GID index " << device_->gidIndex()
              << ")";
    thread_ = std::thread([this] { run(); });
}

XpuEpProxy::~XpuEpProxy() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    thread_.join();
}

std::vector<int32_t> XpuEpProxy::localInfo() const {
    const int num_ranks = static_cast<int>(qps_.size());
    std::vector<int32_t> info(info_ints(num_ranks), 0);
    info[0] = static_cast<int32_t>(base_ & 0xffffffffu);
    info[1] = static_cast<int32_t>(base_ >> 32);
    info[2] = static_cast<int32_t>(region_.rkey);
    for (int r = 0; r < num_ranks; ++r) {
        if (!qps_[r]) continue;
        const RdmaQpInfo qp = qps_[r]->localInfo();
        int32_t* e = info.data() + 4 + 8 * r;
        e[0] = static_cast<int32_t>(qp.qpn);
        e[1] = static_cast<int32_t>(qp.psn);
        e[2] = qp.lid;
        e[3] = qp.mtu;
        std::memcpy(e + 4, qp.gid, sizeof(qp.gid));
    }
    return info;
}

void XpuEpProxy::connect(const std::vector<std::vector<int32_t>>& infos,
                         const std::vector<int>& active) {
    const int num_ranks = static_cast<int>(qps_.size());
    XPU_EP_CHECK(static_cast<int>(infos.size()) == num_ranks &&
                     static_cast<int>(active.size()) == num_ranks,
                 "RDMA connection info must have one entry per rank");
    // Every peer is tried, so one failure does not hide the others.
    std::string errors;
    for (int r = 0; r < num_ranks; ++r) {
        if (!qps_[r] || !active[r]) continue;
        XPU_EP_CHECK(
            infos[r].size() == info_ints(num_ranks),
            "malformed RDMA connection info from rank " + std::to_string(r));
        const int32_t* e = infos[r].data() + 4 + 8 * rank_;
        RdmaQpInfo qp;
        qp.qpn = static_cast<uint32_t>(e[0]);
        qp.psn = static_cast<uint32_t>(e[1]);
        qp.lid = static_cast<uint16_t>(e[2]);
        qp.mtu = static_cast<uint8_t>(e[3]);
        std::memcpy(qp.gid, e + 4, sizeof(qp.gid));
        XPU_EP_CHECK(qp.qpn != 0, "rank " + std::to_string(r) +
                                      " has no queue pair for this rank");
        try {
            qps_[r]->connect(qp);
        } catch (const std::exception& e) {
            errors += "\n  rank " + std::to_string(r) + ": " + e.what();
            continue;
        }
        peers_[r].base =
            static_cast<uint32_t>(infos[r][0]) |
            static_cast<uint64_t>(static_cast<uint32_t>(infos[r][1])) << 32;
        peers_[r].rkey = static_cast<uint32_t>(infos[r][2]);
        peers_[r].connected = true;
    }
    XPU_EP_CHECK(errors.empty(), "rank " + std::to_string(rank_) +
                                     " could not connect to:" + errors);
}

void XpuEpProxy::submit(sycl::event after, Plan plan) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        work_.emplace_back(std::move(after), std::move(plan));
    }
    cv_.notify_all();
}

void XpuEpProxy::drain() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return work_.empty() && !busy_; });
}

void XpuEpProxy::run() {
    for (;;) {
        std::pair<sycl::event, Plan> item;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this] { return stop_ || !work_.empty(); });
            if (work_.empty()) return;
            item = std::move(work_.front());
            work_.pop_front();
            busy_ = true;
        }
        try {
            item.first.wait();
            execute(item.second());
        } catch (const std::exception& e) {
            LOG(ERROR) << "[EP XPU] proxy of rank " << rank_ << ": "
                       << e.what();
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            busy_ = false;
        }
        cv_.notify_all();
    }
}

void XpuEpProxy::fail(int rank, const char* what) {
    peers_[rank].failed = true;
    LOG(ERROR) << "[EP XPU] RDMA writes from rank " << rank_ << " to rank "
               << rank << " failed, skipping it from now on: " << what;
}

void XpuEpProxy::execute(const std::vector<ProxyPut>& puts) {
    std::vector<size_t> last(qps_.size(), 0);
    for (size_t i = 0; i < puts.size(); ++i) last[puts[i].rank] = i;
    for (size_t i = 0; i < puts.size(); ++i) {
        const ProxyPut& p = puts[i];
        Peer& peer = peers_[p.rank];
        if (peer.failed) continue;
        XPU_EP_CHECK(peer.connected,
                     "proxy write to a rank that is not connected");
        RdmaQueuePair& qp = *qps_[p.rank];
        const bool signaled =
            last[p.rank] == i || ++unsignaled_[p.rank] >= kSignalEvery;
        try {
            if (p.bytes)
                qp.write(base_ + p.offset, region_.lkey,
                         peer.base + p.remote_offset, peer.rkey, p.bytes,
                         signaled);
            else
                qp.writeValue(p.value, peer.base + p.remote_offset, peer.rkey,
                              signaled);
            if (signaled) {
                unsignaled_[p.rank] = 0;
                if (++pending_[p.rank] > kMaxPending) {
                    qp.poll(1, kPollTimeoutMs);
                    --pending_[p.rank];
                }
            }
        } catch (const std::exception& e) {
            fail(p.rank, e.what());
        }
    }
    for (size_t r = 0; r < qps_.size(); ++r) {
        if (!pending_[r] || peers_[r].failed) continue;
        try {
            qps_[r]->poll(pending_[r], kPollTimeoutMs);
        } catch (const std::exception& e) {
            fail(static_cast<int>(r), e.what());
        }
        pending_[r] = 0;
    }
}

}  // namespace xpu_ep
}  // namespace mooncake
