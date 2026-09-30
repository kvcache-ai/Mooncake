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

#include "tent/transport/fabric/fabric_context.h"

#include <glog/logging.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>

#include "tent/common/utils/os.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

namespace {

constexpr uint64_t kAppMrMode = FI_MR_LOCAL | FI_MR_VIRT_ADDR |
                                FI_MR_ALLOCATED | FI_MR_PROV_KEY |
                                FI_MR_ENDPOINT;

const FabricProfile kProfiles[] = {
    // EFA: FI_VERSION(1,18) opts in to device RDMA read/write (#2041).
    {"efa", "efa", FI_VERSION(1, 18),
     FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY, "-rdm",
     true, false},
    // tcp;ofi_rxm: for tests on machines without fabric hardware. Its default
    // RMA completion fires once the data is queued on the socket.
    {"tcp", "tcp;ofi_rxm", 0, kAppMrMode, "", false, true},
};

std::string fiError(int rc) {
    return std::string(fi_strerror(rc < 0 ? -rc : rc)) + " (" +
           std::to_string(rc) + ")";
}

int readPciNumaNode(const struct fi_info* info) {
    if (!info->nic || !info->nic->bus_attr ||
        info->nic->bus_attr->bus_type != FI_BUS_PCI)
        return -1;
    const auto& pci = info->nic->bus_attr->attr.pci;
    char path[128];
    snprintf(path, sizeof(path),
             "/sys/bus/pci/devices/%04x:%02x:%02x.%x/numa_node", pci.domain_id,
             pci.bus_id, pci.device_id, pci.function_id);
    std::ifstream in(path);
    int node = -1;
    if (in >> node) return node;
    return -1;
}

bool endsWith(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool whitelisted(const std::vector<std::string>& devices,
                 const std::string& domain, const std::string& suffix) {
    for (const auto& dev : devices) {
        if (dev == domain || dev + suffix == domain) return true;
    }
    return false;
}

}  // namespace

const FabricProfile* findFabricProfile(const std::string& name) {
    for (const auto& profile : kProfiles) {
        if (name == profile.name) return &profile;
    }
    return nullptr;
}

std::vector<const FabricProfile*> autoFabricProfiles() {
    // tcp is never picked automatically: it would shadow the tcp transport.
    return {findFabricProfile("efa")};
}

void FabricTask::unref() {
    if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        Slab<FabricTask>::Get().deallocate(this);
    }
}

void FabricTask::opDone(bool ok, size_t bytes) {
    if (ok) {
        transferred.fetch_add(bytes, std::memory_order_relaxed);
    } else {
        failed.store(true, std::memory_order_relaxed);
    }
    if (pending.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    // Store bytes before status: a reader who acquires the terminal status
    // also sees the final transferred value.
    if (failed.load(std::memory_order_relaxed)) {
        status.store(TransferStatusEnum::FAILED, std::memory_order_release);
    } else {
        transferred.store(length, std::memory_order_release);
        status.store(TransferStatusEnum::COMPLETED, std::memory_order_release);
    }
    if (local_active) local_active->fetch_sub(1, std::memory_order_acq_rel);
    if (notify_progress) notify_progress(progress_batch_id);
}

FabricOp* allocateFabricOp() { return Slab<FabricOp>::Get().allocate(); }

void freeFabricOp(FabricOp* op) { Slab<FabricOp>::Get().deallocate(op); }

FabricTask* allocateFabricTask() { return Slab<FabricTask>::Get().allocate(); }

void freeFabricDomains(std::vector<FabricDomainInfo>& domains) {
    for (auto& domain : domains) {
        if (domain.info) fi_freeinfo(domain.info);
        domain.info = nullptr;
    }
    domains.clear();
}

Status discoverFabricDomains(const FabricProfile& profile,
                             const FabricParams& params,
                             std::vector<FabricDomainInfo>& domains) {
    domains.clear();
    struct fi_info* hints = fi_allocinfo();
    if (!hints) return Status::InternalError("fi_allocinfo failed" LOC_MARK);
    hints->caps =
        FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    hints->mode = FI_CONTEXT | FI_CONTEXT2;
    hints->ep_attr->type = FI_EP_RDM;
    hints->domain_attr->mr_mode = profile.mr_mode_hints;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    hints->fabric_attr->prov_name = strdup(profile.prov_name);

    const uint32_t version =
        profile.api_version ? profile.api_version : fi_version();
    struct fi_info* list = nullptr;
    int rc = fi_getinfo(version, nullptr, nullptr, 0, hints, &list);
    fi_freeinfo(hints);
    if (rc) {
        return Status::DeviceNotFound("fi_getinfo(" +
                                      std::string(profile.prov_name) +
                                      "): " + fiError(rc) + LOC_MARK);
    }

    // One entry per domain. Libfabric may list a domain several times (IPv4
    // and IPv6 for tcp, efa and efa-direct fabrics for EFA).
    const std::string suffix = profile.domain_suffix;
    const std::string wanted_fabric =
        !params.fabric_name.empty()
            ? params.fabric_name
            : (std::string(profile.name) == "efa" ? "efa" : "");
    std::map<std::string, struct fi_info*> chosen;
    std::vector<std::string> order;
    for (auto* cur = list; cur; cur = cur->next) {
        if (!cur->domain_attr || !cur->domain_attr->name) continue;
        if (cur->domain_attr->mr_mode & FI_MR_RAW) continue;
        const std::string domain = cur->domain_attr->name;
        const std::string fabric = cur->fabric_attr && cur->fabric_attr->name
                                       ? cur->fabric_attr->name
                                       : "";
        if (!wanted_fabric.empty() && fabric != wanted_fabric) continue;
        if (!suffix.empty() && !endsWith(domain, suffix)) continue;
        if (!params.devices.empty() &&
            !whitelisted(params.devices, domain, suffix))
            continue;
        auto it = chosen.find(domain);
        if (it == chosen.end()) {
            chosen[domain] = cur;
            order.push_back(domain);
        } else if (it->second->addr_format != FI_SOCKADDR_IN &&
                   cur->addr_format == FI_SOCKADDR_IN) {
            it->second = cur;
        }
    }
    // Loopback is only useful when nothing else is available.
    if (params.devices.empty() && order.size() > 1) {
        order.erase(std::remove(order.begin(), order.end(), "lo"), order.end());
    }
    for (const auto& domain : order) {
        FabricDomainInfo entry;
        entry.name = domain;
        entry.info = fi_dupinfo(chosen[domain]);
        if (!entry.info) {
            fi_freeinfo(list);
            freeFabricDomains(domains);
            return Status::InternalError("fi_dupinfo failed" LOC_MARK);
        }
        entry.numa_node = readPciNumaNode(entry.info);
        domains.push_back(entry);
    }
    fi_freeinfo(list);
    if (domains.empty()) {
        return Status::DeviceNotFound(
            "No usable " + std::string(profile.prov_name) + " domain" LOC_MARK);
    }
    return Status::OK();
}

FabricContext::~FabricContext() { close(); }

Status FabricContext::open(const FabricProfile& profile,
                           FabricDomainInfo& domain,
                           const FabricParams& params) {
    params_ = params;
    name_ = domain.name;
    numa_node_ = domain.numa_node;
    info_ = domain.info;
    domain.info = nullptr;

    auto fail = [&](const char* what, int rc) {
        Status status = Status::RdmaError(std::string(what) + " on " + name_ +
                                          ": " + fiError(rc) + LOC_MARK);
        close();
        return status;
    };

    const uint64_t mr_mode = info_->domain_attr->mr_mode;
    virt_addr_ = mr_mode & FI_MR_VIRT_ADDR;
    mr_bind_ep_ = mr_mode & FI_MR_ENDPOINT;
    prov_key_ = mr_mode & FI_MR_PROV_KEY;
    max_msg_size_ = info_->ep_attr->max_msg_size;
    max_mr_size_ = 0;
    if (profile.delivery_complete) {
        info_->tx_attr->op_flags &=
            ~(FI_INJECT_COMPLETE | FI_TRANSMIT_COMPLETE);
        info_->tx_attr->op_flags |= FI_DELIVERY_COMPLETE;
    }

    int rc = fi_fabric(info_->fabric_attr, &fabric_, nullptr);
    if (rc) return fail("fi_fabric", rc);
    rc = fi_domain(fabric_, info_, &domain_, nullptr);
    if (rc) return fail("fi_domain", rc);

    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    rc = fi_av_open(domain_, &av_attr, &av_, nullptr);
    if (rc) return fail("fi_av_open", rc);

    const size_t tx_size = info_->tx_attr->size ? info_->tx_attr->size : 1024;
    struct fi_cq_attr cq_attr = {};
    cq_attr.size = tx_size;
    cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    cq_attr.wait_obj = FI_WAIT_NONE;
    rc = fi_cq_open(domain_, &cq_attr, &cq_, nullptr);
    if (rc) return fail("fi_cq_open", rc);
    credits_ = cq_attr.size ? std::min(tx_size, cq_attr.size) : tx_size;
    if (params_.max_posted_ops)
        credits_ = std::min(credits_, params_.max_posted_ops);

    rc = fi_endpoint(domain_, info_, &ep_, nullptr);
    if (rc) return fail("fi_endpoint", rc);
    rc = fi_ep_bind(ep_, &av_->fid, 0);
    if (rc) return fail("fi_ep_bind(av)", rc);
    rc = fi_ep_bind(ep_, &cq_->fid, FI_TRANSMIT | FI_RECV);
    if (rc) return fail("fi_ep_bind(cq)", rc);
    rc = fi_enable(ep_);
    if (rc) return fail("fi_enable", rc);

    std::string addr(64, '\0');
    size_t addr_len = addr.size();
    rc = fi_getname(&ep_->fid, &addr[0], &addr_len);
    if (rc == -FI_ETOOSMALL) {
        addr.resize(addr_len);
        rc = fi_getname(&ep_->fid, &addr[0], &addr_len);
    }
    if (rc) return fail("fi_getname", rc);
    addr.resize(addr_len);
    address_ = addr;

    running_.store(true, std::memory_order_release);
    worker_ = std::thread([this] { workerLoop(); });

    LOG(INFO) << "Fabric context " << name_
              << " opened: fabric=" << info_->fabric_attr->name
              << " prov=" << info_->fabric_attr->prov_name
              << " numa=" << numa_node_ << " credits=" << credits_
              << " max_msg=" << max_msg_size_ << " mr_mode=0x" << std::hex
              << mr_mode << std::dec << (virt_addr_ ? " va" : " offset")
              << (mr_bind_ep_ ? " mr-bind" : "");
    return Status::OK();
}

void FabricContext::stop() {
    if (worker_.joinable()) {
        {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            running_.store(false, std::memory_order_release);
        }
        queue_cv_.notify_all();
        worker_.join();
    }
    // Close the endpoint first so the provider no longer touches op storage,
    // then fail everything that did not complete.
    if (ep_) fi_close(&ep_->fid);
    ep_ = nullptr;
    std::vector<FabricOp*> leftovers;
    {
        std::lock_guard<std::mutex> guard(queue_mutex_);
        leftovers.swap(queue_);
        queue_nonempty_.store(false, std::memory_order_release);
    }
    for (size_t i = pending_head_; i < pending_.size(); ++i)
        leftovers.push_back(pending_[i]);
    pending_.clear();
    pending_head_ = 0;
    for (auto* op : posted_) leftovers.push_back(op);
    posted_.clear();
    for (auto* op : leftovers) completeOp(op, false);
}

void FabricContext::close() {
    stop();
    // An AV must not be modified after its endpoint is closed; entries are
    // released together with the AV.
    if (av_) fi_close(&av_->fid);
    av_ = nullptr;
    if (cq_) fi_close(&cq_->fid);
    cq_ = nullptr;
    if (domain_) {
        int rc = fi_close(&domain_->fid);
        if (rc)
            LOG(WARNING) << "Fabric domain " << name_
                         << " close failed: " << fiError(rc);
    }
    domain_ = nullptr;
    if (fabric_) fi_close(&fabric_->fid);
    fabric_ = nullptr;
    if (info_) fi_freeinfo(info_);
    info_ = nullptr;
    peers_.clear();
}

Status FabricContext::registerMemory(void* addr, size_t length,
                                     struct fid_mr*& mr, void*& desc,
                                     uint64_t& key) {
    const uint64_t access =
        FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    const uint64_t requested_key =
        prov_key_ ? 0 : next_key_.fetch_add(1, std::memory_order_relaxed);
    mr = nullptr;
    int rc = fi_mr_reg(domain_, addr, length, access, 0, requested_key, 0, &mr,
                       nullptr);
    if (rc) {
        return Status::RdmaError("fi_mr_reg(" + std::to_string(length) +
                                 " bytes) on " + name_ + ": " + fiError(rc) +
                                 LOC_MARK);
    }
    if (mr_bind_ep_) {
        rc = fi_mr_bind(mr, &ep_->fid, 0);
        if (!rc) rc = fi_mr_enable(mr);
        if (rc) {
            fi_close(&mr->fid);
            mr = nullptr;
            return Status::RdmaError("fi_mr_bind/enable on " + name_ + ": " +
                                     fiError(rc) + LOC_MARK);
        }
    }
    desc = fi_mr_desc(mr);
    key = fi_mr_key(mr);
    return Status::OK();
}

void FabricContext::deregisterMemory(struct fid_mr* mr) {
    if (!mr) return;
    int rc = fi_close(&mr->fid);
    if (rc)
        LOG(WARNING) << "Fabric MR close on " << name_
                     << " failed: " << fiError(rc);
}

Status FabricContext::resolvePeer(const std::string& address, fi_addr_t& peer) {
    std::lock_guard<std::mutex> guard(av_mutex_);
    auto it = peers_.find(address);
    if (it != peers_.end()) {
        peer = it->second;
        return Status::OK();
    }
    fi_addr_t result = FI_ADDR_UNSPEC;
    int rc = fi_av_insert(av_, address.data(), 1, &result, 0, nullptr);
    if (rc != 1) {
        return Status::RdmaError("fi_av_insert on " + name_ + ": " +
                                 fiError(rc) + LOC_MARK);
    }
    peers_[address] = result;
    peer = result;
    return Status::OK();
}

void FabricContext::submit(std::vector<FabricOp*>& ops) {
    if (ops.empty()) return;
    inflight_.fetch_add(ops.size(), std::memory_order_acq_rel);
    std::lock_guard<std::mutex> guard(queue_mutex_);
    queue_.insert(queue_.end(), ops.begin(), ops.end());
    queue_nonempty_.store(true, std::memory_order_release);
    if (worker_idle_) queue_cv_.notify_one();
}

void FabricContext::completeOp(FabricOp* op, bool ok) {
    FabricTask* task = op->task;
    const size_t length = op->length;
    if (op->overdue) {
        auto it = overdue_peers_.find(op->peer);
        if (it != overdue_peers_.end() && --it->second == 0)
            overdue_peers_.erase(it);
    }
    freeFabricOp(op);
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
    task->opDone(ok, length);
    task->unref();
}

void FabricContext::expirePosted(uint64_t now_ns) {
    const uint64_t timeout_ns = params_.op_timeout_ms * 1000000ull;
    size_t overdue = 0;
    for (auto* op : posted_) {
        if (op->overdue || now_ns - op->post_ns <= timeout_ns) continue;
        // The task stays pending: the NIC may still access its buffers, so
        // it must not be failed over or have its buffer unregistered yet.
        op->overdue = true;
        ++overdue_peers_[op->peer];
        ++overdue;
    }
    if (overdue)
        LOG(WARNING) << "Fabric " << name_ << ": " << overdue
                     << " ops without completion after "
                     << params_.op_timeout_ms
                     << " ms; failing new ops to their peers until they return";
}

bool FabricContext::failUnposted(const FabricOp* op, uint64_t now_ns) const {
    if (!overdue_peers_.empty() && overdue_peers_.count(op->peer)) return true;
    const uint64_t timeout_ns = params_.post_timeout_ms * 1000000ull;
    return timeout_ns && now_ns - op->enqueue_ns > timeout_ns;
}

void FabricContext::expirePending(uint64_t now_ns) {
    // Queued ops never reached the provider, so they can fail right away,
    // whether or not a credit frees up.
    size_t kept = pending_head_;
    size_t failed = 0;
    for (size_t i = pending_head_; i < pending_.size(); ++i) {
        FabricOp* op = pending_[i];
        if (!failUnposted(op, now_ns)) {
            pending_[kept++] = op;
            continue;
        }
        completeOp(op, false);
        ++failed;
    }
    pending_.resize(kept);
    if (kept == pending_head_) {
        pending_.clear();
        pending_head_ = 0;
    }
    if (failed)
        LOG(WARNING) << "Fabric " << name_ << ": failed " << failed
                     << " queued ops that could not be posted";
}

bool FabricContext::postPending(uint64_t now_ns) {
    bool progressed = false;
    const uint64_t timeout_ns = params_.post_timeout_ms * 1000000ull;
    while (pending_head_ < pending_.size() && posted_.size() < credits_) {
        FabricOp* op = pending_[pending_head_];
        if (!overdue_peers_.empty() && overdue_peers_.count(op->peer)) {
            ++pending_head_;
            completeOp(op, false);
            progressed = true;
            continue;
        }
        ssize_t rc;
        if (op->is_write) {
            rc = fi_write(ep_, op->local, op->length, op->desc, op->peer,
                          op->remote_addr, op->key, &op->ctx);
        } else {
            rc = fi_read(ep_, op->local, op->length, op->desc, op->peer,
                         op->remote_addr, op->key, &op->ctx);
        }
        if (rc == 0) {
            op->post_ns = now_ns;
            posted_.insert(op);
            ++pending_head_;
            progressed = true;
            continue;
        }
        if (rc == -FI_EAGAIN) {
            // Provider queue is full; keep the op and let CQ polling drive
            // progress. Give up only after a wall-clock timeout.
            if (timeout_ns && now_ns - op->enqueue_ns > timeout_ns) {
                LOG(WARNING) << "Fabric post on " << name_ << " timed out";
                ++pending_head_;
                completeOp(op, false);
                progressed = true;
                continue;
            }
            break;
        }
        LOG(WARNING) << "Fabric " << (op->is_write ? "fi_write" : "fi_read")
                     << " on " << name_ << " failed: " << fiError((int)rc);
        ++pending_head_;
        completeOp(op, false);
        progressed = true;
    }
    if (pending_head_ == pending_.size()) {
        pending_.clear();
        pending_head_ = 0;
    }
    return progressed;
}

bool FabricContext::pollCompletions() {
    constexpr size_t kBatch = 64;
    struct fi_cq_entry entries[kBatch];
    bool progressed = false;
    while (true) {
        ssize_t n = fi_cq_read(cq_, entries, kBatch);
        if (n > 0) {
            for (ssize_t i = 0; i < n; ++i) {
                auto* op = static_cast<FabricOp*>(entries[i].op_context);
                if (!op || !posted_.erase(op)) continue;
                completeOp(op, true);
            }
            progressed = true;
            if ((size_t)n < kBatch) break;
            continue;
        }
        if (n == -FI_EAVAIL) {
            struct fi_cq_err_entry err = {};
            n = fi_cq_readerr(cq_, &err, 0);
            if (n > 0) {
                LOG(WARNING) << "Fabric completion error on " << name_ << ": "
                             << fiError(err.err)
                             << " prov_errno=" << err.prov_errno << " "
                             << fi_cq_strerror(cq_, err.prov_errno,
                                               err.err_data, nullptr, 0);
                auto* op = static_cast<FabricOp*>(err.op_context);
                if (op && posted_.erase(op)) completeOp(op, false);
                progressed = true;
                continue;
            }
        } else if (n != -FI_EAGAIN) {
            LOG_EVERY_N(WARNING, 1000)
                << "fi_cq_read on " << name_ << ": " << fiError((int)n);
        }
        break;
    }
    return progressed;
}

void FabricContext::workerLoop() {
    constexpr uint64_t kExpireIntervalNs = 100 * 1000000ull;
    uint64_t idle_rounds = 0;
    uint64_t last_expire_ns = getCurrentTimeInNano();
    while (running_.load(std::memory_order_acquire)) {
        bool progressed = false;
        if (queue_nonempty_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            pending_.insert(pending_.end(), queue_.begin(), queue_.end());
            queue_.clear();
            queue_nonempty_.store(false, std::memory_order_release);
            progressed = true;
        }
        if (pending_head_ < pending_.size())
            progressed |= postPending(getCurrentTimeInNano());
        // Always poll: providers with manual progress (tcp;ofi_rxm) only
        // serve remote RMA while their CQ is being read.
        progressed |= pollCompletions();
        const bool queued = pending_head_ < pending_.size();
        if (queued || !posted_.empty()) {
            const uint64_t now_ns = getCurrentTimeInNano();
            if (now_ns - last_expire_ns > kExpireIntervalNs) {
                if (params_.op_timeout_ms && !posted_.empty())
                    expirePosted(now_ns);
                if (queued) expirePending(now_ns);
                last_expire_ns = now_ns;
            }
        }
        if (progressed || !posted_.empty()) {
            idle_rounds = 0;
            continue;
        }
        if (++idle_rounds > 1000 && params_.idle_sleep_us) {
            // Park until new work arrives. The bounded wait keeps polling
            // the CQ for providers whose target side needs manual progress.
            std::unique_lock<std::mutex> lock(queue_mutex_);
            worker_idle_ = true;
            queue_cv_.wait_for(
                lock, std::chrono::microseconds(params_.idle_sleep_us), [&] {
                    return !queue_.empty() ||
                           !running_.load(std::memory_order_acquire);
                });
            worker_idle_ = false;
        }
    }
}

}  // namespace tent
}  // namespace mooncake
