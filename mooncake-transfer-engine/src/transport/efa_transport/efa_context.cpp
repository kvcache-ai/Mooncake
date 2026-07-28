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

#include "transport/efa_transport/efa_context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>

#include "config.h"
#include "cuda_alike.h"
#include "transport/efa_transport/efa_endpoint.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

namespace {

// A single unreachable peer can fail every slice of every batch aimed at it.
// One production incident logged 147351 lines (56629 of them for a single
// peer) in 35 minutes, which buried every other message in the log.  Sample
// instead: the AV-teardown WARNING below is emitted once per affected peer and
// carries the actionable information.
constexpr int kCqErrorLogEveryN = 256;

// Same reasoning for peer_map_ eviction: a saturated map on a 16-NIC host logs
// 16 x 16 = 256 evictions for every single new peer.
constexpr int kEvictLogEveryN = 256;

// EFA provider errno values that mean "the address handle backing this peer is
// gone" -- i.e. retrying on the same fi_addr_t can never succeed, but a fresh
// handshake + fi_av_insert can.  These are provider-specific values from
// libfabric's efa_io_comp_status (verified against libfabric 2.4.0amzn1.0 by
// calling fi_cq_strerror() over the range); the EFA provider does not export
// them in a public header, so they are reproduced here with the exact strings
// fi_cq_strerror() returns for them.
//
// Deliberately NOT included: 5/7 (local/remote MR invalid), 13/15
// (unresponsive/unreachable remote).  Those are either not AV-related or are
// transient reachability problems where dropping the handle would just add
// handshake churn on top of a network issue.
inline bool isStalePeerCqError(int prov_errno) {
    switch (prov_errno) {
        case 4:   // "Invalid address handle (local)"
        case 8:   // "Connection was reset by remote peer"
        case 9:   // "Bad queue pair (QP) number (QP does not exist ...)"
        case 14:  // "No valid address handle at remote side ..."
            return true;
        default:
            return false;
    }
}

}  // namespace

// EfaContext implementation

EfaContext::EfaContext(EfaTransport& engine, const std::string& device_name)
    : engine_(engine),
      device_name_(device_name),
      fi_info_(nullptr),
      hints_(nullptr),
      fabric_(nullptr),
      domain_(nullptr),
      av_(nullptr),
      eq_(nullptr),
      eq_poller_stop_(false),
      active_(true),
      shared_ep_(nullptr),
      wr_depth_(0),
      max_wr_depth_(0),
      post_lock_(ATOMIC_FLAG_INIT),
      peer_map_max_(0) {}

EfaContext::~EfaContext() {
    if (fabric_) deconstruct();
}

int EfaContext::construct(size_t num_cq_list, size_t max_cqe,
                          int max_endpoints) {
#if !defined(USE_CUDA) && !defined(USE_HIP)
    // When built without GPU support, prevent libfabric's EFA provider from
    // dlopen-ing libcudart/libcuda at fi_getinfo/fi_domain time. That
    // initialization creates a CUDA primary context on GPU 0 and leaks
    // ~616 MiB of device memory even when no GPU memory is ever registered.
    // Only set if the user hasn't explicitly configured FI_HMEM.
    setenv("FI_HMEM", "system", 0);
#endif

    // Setup hints for EFA provider
    hints_ = fi_allocinfo();
    if (!hints_) {
        LOG(ERROR) << "Failed to allocate fi_info hints";
        return ERR_CONTEXT;
    }

    hints_->caps =
        FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    hints_->mode = FI_CONTEXT;
    hints_->ep_attr->type = FI_EP_RDM;  // EFA uses RDM endpoints
    hints_->fabric_attr->prov_name = strdup("efa");

    // Specify the domain (device) name - append "-rdm" for RDM endpoint
    std::string domain_name = device_name_ + "-rdm";
    hints_->domain_attr->name = strdup(domain_name.c_str());
    hints_->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR |
                                   FI_MR_ALLOCATED | FI_MR_PROV_KEY
#if defined(USE_CUDA) || defined(USE_HIP)
                                   | FI_MR_HMEM
#endif
        ;
    hints_->domain_attr->threading = FI_THREAD_SAFE;

    // Get fabric info.
    //
    // Request libfabric API 1.18+ so the EFA provider's
    // efa_rdm_get_use_device_rdma() takes the "new API" branch and keys
    // the default for FI_EFA_USE_DEVICE_RDMA on hardware capability
    // (hw_support) instead of vendor_part_id.  Under the older 1.14
    // request, the provider's legacy branch hardcoded
    //   default_val = (vendor_part_id == 0xefa0 || 0xefa1) ? false : true
    // which silently disabled device RDMA on Nitro v4 EFA (p5/p5e, part
    // id 0xefa1) while leaving it enabled on Nitro v5+ (p5en and newer).
    // With device RDMA disabled, fi_write falls back to libfabric's
    // emulated RDMA data path, and libfabric 2.4.0 has a thread-safety
    // regression there between fi_av_insert and concurrent fi_cq_read
    // that segfaults Mooncake once the handshake wave finishes and the
    // first real transfers start.  Bumping the requested API to 1.18
    // restores the same default path we already got on newer hardware,
    // and applications that still want emulated RDMA can opt out with
    // FI_EFA_USE_DEVICE_RDMA=0.
    //
    // 1.18 is from March 2023 (EFA installer 1.26+ ships 1.18 or later);
    // all Mooncake deployments today run libfabric >> 1.18.
    int ret =
        fi_getinfo(FI_VERSION(1, 18), nullptr, nullptr, 0, hints_, &fi_info_);
    if (ret) {
        LOG(ERROR) << "fi_getinfo failed for device " << device_name_ << ": "
                   << fi_strerror(-ret);
        fi_freeinfo(hints_);
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Open fabric
    ret = fi_fabric(fi_info_->fabric_attr, &fabric_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_fabric failed: " << fi_strerror(-ret);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Open domain
    ret = fi_domain(fabric_, fi_info_, &domain_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_domain failed: " << fi_strerror(-ret);
        fi_close(&fabric_->fid);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        fabric_ = nullptr;
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Open the EQ before the endpoint so we can bind it in
    // buildSharedEndpoint().  Without an EQ bound to the ep, the EFA
    // provider's efa_base_ep_write_eq_error() falls back to fprintf+abort()
    // for every internal fatal error path (implicit AV-insert from CQ poll,
    // internal-flagged TXE failure, CQ-write-error fallback).  With an EQ
    // bound, those errors land as readable entries instead of killing the
    // process.
    {
        struct fi_eq_attr eq_attr = {};
        eq_attr.size = 64;
        eq_attr.flags = 0;
        eq_attr.wait_obj = FI_WAIT_NONE;
        ret = fi_eq_open(fabric_, &eq_attr, &eq_, nullptr);
        if (ret) {
            LOG(ERROR) << "fi_eq_open failed: " << fi_strerror(-ret);
            fi_close(&domain_->fid);
            fi_close(&fabric_->fid);
            fi_freeinfo(fi_info_);
            fi_freeinfo(hints_);
            domain_ = nullptr;
            fabric_ = nullptr;
            fi_info_ = nullptr;
            hints_ = nullptr;
            eq_ = nullptr;
            return ERR_CONTEXT;
        }
    }

    // Create address vector.  Capacity sized for the largest peer count we
    // expect to support in a single process.  AV entries are cheap (no QP
    // cost), so we over-provision.
    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    // Size the AV with headroom over the peer_map_ cap rather than exactly at
    // it.  peer_map_ may transiently exceed peer_map_max_ when every eviction
    // candidate still has transfers in flight (see endpoint()), and an AV
    // sized to the cap then overflows: fi_av_insert hands back an index past
    // the table, and the next fi_write on it faults inside the provider
    // (observed as a SIGSEGV in libfabric with dest_addr one past the end when
    // MC_MAX_EP_PER_CTX=1).  AV entries cost a few bytes and no QP resources,
    // so headroom is far cheaper than the crash it prevents.
    const size_t kAvHeadroom = 64;
    av_attr.count = max_endpoints > 0
                        ? static_cast<size_t>(max_endpoints) + kAvHeadroom
                        : kAvHeadroom;

    // peer_map_ is bounded to the same capacity as the AV: once that many
    // peers have been inserted, FIFO eviction kicks in (oldest entry
    // disconnected + removed) before adding a new one.  Without this cap,
    // schemes that embed volatile data in peer_nic_path (ip:port:timestamp)
    // cause peer_map_ to grow without bound across peer restarts.
    peer_map_max_ = max_endpoints > 0 ? static_cast<size_t>(max_endpoints) : 0;

    ret = fi_av_open(domain_, &av_attr, &av_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_av_open failed: " << fi_strerror(-ret);
        fi_close(&eq_->fid);
        fi_close(&domain_->fid);
        fi_close(&fabric_->fid);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        eq_ = nullptr;
        domain_ = nullptr;
        fabric_ = nullptr;
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Create completion queues
    cq_list_.resize(num_cq_list);
    for (size_t i = 0; i < num_cq_list; ++i) {
        auto cq = std::make_shared<EfaCq>();

        struct fi_cq_attr cq_attr = {};
        cq_attr.size = max_cqe;
        cq_attr.format = FI_CQ_FORMAT_DATA;
        cq_attr.wait_obj = FI_WAIT_NONE;

        ret = fi_cq_open(domain_, &cq_attr, &cq->cq, nullptr);
        if (ret) {
            LOG(ERROR) << "fi_cq_open failed: " << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
        cq_list_[i] = cq;
    }

    // Build the shared endpoint that services every peer through AV lookup.
    ret = buildSharedEndpoint(globalConfig().max_wr, 64);
    if (ret) {
        LOG(ERROR) << "EfaContext::construct: buildSharedEndpoint failed for "
                   << device_name_;
        return ret;
    }

    LOG(INFO) << "EFA device (libfabric): " << device_name_
              << ", domain: " << fi_info_->domain_attr->name
              << ", provider: " << fi_info_->fabric_attr->prov_name
              << " (shared endpoint, max_wr=" << max_wr_depth_ << ")";

    return 0;
}

int EfaContext::buildSharedEndpoint(size_t max_wr, size_t max_inline) {
    (void)max_inline;
    if (shared_ep_) return 0;

    shared_cq_ = cq_list_.empty() ? nullptr : cq_list_[0];
    if (!shared_cq_) {
        LOG(ERROR) << "EfaContext::buildSharedEndpoint: no CQ available";
        return ERR_CONTEXT;
    }
    max_wr_depth_ = static_cast<int>(max_wr);

    int ret = fi_endpoint(domain_, fi_info_, &shared_ep_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_endpoint failed: " << fi_strerror(-ret);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    ret = fi_ep_bind(shared_ep_, &av_->fid, 0);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind(av) failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    ret = fi_ep_bind(shared_ep_, &eq_->fid, 0);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind(eq) failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    ret = fi_ep_bind(shared_ep_, &shared_cq_->cq->fid, FI_TRANSMIT);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind(tx_cq) failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    ret = fi_ep_bind(shared_ep_, &shared_cq_->cq->fid, FI_RECV);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind(rx_cq) failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    ret = fi_enable(shared_ep_);
    if (ret) {
        LOG(ERROR) << "fi_enable failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Cache our own libfabric address for handshake advertisement.
    size_t addr_len = 64;
    local_ep_addr_.assign(addr_len, 0);
    ret = fi_getname(&shared_ep_->fid, local_ep_addr_.data(), &addr_len);
    if (ret) {
        LOG(ERROR) << "fi_getname failed: " << fi_strerror(-ret);
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
        local_ep_addr_.clear();
        return ERR_ENDPOINT;
    }
    local_ep_addr_.resize(addr_len);

    // Start the EQ poller now that the endpoint is enabled.  Drain any
    // events libfabric posts so the queue does not back up; on error
    // entries, log enough provenance for triage.
    eq_poller_stop_.store(false, std::memory_order_release);
    eq_poller_thread_ = std::thread([this] {
        uint32_t event = 0;
        char buf[256];
        while (!eq_poller_stop_.load(std::memory_order_acquire)) {
            ssize_t n = fi_eq_read(eq_, &event, buf, sizeof(buf), 0);
            if (n == -FI_EAVAIL) {
                struct fi_eq_err_entry err_entry = {};
                ssize_t en = fi_eq_readerr(eq_, &err_entry, 0);
                if (en > 0) {
                    const char* prov_msg =
                        fi_eq_strerror(eq_, err_entry.prov_errno,
                                       err_entry.err_data, nullptr, 0);
                    LOG(ERROR) << "[EFA EQ] device=" << device_name_
                               << " err=" << err_entry.err << " ("
                               << fi_strerror(err_entry.err)
                               << ") prov_errno=" << err_entry.prov_errno
                               << " (" << (prov_msg ? prov_msg : "?") << ")"
                               << " — intercepted; libfabric would have "
                                  "abort()ed without this EQ bind";
                }
                continue;
            }
            if (n > 0) {
                LOG(INFO) << "[EFA EQ] device=" << device_name_
                          << " event=" << event << " bytes=" << n;
                continue;
            }
            if (n == -FI_EAGAIN) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });
    return 0;
}

int EfaContext::deconstruct() {
    // Teardown order matters for the EFA provider: the shared endpoint must
    // be closed before the AV it is bound to.  We also cannot call
    // fi_av_remove() after fi_close(ep) — the provider faults — so we just
    // clear the peer map (dropping shared_ptrs) and let fi_av_close() below
    // invalidate every AV slot in one shot.
    eq_poller_stop_.store(true, std::memory_order_release);
    if (eq_poller_thread_.joinable()) {
        eq_poller_thread_.join();
    }

    if (shared_ep_) {
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
    }

    if (eq_) {
        fi_close(&eq_->fid);
        eq_ = nullptr;
    }

    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        for (auto& entry : peer_map_) {
            if (entry.second.ep) entry.second.ep->markDetachedForTeardown();
        }
        peer_map_.clear();
        peer_lru_.clear();
    }

    {
        RWSpinlock::WriteGuard guard(mr_lock_);
        for (auto& entry : mr_map_) {
            if (entry.second.mr) {
                fi_close(&entry.second.mr->fid);
            }
        }
        mr_map_.clear();
    }

    for (auto& cq : cq_list_) {
        if (cq && cq->cq) {
            fi_close(&cq->cq->fid);
            cq->cq = nullptr;
        }
    }
    cq_list_.clear();
    shared_cq_.reset();

    if (av_) {
        fi_close(&av_->fid);
        av_ = nullptr;
    }

    if (domain_) {
        fi_close(&domain_->fid);
        domain_ = nullptr;
    }

    if (fabric_) {
        fi_close(&fabric_->fid);
        fabric_ = nullptr;
    }

    if (fi_info_) {
        fi_freeinfo(fi_info_);
        fi_info_ = nullptr;
    }

    if (hints_) {
        fi_freeinfo(hints_);
        hints_ = nullptr;
    }

    return 0;
}

int EfaContext::registerMemoryRegionInternal(void* addr, size_t length,
                                             int access,
                                             EfaMemoryRegionMeta& mrMeta) {
    (void)access;
    if (length > (size_t)globalConfig().max_mr_size) {
        LOG(ERROR) << "Buffer length " << length
                   << " exceeds device max_mr_size "
                   << globalConfig().max_mr_size
                   << ". Use EfaTransport::registerLocalMemory() which "
                      "auto-splits large buffers.";
        return ERR_CONTEXT;
    }

    mrMeta.addr = addr;
    mrMeta.length = length;

    // For EFA, we need local read/write and remote read/write
    uint64_t fi_access = FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;

    // Detect memory type and use fi_mr_regattr() for GPU memory.
    // The EFA provider's fi_mr_reg() hardcodes iface=FI_HMEM_SYSTEM,
    // so GPU memory must go through fi_mr_regattr() with explicit
    // iface/device fields (per libfabric spec and EFA provider impl).
    enum fi_hmem_iface iface = FI_HMEM_SYSTEM;
    int device_ordinal = 0;

#if defined(USE_CUDA)
    cudaPointerAttributes attributes;
    cudaError_t cuda_ret = cudaPointerGetAttributes(&attributes, addr);
    if (cuda_ret == cudaSuccess && attributes.type == cudaMemoryTypeDevice) {
        iface = FI_HMEM_CUDA;
        device_ordinal = attributes.device;
    }
#elif defined(USE_HIP)
    hipPointerAttribute_t attributes;
    hipError_t hip_ret = hipPointerGetAttributes(&attributes, addr);
    if (hip_ret == hipSuccess && attributes.type == hipMemoryTypeDevice) {
        iface = FI_HMEM_ROCR;
        device_ordinal = attributes.device;
    }
#endif

    int ret;
    if (iface != FI_HMEM_SYSTEM) {
        // GPU memory: use fi_mr_regattr with explicit iface and device
        struct iovec iov = {.iov_base = addr, .iov_len = length};
        struct fi_mr_attr attr = {};
        attr.mr_iov = &iov;
        attr.iov_count = 1;
        attr.access = fi_access;
        attr.iface = iface;
        attr.device.cuda = device_ordinal;

        ret = fi_mr_regattr(domain_, &attr, 0, &mrMeta.mr);
        if (ret) {
            LOG(ERROR) << "fi_mr_regattr failed for GPU memory " << addr
                       << " (device " << device_ordinal
                       << "): " << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
    } else {
        // CPU memory: fi_mr_reg is sufficient
        ret = fi_mr_reg(domain_, addr, length, fi_access, 0, 0, 0, &mrMeta.mr,
                        nullptr);
        if (ret) {
            LOG(ERROR) << "fi_mr_reg failed for " << addr << ": "
                       << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
    }

    mrMeta.key = fi_mr_key(mrMeta.mr);

    return 0;
}

int EfaContext::registerMemoryRegion(void* addr, size_t length, int access) {
    EfaMemoryRegionMeta mrMeta;
    int ret = registerMemoryRegionInternal(addr, length, access, mrMeta);
    if (ret != 0) {
        return ret;
    }
    RWSpinlock::WriteGuard guard(mr_lock_);
    mr_map_[(uint64_t)addr] = mrMeta;
    return 0;
}

int EfaContext::unregisterMemoryRegion(void* addr) {
    RWSpinlock::WriteGuard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it == mr_map_.end()) {
        return 0;
    }
    if (it->second.mr) {
        fi_close(&it->second.mr->fid);
    }
    mr_map_.erase(it);
    return 0;
}

int EfaContext::preTouchMemory(void* addr, size_t length) {
    volatile char* p = static_cast<char*>(addr);
    const long sc = sysconf(_SC_PAGESIZE);
    const size_t page_size = sc > 0 ? static_cast<size_t>(sc) : 4096;
    for (size_t off = 0; off < length; off += page_size) {
        p[off] = p[off];
    }
    return 0;
}

uint64_t EfaContext::rkey(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return fi_mr_key(it->second.mr);
        }
    }
    return 0;
}

uint64_t EfaContext::lkey(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return fi_mr_key(it->second.mr);
        }
    }
    return 0;
}

void* EfaContext::mrDesc(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return fi_mr_desc(it->second.mr);
        }
    }
    return nullptr;
}

std::shared_ptr<EfaEndPoint> EfaContext::endpoint(
    const std::string& peer_nic_path) {
    // Key the peer map by the full peer_nic_path verbatim.  Each distinct
    // value (including sglang DP>1 workers that share a host but own
    // different RPC ports, or successive generations of the same process
    // that encode a timestamp) gets its own EfaEndPoint + AV slot.  This
    // preserves per-worker identity.  Growth is bounded by FIFO eviction
    // when peer_map_.size() would exceed peer_map_max_ (sized from
    // MC_MAX_EP_PER_CTX at construct()).  The oldest inserted entry is
    // disconnected (fi_av_remove) and erased; this keeps the AV table
    // bounded without aliasing concurrent peers onto a shared slot.
    const std::string& key = peer_nic_path;

    {
        RWSpinlock::ReadGuard guard(peer_map_lock_);
        auto it = peer_map_.find(key);
        if (it != peer_map_.end()) {
            return it->second.ep;
        }
    }

    auto new_ep = std::make_shared<EfaEndPoint>(*this);
    new_ep->setPeerNicPath(peer_nic_path);

    // Items set under the write lock and consumed after releasing it, so
    // disconnect() (which takes EfaEndPoint's own lock + calls
    // fi_av_remove) can run without holding peer_map_lock_.
    std::shared_ptr<EfaEndPoint> evicted_ep;
    std::string evicted_path;
    size_t post_evict_size = 0;
    // Set when the map is at capacity but every candidate victim still has
    // operations in flight, so the map grows past peer_map_max_ instead.
    bool over_capacity = false;
    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        auto it = peer_map_.find(key);
        if (it != peer_map_.end()) {
            return it->second.ep;
        }

        // Enforce peer_map_ capacity.  Evict the oldest entry if adding
        // this one would exceed peer_map_max_.  peer_map_max_ == 0 means
        // "unbounded" (keeps legacy behaviour for tests that skip
        // construct()).
        if (peer_map_max_ > 0 && peer_map_.size() >= peer_map_max_ &&
            !peer_lru_.empty()) {
            // Walk the LRU oldest-first for a victim with nothing in flight.
            // fi_av_remove() on a slot that still has posted operations makes
            // the EFA provider drop their completions entirely: the slices
            // stay PENDING, wr_depth_ / cq outstanding never drain, and the
            // caller's getTransferStatus() loop spins on WAITING forever.
            // Skipping busy peers means the map can transiently exceed
            // peer_map_max_ (see over_capacity below) -- correct, because an
            // AV entry on EFA/SRD costs a few bytes and there is no hard QP
            // limit, whereas a stranded transfer costs the whole request.
            for (auto lru_it = peer_lru_.begin(); lru_it != peer_lru_.end();
                 ++lru_it) {
                auto evict_it = peer_map_.find(*lru_it);
                if (evict_it == peer_map_.end()) continue;
                if (evict_it->second.ep &&
                    evict_it->second.ep->hasOutstandingSlice())
                    continue;
                evicted_path = *lru_it;
                evicted_ep = evict_it->second.ep;
                peer_map_.erase(evict_it);
                peer_lru_.erase(lru_it);
                break;
            }
            if (!evicted_ep) over_capacity = true;
        }

        peer_lru_.push_back(key);
        auto lru_it = std::prev(peer_lru_.end());
        peer_map_[key] = PeerMapEntry{new_ep, lru_it};
        post_evict_size = peer_map_.size();
    }

    if (evicted_ep) {
        // WARNING, not INFO: reaching the cap means peer_map_ is saturated and
        // every new peer now costs an eviction.  Rate-limited because a
        // saturated map evicts once per (local NIC x remote NIC) pair per new
        // peer -- 16x16 = 256 lines per peer on a 16-NIC host.
        LOG_EVERY_N(WARNING, kEvictLogEveryN)
            << "EFA peer_map_ is at capacity on " << nicPath()
            << "; evicting oldest peer to make room: " << evicted_path << " -> "
            << peer_nic_path << " (peer_count=" << post_evict_size
            << ", peer_map_max=" << peer_map_max_
            << ").  Raise MC_MAX_EP_PER_CTX or leave it unset if peers are"
            << " still active; on EFA (SRD) an AV entry is a few bytes and"
            << " there is no hard QP limit to protect."
            << " [logged 1 of every " << kEvictLogEveryN << "]";
        // disconnectIfIdle(), not disconnect(): it takes the victim's own write
        // lock, so no submitter can be mid-post on that handle when the AV slot
        // is released.  If the lock is contended it declines, and we put the
        // entry back rather than tear down a handle in active use -- the map
        // runs one over capacity until the next insert, which is far cheaper
        // than a stranded transfer.  Any operations still outstanding against
        // the underlying AV slot (possibly another endpoint's) keep it alive:
        // removePeerAddr defers the fi_av_remove until they complete.
        if (!evicted_ep->disconnectIfIdle()) {
            RWSpinlock::WriteGuard guard(peer_map_lock_);
            if (peer_map_.find(evicted_path) == peer_map_.end()) {
                peer_lru_.push_back(evicted_path);
                peer_map_[evicted_path] =
                    PeerMapEntry{evicted_ep, std::prev(peer_lru_.end())};
                over_capacity = true;
            }
        }
    }
    if (over_capacity) {
        LOG_EVERY_N(WARNING, kEvictLogEveryN)
            << "EFA peer_map_ is over capacity on " << nicPath()
            << " and every eviction candidate still has transfers in flight;"
            << " admitting " << peer_nic_path
            << " anyway (peer_count=" << post_evict_size
            << ", peer_map_max=" << peer_map_max_
            << ").  MC_MAX_EP_PER_CTX is set too low for the number of"
            << " concurrently active peers; raise it or leave it unset."
            << " [logged 1 of every " << kEvictLogEveryN << "]";
    }
    return new_ep;
}

std::shared_ptr<EfaEndPoint> EfaContext::peekEndpoint(
    const std::string& peer_nic_path) {
    RWSpinlock::ReadGuard guard(peer_map_lock_);
    auto it = peer_map_.find(peer_nic_path);
    if (it == peer_map_.end()) return nullptr;
    return it->second.ep;
}

int EfaContext::deleteEndpoint(const std::string& peer_nic_path) {
    std::shared_ptr<EfaEndPoint> ep;
    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        auto it = peer_map_.find(peer_nic_path);
        if (it == peer_map_.end()) return 0;
        ep = it->second.ep;
        peer_lru_.erase(it->second.lru_it);
        peer_map_.erase(it);
    }
    if (ep) ep->disconnect();  // runs fi_av_remove
    return 0;
}

bool EfaContext::deleteEndpointIfIdle(const std::string& peer_nic_path) {
    // Two-phase so the fi_av_remove() happens outside peer_map_lock_ (it takes
    // the endpoint's own lock), and so a busy peer keeps its map entry: we must
    // not erase first and discover afterwards that we cannot retire it.
    std::shared_ptr<EfaEndPoint> ep;
    {
        RWSpinlock::ReadGuard guard(peer_map_lock_);
        auto it = peer_map_.find(peer_nic_path);
        if (it == peer_map_.end()) return true;  // already gone
        ep = it->second.ep;
    }
    if (ep && !ep->disconnectIfIdle()) return false;
    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        auto it = peer_map_.find(peer_nic_path);
        // Only erase the entry we actually retired; a concurrent
        // endpoint() call may have replaced it with a fresh handle.
        if (it != peer_map_.end() && it->second.ep == ep) {
            peer_lru_.erase(it->second.lru_it);
            peer_map_.erase(it);
        }
    }
    return true;
}

int EfaContext::disconnectAllEndpoints() {
    RWSpinlock::WriteGuard guard(peer_map_lock_);
    for (auto& entry : peer_map_) {
        if (entry.second.ep) entry.second.ep->disconnect();
    }
    return 0;
}

size_t EfaContext::getTotalQPNumber() const {
    RWSpinlock::ReadGuard guard(peer_map_lock_);
    return peer_map_.size();
}

std::string EfaContext::nicPath() const {
    return engine_.local_server_name() + "@" + device_name_;
}

std::string EfaContext::localAddr() const {
    if (!fi_info_ || !fi_info_->src_addr) return "";
    std::ostringstream oss;
    const uint8_t* addr = static_cast<const uint8_t*>(fi_info_->src_addr);
    for (size_t i = 0; i < fi_info_->src_addrlen; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)addr[i];
    }
    return oss.str();
}

std::string EfaContext::localEpAddr() const {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.resize(local_ep_addr_.size() * 2);
    for (size_t i = 0; i < local_ep_addr_.size(); ++i) {
        out[2 * i] = kHex[(local_ep_addr_[i] >> 4) & 0xF];
        out[2 * i + 1] = kHex[local_ep_addr_[i] & 0xF];
    }
    return out;
}

// Decode one hex nibble, -1 on invalid input.
static inline int hexNibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

int EfaContext::insertPeerAddr(const std::string& peer_hex_addr,
                               fi_addr_t& out) {
    if (peer_hex_addr.empty() || (peer_hex_addr.size() % 2) != 0) {
        LOG(ERROR) << "insertPeerAddr: invalid hex length "
                   << peer_hex_addr.size();
        return ERR_INVALID_ARGUMENT;
    }
    const size_t n = peer_hex_addr.size() / 2;
    std::vector<uint8_t> bin(n);
    for (size_t i = 0; i < n; ++i) {
        int hi = hexNibble(peer_hex_addr[2 * i]);
        int lo = hexNibble(peer_hex_addr[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            LOG(ERROR) << "insertPeerAddr: non-hex char at offset " << (2 * i);
            return ERR_INVALID_ARGUMENT;
        }
        bin[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return insertPeerAddrBytes(bin.data(), bin.size(), out);
}

int EfaContext::insertPeerAddrBytes(const uint8_t* addr, size_t len,
                                    fi_addr_t& out) {
    if (!addr || len == 0) return ERR_INVALID_ARGUMENT;

    // Defensive validation before handing bytes to libfabric.
    //
    // Some peer-restart races / partial-init bugs surface handshake
    // payloads with malformed addresses: wrong length, all-zero bytes,
    // truncated bytes, etc. libfabric's EFA provider does NOT validate
    // the address layout; it will happily accept and later SEGV deep
    // inside internal libs (observed: GPF in libc / libnuma / libfabric
    // from provider-internal pointer deref).
    //
    // The legitimate endpoint address length on this system is set by
    // fi_getname() when we built our own shared endpoint.  Any peer
    // address must be exactly that length; otherwise the bytes are
    // definitely malformed and must not be passed to fi_av_insert.
    if (!local_ep_addr_.empty() && len != local_ep_addr_.size()) {
        LOG(ERROR) << "insertPeerAddrBytes: peer address length mismatch on "
                   << nicPath() << " (expected " << local_ep_addr_.size()
                   << ", got " << len
                   << ") — refusing to call fi_av_insert to avoid libfabric"
                      " provider crash on malformed address";
        return ERR_INVALID_ARGUMENT;
    }
    bool all_zero = true;
    for (size_t i = 0; i < len; ++i) {
        if (addr[i] != 0) {
            all_zero = false;
            break;
        }
    }
    if (all_zero) {
        LOG(ERROR) << "insertPeerAddrBytes: peer address is all-zero on "
                   << nicPath()
                   << " — refusing (likely half-initialized / uninitialized"
                      " peer)";
        return ERR_INVALID_ARGUMENT;
    }

    int ret = fi_av_insert(av_, addr, 1, &out, 0, nullptr);
    if (ret != 1) {
        // libfabric's fi_av_insert returns three-valued:
        //   > 0 : number of addresses inserted (we expect 1)
        //   == 0 : request accepted but no address inserted — "silent refuse",
        //          usually means a stale / duplicate / malformed peer address
        //          that the provider recognizes but rejects without setting
        //          errno.  `fi_strerror(-0)` literally returns "Success" here,
        //          which would mislead diagnosis; log the raw ret instead.
        //   < 0 : negative errno from libfabric.
        size_t av_usage = 0;
        {
            RWSpinlock::ReadGuard guard(peer_map_lock_);
            av_usage = peer_map_.size();
        }
        // Prefix of the peer address in hex, for cross-host correlation.
        std::string addr_prefix;
        const size_t kPrefixBytes = 8;
        const size_t show = std::min<size_t>(len, kPrefixBytes);
        addr_prefix.reserve(show * 2);
        static const char kHex[] = "0123456789abcdef";
        for (size_t i = 0; i < show; ++i) {
            addr_prefix.push_back(kHex[(addr[i] >> 4) & 0xF]);
            addr_prefix.push_back(kHex[addr[i] & 0xF]);
        }
        LOG(ERROR) << "fi_av_insert failed on " << nicPath()
                   << ": expected 1 inserted, got ret=" << ret
                   << (ret < 0 ? std::string(" (libfabric error: ") +
                                     fi_strerror(-ret) + ")"
                               : std::string(
                                     " (libfabric silently refused — likely "
                                     "stale/duplicate/malformed peer address)"))
                   << ", peer_count=" << av_usage << ", addr_len=" << len
                   << ", addr_prefix=0x" << addr_prefix
                   << (len > kPrefixBytes ? "..." : "");
        return ERR_ENDPOINT;
    }
    // Claim a reference on the slot.  See av_slots_: the provider hands the
    // same index back for the same peer address, so concurrent handles share
    // it and only the last release may call fi_av_remove().
    //
    // A slot being re-inserted cancels any deferred removal: the address is
    // live again, so retiring it would strand the new holder.
    auto* slot = avSlot(out);
    slot->refcount.fetch_add(1, std::memory_order_acq_rel);
    slot->remove_pending.store(false, std::memory_order_release);
    return 0;
}

EfaContext::AvSlotState* EfaContext::avSlot(fi_addr_t fi_addr) {
    {
        RWSpinlock::ReadGuard guard(av_ref_lock_);
        auto it = av_slots_.find(fi_addr);
        if (it != av_slots_.end()) return it->second.get();
    }
    RWSpinlock::WriteGuard guard(av_ref_lock_);
    auto& entry = av_slots_[fi_addr];
    if (!entry) entry = std::make_unique<AvSlotState>();
    return entry.get();
}

EfaContext::AvSlotState* EfaContext::avSlotIfPresent(fi_addr_t fi_addr) {
    RWSpinlock::ReadGuard guard(av_ref_lock_);
    auto it = av_slots_.find(fi_addr);
    if (it == av_slots_.end()) return nullptr;
    return it->second.get();
}

bool EfaContext::slotHasInflight(fi_addr_t fi_addr) {
    if (fi_addr == FI_ADDR_UNSPEC) return false;
    auto* slot = avSlotIfPresent(fi_addr);
    if (!slot) return false;
    return slot->inflight.load(std::memory_order_acquire) > 0;
}

void EfaContext::retireSlotIfPending(AvSlotState* slot, fi_addr_t fi_addr) {
    if (!slot || fi_addr == FI_ADDR_UNSPEC) return;
    // Only the thread that both sees remove_pending and drains the last
    // in-flight operation performs the removal, and the CAS makes sure exactly
    // one of them does.  Re-check refcount too: a fresh handshake may have
    // re-admitted this address, in which case insertPeerAddrBytes has already
    // cleared remove_pending and the CAS below fails harmlessly.
    if (!slot->remove_pending.load(std::memory_order_acquire)) return;
    if (slot->inflight.load(std::memory_order_acquire) != 0) return;
    if (slot->refcount.load(std::memory_order_acquire) != 0) return;
    bool expected = true;
    if (!slot->remove_pending.compare_exchange_strong(
            expected, false, std::memory_order_acq_rel))
        return;
    removeSlotNow(fi_addr);
}

void EfaContext::removeSlotNow(fi_addr_t fi_addr) {
    // Serialize the removal against the post burst on the same lock the
    // submitters use.  Refcounting alone is not enough: fi_av_remove() frees
    // the index for reuse, so a holder that has already latched peer_fi_addr_
    // can call fi_write() on a number the provider has meanwhile handed to a
    // different peer -- observed as SIGSEGV in libfabric with dest_addr=0.
    // Taking post_lock_ means a removal can never land between a submitter's
    // check and its fi_write, and any post already inside the burst finishes
    // first.
    while (post_lock_.test_and_set(std::memory_order_acquire)) {
    }
    int ret = fi_av_remove(av_, &fi_addr, 1, 0);
    post_lock_.clear(std::memory_order_release);
    if (ret) {
        LOG(WARNING) << "fi_av_remove failed on " << nicPath() << ": "
                     << fi_strerror(-ret) << " (fi_addr=" << fi_addr << ")";
    }
}

void EfaContext::removePeerAddr(fi_addr_t fi_addr) {
    if (fi_addr == FI_ADDR_UNSPEC) return;

    auto* slot = avSlotIfPresent(fi_addr);
    if (!slot) {
        // Not tracked: either already retired or inserted before this
        // bookkeeping existed.  Removing again risks freeing a slot the
        // provider has since handed to a different peer, so don't.
        LOG(WARNING) << "EFA skipping fi_av_remove of untracked slot "
                     << fi_addr << " on " << nicPath();
        return;
    }

    // Drop our reference; only the last holder may retire the slot.  Removing
    // it while another EfaEndPoint still posts against the same index faults
    // inside the provider (see av_slots_).
    if (slot->refcount.fetch_sub(1, std::memory_order_acq_rel) > 1) return;

    // Last holder.  If operations are still posted against this slot, do NOT
    // remove it now: the provider would drop their completions and the slices
    // would sit in PENDING forever.  Mark it and let the CQ poller retire it
    // once the last completion lands.
    //
    // Checking inflight here (a slot-wide count) rather than the endpoint's own
    // counter is the whole point: one slot is shared by several endpoints, so
    // an endpoint can be perfectly idle while the slot it is releasing still
    // has a sibling's operations outstanding.
    if (slot->inflight.load(std::memory_order_acquire) > 0) {
        slot->remove_pending.store(true, std::memory_order_release);
        // Re-check: the last completion may have landed between the load above
        // and the store, in which case the poller has already walked away and
        // nobody would ever retire the slot.
        retireSlotIfPending(slot, fi_addr);
        return;
    }

    removeSlotNow(fi_addr);
}

int EfaContext::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
    // Route slices to appropriate peer handles.  Group by peer NIC path.
    std::unordered_map<std::string, std::vector<Transport::Slice*>>
        slices_by_peer;

    for (auto* slice : slice_list) {
        if (!slice) continue;

        // Fast path: peer info already filled in by the caller
        // (dest_rkey and peer_nic_path set on the slice before dispatch).
        if (!slice->peer_nic_path.empty()) {
            slices_by_peer[slice->peer_nic_path].push_back(slice);
            continue;
        }

        // Slow path: resolve peer info per-slice.
        auto peer_segment_desc =
            engine_.meta()->getSegmentDescByID(slice->target_id);
        if (!peer_segment_desc) {
            LOG(ERROR) << "Cannot get segment descriptor for target "
                       << slice->target_id;
            slice->markFailed();
            continue;
        }

        int buffer_id = -1, device_id = -1;
        if (EfaTransport::selectDevice(peer_segment_desc.get(),
                                       slice->rdma.dest_addr, slice->length,
                                       buffer_id, device_id)) {
            LOG(ERROR) << "Cannot select device for dest_addr "
                       << (void*)slice->rdma.dest_addr;
            slice->markFailed();
            continue;
        }

        slice->rdma.dest_rkey =
            peer_segment_desc->buffers[buffer_id].rkey[device_id];

        std::string peer_nic_path = peer_segment_desc->name + "@" +
                                    peer_segment_desc->devices[device_id].name;
        slice->peer_nic_path = peer_nic_path;
        slices_by_peer[peer_nic_path].push_back(slice);
    }

    for (auto& entry : slices_by_peer) {
        const std::string& peer_nic_path = entry.first;
        auto& peer_slices = entry.second;

        auto ep = endpoint(peer_nic_path);
        if (!ep) {
            LOG(ERROR) << "Cannot create peer handle for " << peer_nic_path;
            for (auto* slice : peer_slices) slice->markFailed();
            continue;
        }

        std::vector<Transport::Slice*> failed_slice_list;
        int rc = ep->submitPostSend(peer_slices, failed_slice_list);
        for (auto* slice : failed_slice_list) slice->markFailed();

        // Drop peer handle if it is no longer connected after submit,
        // freeing its AV entry for reuse.  Under the shared-endpoint model
        // this is cheap (no fid_ep to destroy).
        //
        // deleteEndpointIfIdle(), not deleteEndpoint(): this runs on a submit
        // thread while OTHER submit threads may still have operations posted
        // against the same AV slot (one slot is shared by every handle for the
        // same peer address).  The unconditional variant fi_av_remove()s the
        // slot regardless, and the provider then completes those live
        // operations with prov_errno=1 "Flushed during queue pair destroy" --
        // or faults inside fi_cq_read while doing so.  If the peer is still
        // busy we simply leave the handle in place; the CQ poller's stale-peer
        // path retires it once the slot quiesces.
        if (rc != 0 && !ep->connected()) {
            deleteEndpointIfIdle(peer_nic_path);
        }
    }

    return 0;
}

int EfaContext::submitSlicesOnPeer(
    fi_addr_t peer_fi_addr, std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    // In-flight accounting lives on the AV slot, not on the calling endpoint:
    // several endpoints can share one slot, so only a slot-wide count can tell
    // a teardown whether completions are still outstanding against it.
    AvSlotState* slot = avSlot(peer_fi_addr);
    // Batched submission against the shared endpoint.  Mirrors the previous
    // per-endpoint submit path but uses context-level wr_depth / post_lock.
    //
    // 1. Reserve N WR+CQ slots in bulk (single CAS each)
    // 2. Prepare MR descriptors and op contexts outside the lock
    // 3. Hold post_lock_ once for the entire batch of fi_write calls
    const int kMaxBackoffYields = 100000;
    const int cq_limit = static_cast<int>(globalConfig().max_cqe);
    std::atomic<int>* cq_outstanding =
        shared_cq_ ? &shared_cq_->outstanding : nullptr;

    struct BatchEntry {
        Transport::Slice* slice;
        void* local_desc;
        EfaOpContext* op_ctx;
    };

    // Consume slice_list via a moving index instead of erase-from-front,
    // which was O(N^2) on large batches.  retry_slices accumulate across
    // passes and are applied by rewinding the cursor.
    size_t cursor = 0;
    std::vector<Transport::Slice*> retry_slices;
    // Escape hatch for a permanently un-postable batch.  Without one this loop
    // spins forever when the provider keeps returning FI_EAGAIN and nothing
    // ever completes -- e.g. the peer's AV slot was removed, so every fi_write
    // against it is refused even though WR credit is free (observed as
    // wr_depth=0 with unbroken FI_EAGAIN).
    //
    // Trigger on CONSECUTIVE waves that posted nothing, not on total waves and
    // not on elapsed time.  Total waves fails healthy long-lived batches that
    // retry while making progress, and a multi-second timer makes each stall a
    // multi-second stall: the request layer can only re-handshake once these
    // slices reach a terminal state, so failing fast is what lets the transfer
    // recover (0.01 GB/s on a 10s timer vs ~10 GB/s failing promptly).
    const int kMaxStalledWaves = 1024;
    int stalled_waves = 0;
    while (cursor < slice_list.size() || !retry_slices.empty()) {
        if (stalled_waves > kMaxStalledWaves) {
            LOG(WARNING) << "EFA submitSlicesOnPeer: " << kMaxStalledWaves
                         << " consecutive FI_EAGAIN waves posted nothing on "
                         << nicPath() << "; failing "
                         << (retry_slices.size() + slice_list.size() - cursor)
                         << " slice(s) (wr_depth="
                         << wr_depth_.load(std::memory_order_relaxed)
                         << ", max=" << max_wr_depth_ << ")";
            for (auto* slice : retry_slices) failed_slice_list.push_back(slice);
            for (size_t i = cursor; i < slice_list.size(); ++i)
                failed_slice_list.push_back(slice_list[i]);
            slice_list.clear();
            return 0;
        }
        if (!retry_slices.empty()) {
            // Splice retry slices back in at the head of the un-consumed
            // region so the next pass picks them up.  Drop the consumed prefix
            // at the same time and rewind the cursor: re-inserting without
            // trimming grew slice_list by one entry per retried slice per wave
            // and was observed at 188 million entries in a hung process.
            // Trimming keeps it bounded by the original batch size, at the same
            // O(remaining) cost the insert already paid.
            slice_list.erase(slice_list.begin(), slice_list.begin() + cursor);
            cursor = 0;
            slice_list.insert(slice_list.begin(), retry_slices.begin(),
                              retry_slices.end());
            retry_slices.clear();
            std::this_thread::yield();
        }

        const size_t remaining = slice_list.size() - cursor;
        int batch_count = 0;
        int backoff = 0;
        bool timed_out = false;
        while (batch_count == 0) {
            int cur_wr = wr_depth_.load(std::memory_order_relaxed);
            int wr_avail = max_wr_depth_ - cur_wr;
            if (wr_avail <= 0) {
                if (++backoff > kMaxBackoffYields) {
                    timed_out = true;
                    break;
                }
                std::this_thread::yield();
                continue;
            }
            int want = std::min(wr_avail, (int)remaining);
            if (cq_outstanding) {
                int cur_cq = cq_outstanding->load(std::memory_order_relaxed);
                int cq_avail = cq_limit - cur_cq;
                if (cq_avail <= 0) {
                    if (++backoff > kMaxBackoffYields) {
                        timed_out = true;
                        break;
                    }
                    std::this_thread::yield();
                    continue;
                }
                want = std::min(want, cq_avail);
                if (!wr_depth_.compare_exchange_weak(
                        cur_wr, cur_wr + want, std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    continue;
                }
                cur_cq = cq_outstanding->load(std::memory_order_relaxed);
                cq_avail = cq_limit - cur_cq;
                if (cq_avail < want) {
                    wr_depth_.fetch_sub(want, std::memory_order_acq_rel);
                    continue;
                }
                if (!cq_outstanding->compare_exchange_weak(
                        cur_cq, cur_cq + want, std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    wr_depth_.fetch_sub(want, std::memory_order_acq_rel);
                    continue;
                }
            } else {
                if (!wr_depth_.compare_exchange_weak(
                        cur_wr, cur_wr + want, std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    continue;
                }
            }
            batch_count = want;
        }

        if (timed_out) {
            LOG(WARNING) << "EFA submitSlicesOnPeer: timed out waiting for CQ"
                         << " drain (wr_depth="
                         << wr_depth_.load(std::memory_order_relaxed)
                         << ", max=" << max_wr_depth_ << ", cq_outstanding="
                         << (cq_outstanding ? cq_outstanding->load(
                                                  std::memory_order_relaxed)
                                            : -1)
                         << ", max_cqe=" << cq_limit << ")";
            for (size_t i = cursor; i < slice_list.size(); ++i) {
                failed_slice_list.push_back(slice_list[i]);
            }
            slice_list.clear();
            return 0;
        }

        std::vector<BatchEntry> batch(batch_count);
        int valid_count = 0;

        for (int i = 0; i < batch_count; i++) {
            Transport::Slice* slice = slice_list[cursor + i];
            void* local_desc = mrDesc(slice->source_addr);
            if (!local_desc) {
                LOG(ERROR) << "No MR descriptor found for address "
                           << slice->source_addr;
                failed_slice_list.push_back(slice);
                continue;
            }
            EfaOpContext* op_ctx = new EfaOpContext();
            memset(op_ctx, 0, sizeof(EfaOpContext));
            op_ctx->slice = slice;
            op_ctx->wr_depth = &wr_depth_;
            op_ctx->slot = slot;
            op_ctx->slot_addr = peer_fi_addr;
            batch[valid_count++] = {slice, local_desc, op_ctx};
        }

        int mr_failures = batch_count - valid_count;
        if (mr_failures > 0) {
            wr_depth_.fetch_sub(mr_failures, std::memory_order_acq_rel);
            if (cq_outstanding)
                cq_outstanding->fetch_sub(mr_failures,
                                          std::memory_order_acq_rel);
        }

        if (valid_count > 0) {
            while (post_lock_.test_and_set(std::memory_order_acquire)) {
            }
            for (int i = 0; i < valid_count; i++) {
                auto& entry = batch[i];
                ssize_t ret;
                // Count the operation BEFORE handing it to the provider: once
                // fi_write returns, its completion may already have been reaped
                // by the CQ poller on another thread.  Incrementing afterwards
                // would let that poller decrement first (driving the count
                // negative) and, worse, let a concurrent teardown see inflight
                // == 0 and fi_av_remove a slot with a live operation on it.
                // Rolled back below on every path that does not post.
                slot->inflight.fetch_add(1, std::memory_order_acq_rel);
                if (entry.slice->opcode == Transport::TransferRequest::READ) {
                    ret = fi_read(shared_ep_, (void*)entry.slice->source_addr,
                                  entry.slice->length, entry.local_desc,
                                  peer_fi_addr, entry.slice->rdma.dest_addr,
                                  entry.slice->rdma.dest_rkey,
                                  &entry.op_ctx->fi_ctx);
                } else {
                    ret = fi_write(shared_ep_, (void*)entry.slice->source_addr,
                                   entry.slice->length, entry.local_desc,
                                   peer_fi_addr, entry.slice->rdma.dest_addr,
                                   entry.slice->rdma.dest_rkey,
                                   &entry.op_ctx->fi_ctx);
                }
                if (ret == 0) {
                    entry.slice->status = Transport::Slice::PENDING;
                    // Stamp the post time so MC_SLICE_TIMEOUT can fire.  EFA
                    // used to leave ts at 0, which made
                    // MultiTransport::checkSliceTimeout() skip the slice
                    // unconditionally: any completion the provider dropped
                    // (e.g. fi_av_remove of a slot with work in flight) hung
                    // the caller's status loop forever with no escape.  RDMA
                    // already stamps here (rdma_endpoint.cpp).
                    entry.slice->ts = getCurrentTimeInNano();
                } else if (ret == -FI_EAGAIN) {
                    // Nothing was posted, so give the speculative count back.
                    slot->inflight.fetch_sub(1, std::memory_order_acq_rel);
                    delete entry.op_ctx;
                    int not_posted = valid_count - i;
                    wr_depth_.fetch_sub(not_posted, std::memory_order_acq_rel);
                    if (cq_outstanding)
                        cq_outstanding->fetch_sub(not_posted,
                                                  std::memory_order_acq_rel);
                    for (int j = i; j < valid_count; j++) {
                        if (j > i) delete batch[j].op_ctx;
                        retry_slices.push_back(batch[j].slice);
                    }
                    break;
                } else {
                    slot->inflight.fetch_sub(1, std::memory_order_acq_rel);
                    LOG(ERROR)
                        << "fi_read/fi_write failed: " << fi_strerror(-ret)
                        << " (source=" << entry.slice->source_addr
                        << ", len=" << entry.slice->length
                        << ", dest=" << (void*)entry.slice->rdma.dest_addr
                        << ", rkey=" << entry.slice->rdma.dest_rkey << ")";
                    delete entry.op_ctx;
                    wr_depth_.fetch_sub(1, std::memory_order_acq_rel);
                    if (cq_outstanding)
                        cq_outstanding->fetch_sub(1, std::memory_order_acq_rel);
                    failed_slice_list.push_back(entry.slice);
                }
            }
            post_lock_.clear(std::memory_order_release);
            // The rollback paths above may have been the decrement that
            // quiesced a slot whose last holder already released it.  Settle it
            // here rather than leaking the AV entry until process exit.  Must
            // be outside post_lock_: removeSlotNow() takes that same lock.
            retireSlotIfPending(slot, peer_fi_addr);
        }

        cursor += batch_count;

        // A wave made forward progress if any slice it claimed left the queue
        // -- posted, MR-rejected, or hard-failed.  Only when every one bounced
        // straight back onto retry_slices is submission genuinely stuck, so
        // that is the only case that keeps the stall clock running.
        if (batch_count > 0 &&
            static_cast<int>(retry_slices.size()) >= batch_count)
            ++stalled_waves;
        else
            stalled_waves = 0;
    }

    slice_list.clear();
    return 0;
}

int EfaContext::pollCq(int max_entries, int cq_index) {
    if (cq_index < 0 || (size_t)cq_index >= cq_list_.size()) {
        return 0;
    }

    struct fid_cq* cq = cq_list_[cq_index]->cq;
    if (!cq) return 0;

    struct fi_cq_data_entry entries[64];
    int to_poll = std::min(max_entries, 64);

    ssize_t ret = fi_cq_read(cq, entries, to_poll);

    if (ret > 0) {
        std::unordered_map<std::atomic<int>*, int> wr_depth_set;
        // Slots that just saw a completion; any whose deferred removal is now
        // unblocked is retired after the loop (retireSlotIfPending takes
        // post_lock_, so it must not run while we still hold CQ state).
        std::vector<std::pair<AvSlotState*, fi_addr_t>> touched_slots;
        for (ssize_t i = 0; i < ret; i++) {
            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(entries[i].op_context);
            if (op_ctx && op_ctx->slice) {
                op_ctx->slice->markSuccess();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                if (op_ctx->slot) {
                    op_ctx->slot->inflight.fetch_sub(1,
                                                     std::memory_order_acq_rel);
                    touched_slots.emplace_back(op_ctx->slot, op_ctx->slot_addr);
                }
                delete op_ctx;
            }
        }
        for (auto& entry : wr_depth_set) {
            entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);
        }
        for (auto& s : touched_slots) retireSlotIfPending(s.first, s.second);
        cq_list_[cq_index]->outstanding.fetch_sub(static_cast<int>(ret),
                                                  std::memory_order_acq_rel);
        return static_cast<int>(ret);
    } else if (ret == -FI_EAGAIN) {
        return 0;
    } else if (ret < 0) {
        int err_count = 0;
        std::unordered_map<std::atomic<int>*, int> wr_depth_set;
        // Peers whose AV entry the provider says is gone; their handles are
        // dropped after the drain loop so the next batch re-handshakes.
        std::vector<std::string> stale_peers;
        // See the success path: slots settled after the drain, not inside it.
        std::vector<std::pair<AvSlotState*, fi_addr_t>> touched_slots;

        for (;;) {
            // MUST be re-zeroed every iteration.  libfabric only copies
            // provider error details into err_data when the caller supplies
            // a buffer AND err_data_size; otherwise it hands back an internal
            // pointer that is documented as valid only "until the next time
            // the CQ is read".  Leaving the struct uninitialized made
            // fi_cq_strerror() below format whatever stack garbage sat in
            // err_data, producing unreadable log lines for every error after
            // the first in a drain loop.
            struct fi_cq_err_entry err_entry = {};
            char err_data_buf[64] = {};
            err_entry.err_data = err_data_buf;
            err_entry.err_data_size = sizeof(err_data_buf);

            ret = fi_cq_readerr(cq, &err_entry, 0);
            if (ret <= 0) break;

            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(err_entry.op_context);
            if (op_ctx && op_ctx->slice) {
                if (isStalePeerCqError(err_entry.prov_errno)) {
                    // The AV slot backing this peer is no longer usable (peer
                    // process restarted, or its address was removed on the
                    // remote side).  Nothing in the async CQ path used to act
                    // on this, so the endpoint stayed CONNECTED with a dead
                    // fi_addr_t and every subsequent batch re-posted against
                    // it -- an unbounded error loop that only ended when the
                    // process was restarted.  Record the peer and tear the
                    // handle down below.
                    if (!op_ctx->slice->peer_nic_path.empty())
                        stale_peers.push_back(op_ctx->slice->peer_nic_path);
                }
                LOG_EVERY_N(ERROR, kCqErrorLogEveryN)
                    << "EFA CQ error on " << nicPath() << ": "
                    << fi_cq_strerror(cq, err_entry.prov_errno,
                                      err_entry.err_data, nullptr, 0)
                    << " (prov_errno=" << err_entry.prov_errno << ", peer="
                    << (op_ctx->slice->peer_nic_path.empty()
                            ? "unknown"
                            : op_ctx->slice->peer_nic_path)
                    << ", slice at " << op_ctx->slice->source_addr
                    << ") [logged 1 of every " << kCqErrorLogEveryN << "]";
                op_ctx->slice->markFailed();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                if (op_ctx->slot) {
                    op_ctx->slot->inflight.fetch_sub(1,
                                                     std::memory_order_acq_rel);
                    touched_slots.emplace_back(op_ctx->slot, op_ctx->slot_addr);
                }
                delete op_ctx;
            }
            err_count++;
        }

        for (auto& entry : wr_depth_set) {
            entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);
        }
        for (auto& s : touched_slots) retireSlotIfPending(s.first, s.second);
        if (err_count > 0) {
            cq_list_[cq_index]->outstanding.fetch_sub(
                err_count, std::memory_order_acq_rel);
        }

        // Drop stale peer handles outside the drain loop: deleteEndpoint()
        // takes peer_map_lock_ and calls fi_av_remove().  Duplicates within
        // one drain are harmless -- deleteEndpoint() is a no-op once the
        // entry is gone.
        std::sort(stale_peers.begin(), stale_peers.end());
        stale_peers.erase(std::unique(stale_peers.begin(), stale_peers.end()),
                          stale_peers.end());
        for (const auto& peer : stale_peers) {
            // Same constraint as eviction: fi_av_remove() while operations are
            // still posted against the slot loses their completions and hangs
            // those slices.  Deferring is safe and converges -- every op aimed
            // at a dead peer fails the same way, so a later drain sees the
            // counter at zero and does the teardown then.
            if (!deleteEndpointIfIdle(peer)) {
                LOG_EVERY_N(WARNING, kCqErrorLogEveryN)
                    << "EFA deferring peer-handle drop on " << nicPath()
                    << " for " << peer << ": transfer(s) still in flight;"
                    << " will retry when they complete [logged 1 of every "
                    << kCqErrorLogEveryN << "]";
                continue;
            }
            LOG(WARNING) << "EFA dropped peer handle on " << nicPath()
                         << " after unrecoverable CQ error: " << peer
                         << " (will re-handshake on next transfer)";
        }
        return err_count;
    }

    return 0;
}

}  // namespace mooncake
