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

// Rate-limit CQ error logs: a single dead peer can fail every slice of every
// batch aimed at it and flood the log.  The AV-teardown WARNING below is
// emitted once per affected peer and carries the actionable information.
constexpr int kCqErrorLogEveryN = 256;

// Same reasoning for peer_map_ eviction: a saturated map evicts once per
// (local NIC x remote NIC) pair for every new peer.
constexpr int kEvictLogEveryN = 256;

// Monotonic nanosecond stamp for the credit-flow diagnostics.  steady_clock
// rather than getCurrentTimeInNano(): an age derived from CLOCK_REALTIME jumps
// whenever NTP steps the clock, and this shares a clock domain with the
// wall-clock wait measured in submitSlicesOnPeer().  vDSO, no syscall.
inline uint64_t monotonicNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

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
        FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ |
        FI_REMOTE_WRITE
#if defined(USE_CUDA) || defined(USE_HIP)
        // Declare FI_HMEM so the provider wires up HMEM-aware copy routines
        // (cudaMemcpy) on every data path, including the intra-node SHM SAR
        // segmentation/reassembly path. Registering device memory via
        // FI_MR_HMEM alone is NOT enough: without FI_HMEM in caps the SHM
        // sub-provider initializes its copy callbacks in plain-host mode and
        // does a host memcpy() straight into a CUDA device VA during SAR,
        // which SIGSEGVs in __memcpy_avx512_unaligned_erms.
        // See https://github.com/ofiwg/libfabric/issues/12328.
        | FI_HMEM
#endif
        ;
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

    // Create completion queues.
    //
    // Same design as max_wr_depth_ in buildSharedEndpoint(): the submit path
    // also paces against a CQ-occupancy counter (EfaCq::outstanding vs
    // max_cqe_), so that ceiling must be the CQ the provider created, not the
    // one we asked for.  EFA raises any request below
    // MAX(rx_attr->size + tx_attr->size, FI_EFA_CQ_SIZE) to that value
    // (efa_domain.c), e.g. 12288 on p5 -- 3x the 4096 default, so the counter
    // would stop submission long before the CQ is full.
    //
    // fi_cq_open() writes the size it chose back into cq_attr.size; read that
    // instead of recomputing the formula, which folds in the operator-settable
    // FI_EFA_CQ_SIZE and may change in a future provider.
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
        // Every CQ on this domain gets the same treatment, so one value covers
        // the list; guard anyway so a future provider that sizes them
        // independently cannot leave the counter above a smaller CQ.
        if (i == 0 || cq_attr.size < max_cqe_) max_cqe_ = cq_attr.size;
        cq_list_[i] = cq;
    }
    if (max_cqe_ > max_cqe) {
        VLOG(1) << "EFA " << device_name_ << ": provider opened a CQ of "
                << max_cqe_ << " entries for the requested " << max_cqe
                << "; pacing against the larger real depth.";
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
              << ", fabric: " << fi_info_->fabric_attr->name
              << ", provider: " << fi_info_->fabric_attr->prov_name
              << " (shared endpoint, max_wr=" << max_wr_depth_
              << ", provider tx queue=" << fi_info_->tx_attr->size
              << ", max_cqe=" << max_cqe_ << ")";

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

    // Design: adopt the provider's transmit depth instead of pacing against an
    // independent number.  We never tell libfabric how deep we want the queue
    // (fi_endpoint() below takes its depth from fi_info_), and the depth is a
    // per-device attribute -- 4096 on p5, 2048 on p6-b300 -- so no compiled-in
    // default can match it.  Either direction of disagreement hurts: too
    // shallow and submitters exhaust credit while the queue is mostly empty;
    // too deep and we hand out credit fi_write must refuse with FI_EAGAIN.
    // Both were observed in production.
    //
    // Read-back rather than a hint: asking for more than the device supports
    // makes the EFA provider fail fi_getinfo() with -FI_ENODATA, turning a
    // misconfigured MC_MAX_WR into a failure to initialize.  MC_MAX_WR still
    // throttles a NIC when it asks for less; it can no longer ask for more.
    const size_t provider_tx_depth =
        fi_info_ && fi_info_->tx_attr ? fi_info_->tx_attr->size : 0;
    if (provider_tx_depth == 0) {
        LOG(ERROR) << "EfaContext::buildSharedEndpoint: provider reported no "
                      "transmit queue depth for "
                   << device_name_;
        return ERR_CONTEXT;
    }
    if (!globalConfig().max_wr_from_env) {
        // No override: take the provider's depth verbatim, so the default is
        // correct on an instance type nobody has measured yet.
        max_wr = provider_tx_depth;
    } else if (max_wr > provider_tx_depth) {
        // FIRST_N(1): MC_MAX_WR is process-wide, so an over-large value is one
        // mistake, not one per NIC -- a p5 has 32.  The value that took effect
        // is in the per-device "EFA device" INFO line below.
        LOG_FIRST_N(WARNING, 1)
            << "EFA " << device_name_ << ": MC_MAX_WR=" << max_wr
            << " exceeds the provider's transmit queue depth ("
            << provider_tx_depth
            << "); clamping.  A larger value cannot increase the number of "
            << "operations the NIC accepts -- it only hands out credit that "
            << "fi_write must refuse with FI_EAGAIN.  Unset MC_MAX_WR to track "
            << "the provider's depth automatically.";
        max_wr = provider_tx_depth;
    }
    max_wr_depth_ = static_cast<int>(max_wr);

    int ret = fi_endpoint(domain_, fi_info_, &shared_ep_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_endpoint failed: " << fi_strerror(-ret);
        shared_ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Skip the EFA RDM handshake, which otherwise gates the very first RMA to
    // each new peer.  Before the handshake completes, fi_write()/fi_read()
    // queue into a fixed 16-slot allowance
    // (EFA_RDM_MAX_QUEUED_OPE_BEFORE_HANDSHAKE) and the 17th onward return
    // -FI_EAGAIN.  The counter lives on the *endpoint*, not the peer, so with
    // one shared_ep_ per NIC a single peer's first batch stalls submission for
    // every other peer on that NIC.  Measured on p5: ops 17..N all EAGAIN,
    // clearing only after ~14-29 ms of retries -- exactly the "pod became
    // ready, then a burst of first transfers" failure seen in production.
    //
    // The option tells the provider that all peers share our platform,
    // software version, and capabilities, so no negotiation is needed.  That
    // holds for a Mooncake cluster by construction: peers run the same build
    // against the same EFA fabric.  What the provider then assumes on our word
    // is p2p / RDMA-read / RDMA-write support (efa_rdm_ep.h), i.e. it uses OUR
    // capabilities for the peer -- safe between identical instance types, so
    // MC_EFA_HOMOGENEOUS_PEERS=0 exists for a genuinely mixed fleet.
    //
    // Must precede fi_enable(): the flag changes protocol selection at enable
    // time.  It is a local endpoint property, so it needs no coordination with
    // the peer and can be rolled out one side at a time.
    //
    // One documented constraint (fi_efa.7): the target of an RMA must have
    // inserted the initiator's address into its AV before the op starts, else
    // completions fail with prov_errno=14.  EfaEndPoint satisfies this --
    // setupConnectionsByPassive() calls insertPeerAddr() before it replies, and
    // the active side only marks CONNECTED after that reply arrives.  Should it
    // ever be violated, 14 is already in isStalePeerCqError() and drops the
    // peer handle for a fresh handshake.
    //
    // Support is decided at RUNTIME, not compile time: the option's value is
    // resolved from headers if we have them and vendored if we do not (see
    // kEfaOptHomogeneousPeers), but whether it takes effect depends on the
    // libfabric actually loaded.  A provider too old to know the option answers
    // -FI_ENOPROTOOPT / -FI_EOPNOTSUPP, which we treat as "keep the handshake".
    if (globalConfig().efa_homogeneous_peers) {
        bool homogeneous = true;
        ret = fi_setopt(&shared_ep_->fid, FI_OPT_ENDPOINT,
                        kEfaOptHomogeneousPeers, &homogeneous,
                        sizeof(homogeneous));
        if (ret == -FI_EOPNOTSUPP || ret == -FI_ENOPROTOOPT) {
            // libfabric predates the option (added in 2.2), or it was built
            // without it: keep the handshake path.
            LOG(INFO) << "EFA: FI_OPT_EFA_HOMOGENEOUS_PEERS unsupported on "
                      << device_name_ << " (" << fi_strerror(-ret)
                      << "), using the default handshake path";
        } else if (ret) {
            LOG(ERROR) << "fi_setopt(FI_OPT_EFA_HOMOGENEOUS_PEERS) failed on "
                       << device_name_ << ": " << fi_strerror(-ret);
            fi_close(&shared_ep_->fid);
            shared_ep_ = nullptr;
            return ERR_ENDPOINT;
        } else {
            VLOG(1) << "EFA: homogeneous peers enabled on " << device_name_
                    << ", skipping the per-peer handshake";
        }
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

#if defined(USE_CUDA)
// A silent failure here resurfaces as the provider's opaque "Operation not
// supported" from fi_mr_regattr(), which is the attribution problem this whole
// helper exists to remove -- so name the driver call that actually failed.
static void logCudaFailure(const char* call, CUresult ret, int device_ordinal) {
    const char* err = nullptr;
    cuGetErrorString(ret, &err);
    LOG(WARNING) << "EFA: " << call << " failed for CUDA device "
                 << device_ordinal << ": " << (err ? err : "unknown") << " ("
                 << ret
                 << "); GPU memory registration may fail with a bare"
                    " \"Operation not supported\"";
}

// Make `device_ordinal`'s primary context current on the calling thread, unless
// a context for that same device already is.
//
// Why a context is needed at all: fi_mr_regattr() on FI_HMEM_CUDA memory
// reaches libfabric's cuda_get_dmabuf_fd(), which calls the driver API
// cuMemGetHandleForAddressRange() and does no context management of its own.
// The export also runs against the CURRENT context, so a context belonging to
// another device is no better than none -- both end up as a bare "Operation not
// supported" from the provider.
//
// Who arrives here without the right context: registerLocalMemoryBatch() runs
// one std::async(std::launch::async) per buffer and registerLocalMemory() can
// fan out one std::thread per NIC, so a registering thread may have touched no
// CUDA API at all; and since std::async may reuse threads, one thread can
// register buffers on several devices in turn.
//
// cuDevicePrimaryCtxRetain() returns the same primary context the CUDA runtime
// uses, so this attaches to the process's existing context rather than creating
// another.  The retain is intentionally not released: the primary context
// outlives every registration, and dropping the last reference here would tear
// down the context the rest of the process is using.
static void bindCudaContextIfNeeded(int device_ordinal) {
    CUdevice want;
    CUresult ret = cuDeviceGet(&want, device_ordinal);
    if (ret != CUDA_SUCCESS) {
        logCudaFailure("cuDeviceGet", ret, device_ordinal);
        return;
    }

    CUcontext cur = nullptr;
    CUdevice cur_dev;
    if (cuCtxGetCurrent(&cur) == CUDA_SUCCESS && cur != nullptr &&
        cuCtxGetDevice(&cur_dev) == CUDA_SUCCESS && cur_dev == want)
        return;

    CUcontext primary = nullptr;
    ret = cuDevicePrimaryCtxRetain(&primary, want);
    if (ret != CUDA_SUCCESS) {
        logCudaFailure("cuDevicePrimaryCtxRetain", ret, device_ordinal);
        return;
    }
    ret = cuCtxSetCurrent(primary);
    if (ret != CUDA_SUCCESS)
        logCudaFailure("cuCtxSetCurrent", ret, device_ordinal);
}
#endif

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
#if defined(USE_CUDA)
        bindCudaContextIfNeeded(device_ordinal);
#endif
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
    // that encode a timestamp) gets its own handle / peer_map slot; the AV
    // index may be shared between them, which is why av_slots_ refcounts.
    // This preserves per-worker identity.  Growth is bounded by FIFO eviction
    // when peer_map_.size() would exceed peer_map_max_ (sized from
    // MC_MAX_EP_PER_CTX at construct()): the oldest inserted entry is
    // disconnected and erased, keeping the AV table bounded without aliasing
    // concurrent peers onto a shared slot.
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
            // Walk the FIFO list oldest-first for a victim with nothing in
            // flight (see av_slots_ for why a busy slot must not be retired).
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
        // every new peer now costs an eviction.
        LOG_EVERY_N(WARNING, kEvictLogEveryN)
            << "EFA peer_map_ is at capacity on " << nicPath()
            << "; evicting oldest peer to make room: " << evicted_path << " -> "
            << peer_nic_path << " (peer_count=" << post_evict_size
            << ", peer_map_max=" << peer_map_max_
            << ").  Raise MC_MAX_EP_PER_CTX or leave it unset if peers are"
            << " still active; on EFA (SRD) an AV entry is a few bytes and"
            << " there is no hard QP limit to protect."
            << " [logged 1 of every " << kEvictLogEveryN << "]";
        // disconnectIfIdle: put the entry back if the victim turns out to be
        // busy (see disconnectIfIdle / av_slots_).
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
    if (ep) ep->disconnect();  // disconnect; AV remove may be deferred
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

    // Hold post_lock_ across the insert AND the reference claim below, which is
    // what makes "insert" atomic with respect to removeSlotNow().  Without it
    // the pair races: fi_av_insert can return a live index and, before the
    // refcount.fetch_add lands, a concurrent last-holder release sees
    // refcount == 0 and fi_av_remove()s the slot out from under the new holder.
    // Lock order is post_lock_ -> av_ref_lock_ (via avSlot below), never the
    // reverse, so there is no cycle.
    while (post_lock_.test_and_set(std::memory_order_acquire)) {
    }
    int ret = fi_av_insert(av_, addr, 1, &out, 0, nullptr);
    if (ret != 1) {
        // Nothing was inserted, so there is no slot to protect; release before
        // the (relatively expensive) diagnostic formatting below rather than
        // holding the submit path off while we build a log line.
        post_lock_.clear(std::memory_order_release);
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
    // live again, so retiring it would strand the new holder.  Still under
    // post_lock_, so a remover cannot observe the intermediate state where the
    // index is live but unreferenced.
    auto* slot = avSlot(out);
    slot->refcount.fetch_add(1, std::memory_order_acq_rel);
    slot->remove_pending.store(false, std::memory_order_release);
    post_lock_.clear(std::memory_order_release);
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
    // The checks above are advisory -- they can all pass and then be
    // invalidated before we take post_lock_.  removeSlotNow() re-validates
    // under the lock and re-arms remove_pending if it has to back out, so
    // losing this race costs one retry, not a lost completion or a leaked slot.
    removeSlotNow(slot, fi_addr);
}

void EfaContext::removeSlotNow(AvSlotState* slot, fi_addr_t fi_addr) {
    // Serialize the removal against the post burst on the same lock the
    // submitters use, so a removal can never land between a submitter's check
    // and its fi_write (see av_slots_).
    while (post_lock_.test_and_set(std::memory_order_acquire)) {
    }
    // Re-validate under the lock; this is the authoritative gate, and callers
    // may have decided to remove based on counters read before they acquired
    // it.  insertPeerAddrBytes() claims its reference while holding this same
    // lock, so whatever we observe here is a settled state, not a torn one.
    if (slot) {
        if (slot->refcount.load(std::memory_order_acquire) != 0) {
            // A fresh handshake re-admitted this address between the caller's
            // check and now (expected: the provider returns the same index for
            // the same peer).  Removing would strand the new holder.  The
            // inserter already cleared remove_pending, so nothing to undo.
            post_lock_.clear(std::memory_order_release);
            return;
        }
        if (slot->inflight.load(std::memory_order_acquire) != 0) {
            // Operations were posted after the caller looked.  Re-arm the
            // deferred path so the CQ poller retires the slot once the last
            // one lands, rather than leaking it here.
            slot->remove_pending.store(true, std::memory_order_release);
            post_lock_.clear(std::memory_order_release);
            return;
        }
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

    // Drop our reference; only the last holder may retire the slot
    // (see av_slots_).
    if (slot->refcount.fetch_sub(1, std::memory_order_acq_rel) > 1) return;

    // Last holder, but the slot-wide inflight count -- not this endpoint's --
    // decides whether it can go now: a sibling's operations may still be
    // posted.  Defer to the CQ poller in that case.
    if (slot->inflight.load(std::memory_order_acquire) > 0) {
        slot->remove_pending.store(true, std::memory_order_release);
        // Re-check: the last completion may have landed between the load above
        // and the store, in which case the poller has already walked away and
        // nobody would ever retire the slot.
        retireSlotIfPending(slot, fi_addr);
        return;
    }

    // Both counters read zero, but neither read was atomic with the removal:
    // a concurrent handshake may re-insert this address (and re-claim the slot)
    // before we get the lock.  removeSlotNow() re-checks under post_lock_ and
    // declines in that case, so the new holder is never stranded.
    removeSlotNow(slot, fi_addr);
}

bool EfaContext::tryLoopbackCopy(Transport::Slice* slice) {
    // Only intra-process self-transfers qualify.  We compare the SERVER name
    // (host:rpc_port), NOT the full nic path: local_server_name embeds this
    // process's unique RPC port, so a server-name match guarantees the peer
    // is *this very process* — meaning both source_addr and dest_addr are
    // pointers we can dereference directly.  We deliberately ignore the
    // device suffix: with multiple NICs the source slice is routed by its
    // source buffer's device while peer_nic_path names the destination
    // buffer's device, so the two device names often differ even for a pure
    // self-loopback.  Same-host *cross-process* peers carry a different RPC
    // port and never match, so we never memcpy across address spaces.
    if (getServerNameFromNicPath(slice->peer_nic_path) !=
        engine_.local_server_name()) {
        return false;
    }

    // Direction depends on opcode, mirroring fi_read / fi_write below:
    //   WRITE: fi_write(buf=source_addr -> addr=dest_addr)  data src->dst
    //   READ : fi_read (buf=source_addr <- addr=dest_addr)  data dst->src
    // source_addr and rdma.dest_addr are two distinct local buffers here, so
    // the copy is NOT symmetric — we must honor the opcode's direction.
    void* local_buf = slice->source_addr;
    void* remote_buf = reinterpret_cast<void*>(slice->rdma.dest_addr);
    void* dst;
    void* src;
    if (slice->opcode == Transport::TransferRequest::READ) {
        dst = local_buf;   // read INTO local
        src = remote_buf;  // FROM remote (== local) buffer
    } else {
        dst = remote_buf;  // write INTO remote (== local) buffer
        src = local_buf;   // FROM local
    }
    size_t len = slice->length;
    // GPU-aware copy.  Guard on the SAME backends that register GPU memory
    // as FI_HMEM (see the FI_MR_HMEM hint and the registration path: only
    // USE_CUDA / USE_HIP tag MRs with a device iface).  Every other build —
    // including non-EFA GPU backends such as MUSA/MLU/MACA, which never run
    // on AWS EFA hardware — registers loopback buffers as host memory, so a
    // plain memcpy is both correct and the only portable option (those
    // backends do not expose the cuda* symbols).  *MemcpyDefault picks
    // H2H/H2D/D2H/D2D from the pointer attributes.
#if defined(USE_CUDA)
    auto rc = cudaMemcpy(dst, src, len, cudaMemcpyDefault);
    if (rc != cudaSuccess) {
        LOG(ERROR) << "EFA loopback cudaMemcpy failed: "
                   << cudaGetErrorString(rc) << " (dst=" << dst
                   << ", src=" << src << ", len=" << len << ")";
        slice->markFailed();
        return true;
    }
#elif defined(USE_HIP)
    auto rc = hipMemcpy(dst, src, len, hipMemcpyDefault);
    if (rc != hipSuccess) {
        LOG(ERROR) << "EFA loopback hipMemcpy failed: " << hipGetErrorString(rc)
                   << " (dst=" << dst << ", src=" << src << ", len=" << len
                   << ")";
        slice->markFailed();
        return true;
    }
#else
    memcpy(dst, src, len);
#endif
    slice->markSuccess();
    return true;
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
            // Self-loopback: satisfy locally, skip EFA entirely.
            if (tryLoopbackCopy(slice)) continue;
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

        // device_id comes from the peer-supplied topology, whose HCA list is
        // independent of the peer 'devices' array, and selectDevice() bounds it
        // against rkey only. decodeSegmentDesc() now rejects a descriptor whose
        // key count and device count disagree; bound the value used to index
        // devices[] locally as well.
        if (static_cast<size_t>(device_id) >=
            peer_segment_desc->devices.size()) {
            LOG(ERROR) << "Peer device index out of range for target "
                       << slice->target_id << ": device_id=" << device_id
                       << " devices=" << peer_segment_desc->devices.size();
            slice->markFailed();
            continue;
        }

        slice->rdma.dest_rkey =
            peer_segment_desc->buffers[buffer_id].rkey[device_id];

        std::string peer_nic_path = peer_segment_desc->nicPathServerName() +
                                    "@" +
                                    peer_segment_desc->devices[device_id].name;
        slice->peer_nic_path = peer_nic_path;
        // Self-loopback: satisfy locally, skip EFA entirely.
        if (tryLoopbackCopy(slice)) continue;
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
        // this is cheap (no fid_ep to destroy).  ...IfIdle so a peer another
        // submit thread is still posting on is left in place; see
        // deleteEndpointIfIdle / av_slots_.
        if (rc != 0 && !ep->connected()) {
            deleteEndpointIfIdle(peer_nic_path);
        }
    }

    return 0;
}

int64_t EfaContext::msSinceLastCompletion() const {
    uint64_t last = last_completion_ns_.load(std::memory_order_relaxed);
    if (last == 0) return -1;
    uint64_t now = monotonicNowNs();
    // The stamp is taken by another thread, so it can be marginally ahead of
    // our own read of the clock; report 0 rather than a negative age.
    return now > last ? static_cast<int64_t>((now - last) / 1000000) : 0;
}

std::string EfaContext::describeInflightBySlot(size_t top_n) const {
    std::vector<std::pair<fi_addr_t, int>> busy;
    int total = 0;
    size_t slot_count = 0;
    {
        RWSpinlock::ReadGuard guard(av_ref_lock_);
        slot_count = av_slots_.size();
        busy.reserve(slot_count);
        for (const auto& entry : av_slots_) {
            int inflight =
                entry.second->inflight.load(std::memory_order_relaxed);
            if (inflight <= 0) continue;
            total += inflight;
            busy.emplace_back(entry.first, inflight);
        }
    }
    std::partial_sort(
        busy.begin(), busy.begin() + std::min(top_n, busy.size()), busy.end(),
        [](const std::pair<fi_addr_t, int>& a,
           const std::pair<fi_addr_t, int>& b) { return a.second > b.second; });

    std::ostringstream os;
    os << "slots=" << slot_count << " busy=" << busy.size()
       << " sum_inflight=" << total << " top=[";
    for (size_t i = 0; i < busy.size() && i < top_n; ++i) {
        if (i) os << ", ";
        os << "fi_addr=" << busy[i].first << ":" << busy[i].second;
    }
    os << "]";
    return os.str();
}

int EfaContext::submitSlicesOnPeer(
    fi_addr_t peer_fi_addr, std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    // In-flight accounting lives on the AV slot, not the calling endpoint
    // (see av_slots_).
    AvSlotState* slot = avSlot(peer_fi_addr);
    // Batched submission against the shared endpoint.  Mirrors the previous
    // per-endpoint submit path but uses context-level wr_depth / post_lock.
    //
    // 1. Reserve N WR+CQ slots in bulk (single CAS each)
    // 2. Prepare MR descriptors and op contexts outside the lock
    // 3. Hold post_lock_ once for the entire batch of fi_write calls
    // Deadline for the WR/CQ credit wait below.  It is deliberately a wall
    // clock and not a yield budget: a yield costs a few hundred nanoseconds on
    // an idle core and microseconds on a loaded one, so a fixed yield count
    // gives up after wildly different amounts of real time depending only on
    // how many threads happen to be runnable.  Measured on a healthy 2-node
    // p6-b300 pair, the old budget of 100000 yields was worth ~22 ms at 64
    // submitter threads and ~460 ms at 192 -- and self-resolving stalls of up
    // to 21 ms occur under ordinary load, so the 64-thread case failed live
    // transfers that were about to make progress.
    const double drain_timeout_ms =
        static_cast<double>(globalConfig().efa_cq_drain_timeout_ms);
    // Not globalConfig().max_cqe: that is the requested value, while max_cqe_
    // is what this device's CQ was actually opened with (see construct()).
    const int cq_limit = static_cast<int>(max_cqe_);
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
            // retry_slices holds the previous wave's bounced slices here (they
            // are spliced back in just below), so either side can name the
            // peer.
            const Transport::Slice* probe =
                !retry_slices.empty()
                    ? retry_slices.front()
                    : (cursor < slice_list.size() ? slice_list[cursor]
                                                  : nullptr);
            LOG(WARNING) << "EFA submitSlicesOnPeer: " << kMaxStalledWaves
                         << " consecutive FI_EAGAIN waves posted nothing on "
                         << nicPath() << " -> "
                         << ((probe && !probe->peer_nic_path.empty())
                                 ? probe->peer_nic_path
                                 : "unknown")
                         << " (fi_addr=" << peer_fi_addr << "); wr_depth="
                         << wr_depth_.load(std::memory_order_relaxed) << "/"
                         << max_wr_depth_ << "; cq_outstanding="
                         << (cq_outstanding ? cq_outstanding->load(
                                                  std::memory_order_relaxed)
                                            : -1)
                         << "/" << cq_limit
                         << "; last_completion=" << msSinceLastCompletion()
                         << "ms ago"
                         << "; this_peer_inflight="
                         << slot->inflight.load(std::memory_order_relaxed)
                         << "; " << describeInflightBySlot(4)
                         << "; totals posted="
                         << total_posted_.load(std::memory_order_relaxed)
                         << " completed="
                         << total_completions_.load(std::memory_order_relaxed)
                         << " cq_errors="
                         << total_cq_errors_.load(std::memory_order_relaxed)
                         << " orphans="
                         << orphan_completions_.load(std::memory_order_relaxed)
                         << "; failing "
                         << (retry_slices.size() + slice_list.size() - cursor)
                         << " slice(s)";
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

        // Diagnostics for the starvation log below, armed lazily on the FIRST
        // backoff so that the common case -- credit available on the first
        // iteration -- pays nothing, not even a clock read.
        //
        // Two things the old log could not be reconstructed from.  First, the
        // wall-clock wait, which is now what decides the timeout as well; the
        // yield count is still reported because its ratio to the elapsed time
        // says how contended the box was.  Second, whether wr_depth_ moved at
        // all while we waited: a counter pinned at max for the entire wait
        // means completions are not coming back (stalled peer, starved poller,
        // or leaked credit -- see orphan_completions_), whereas one oscillating
        // just below max means we are simply saturated and the batch is
        // ordinary backpressure.  Those two want opposite investigations.
        std::chrono::steady_clock::time_point wait_t0;
        const char* starved_on = "none";
        int wr_first = -1, wr_min = 0, wr_max = 0, wr_moves = 0, wr_prev = -1;
        // Runs once per yield: a handful of register-resident int compares next
        // to a syscall.  Must be called before `backoff` is bumped, so that the
        // first call is the one that arms the clock.
        auto note_starved = [&](const char* which, int cur_wr) {
            starved_on = which;
            if (backoff == 0) {
                wait_t0 = std::chrono::steady_clock::now();
                wr_first = wr_min = wr_max = wr_prev = cur_wr;
                return;
            }
            if (cur_wr < wr_min) wr_min = cur_wr;
            if (cur_wr > wr_max) wr_max = cur_wr;
            if (cur_wr != wr_prev) {
                ++wr_moves;
                wr_prev = cur_wr;
            }
        };
        // Counts the yield and reports whether the deadline has passed.  Must
        // be called after note_starved(), which arms wait_t0 on the first call.
        // The clock is read once per 1024 yields: at ~0.2 us per yield that
        // bounds the overshoot at well under a millisecond while keeping
        // clock_gettime off the hot path.
        auto drain_deadline_hit = [&]() -> bool {
            ++backoff;
            if (drain_timeout_ms <= 0.0) return false;
            if ((backoff & 1023) != 0) return false;
            return std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - wait_t0)
                       .count() > drain_timeout_ms;
        };

        while (batch_count == 0) {
            int cur_wr = wr_depth_.load(std::memory_order_relaxed);
            int wr_avail = max_wr_depth_ - cur_wr;
            if (wr_avail <= 0) {
                note_starved("wr_depth", cur_wr);
                if (drain_deadline_hit()) {
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
                    note_starved("cq_outstanding", cur_wr);
                    if (drain_deadline_hit()) {
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
            const double waited_ms =
                std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - wait_t0)
                    .count();
            const std::string& peer_path = slice_list[cursor]->peer_nic_path;
            LOG(WARNING)
                << "EFA submitSlicesOnPeer: timed out waiting for CQ drain on "
                << nicPath() << " -> "
                << (peer_path.empty() ? "unknown" : peer_path)
                << " (fi_addr=" << peer_fi_addr
                << "): starved_on=" << starved_on << ", waited=" << waited_ms
                << "ms over " << backoff << " yields (deadline "
                << drain_timeout_ms << "ms, MC_EFA_CQ_DRAIN_TIMEOUT_MS)"
                << "; wr_depth=" << wr_depth_.load(std::memory_order_relaxed)
                << "/" << max_wr_depth_ << " (at first backoff " << wr_first
                << ", observed min=" << wr_min << " max=" << wr_max
                << ", moved " << wr_moves << "x while waiting)"
                << "; cq_outstanding="
                << (cq_outstanding
                        ? cq_outstanding->load(std::memory_order_relaxed)
                        : -1)
                << "/" << cq_limit
                << "; last_completion=" << msSinceLastCompletion() << "ms ago"
                << "; this_peer_inflight="
                << slot->inflight.load(std::memory_order_relaxed) << "; "
                << describeInflightBySlot(4) << "; totals posted="
                << total_posted_.load(std::memory_order_relaxed)
                << " completed="
                << total_completions_.load(std::memory_order_relaxed)
                << " cq_errors="
                << total_cq_errors_.load(std::memory_order_relaxed)
                << " orphans="
                << orphan_completions_.load(std::memory_order_relaxed)
                << "; failing " << (slice_list.size() - cursor) << " slice(s)";
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
            // Tallied once for the whole batch after the post burst, not per
            // operation: one relaxed add per up-to-max_wr_depth_ slices.
            int posted = 0;
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
                    ++posted;
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
            if (posted)
                total_posted_.fetch_add(static_cast<uint64_t>(posted),
                                        std::memory_order_relaxed);
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
            } else {
                // The CQ reservation is returned below for every entry in
                // `ret`, but the wr_depth_ reservation is reachable only
                // through op_ctx, so this entry leaks one WR credit.  See
                // orphan_completions_.
                orphan_completions_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        for (auto& entry : wr_depth_set) {
            entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);
        }
        for (auto& s : touched_slots) retireSlotIfPending(s.first, s.second);
        cq_list_[cq_index]->outstanding.fetch_sub(static_cast<int>(ret),
                                                  std::memory_order_acq_rel);
        // Liveness stamp for the starvation logs: once per non-empty batch of
        // up to 64 completions, not once per completion.
        total_completions_.fetch_add(static_cast<uint64_t>(ret),
                                     std::memory_order_relaxed);
        last_completion_ns_.store(monotonicNowNs(), std::memory_order_relaxed);
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
            } else {
                // Same asymmetry as the success path: err_count returns the CQ
                // reservation, but the WR reservation is unreachable without
                // op_ctx.  See orphan_completions_.
                orphan_completions_.fetch_add(1, std::memory_order_relaxed);
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
            // An error completion is still the poller making progress and
            // releasing credit, so it counts for liveness.
            total_cq_errors_.fetch_add(static_cast<uint64_t>(err_count),
                                       std::memory_order_relaxed);
            last_completion_ns_.store(monotonicNowNs(),
                                      std::memory_order_relaxed);
        }

        // Drop stale peer handles outside the drain loop:
        // deleteEndpointIfIdle() takes peer_map_lock_ and may call
        // fi_av_remove().  Duplicates within one drain are harmless -- it is a
        // no-op once the entry is gone.  A busy peer keeps its entry and
        // nothing schedules a retry; teardown happens whenever a later call
        // path reaches this peer again.
        std::sort(stale_peers.begin(), stale_peers.end());
        stale_peers.erase(std::unique(stale_peers.begin(), stale_peers.end()),
                          stale_peers.end());
        for (const auto& peer : stale_peers) {
            // Same constraint as eviction (see av_slots_).  Deferring
            // converges here: every op aimed at a dead peer fails the same
            // way, so a later drain sees the counter at zero and tears down.
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
