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

#include <atomic>
#include <cassert>
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

// EfaContext implementation

EfaContext::EfaContext(EfaTransport& engine, const std::string& device_name)
    : engine_(engine),
      device_name_(device_name),
      fi_info_(nullptr),
      hints_(nullptr),
      fabric_(nullptr),
      domain_(nullptr),
      av_(nullptr),
      active_(true),
      shared_ep_(nullptr),
      wr_depth_(0),
      max_wr_depth_(0),
      post_lock_(ATOMIC_FLAG_INIT) {}

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

    // Create address vector.  Capacity sized for the largest peer count we
    // expect to support in a single process.  AV entries are cheap (no QP
    // cost), so we over-provision.
    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    av_attr.count = max_endpoints;

    ret = fi_av_open(domain_, &av_attr, &av_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_av_open failed: " << fi_strerror(-ret);
        fi_close(&domain_->fid);
        fi_close(&fabric_->fid);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
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
    return 0;
}

int EfaContext::deconstruct() {
    // Teardown order matters for the EFA provider: the shared endpoint must
    // be closed before the AV it is bound to.  We also cannot call
    // fi_av_remove() after fi_close(ep) — the provider faults — so we just
    // clear the peer map (dropping shared_ptrs) and let fi_av_close() below
    // invalidate every AV slot in one shot.
    if (shared_ep_) {
        fi_close(&shared_ep_->fid);
        shared_ep_ = nullptr;
    }

    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        for (auto& entry : peer_map_) {
            if (entry.second) entry.second->markDetachedForTeardown();
        }
        peer_map_.clear();
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
    // Key the peer map by the full "host:port@nic" path, not the
    // port-stripped form.  Under sglang DP>1 every DP worker on a peer
    // host is a distinct process with its own Mooncake TransferEngine
    // and its own P2PHANDSHAKE RPC port.  They share the same host+NIC
    // but map to different EFA QPNs / memory regions.  If we normalize
    // the port away, every DP worker on that host collapses onto the
    // same EfaEndPoint slot: each arriving handshake looks like a
    // "peer reconnected with new address" to the previous holder,
    // triggering fi_av_remove + fi_av_insert (and an AH warm-up) on
    // every KV transfer.  At high DP this devolves into permanent
    // thrashing and eventually "Remote MR invalid" when an in-flight
    // fi_write runs against a stale AV slot.
    //
    // The port is stable for the lifetime of a sglang worker process
    // (Mooncake only calls initialize() once), so keying by full path
    // costs nothing in steady state.  A genuine peer restart (process
    // re-launch → new RPC port) creates a new map entry and leaks the
    // old EfaEndPoint — that's at most a few bytes per ex-worker and
    // far cheaper than the churn the old normalization caused.
    const std::string& key = peer_nic_path;

    {
        RWSpinlock::ReadGuard guard(peer_map_lock_);
        auto it = peer_map_.find(key);
        if (it != peer_map_.end()) {
            return it->second;
        }
    }

    auto new_ep = std::make_shared<EfaEndPoint>(*this);
    new_ep->setPeerNicPath(peer_nic_path);

    RWSpinlock::WriteGuard guard(peer_map_lock_);
    auto it = peer_map_.find(key);
    if (it != peer_map_.end()) {
        return it->second;
    }
    peer_map_[key] = new_ep;
    return new_ep;
}

std::shared_ptr<EfaEndPoint> EfaContext::peekEndpoint(
    const std::string& peer_nic_path) {
    RWSpinlock::ReadGuard guard(peer_map_lock_);
    auto it = peer_map_.find(peer_nic_path);
    if (it == peer_map_.end()) return nullptr;
    return it->second;
}

int EfaContext::deleteEndpoint(const std::string& peer_nic_path) {
    std::shared_ptr<EfaEndPoint> ep;
    {
        RWSpinlock::WriteGuard guard(peer_map_lock_);
        auto it = peer_map_.find(peer_nic_path);
        if (it == peer_map_.end()) return 0;
        ep = it->second;
        peer_map_.erase(it);
    }
    if (ep) ep->disconnect();  // runs fi_av_remove
    return 0;
}

int EfaContext::disconnectAllEndpoints() {
    RWSpinlock::WriteGuard guard(peer_map_lock_);
    for (auto& entry : peer_map_) {
        if (entry.second) entry.second->disconnect();
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
    int ret = fi_av_insert(av_, addr, 1, &out, 0, nullptr);
    if (ret != 1) {
        LOG(ERROR) << "fi_av_insert failed: " << fi_strerror(-ret);
        return ERR_ENDPOINT;
    }
    return 0;
}

void EfaContext::removePeerAddr(fi_addr_t fi_addr) {
    if (fi_addr == FI_ADDR_UNSPEC) return;
    int ret = fi_av_remove(av_, &fi_addr, 1, 0);
    if (ret) {
        LOG(WARNING) << "fi_av_remove failed: " << fi_strerror(-ret);
    }
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
        if (rc != 0 && !ep->connected()) {
            deleteEndpoint(peer_nic_path);
        }
    }

    return 0;
}

int EfaContext::submitSlicesOnPeer(
    fi_addr_t peer_fi_addr, std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
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
    while (cursor < slice_list.size() || !retry_slices.empty()) {
        if (!retry_slices.empty()) {
            // Splice retry slices back in at the current cursor so the next
            // pass picks them up.  O(retry_slices.size()) per retry wave,
            // which is bounded by valid_count of the last batch.
            slice_list.insert(slice_list.begin() + cursor, retry_slices.begin(),
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
                } else if (ret == -FI_EAGAIN) {
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
        }

        cursor += batch_count;
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
        for (ssize_t i = 0; i < ret; i++) {
            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(entries[i].op_context);
            if (op_ctx && op_ctx->slice) {
                op_ctx->slice->markSuccess();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                delete op_ctx;
            }
        }
        for (auto& entry : wr_depth_set) {
            entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);
        }
        cq_list_[cq_index]->outstanding.fetch_sub(static_cast<int>(ret),
                                                  std::memory_order_acq_rel);
        return static_cast<int>(ret);
    } else if (ret == -FI_EAGAIN) {
        return 0;
    } else if (ret < 0) {
        int err_count = 0;
        struct fi_cq_err_entry err_entry;
        std::unordered_map<std::atomic<int>*, int> wr_depth_set;

        while ((ret = fi_cq_readerr(cq, &err_entry, 0)) > 0) {
            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(err_entry.op_context);
            if (op_ctx && op_ctx->slice) {
                LOG(ERROR) << "EFA CQ error: "
                           << fi_cq_strerror(cq, err_entry.prov_errno,
                                             err_entry.err_data, nullptr, 0)
                           << " for slice at " << op_ctx->slice->source_addr;
                op_ctx->slice->markFailed();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                delete op_ctx;
            }
            err_count++;
        }

        for (auto& entry : wr_depth_set) {
            entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);
        }
        if (err_count > 0) {
            cq_list_[cq_index]->outstanding.fetch_sub(
                err_count, std::memory_order_acq_rel);
        }
        return err_count;
    }

    return 0;
}

}  // namespace mooncake
