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

#include "transport/efa_transport/efa_endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <thread>

#include "config.h"

namespace mooncake {

EfaEndPoint::EfaEndPoint(EfaContext& context)
    : context_(context),
      status_(INITIALIZING),
      ep_(nullptr),
      tx_cq_(nullptr),
      rx_cq_(nullptr),
      peer_fi_addr_(FI_ADDR_UNSPEC),
      local_addr_len_(0),
      wr_depth_(0),
      max_wr_depth_(0),
      cq_outstanding_(nullptr),
      active_(true),
      inactive_time_(0),
      last_used_time_(getCurrentTimeInNano()) {}

EfaEndPoint::~EfaEndPoint() {
    if (ep_) deconstruct();
}

int EfaEndPoint::construct(struct fid_cq* cq, volatile int* cq_outstanding,
                           size_t num_qp_list, size_t max_sge, size_t max_wr,
                           size_t max_inline) {
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "EFA Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    tx_cq_ = cq;
    rx_cq_ = cq;  // Use same CQ for TX and RX
    max_wr_depth_ = max_wr;
    cq_outstanding_ = cq_outstanding;

    // Create endpoint
    int ret = fi_endpoint(context_.domain(), context_.info(), &ep_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_endpoint failed: " << fi_strerror(-ret);
        return ERR_ENDPOINT;
    }

    // Bind endpoint to AV
    ret = fi_ep_bind(ep_, &context_.av()->fid, 0);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind (av) failed: " << fi_strerror(-ret);
        fi_close(&ep_->fid);
        ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Bind endpoint to TX CQ
    ret = fi_ep_bind(ep_, &tx_cq_->fid, FI_TRANSMIT);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind (tx_cq) failed: " << fi_strerror(-ret);
        fi_close(&ep_->fid);
        ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Bind endpoint to RX CQ
    ret = fi_ep_bind(ep_, &rx_cq_->fid, FI_RECV);
    if (ret) {
        LOG(ERROR) << "fi_ep_bind (rx_cq) failed: " << fi_strerror(-ret);
        fi_close(&ep_->fid);
        ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Enable endpoint
    ret = fi_enable(ep_);
    if (ret) {
        LOG(ERROR) << "fi_enable failed: " << fi_strerror(-ret);
        fi_close(&ep_->fid);
        ep_ = nullptr;
        return ERR_ENDPOINT;
    }

    // Get local endpoint address
    local_addr_len_ = 64;  // EFA addresses are typically 32 bytes
    local_addr_.resize(local_addr_len_);
    ret = fi_getname(&ep_->fid, local_addr_.data(), &local_addr_len_);
    if (ret) {
        LOG(ERROR) << "fi_getname failed: " << fi_strerror(-ret);
        fi_close(&ep_->fid);
        ep_ = nullptr;
        return ERR_ENDPOINT;
    }
    local_addr_.resize(local_addr_len_);

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int EfaEndPoint::deconstruct() {
    if (ep_) {
        fi_close(&ep_->fid);
        ep_ = nullptr;
    }
    return 0;
}

int EfaEndPoint::destroyQP() { return deconstruct(); }

void EfaEndPoint::setPeerNicPath(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (peer_nic_path_ == peer_nic_path) return;  // No change
    if (connected()) {
        LOG(INFO) << "Peer reconnected with new address, re-establishing: "
                  << peer_nic_path_ << " -> " << peer_nic_path;
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

std::string EfaEndPoint::getLocalAddr() const {
    std::ostringstream oss;
    for (size_t i = 0; i < local_addr_.size(); ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << (int)local_addr_[i];
    }
    return oss.str();
}

int EfaEndPoint::insertPeerAddr(const std::string& peer_addr) {
    // Convert hex string to binary address
    std::vector<uint8_t> addr_bin;
    addr_bin.reserve(peer_addr.size() / 2);

    for (size_t i = 0; i < peer_addr.size(); i += 2) {
        std::string byte_str = peer_addr.substr(i, 2);
        uint8_t byte = (uint8_t)strtol(byte_str.c_str(), nullptr, 16);
        addr_bin.push_back(byte);
    }

    // Insert into address vector
    int ret = fi_av_insert(context_.av(), addr_bin.data(), 1, &peer_fi_addr_, 0,
                           nullptr);
    if (ret != 1) {
        LOG(ERROR) << "fi_av_insert failed: " << fi_strerror(-ret);
        return ERR_ENDPOINT;
    }

    return 0;
}

int EfaEndPoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(INFO) << "EFA Connection has been established";
        return 0;
    }

    // Loopback mode
    if (context_.nicPath() == peer_nic_path_) {
        // For loopback, insert our own address
        int ret = insertPeerAddr(getLocalAddr());
        if (ret != 0) {
            return ret;
        }
        status_.store(CONNECTED, std::memory_order_release);
        LOG(INFO) << "EFA loopback connection established: " << toString();
        return 0;
    }

    // Exchange addresses via handshake
    TransferMetadata::HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    // Store our EFA endpoint address in efa_addr field (hex encoded)
    local_desc.efa_addr = getLocalAddr();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer EFA nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                             peer_desc);
    if (rc) return rc;

    if (peer_desc.efa_addr.empty()) {
        LOG(ERROR) << "Peer did not provide EFA address in handshake";
        return ERR_REJECT_HANDSHAKE;
    }

    // Insert peer's address into our AV
    rc = insertPeerAddr(peer_desc.efa_addr);
    if (rc != 0) {
        return rc;
    }

    status_.store(CONNECTED, std::memory_order_release);
    touchLastUsed();
    VLOG(1) << "EFA connection established: " << toString()
            << " peer_fi_addr=" << peer_fi_addr_;
    return 0;
}

int EfaEndPoint::setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                           HandShakeDesc& local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Re-establish EFA connection: " << toString();
        disconnectUnlocked();
    }

    if (peer_desc.peer_nic_path != context_.nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg = "EFA nic path inconsistency";
        LOG(ERROR) << "Invalid argument: peer EFA nic path inconsistency"
                   << " peer_nic_path=" << peer_desc.peer_nic_path
                   << " context_.nicPath()=" << context_.nicPath()
                   << " local_nic_path=" << peer_desc.local_nic_path
                   << " peer_nic_path_=" << peer_nic_path_;
        return ERR_REJECT_HANDSHAKE;
    }

    // Insert peer's address from handshake
    if (peer_desc.efa_addr.empty()) {
        local_desc.reply_msg = "No EFA address provided";
        LOG(ERROR) << "Peer did not provide EFA address";
        return ERR_REJECT_HANDSHAKE;
    }

    int ret = insertPeerAddr(peer_desc.efa_addr);
    if (ret != 0) {
        local_desc.reply_msg = "Failed to insert peer address";
        return ret;
    }

    // Provide our address to peer (using efa_addr field, not reply_msg)
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.efa_addr = getLocalAddr();
    // reply_msg should be empty on success

    status_.store(CONNECTED, std::memory_order_release);
    touchLastUsed();
    VLOG(1) << "EFA connection established (passive): " << toString();
    return 0;
}

void EfaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    disconnectUnlocked();
}

void EfaEndPoint::disconnectUnlocked() {
    // Remove peer address from AV to free the slot for reuse.
    // Without this, AV entries leak on every initiator disconnect,
    // eventually exhausting the AV and causing fi_av_insert/fi_endpoint
    // failures ("No data available") after 3-5 initiator runs.
    if (peer_fi_addr_ != FI_ADDR_UNSPEC) {
        int ret = fi_av_remove(context_.av(), &peer_fi_addr_, 1, 0);
        if (ret) {
            LOG(WARNING) << "fi_av_remove failed: " << fi_strerror(-ret);
        }
    }
    peer_fi_addr_ = FI_ADDR_UNSPEC;
    status_.store(UNCONNECTED, std::memory_order_release);
}

const std::string EfaEndPoint::toString() const {
    return "EfaEndPoint[" + context_.nicPath() + " <-> " + peer_nic_path_ + "]";
}

bool EfaEndPoint::hasOutstandingSlice() const { return wr_depth_ > 0; }

int EfaEndPoint::doSetupConnection(const std::string& peer_addr,
                                   std::string* reply_msg) {
    int ret = insertPeerAddr(peer_addr);
    if (ret != 0) {
        if (reply_msg) *reply_msg = "Failed to insert peer address into AV";
        return ret;
    }

    status_.store(CONNECTED, std::memory_order_release);
    return 0;
}

int EfaEndPoint::submitPostSend(
    std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    if (!connected()) {
        // Try to establish connection first
        int ret = setupConnectionsByActive();
        if (ret != 0) {
            // Move all slices to failed list
            for (auto* slice : slice_list) {
                failed_slice_list.push_back(slice);
            }
            slice_list.clear();
            return ret;
        }
    }

    // Batched submission — mirrors RDMA's ibv_post_send approach:
    // 1. Reserve N WR+CQ slots in bulk (single CAS each)
    // 2. Prepare MR descriptors and op contexts outside the lock
    // 3. Hold post_lock once for the entire batch of fi_write calls
    // This reduces per-slice overhead from 2 CAS + 1 spinlock to amortized
    // 2/N CAS + 1/N spinlock per slice.
    const int kMaxBackoffYields = 100000;
    const int cq_limit = static_cast<int>(globalConfig().max_cqe);

    struct BatchEntry {
        Transport::Slice* slice;
        void* local_desc;
        EfaOpContext* op_ctx;
    };

    while (!slice_list.empty()) {
        // --- Step 1: Atomically reserve a batch of WR+CQ slots ---
        int batch_count = 0;
        int backoff = 0;
        bool timed_out = false;
        while (batch_count == 0) {
            int cur_wr = wr_depth_;
            int wr_avail = max_wr_depth_ - cur_wr;
            if (wr_avail <= 0) {
                if (++backoff > kMaxBackoffYields) {
                    timed_out = true;
                    break;
                }
                std::this_thread::yield();
                continue;
            }
            int want = std::min(wr_avail, (int)slice_list.size());
            if (cq_outstanding_) {
                int cur_cq = *cq_outstanding_;
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
                if (!__sync_bool_compare_and_swap(&wr_depth_, cur_wr,
                                                  cur_wr + want)) {
                    continue;
                }
                cur_cq = *cq_outstanding_;
                cq_avail = cq_limit - cur_cq;
                if (cq_avail < want) {
                    __sync_fetch_and_sub(&wr_depth_, want);
                    continue;
                }
                if (!__sync_bool_compare_and_swap(cq_outstanding_, cur_cq,
                                                  cur_cq + want)) {
                    __sync_fetch_and_sub(&wr_depth_, want);
                    continue;
                }
            } else {
                if (!__sync_bool_compare_and_swap(&wr_depth_, cur_wr,
                                                  cur_wr + want)) {
                    continue;
                }
            }
            batch_count = want;
        }

        if (timed_out) {
            LOG(WARNING) << "EFA submitPostSend: timed out waiting for CQ drain"
                         << " (wr_depth=" << wr_depth_
                         << ", max=" << max_wr_depth_ << ", cq_outstanding="
                         << (cq_outstanding_ ? *cq_outstanding_ : -1)
                         << ", max_cqe=" << cq_limit << ")";
            for (auto* slice : slice_list) {
                failed_slice_list.push_back(slice);
            }
            slice_list.clear();
            return 0;
        }

        // --- Step 2: Prepare MR descriptors outside the lock ---
        std::vector<BatchEntry> batch(batch_count);
        int valid_count = 0;

        for (int i = 0; i < batch_count; i++) {
            Transport::Slice* slice = slice_list[i];
            void* local_desc = context_.mrDesc(slice->source_addr);
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
            __sync_fetch_and_sub(&wr_depth_, mr_failures);
            if (cq_outstanding_)
                __sync_fetch_and_sub(cq_outstanding_, mr_failures);
        }

        // --- Step 3: Post entire batch under a single lock hold ---
        std::vector<Transport::Slice*> retry_slices;
        if (valid_count > 0) {
            while (post_lock_.test_and_set(std::memory_order_acquire)) {
            }
            for (int i = 0; i < valid_count; i++) {
                auto& entry = batch[i];
                ssize_t ret;
                if (entry.slice->opcode == Transport::TransferRequest::READ) {
                    ret = fi_read(ep_, (void*)entry.slice->source_addr,
                                  entry.slice->length, entry.local_desc,
                                  peer_fi_addr_, entry.slice->rdma.dest_addr,
                                  entry.slice->rdma.dest_rkey,
                                  &entry.op_ctx->fi_ctx);
                } else {
                    ret = fi_write(ep_, (void*)entry.slice->source_addr,
                                   entry.slice->length, entry.local_desc,
                                   peer_fi_addr_, entry.slice->rdma.dest_addr,
                                   entry.slice->rdma.dest_rkey,
                                   &entry.op_ctx->fi_ctx);
                }
                if (ret == 0) {
                    entry.slice->status = Transport::Slice::PENDING;
                } else if (ret == -FI_EAGAIN) {
                    delete entry.op_ctx;
                    int not_posted = valid_count - i;
                    __sync_fetch_and_sub(&wr_depth_, not_posted);
                    if (cq_outstanding_)
                        __sync_fetch_and_sub(cq_outstanding_, not_posted);
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
                    __sync_fetch_and_sub(&wr_depth_, 1);
                    if (cq_outstanding_)
                        __sync_fetch_and_sub(cq_outstanding_, 1);
                    failed_slice_list.push_back(entry.slice);
                }
            }
            post_lock_.clear(std::memory_order_release);
            touchLastUsed();
        }

        // Remove consumed batch from slice_list, prepend any retry slices
        slice_list.erase(slice_list.begin(), slice_list.begin() + batch_count);
        if (!retry_slices.empty()) {
            slice_list.insert(slice_list.begin(), retry_slices.begin(),
                              retry_slices.end());
            std::this_thread::yield();
        }
    }

    return 0;
}

}  // namespace mooncake
