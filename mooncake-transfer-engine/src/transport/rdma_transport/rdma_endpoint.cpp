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

#include "transport/rdma_transport/rdma_endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <chrono>
#include <thread>
#include <sstream>

#ifdef USE_MLX5DV
#include <infiniband/mlx5dv.h>
#endif

#include "common.h"
#include "config.h"
#include "transport/rdma_transport/rdma_gid_probe.h"

namespace mooncake {
constexpr uint8_t kMaxHopLimit = 16;
constexpr uint8_t kTimeout = 14;
constexpr uint8_t kRetryCount = 7;

static GidSelectionSnapshot fillLocalHandshakeDesc(
    RdmaContext &context, const std::string &peer_nic,
    const std::vector<uint32_t> &qp_num,
    RdmaEndPoint::HandShakeDesc &local_desc) {
    auto gid_selection = context.gidSelection();
    local_desc.local_nic_path = context.nicPath();
    local_desc.local_lid = context.lid();
    local_desc.local_gid = gid_selection.gid;
    local_desc.peer_nic_path = peer_nic;
    local_desc.qp_num = qp_num;
    local_desc.reply_msg.clear();
    return gid_selection;
}

static void rememberAutoGidSelection(
    std::vector<AutoGidSelectionIdentity> &attempted_selections,
    const GidSelectionSnapshot &selection) {
    auto already_attempted =
        std::any_of(attempted_selections.begin(), attempted_selections.end(),
                    [&](const AutoGidSelectionIdentity &attempted) {
                        return attempted.gid_index == selection.gid_index &&
                               attempted.gid == selection.gid;
                    });
    if (!already_attempted) {
        attempted_selections.push_back({selection.gid_index, selection.gid});
    }
}

RdmaEndPoint::RdmaEndPoint(RdmaContext &context)
    : context_(context),
      status_(INITIALIZING),
      wr_depth_list_(nullptr),
      active_(true),
      cq_outstanding_(nullptr) {}

RdmaEndPoint::~RdmaEndPoint() {
    if (!qp_list_.empty()) {
        // In normal flow, beginDestroy()+finishDestroy() should have been
        // called already via endpoint_store. This is a fallback for abnormal
        // shutdown (e.g., process exit).
        RWSpinlock::WriteGuard guard(lock_);
        deconstructLocked();
    }
}

void RdmaEndPoint::addPendingTokens(
    const std::vector<CompletionToken *> &tokens) {
    std::lock_guard<std::mutex> guard(pending_slices_mutex_);
    pending_tokens_.reserve(pending_tokens_.size() + tokens.size());
    for (auto *token : tokens) pending_tokens_.insert(token);
}

void RdmaEndPoint::removePendingTokens(
    const std::vector<CompletionToken *> &tokens, size_t start, size_t count) {
    std::lock_guard<std::mutex> guard(pending_slices_mutex_);
    for (size_t i = 0; i < count; ++i) {
        pending_tokens_.erase(tokens[start + i]);
    }
}

std::vector<uint8_t> RdmaEndPoint::claimPendingSlices(
    const std::vector<CompletionToken *> &tokens) {
    std::vector<uint8_t> claimed(tokens.size(), 0);
    std::lock_guard<std::mutex> guard(pending_slices_mutex_);
    for (size_t i = 0; i < tokens.size(); ++i) {
        auto *token = tokens[i];
        if (!token) continue;
        auto iter = pending_tokens_.find(token);
        if (iter == pending_tokens_.end()) continue;
        pending_tokens_.erase(iter);
        claimed[i] = 1;
    }
    return claimed;
}

std::vector<RdmaEndPoint::CompletionToken *>
RdmaEndPoint::claimAllPendingTokens() {
    std::lock_guard<std::mutex> guard(pending_slices_mutex_);
    std::vector<CompletionToken *> pending_tokens;
    pending_tokens.reserve(pending_tokens_.size());
    for (auto *token : pending_tokens_) pending_tokens.push_back(token);
    pending_tokens_.clear();
    return pending_tokens;
}

void RdmaEndPoint::completePendingSlicesAsFailed(
    const std::vector<CompletionToken *> &pending_tokens) {
    for (auto *token : pending_tokens) {
        if (!token) continue;
        auto *slice = token->slice.load(std::memory_order_acquire);
        if (!slice) continue;
        auto *qp_depth = token->qp_depth.load(std::memory_order_acquire);
        if (qp_depth) {
            __sync_fetch_and_sub(qp_depth, 1);
        }
        __sync_fetch_and_sub(cq_outstanding_, 1);
        slice->markFailed();
        // A CQE may still arrive after the destruction fallback has already
        // completed this slice. Keep the token alive for that late CQE, but
        // detach it from objects whose lifetime is controlled elsewhere.
        slice->rdma.endpoint = nullptr;
        slice->rdma.qp_depth = nullptr;
        token->endpoint.store(nullptr, std::memory_order_release);
        token->slice.store(nullptr, std::memory_order_release);
        token->qp_depth.store(nullptr, std::memory_order_release);
        token->fallback_completed.store(true, std::memory_order_release);
    }
}

uint64_t RdmaEndPoint::beginHandshakeAttempt() {
    return handshake_version_.fetch_add(1, std::memory_order_relaxed) + 1;
}

bool RdmaEndPoint::verifyHandshakeVersion(uint64_t response_version) const {
    return response_version == 0 ||
           handshake_version_.load(std::memory_order_relaxed) ==
               response_version;
}

bool RdmaEndPoint::validateHandshakeTimestamp(uint64_t timestamp) const {
    if (timestamp == 0) return true;
    // Wall-clock timestamps come from a remote host and clocks may drift.
    // Treat them as a replay/staleness hint only; never use them for strict
    // ordering between hosts.
    uint64_t now = getCurrentTimeInNano();
    if (timestamp > now + kHandshakeStalenessNs) return false;
    if (now > timestamp && now - timestamp > kHandshakeTimeoutNs) return false;
    return true;
}

void RdmaEndPoint::recordSuccessfulHandshake(
    const std::vector<uint32_t> &peer_qp_num, uint64_t version) {
    established_peer_.qp_num = peer_qp_num.empty() ? 0 : peer_qp_num[0];
    established_peer_.handshake_version = version;
    established_peer_.timestamp = getCurrentTimeInNano();
}

int RdmaEndPoint::construct(ibv_cq *cq, size_t num_qp_list,
                            size_t max_sge_per_wr, size_t max_wr_depth,
                            size_t max_inline_bytes) {
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    qp_list_.resize(num_qp_list);
    cq_outstanding_ = (volatile int *)cq->cq_context;

    max_wr_depth_ = (int)max_wr_depth;
    max_sge_per_wr_ = max_sge_per_wr;
    max_inline_bytes_ = max_inline_bytes;

    wr_depth_list_ = new volatile int[num_qp_list]();
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }
    for (size_t i = 0; i < num_qp_list; ++i) {
        wr_depth_list_[i] = 0;
        ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_RC;
        attr.qp_context = this;
        attr.cap.max_send_wr = attr.cap.max_recv_wr = max_wr_depth;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = max_sge_per_wr;
        attr.cap.max_inline_data = max_inline_bytes;
        qp_list_[i] = ibv_create_qp(context_.pd(), &attr);
        if (!qp_list_[i]) {
            PLOG(ERROR) << "Failed to create QP";
            for (size_t j = 0; j < i; ++j) {
                if (qp_list_[j]) {
                    ibv_destroy_qp(qp_list_[j]);
                    qp_list_[j] = nullptr;
                }
            }
            qp_list_.clear();
            delete[] wr_depth_list_;
            wr_depth_list_ = nullptr;
            return ERR_ENDPOINT;
        }
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int RdmaEndPoint::deconstruct() {
    RWSpinlock::WriteGuard guard(lock_);
    return deconstructLocked();
}

int RdmaEndPoint::deconstructLocked() {
    auto pending_tokens = claimAllPendingTokens();
    if (!pending_tokens.empty()) {
        LOG(WARNING) << "Completing " << pending_tokens.size()
                     << " pending RDMA slices as failed before endpoint "
                        "deconstruction";
        completePendingSlicesAsFailed(pending_tokens);
    }

    // Adjust cq_outstanding_ before destroying QPs, so the counter is
    // always corrected even if ibv_destroy_qp fails and we return early.
    bool displayed = false;
    if (wr_depth_list_) {
        for (size_t i = 0; i < qp_list_.size(); ++i) {
            if (wr_depth_list_[i] != 0) {
                if (!displayed) {
                    LOG(WARNING)
                        << "Outstanding work requests found, CQ will not "
                           "be generated";
                    displayed = true;
                }
                __sync_fetch_and_sub(cq_outstanding_, wr_depth_list_[i]);
                wr_depth_list_[i] = 0;
            }
        }
    }

    int result = 0;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (!qp_list_[i]) continue;  // already destroyed in a previous call
        if (ibv_destroy_qp(qp_list_[i])) {
            PLOG(ERROR) << "Failed to destroy QP[" << i << "]";
            result = ERR_ENDPOINT;
        } else {
            qp_list_[i] = nullptr;
        }
    }

    if (result) return result;

    qp_list_.clear();
    peer_qp_num_list_.clear();
    {
        std::lock_guard<std::mutex> guard(pending_slices_mutex_);
        pending_tokens_.clear();
    }
    delete[] wr_depth_list_;
    wr_depth_list_ = nullptr;
    return 0;
}

int RdmaEndPoint::destroyQP() { return deconstruct(); }

void RdmaEndPoint::beginDestroy() {
    RWSpinlock::WriteGuard guard(lock_);
    beginDestroyNoLock();
}

void RdmaEndPoint::beginDestroyNoLock() {
    auto current_status = status_.load(std::memory_order_relaxed);
    if (current_status == DESTROYING || current_status == DESTROYED) return;

    active_.store(false, std::memory_order_release);
    inactive_time_.store(getCurrentTimeInNano(), std::memory_order_release);
    status_.store(DESTROYING, std::memory_order_release);

    // Transition QPs to ERR state so hardware flushes all inflight WRs to CQ.
    // This allows performPollCq to drain them naturally.
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_ERR;
    bool any_qp_failed = false;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE)) {
            PLOG(WARNING) << "Failed to modify QP to ERR during beginDestroy";
            any_qp_failed = true;
        }
    }
    needs_manual_completion_ = any_qp_failed;
}

bool RdmaEndPoint::finishDestroy() {
    RWSpinlock::WriteGuard guard(lock_);
    auto current_status = status_.load(std::memory_order_relaxed);

    // Gate 1: already done.
    if (current_status == DESTROYED) return true;

    // Gate 2: non-two-phase path (status != DESTROYING). The endpoint
    // reached waiting_list_ without going through beginDestroy(). This is
    // the contract expected by EndpointStore::testOnlyInsertWaiting() and
    // serves as a safety net for any future non-two-phase path. Mirror the
    // pre-two-phase predicate (!hasOutstandingSlice == !active_): only
    // inactive endpoints are eligible for reclaim; active ones must stay.
    if (current_status != DESTROYING) {
        if (active_.load(std::memory_order_acquire)) return false;
        // Endpoints that never reached construct() own no RDMA resources
        // and have wr_depth_list_ uninitialized; deconstructLocked() would
        // delete[] a wild pointer. Drop them directly.
        if (qp_list_.empty()) {
            status_.store(DESTROYED, std::memory_order_relaxed);
            return true;
        }
        LOG(WARNING) << "finishDestroy called in unexpected state: "
                     << current_status
                     << ", forcing destruction to avoid waiting_list_ leak";
        // Fall through to the unified destroy path.
    } else {
        // Gate 3: two-phase path. Wait for inflight WRs to drain via CQ
        // polling. If ibv_modify_qp-to-ERR failed in beginDestroy, WRs may
        // never be flushed; enforce a timeout to avoid leaking forever.
        bool has_outstanding = false;
        for (size_t i = 0; i < qp_list_.size(); ++i) {
            if (wr_depth_list_[i] != 0) {
                has_outstanding = true;
                break;
            }
        }
        if (has_outstanding) {
            double elapsed = (getCurrentTimeInNano() -
                              inactive_time_.load(std::memory_order_acquire)) /
                             1e9;
            if (!needs_manual_completion_ &&
                elapsed < kFinishDestroyTimeoutSec) {
                return false;
            }

            auto pending_tokens = claimAllPendingTokens();
            if (!pending_tokens.empty()) {
                LOG(WARNING)
                    << "Completing " << pending_tokens.size()
                    << " pending RDMA slices as failed during endpoint destroy"
                    << " (elapsed=" << elapsed
                    << "s, manual_required=" << needs_manual_completion_ << ")";
                completePendingSlicesAsFailed(pending_tokens);
            }
        }
    }

    // Unified destroy: tear down QPs (deconstructLocked handles
    // cq_outstanding_ adjustment internally) and bound retries to avoid
    // log flooding when ibv_destroy_qp fails permanently.
    int ret = deconstructLocked();
    if (ret) {
        finish_destroy_retries_++;
        LOG(ERROR) << "Failed to finish destroying endpoint (attempt "
                   << finish_destroy_retries_ << "/" << kFinishDestroyMaxRetries
                   << "): " << ret;
        if (finish_destroy_retries_ < kFinishDestroyMaxRetries) return false;
        LOG(ERROR) << "Giving up after " << finish_destroy_retries_
                   << " retries (possible resource leak)";
    }
    status_.store(DESTROYED, std::memory_order_relaxed);
    return true;
}

void RdmaEndPoint::setPeerNicPath(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(ERROR) << "Cannot change peer path on connected endpoint "
                      "(unidirectional lifecycle)";
        return;
    }
    peer_nic_path_ = peer_nic_path;
}

int RdmaEndPoint::setupConnectionsByActive() {
    HandShakeDesc local_desc, peer_desc;
    std::string peer_server_name, peer_nic_name;
    bool do_rpc = false;
    int auto_gid_retry_count = 0;
    std::vector<AutoGidSelectionIdentity> attempted_auto_gid_selections;

    {
        RWSpinlock::WriteGuard guard(lock_);
        if (connected()) {
            LOG(INFO) << "Connection has been established";
            return 0;
        }

        // loopback mode
        if (context_.nicPath() == peer_nic_path_) {
            return doSetupConnection(context_.gid(), context_.lid(), qpNum());
        }

        // Only proceed with RPC if we are the first to transition from
        // UNCONNECTED. This prevents duplicate concurrent handshake attempts
        // from the same endpoint.
        Status expected = UNCONNECTED;
        if (status_.compare_exchange_strong(expected, CONNECTING,
                                            std::memory_order_relaxed)) {
            do_rpc = true;

            peer_server_name = getServerNameFromNicPath(peer_nic_path_);
            peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
            if (peer_server_name.empty() || peer_nic_name.empty()) {
                LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
                disconnectUnlocked();
                return ERR_INVALID_ARGUMENT;
            }
        }
    }

    if (!do_rpc) {
        LOG(INFO) << "Another thread is already performing the endpoint "
                     "handshake, waiting for it to complete";
        uint64_t start_time = getCurrentTimeInNano();
        uint32_t spin_count = 0;
        uint32_t sleep_us = kWaitExistingHandshakeInitialSleepUs;
        while (status_.load(std::memory_order_acquire) == CONNECTING) {
            if (spin_count < kWaitExistingHandshakeSpinCount) {
                PAUSE();
            } else {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(sleep_us));
                uint32_t next = sleep_us * 2;
                sleep_us = next > kWaitExistingHandshakeMaxSleepUs
                               ? kWaitExistingHandshakeMaxSleepUs
                               : next;
            }
            ++spin_count;
            // Prevent infinite wait with a timeout
            if (getCurrentTimeInNano() - start_time >
                kWaitExistingHandshakeTimeoutNano) {
                // Timeout while waiting for another thread's handshake.
                // The QP state on this endpoint may have changed; therefore,
                // reset the connection so that subsequent callers can retry.
                RWSpinlock::WriteGuard write_guard(lock_);
                resetConnection("wait existing handshake timeout");
                return ERR_ENDPOINT;
            }
        }
        RWSpinlock::ReadGuard guard(lock_);
        return connected() ? 0 : ERR_ENDPOINT;
    }

    for (;;) {
        const uint64_t my_version = beginHandshakeAttempt();
        const uint64_t handshake_start_time = getCurrentTimeInNano();
        std::vector<uint32_t> local_qp_num;
        {
            RWSpinlock::ReadGuard guard(lock_);
            local_qp_num = qpNum();
        }
        auto local_gid_selection = fillLocalHandshakeDesc(
            context_, peer_nic_path_, local_qp_num, local_desc);
        rememberAutoGidSelection(attempted_auto_gid_selections,
                                 local_gid_selection);
        local_desc.handshake_version = my_version;
        local_desc.timestamp = handshake_start_time;
        local_desc.flags = 0;
        peer_desc = HandShakeDesc();

        // Perform the RPC without holding the lock to avoid deadlock and allow
        // "simultaneous open" handshake handling.
        int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                                 peer_desc);

        // We should check the RPC return code before comparing
        // `peer_qp_num_list_` with `peer_desc.qp_num`, since a failed RPC may
        // result in an invalid `peer_desc.qp_num`.
        //
        if (rc) {
            RWSpinlock::WriteGuard write_guard(lock_);
            if (connected()) {
                LOG(INFO) << "Active handshake RPC failed after passive "
                             "handshake already established the endpoint";
                return 0;
            }
            resetConnection("handshake RPC failure");
            return rc;
        }

        bool retry_with_new_gid = false;
        {
            // Re-acquire lock after RPC to finalize state transition
            RWSpinlock::WriteGuard guard(lock_);

            if (!verifyHandshakeVersion(peer_desc.handshake_version)) {
                LOG(WARNING)
                    << "Rejecting stale handshake response from "
                    << peer_nic_path_ << ", current_version="
                    << handshake_version_.load(std::memory_order_relaxed)
                    << ", response_version=" << peer_desc.handshake_version;
                resetConnection("stale active handshake response");
                return ERR_REJECT_HANDSHAKE;
            }

            uint64_t elapsed = getCurrentTimeInNano() - handshake_start_time;
            if (elapsed > kHandshakeTimeoutNs) {
                LOG(ERROR) << "Handshake response timed out after " << elapsed
                           << " ns for " << peer_nic_path_;
                resetConnection("active handshake timeout");
                return ERR_REJECT_HANDSHAKE;
            }

            if (!validateHandshakeTimestamp(peer_desc.timestamp)) {
                LOG(WARNING) << "Rejecting invalid or stale handshake response "
                             << "timestamp=" << peer_desc.timestamp
                             << ", request_timestamp=" << local_desc.timestamp;
                resetConnection("invalid active handshake timestamp");
                return ERR_REJECT_HANDSHAKE;
            }

            if (peer_desc.flags & HandShakeDesc::FLAG_PEER_RESTART_DETECTED) {
                LOG(WARNING)
                    << "Peer reported restart detection for " << peer_nic_path_;
                resetConnection("peer restart flag in handshake response");
                return ERR_REJECT_HANDSHAKE;
            }

            // Handle simultaneous open: if the peer initiates a connection
            // during our RPC and it is passively established in
            // setupConnectionsByPassive, simply reuse the existing endpoint.
            if (connected()) {
                if (peer_qp_num_list_ == peer_desc.qp_num) {
                    LOG(INFO)
                        << "Received same peer QP numbers, reusing connection.";
                    return 0;
                }

                // This mismatch scenario should be rare. It may occur when a
                // peer first sends us an Active RPC and establishes a
                // connection, then restarts, and eventually accepts and
                // responds to our Active RPC.
                LOG(WARNING) << "Peer QP list mismatch on connected endpoint, "
                                "re-establishing connection: "
                             << toString();

                int ret =
                    resetConnection("re-establishing connection (active)");
                if (ret) return ret;
            }

            if (!peer_desc.reply_msg.empty()) {
                LOG(ERROR) << "Rejected handshake request by peer "
                           << local_desc.peer_nic_path;
                disconnectUnlocked();
                return ERR_REJECT_HANDSHAKE;
            }

            if (peer_desc.local_nic_path != peer_nic_path_ ||
                peer_desc.peer_nic_path != local_desc.local_nic_path) {
                LOG(ERROR) << "Invalid argument: received packet mismatch, "
                              "local.local_nic_path: "
                           << local_desc.local_nic_path
                           << ", local.peer_nic_path: "
                           << local_desc.peer_nic_path
                           << ", peer.local_nic_path: "
                           << peer_desc.local_nic_path
                           << ", peer.peer_nic_path: "
                           << peer_desc.peer_nic_path;
                disconnectUnlocked();
                return ERR_REJECT_HANDSHAKE;
            }

            int ret = ERR_DEVICE_NOT_FOUND;
            std::string failure_message;
            SetupConnectionFailureInfo failure_info;
            if (!peer_desc.local_gid.empty()) {
                ret = doSetupConnection(peer_desc.local_gid,
                                        peer_desc.local_lid, peer_desc.qp_num,
                                        &failure_message, &failure_info);
            } else {
                auto segment_desc =
                    context_.engine().meta()->getSegmentDescByName(
                        peer_server_name);
                if (segment_desc) {
                    for (auto &nic : segment_desc->devices) {
                        if (nic.name == peer_nic_name) {
                            ret = doSetupConnection(
                                nic.gid, nic.lid, peer_desc.qp_num,
                                &failure_message, &failure_info);
                            break;
                        }
                    }
                }
            }

            if (ret == 0) {
                recordSuccessfulHandshake(peer_desc.qp_num,
                                          peer_desc.handshake_version);
                return 0;
            }

            if (shouldAttemptAutoGidHandshakeRetry(
                    context_.autoGidSelectionEnabled(), auto_gid_retry_count,
                    globalConfig().auto_gid_max_retries,
                    failure_info.stage == SetupConnectionFailureStage::kRtr,
                    failure_info.sys_errno)) {
                std::string previous_gid;
                std::string next_gid;
                bool reprobe_changed = context_.reprobeAutoGid(
                    local_gid_selection, attempted_auto_gid_selections,
                    &previous_gid, &next_gid);
                auto current_gid_selection = context_.gidSelection();
                auto retry_action = decideAutoGidRetryAction(
                    reprobe_changed, local_gid_selection.gid_index,
                    local_gid_selection.gid, current_gid_selection.gid_index,
                    current_gid_selection.gid);
                if (retry_action != AutoGidRetryAction::kDoNotRetry) {
                    int reset_ret = resetConnection(
                        retry_action ==
                                AutoGidRetryAction::kRetryWithReprobedGid
                            ? "retry after auto GID reprobe (active)"
                            : "retry with externally reprobed GID (active)");
                    if (reset_ret) return reset_ret;
                    status_.store(CONNECTING, std::memory_order_relaxed);
                    ++auto_gid_retry_count;
                    retry_with_new_gid = true;
                    LOG(WARNING)
                        << "Retry active handshake with updated local GID on "
                        << context_.deviceName() << ": "
                        << local_gid_selection.gid << " -> "
                        << current_gid_selection.gid << " (attempt "
                        << auto_gid_retry_count << "/"
                        << globalConfig().auto_gid_max_retries << ")";
                }
            }

            if (!retry_with_new_gid) {
                if (ret == ERR_DEVICE_NOT_FOUND) {
                    LOG(ERROR) << "Peer NIC " << peer_nic_name
                               << " not found in " << peer_server_name;
                    disconnectUnlocked();
                } else {
                    resetConnection("failed connection setup (active)");
                }
                return ret;
            }
        }
    }
}

int RdmaEndPoint::setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                            HandShakeDesc &local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    fillLocalHandshakeDesc(context_, peer_nic_path_, qpNum(), local_desc);
    local_desc.handshake_version = peer_desc.handshake_version;
    local_desc.timestamp = getCurrentTimeInNano();
    local_desc.flags = 0;

    if (!validateHandshakeTimestamp(peer_desc.timestamp)) {
        local_desc.reply_msg = "Invalid or stale handshake timestamp";
        LOG(WARNING) << local_desc.reply_msg << " from "
                     << peer_desc.local_nic_path
                     << ", timestamp=" << peer_desc.timestamp;
        return ERR_REJECT_HANDSHAKE;
    }

    if (connected()) {
        // If already connected with the same peer QP info, return success
        if (peer_qp_num_list_ == peer_desc.qp_num) {
            LOG(INFO) << "Received same peer QP numbers, reusing connection.";
            return 0;
        }
        // Different peer (e.g., peer restarted)
        LOG(WARNING) << "Re-establish connection: " << toString();
        local_desc.reply_msg =
            "Peer QP changed, destroying connected endpoint for replacement";
        local_desc.flags |= HandShakeDesc::FLAG_PEER_RESTART_DETECTED;
        beginDestroyNoLock();
        return ERR_REJECT_HANDSHAKE;
    }

    auto current_status = status_.load(std::memory_order_relaxed);
    if (current_status == CONNECTING) {
        bool we_win = context_.nicPath() < peer_desc.local_nic_path;
        if (we_win) {
            local_desc.reply_msg =
                "Simultaneous handshake rejected by deterministic ordering";
            LOG(INFO) << local_desc.reply_msg
                      << ": local=" << context_.nicPath()
                      << ", peer=" << peer_desc.local_nic_path;
            return ERR_REJECT_HANDSHAKE;
        }
        disconnectUnlocked();
    } else if (current_status != UNCONNECTED) {
        local_desc.reply_msg =
            "Endpoint is not available for passive handshake";
        LOG(WARNING) << local_desc.reply_msg << ": status=" << current_status;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.peer_nic_path != context_.nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg =
            "Invalid argument: peer nic path inconsistency, expect " +
            context_.nicPath() + " + " + peer_nic_path_ + ", while got " +
            peer_desc.peer_nic_path + " + " + peer_desc.local_nic_path;

        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    status_.store(CONNECTING, std::memory_order_relaxed);

    auto attempt_setup_with_peer = [&](const std::string &peer_gid,
                                       uint16_t peer_lid) -> int {
        int auto_gid_retry_count = 0;
        std::vector<AutoGidSelectionIdentity> attempted_auto_gid_selections;
        for (;;) {
            auto local_gid_selection = fillLocalHandshakeDesc(
                context_, peer_nic_path_, qpNum(), local_desc);
            local_desc.handshake_version = peer_desc.handshake_version;
            local_desc.timestamp = getCurrentTimeInNano();
            local_desc.flags = 0;
            rememberAutoGidSelection(attempted_auto_gid_selections,
                                     local_gid_selection);

            SetupConnectionFailureInfo failure_info;
            int ret = doSetupConnection(peer_gid, peer_lid, peer_desc.qp_num,
                                        &local_desc.reply_msg, &failure_info);
            if (ret == 0) {
                recordSuccessfulHandshake(peer_desc.qp_num,
                                          peer_desc.handshake_version);
                return 0;
            }

            if (!shouldAttemptAutoGidHandshakeRetry(
                    context_.autoGidSelectionEnabled(), auto_gid_retry_count,
                    globalConfig().auto_gid_max_retries,
                    failure_info.stage == SetupConnectionFailureStage::kRtr,
                    failure_info.sys_errno)) {
                resetConnection("failed connection setup (passive)");
                return ret;
            }

            std::string previous_gid;
            std::string next_gid;
            bool reprobe_changed = context_.reprobeAutoGid(
                local_gid_selection, attempted_auto_gid_selections,
                &previous_gid, &next_gid);
            auto current_gid_selection = context_.gidSelection();
            auto retry_action = decideAutoGidRetryAction(
                reprobe_changed, local_gid_selection.gid_index,
                local_gid_selection.gid, current_gid_selection.gid_index,
                current_gid_selection.gid);
            if (retry_action == AutoGidRetryAction::kDoNotRetry) {
                resetConnection("failed connection setup (passive)");
                return ret;
            }

            int reset_ret = resetConnection(
                retry_action == AutoGidRetryAction::kRetryWithReprobedGid
                    ? "retry after auto GID reprobe (passive)"
                    : "retry with externally reprobed GID (passive)");
            if (reset_ret) return reset_ret;
            status_.store(CONNECTING, std::memory_order_relaxed);
            ++auto_gid_retry_count;
            LOG(WARNING) << "Retry passive handshake with updated local GID on "
                         << context_.deviceName() << ": "
                         << local_gid_selection.gid << " -> "
                         << current_gid_selection.gid << " (attempt "
                         << auto_gid_retry_count << "/"
                         << globalConfig().auto_gid_max_retries << ")";
        }
    };

    if (!peer_desc.local_gid.empty()) {
        return attempt_setup_with_peer(peer_desc.local_gid,
                                       peer_desc.local_lid);
    } else {
        auto segment_desc =
            context_.engine().meta()->getSegmentDescByName(peer_server_name);
        if (segment_desc) {
            for (auto &nic : segment_desc->devices) {
                if (nic.name == peer_nic_name) {
                    return attempt_setup_with_peer(nic.gid, nic.lid);
                }
            }
        }
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    status_.store(UNCONNECTED, std::memory_order_relaxed);
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

void RdmaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    beginDestroyNoLock();
}

int RdmaEndPoint::disconnectUnlocked() {
    auto curr_status = status_.load(std::memory_order_acquire);
    if (curr_status != CONNECTING) return 0;

    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    int ret = 0;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int curr_ret = ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (curr_ret) {
            PLOG(ERROR) << "Failed to modify QP to RESET";
            ret = ERR_ENDPOINT;
        }
    }

    auto pending_tokens = claimAllPendingTokens();
    if (!pending_tokens.empty()) {
        LOG(WARNING) << "Completing " << pending_tokens.size()
                     << " pending RDMA slices as failed during endpoint reset";
        completePendingSlicesAsFailed(pending_tokens);
    }

    if (wr_depth_list_) {
        for (size_t i = 0; i < qp_list_.size(); ++i) {
            if (wr_depth_list_[i] != 0) {
                LOG(WARNING)
                    << "Outstanding work requests remained after pending "
                       "completion, correcting counters";
                __sync_fetch_and_sub(cq_outstanding_, wr_depth_list_[i]);
                wr_depth_list_[i] = 0;
            }
        }
    }
    peer_qp_num_list_.clear();
    status_.store(UNCONNECTED, std::memory_order_release);
    return ret;
}

int RdmaEndPoint::resetConnection(const std::string &reason) {
    auto curr_status = status_.load(std::memory_order_acquire);
    if (curr_status != CONNECTING && curr_status != CONNECTED) return 0;

    if (curr_status == CONNECTED) {
        beginDestroyNoLock();
        LOG(WARNING) << "Endpoint marked for destruction instead of reset "
                     << "(triggered by: " << reason << ")";
        return ERR_ENDPOINT;
    }

    int ret = disconnectUnlocked();

    if (ret) {
        LOG(ERROR) << "Failed to reset the endpoint (triggered by: " << reason
                   << "): error=" << ret;
    } else {
        LOG(INFO) << "Successfully reset the endpoint (triggered by: " << reason
                  << ").";
    }
    return ret;
}

const std::string RdmaEndPoint::toString() const {
    auto status = status_.load(std::memory_order_relaxed);
    if (status == CONNECTED)
        return "EndPoint: local " + context_.nicPath() + ", peer " +
               peer_nic_path_;
    else if (status == DESTROYING)
        return "EndPoint: local " + context_.nicPath() + ", peer " +
               peer_nic_path_ + " (destroying)";
    else if (status == DESTROYED)
        return "EndPoint: local " + context_.nicPath() + " (destroyed)";
    else
        return "EndPoint: local " + context_.nicPath() + " (unconnected)";
}

int RdmaEndPoint::submitPostSend(
    std::vector<Transport::Slice *> &slice_list,
    std::vector<Transport::Slice *> &failed_slice_list) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!active_.load(std::memory_order_acquire) ||
        status_.load(std::memory_order_relaxed) != CONNECTED) {
        for (auto &slice : slice_list) failed_slice_list.push_back(slice);
        slice_list.clear();
        return 0;
    }

    const size_t num_qp = qp_list_.size();
    if (slice_list.empty()) return 0;
    const size_t requested = slice_list.size();
    int cq_remaining = int(globalConfig().max_cqe) - *cq_outstanding_;
    if (cq_remaining <= 0) return 0;

    // Only allocate for the max number of WRs we can actually post per QP,
    // not the entire requested slice count. Each QP iteration reuses the
    // wr_list/sge_list from index 0, so we only need max_wr_depth_ entries.
    size_t max_postable_per_qp =
        std::min({(size_t)max_wr_depth_, (size_t)cq_remaining, requested});
    std::vector<ibv_send_wr> wr_list(max_postable_per_qp, ibv_send_wr{});
    std::vector<ibv_sge> sge_list(max_postable_per_qp);
    size_t total_posted = 0;
    size_t cursor = 0;

    for (size_t qp_index = 0;
         qp_index < num_qp && cq_remaining > 0 && cursor < requested;
         ++qp_index) {
        int qp_avail = max_wr_depth_ - wr_depth_list_[qp_index];
        if (qp_avail <= 0) continue;

        size_t remaining_qps = num_qp - qp_index;
        size_t remaining_slices = requested - cursor;
        size_t chunk = (remaining_slices + remaining_qps - 1) / remaining_qps;
        size_t start = cursor;
        size_t end = std::min(start + chunk, requested);
        int assigned_count = (int)(end - start);

        int wr_count = std::min(assigned_count, qp_avail);
        wr_count = std::min(wr_count, cq_remaining);
        std::vector<CompletionToken *> token_list(wr_count, nullptr);

        for (int i = 0; i < wr_count; ++i) {
            auto *slice = slice_list[start + i];
            auto *token = new CompletionToken();
            token->endpoint.store(this, std::memory_order_relaxed);
            token->slice.store(slice, std::memory_order_relaxed);
            token->qp_depth.store(&wr_depth_list_[qp_index],
                                  std::memory_order_relaxed);
            token->fallback_completed.store(false, std::memory_order_relaxed);
            token_list[i] = token;

            auto &sge = sge_list[i];
            sge.addr = (uint64_t)slice->source_addr;
            sge.length = slice->length;
            sge.lkey = slice->rdma.source_lkey;

            auto &wr = wr_list[i];
            memset(&wr, 0, sizeof(ibv_send_wr));
            wr.wr_id = (uint64_t)token;
            wr.opcode = slice->opcode == Transport::TransferRequest::READ
                            ? IBV_WR_RDMA_READ
                            : IBV_WR_RDMA_WRITE;
            wr.num_sge = 1;
            wr.sg_list = &sge;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
            wr.wr.rdma.remote_addr = slice->rdma.dest_addr;
            wr.wr.rdma.rkey = slice->rdma.dest_rkey;
            slice->ts = getCurrentTimeInNano();
            slice->status = Transport::Slice::POSTED;
            slice->rdma.qp_depth = &wr_depth_list_[qp_index];
        }
        addPendingTokens(token_list);

        ibv_send_wr *bad_wr = nullptr;
        __sync_fetch_and_add(&wr_depth_list_[qp_index], wr_count);
        __sync_fetch_and_add(cq_outstanding_, wr_count);
        int rc = ibv_post_send(qp_list_[qp_index], wr_list.data(), &bad_wr);
        if (rc) {
            PLOG(ERROR) << "Failed to ibv_post_send";
            if (!bad_wr) bad_wr = wr_list.data();
            int first_bad = bad_wr - wr_list.data();
            removePendingTokens(token_list, first_bad, wr_count - first_bad);
            while (bad_wr) {
                int i = bad_wr - wr_list.data();
                auto *slice = slice_list[start + i];
                failed_slice_list.push_back(slice);
                slice->rdma.endpoint = nullptr;
                slice->rdma.qp_depth = nullptr;
                delete token_list[i];
                token_list[i] = nullptr;
                __sync_fetch_and_sub(&wr_depth_list_[qp_index], 1);
                __sync_fetch_and_sub(cq_outstanding_, 1);
                bad_wr = bad_wr->next;
            }
            total_posted += wr_count;
            cursor += wr_count;
            break;
        }
        total_posted += wr_count;
        cursor += wr_count;
        cq_remaining -= wr_count;
    }
    slice_list.erase(slice_list.begin(),
                     slice_list.begin() + (ptrdiff_t)total_posted);
    return 0;
}

size_t RdmaEndPoint::getQPNumber() const { return qp_list_.size(); }

std::vector<uint32_t> RdmaEndPoint::qpNum() const {
    std::vector<uint32_t> ret;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        ret.push_back(qp_list_[qp_index]->qp_num);
    return ret;
}

static int parseGidString(const std::string &gid_str, ibv_gid &gid_out) {
    if (gid_str.empty()) {
        LOG(ERROR) << "GID string is empty";
        return ERR_INVALID_ARGUMENT;
    }

    // Prepend a colon to the GID string to simplify parsing.
    std::istringstream iss(":" + gid_str);
    for (size_t i = 0; i < sizeof(gid_out.raw); i++) {
        if (iss.get() != ':') {
            LOG(ERROR) << "Invalid GID format at byte " << i
                       << ", peer_gid=" << gid_str;
            return ERR_INVALID_ARGUMENT;
        }

        uint32_t byte = 0;
        iss >> std::hex >> byte;

        if (iss.fail() || byte > 0xFF) {
            LOG(ERROR) << "Invalid GID format at byte " << i
                       << ", peer_gid=" << gid_str;
            return ERR_INVALID_ARGUMENT;
        }

        gid_out.raw[i] = static_cast<uint8_t>(byte);
    }

    // Ensure no trailing data remains after 16 bytes
    char extra;
    if (iss.get(extra)) {
        LOG(ERROR) << "GID string has trailing data after 16 bytes"
                   << ", peer_gid=" << gid_str;
        return ERR_INVALID_ARGUMENT;
    }

    return 0;
}

int RdmaEndPoint::doSetupConnection(const std::string &peer_gid,
                                    uint16_t peer_lid,
                                    std::vector<uint32_t> peer_qp_num_list,
                                    std::string *reply_msg,
                                    SetupConnectionFailureInfo *failure_info) {
    if (qp_list_.size() != peer_qp_num_list.size()) {
        std::string message =
            "QP count mismatch in peer and local endpoints, check "
            "MC_MAX_EP_PER_CTX";
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kPeerValidation;
            failure_info->sys_errno = 0;
        }
        return ERR_INVALID_ARGUMENT;
    }

    // Verify and parse the peer GID before proceeding.
    ibv_gid peer_gid_raw = {};
    int ret = parseGidString(peer_gid, peer_gid_raw);
    if (ret) {
        std::string message = "Invalid peer GID: " + peer_gid;
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kPeerValidation;
            failure_info->sys_errno = 0;
        }
        return ret;
    }

    int local_gid_index = context_.gidIndex();
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        int ret = doSetupConnection(qp_index, peer_gid_raw, peer_lid,
                                    peer_qp_num_list[qp_index], local_gid_index,
                                    reply_msg, failure_info);
        if (ret) return ret;
    }

    peer_qp_num_list_ = std::move(peer_qp_num_list);
    status_.store(CONNECTED, std::memory_order_relaxed);
    return 0;
}

int RdmaEndPoint::doSetupConnection(int qp_index, const ibv_gid &peer_gid,
                                    uint16_t peer_lid, uint32_t peer_qp_num,
                                    int local_gid_index, std::string *reply_msg,
                                    SetupConnectionFailureInfo *failure_info) {
    if (qp_index < 0 || qp_index >= (int)qp_list_.size())
        return ERR_INVALID_ARGUMENT;
    auto &qp = qp_list_[qp_index];

    // Any state -> RESET
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    int ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    if (ret) {
        std::string message = "Failed to modify QP to RESET";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kReset;
            failure_info->sys_errno = errno;
        }
        return ERR_ENDPOINT;
    }

    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context_.portNum();
    attr.pkey_index = globalConfig().pkey_index;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    ret = ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret) {
        std::string message =
            "Failed to modify QP to INIT, check local context port num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kInit;
            failure_info->sys_errno = errno;
        }
        return ERR_ENDPOINT;
    }

    // INIT -> RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = context_.activeMTU();
    if (globalConfig().mtu_length < attr.path_mtu)
        attr.path_mtu = globalConfig().mtu_length;
    attr.ah_attr.grh.dgid = peer_gid;
    // TODO gidIndex and portNum must fetch from REMOTE
    attr.ah_attr.grh.sgid_index = local_gid_index;
    attr.ah_attr.grh.hop_limit = kMaxHopLimit;
    // Set traffic class if configured (-1 means use default)
    if (globalConfig().ib_traffic_class >= 0) {
        attr.ah_attr.grh.traffic_class =
            static_cast<uint8_t>(globalConfig().ib_traffic_class);
    }
    attr.ah_attr.dlid = peer_lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.static_rate = 0;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = context_.portNum();
    attr.dest_qp_num = peer_qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;  // 12 in previous implementation
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER |
                            IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN);
    if (ret) {
        std::string message =
            "Failed to modify QP to RTR, check mtu, gid, peer lid, peer qp num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kRtr;
            failure_info->sys_errno = errno;
        }
        return ERR_ENDPOINT;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = kTimeout;
    attr.retry_cnt = kRetryCount;
    attr.rnr_retry = 7;  // or 7,RNR error
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        std::string message = "Failed to modify QP to RTS";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        if (failure_info) {
            failure_info->stage = SetupConnectionFailureStage::kRts;
            failure_info->sys_errno = errno;
        }
        return ERR_ENDPOINT;
    }

    // Optional: pin QP to a specific LAG port for even traffic distribution
    // across bonded physical ports. num_lag_ports is queried from hardware at
    // context construction time; this block is a no-op on non-LAG devices.
    if (globalConfig().mlx5_qp_lag_port_balance) {
#ifdef USE_MLX5DV
        uint8_t n = context_.numLagPorts();
        if (n > 1) {
            uint8_t target = (uint8_t)(qp_index % n) + 1;
            int lag_ret = mlx5dv_modify_qp_lag_port(qp, target);
            if (lag_ret) {
                LOG_FIRST_N(WARNING, 4)
                    << "[RDMA] mlx5dv_modify_qp_lag_port failed"
                    << " (qp_index=" << qp_index
                    << ", target_port=" << (int)target
                    << "): " << strerror(lag_ret);
            } else {
                uint8_t cfg = 0, active = 0;
                if (mlx5dv_query_qp_lag_port(qp, &cfg, &active) == 0) {
                    VLOG(1)
                        << "[RDMA] QP[" << qp_index << "] qpn=" << qp->qp_num
                        << " lag_port cfg=" << (int)cfg
                        << " active=" << (int)active;
                } else {
                    LOG_FIRST_N(WARNING, 4)
                        << "[RDMA] mlx5dv_query_qp_lag_port failed"
                        << " (qp_index=" << qp_index << ")";
                }
            }
        }
#else
        LOG_FIRST_N(WARNING, 1)
            << "MC_MLX5_QP_LAG_PORT_BALANCE is set but binary was not built "
               "with USE_MLX5DV; ignoring";
#endif
    }

    // Optional: override the RoCEv2 UDP source port to spread QPs across
    // different ECMP/LAG paths. Empty list = leave whatever the driver
    // picked. Failure is non-fatal so unsupported devices/firmware degrade
    // gracefully.
    const auto &sports = globalConfig().mlx5_qp_udp_sports;
    if (!sports.empty()) {
#ifdef USE_MLX5DV
        uint16_t sport = sports[qp_index % sports.size()];
        int sp_ret = mlx5dv_modify_qp_udp_sport(qp, sport);
        if (sp_ret) {
            LOG_FIRST_N(WARNING, 4)
                << "[RDMA] mlx5dv_modify_qp_udp_sport failed (qp_index="
                << qp_index << ", sport=" << sport << "): " << strerror(sp_ret);
        } else {
            VLOG(1) << "[RDMA] QP[" << qp_index << "] qpn=" << qp->qp_num
                    << " udp_sport=" << sport;
        }
#else
        LOG_FIRST_N(WARNING, 1)
            << "MC_MLX5_QP_UDP_SPORTS is set but binary was not built with "
               "USE_MLX5DV; ignoring";
#endif
    }

    return 0;
}

}  // namespace mooncake
