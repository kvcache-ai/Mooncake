#include "replication_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include "master_service.h"
#include "oplog_manager.h"

namespace mooncake {

ReplicationService::ReplicationService(OpLogManager& oplog_manager,
                                         MasterService& master_service)
    : oplog_manager_(oplog_manager), master_service_(master_service) {}

ReplicationService::~ReplicationService() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    standbys_.clear();
}

void ReplicationService::RegisterStandby(const std::string& standby_id,
                                         std::shared_ptr<ReplicationStream> stream) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    StandbyState state;
    state.stream = std::move(stream);
    state.acked_seq_id = 0;
    state.last_ack_time = std::chrono::steady_clock::now();

    standbys_[standby_id] = std::move(state);

    LOG(INFO) << "Registered Standby: " << standby_id
              << ", total standbys: " << standbys_.size();
}

void ReplicationService::UnregisterStandby(const std::string& standby_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto it = standbys_.find(standby_id);
    if (it != standbys_.end()) {
        standbys_.erase(it);
        LOG(INFO) << "Unregistered Standby: " << standby_id
                  << ", remaining standbys: " << standbys_.size();
    }
}

void ReplicationService::OnNewOpLog(const OpLogEntry& entry) {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (standbys_.empty()) {
        // No standbys connected, nothing to do
        return;
    }

    // Broadcast to all standbys
    BroadcastEntry(entry);
}

void ReplicationService::BroadcastEntry(const OpLogEntry& entry) {
    // For now, we just add the entry to each standby's pending batch.
    // In a full implementation, this would trigger immediate or batched sending.
    for (auto& [standby_id, state] : standbys_) {
        state.pending_batch.push_back(entry);

        // If batch is full, send it immediately
        if (state.pending_batch.size() >= kBatchSize) {
            SendBatch(standby_id, state.pending_batch);
            state.pending_batch.clear();
        }
    }
}

void ReplicationService::SendBatch(const std::string& standby_id,
                                   const std::vector<OpLogEntry>& entries) {
    auto it = standbys_.find(standby_id);
    if (it == standbys_.end()) {
        LOG(WARNING) << "Attempted to send batch to unknown Standby: "
                     << standby_id;
        return;
    }

    auto& state = it->second;
    if (!state.stream || !state.stream->IsConnected()) {
        LOG(WARNING) << "Standby stream not connected: " << standby_id;
        return;
    }

    // For now, this is a placeholder. In the full implementation,
    // this would send via gRPC stream.
    bool success = state.stream->Send(entries);
    if (success) {
        // Update ACK tracking (in full implementation, this would be
        // updated when we receive actual ACK from Standby)
        if (!entries.empty()) {
            state.acked_seq_id = entries.back().sequence_id;
            state.last_ack_time = std::chrono::steady_clock::now();
        }
    } else {
        LOG(ERROR) << "Failed to send batch to Standby: " << standby_id;
    }
}

VerificationResponse ReplicationService::HandleVerification(
    const VerificationRequest& request) {
    VerificationResponse response;
    response.is_consistent = true;

    // TODO: Implement actual verification logic by comparing checksums
    // with MasterService metadata. For now, this is a placeholder.

    LOG(INFO) << "Verification request from Standby: " << request.standby_id
              << ", samples: " << request.samples.size();

    // Placeholder: assume consistent for now
    response.is_consistent = true;
    response.mismatched_keys.clear();

    return response;
}

std::map<std::string, uint64_t> ReplicationService::GetReplicationLag() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::map<std::string, uint64_t> lag_map;

    uint64_t primary_seq_id = oplog_manager_.GetLastSequenceId();

    for (const auto& [standby_id, state] : standbys_) {
        uint64_t lag = 0;
        if (primary_seq_id > state.acked_seq_id) {
            lag = primary_seq_id - state.acked_seq_id;
        }
        lag_map[standby_id] = lag;
    }

    return lag_map;
}

size_t ReplicationService::GetStandbyCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return standbys_.size();
}

}  // namespace mooncake

