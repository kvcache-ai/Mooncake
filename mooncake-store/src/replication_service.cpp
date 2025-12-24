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
    state.last_send_time = std::chrono::steady_clock::now();
    state.state = StandbyHealthState::HEALTHY;
    state.consecutive_failures = 0;

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
    // Skip Standbys that are in TIMEOUT or DISCONNECTED state
    for (auto& [standby_id, state] : standbys_) {
        // Skip unhealthy Standbys
        if (state.state == StandbyHealthState::TIMEOUT ||
            state.state == StandbyHealthState::DISCONNECTED) {
            continue;
        }

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
    
    // Check connection status
    if (!state.stream || !state.stream->IsConnected()) {
        state.state = StandbyHealthState::DISCONNECTED;
        LOG(WARNING) << "Standby stream not connected: " << standby_id;
        return;
    }

    // Update last send time
    state.last_send_time = std::chrono::steady_clock::now();

    // For now, this is a placeholder. In the full implementation,
    // this would send via gRPC stream.
    bool success = state.stream->Send(entries);
    if (success) {
        // Reset failure count on successful send
        // Note: ACK will be updated via OnAck() when we receive actual ACK
        state.consecutive_failures = 0;
        if (state.state == StandbyHealthState::SLOW) {
            state.state = StandbyHealthState::HEALTHY;
        }
    } else {
        state.consecutive_failures++;
        LOG(ERROR) << "Failed to send batch to Standby: " << standby_id
                   << ", consecutive failures: " << state.consecutive_failures;
        
        // Mark as TIMEOUT if too many failures
        if (state.consecutive_failures >= kMaxConsecutiveFailures) {
            state.state = StandbyHealthState::TIMEOUT;
            LOG(WARNING) << "Standby " << standby_id
                        << " marked as TIMEOUT after "
                        << state.consecutive_failures << " failures";
        } else {
            state.state = StandbyHealthState::SLOW;
        }
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

void ReplicationService::OnAck(const std::string& standby_id, uint64_t acked_seq_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = standbys_.find(standby_id);
    if (it == standbys_.end()) {
        LOG(WARNING) << "Received ACK from unknown Standby: " << standby_id;
        return;
    }
    
    auto& state = it->second;
    if (acked_seq_id > state.acked_seq_id) {
        state.acked_seq_id = acked_seq_id;
        state.last_ack_time = std::chrono::steady_clock::now();
        state.consecutive_failures = 0;  // Reset failure count on successful ACK
        
        // Recover from SLOW or TIMEOUT state if we get an ACK
        if (state.state == StandbyHealthState::SLOW ||
            state.state == StandbyHealthState::TIMEOUT) {
            state.state = StandbyHealthState::HEALTHY;
            LOG(INFO) << "Standby " << standby_id << " recovered, acked_seq_id="
                     << acked_seq_id;
        }
    }
}

void ReplicationService::CheckStandbyHealth() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    
    for (auto& [standby_id, state] : standbys_) {
        // Check connection status first
        if (!state.stream || !state.stream->IsConnected()) {
            if (state.state != StandbyHealthState::DISCONNECTED) {
                state.state = StandbyHealthState::DISCONNECTED;
                LOG(WARNING) << "Standby " << standby_id << " disconnected";
            }
            continue;
        }
        
        // Check ACK timeout
        auto ack_age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - state.last_ack_time).count();
        
        if (ack_age_ms > kAckTimeoutMs) {
            state.consecutive_failures++;
            
            if (state.consecutive_failures >= kMaxConsecutiveFailures) {
                if (state.state != StandbyHealthState::TIMEOUT) {
                    state.state = StandbyHealthState::TIMEOUT;
                    LOG(WARNING) << "Standby " << standby_id
                               << " marked as TIMEOUT (ack_age=" << ack_age_ms
                               << "ms, failures=" << state.consecutive_failures << ")";
                }
            } else {
                if (state.state == StandbyHealthState::HEALTHY) {
                    state.state = StandbyHealthState::SLOW;
                    LOG(WARNING) << "Standby " << standby_id
                               << " is slow (ack_age=" << ack_age_ms << "ms)";
                }
            }
        } else {
            // ACK received recently, reset failure count if healthy
            if (state.state == StandbyHealthState::HEALTHY) {
                state.consecutive_failures = 0;
            }
        }
    }
}

}  // namespace mooncake

