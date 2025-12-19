#include "hot_standby_service.h"

#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "master_service.h"
#include "oplog_manager.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    metadata_store_ = std::make_unique<MetadataStore>();
}

HotStandbyService::~HotStandbyService() {
    Stop();
}

ErrorCode HotStandbyService::Start(const std::string& primary_address) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (running_.load()) {
        LOG(WARNING) << "HotStandbyService is already running";
        return ErrorCode::OK;
    }

    config_.primary_address = primary_address;
    running_.store(true);

    // Start background threads
    replication_thread_ = std::thread(&HotStandbyService::ReplicationLoop, this);
    if (config_.enable_verification) {
        verification_thread_ =
            std::thread(&HotStandbyService::VerificationLoop, this);
    }

    LOG(INFO) << "HotStandbyService started, connecting to Primary: "
              << primary_address;
    return ErrorCode::OK;
}

void HotStandbyService::Stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    DisconnectFromPrimary();

    // Wait for threads to finish
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }

    LOG(INFO) << "HotStandbyService stopped";
}

StandbySyncStatus HotStandbyService::GetSyncStatus() const {
    StandbySyncStatus status;
    status.applied_seq_id = applied_seq_id_.load();
    status.primary_seq_id = primary_seq_id_.load();
    status.is_connected = is_connected_.load();

    if (status.primary_seq_id > status.applied_seq_id) {
        status.lag_entries = status.primary_seq_id - status.applied_seq_id;
    } else {
        status.lag_entries = 0;
    }

    // Calculate lag time (placeholder - in full implementation this would
    // track actual time differences)
    status.lag_time = std::chrono::milliseconds(0);
    status.is_syncing = running_.load() && is_connected_.load();

    return status;
}

bool HotStandbyService::IsReadyForPromotion() const {
    StandbySyncStatus status = GetSyncStatus();
    if (!status.is_connected) {
        return false;
    }

    // Check if lag is within threshold
    return status.lag_entries <= config_.max_replication_lag_entries;
}

std::unique_ptr<MasterService> HotStandbyService::Promote() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion. Lag: "
                   << GetSyncStatus().lag_entries << " entries";
        return nullptr;
    }

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << applied_seq_id_.load();

    // Stop replication
    Stop();

    // In full implementation, we would:
    // 1. Create a new MasterService instance
    // 2. Initialize it with the replicated metadata from metadata_store_
    // 3. Return the MasterService instance

    // For now, this is a placeholder
    // TODO: Implement full promotion logic
    auto master_service = std::make_unique<MasterService>();

    LOG(INFO) << "Standby promoted to Primary successfully";
    return master_service;
}

size_t HotStandbyService::GetMetadataCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_store_ ? metadata_store_->entry_count : 0;
}

void HotStandbyService::ReplicationLoop() {
    LOG(INFO) << "Replication loop started";

    while (running_.load()) {
        // Try to connect if not connected
        if (!is_connected_.load()) {
            if (ConnectToPrimary()) {
                is_connected_.store(true);
                LOG(INFO) << "Connected to Primary: " << config_.primary_address;
            } else {
                // Retry after a delay
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
        }

        // In full implementation, this would:
        // 1. Receive OpLog entries from Primary via gRPC stream
        // 2. Process them in batches
        // 3. Apply to local metadata store

        // For now, this is a placeholder that simulates receiving entries
        // In the actual implementation, this would block on the gRPC stream
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Placeholder: Simulate receiving entries
        // TODO: Replace with actual gRPC stream reading
    }

    LOG(INFO) << "Replication loop stopped";
}

void HotStandbyService::VerificationLoop() {
    LOG(INFO) << "Verification loop started";

    while (running_.load()) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.verification_interval_sec));

        if (!is_connected_.load()) {
            continue;
        }

        // In full implementation, this would:
        // 1. Sample keys from local metadata store
        // 2. Calculate checksums
        // 3. Send verification request to Primary
        // 4. Handle mismatches if any

        // Placeholder: Log that verification would happen
        VLOG(1) << "Verification check (placeholder)";
    }

    LOG(INFO) << "Verification loop stopped";
}

void HotStandbyService::ApplyOpLogEntry(const OpLogEntry& entry) {
    // In full implementation, this would apply the OpLog entry to
    // the local metadata store, mirroring the operations in MasterService.

    // For now, this is a placeholder that just updates counters
    switch (entry.op_type) {
        case OpType::PUT_END:
            // Create or update metadata for the key
            if (metadata_store_) {
                metadata_store_->entry_count++;
            }
            break;
        case OpType::PUT_REVOKE:
        case OpType::REMOVE:
            // Remove metadata for the key
            if (metadata_store_ && metadata_store_->entry_count > 0) {
                metadata_store_->entry_count--;
            }
            break;
        case OpType::LEASE_RENEW:
            // Update lease timeout (no change to entry count)
            break;
        default:
            LOG(WARNING) << "Unknown OpType: "
                        << static_cast<int>(entry.op_type);
            break;
    }

    applied_seq_id_.store(entry.sequence_id);
}

void HotStandbyService::ProcessOpLogBatch(
    const std::vector<OpLogEntry>& entries) {
    for (const auto& entry : entries) {
        ApplyOpLogEntry(entry);
    }
}

bool HotStandbyService::ConnectToPrimary() {
    // In full implementation, this would:
    // 1. Create a gRPC channel to Primary
    // 2. Establish a bidirectional stream for OpLog replication
    // 3. Send initial sync request with current applied_seq_id
    // 4. Start receiving OpLog entries

    // For now, this is a placeholder
    LOG(INFO) << "Connecting to Primary: " << config_.primary_address
              << " (placeholder)";
    return false;  // Return false to indicate not yet implemented
}

void HotStandbyService::DisconnectFromPrimary() {
    if (is_connected_.load()) {
        is_connected_.store(false);
        replication_stream_.reset();
        LOG(INFO) << "Disconnected from Primary";
    }
}

}  // namespace mooncake

