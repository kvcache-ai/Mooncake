#include "ha/snapshot/master_snapshot_codec.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "master_service.h"
#include "segment.h"
#include "serialize/serializer.h"
#include "task_manager.h"

namespace mooncake::ha {

std::string MasterSnapshotCodec::GetManifestContent() {
    return "messagepack|1.0.0|master";
}

tl::expected<std::unordered_map<std::string, std::vector<uint8_t>>,
             SerializationError>
MasterSnapshotCodec::Encode(const MasterSnapshotStateView& state_view) const {
    std::unordered_map<std::string, std::vector<uint8_t>> payloads;

    // 1. Encode metadata (shards, discarded replicas, replica_next_id)
    auto metadata_result = EncodeMetadata(state_view.master_service);
    if (!metadata_result) {
        return tl::make_unexpected(metadata_result.error());
    }
    payloads["metadata"] = std::move(metadata_result.value());

    // 2. Encode segments (memory segments + NoF segments)
    auto segments_result =
        EncodeSegments(state_view.segment_manager, state_view.nof_segment_manager);
    if (!segments_result) {
        return tl::make_unexpected(segments_result.error());
    }
    payloads["segments"] = std::move(segments_result.value());

    // 3. Encode task manager
    auto task_manager_result = EncodeTaskManager(state_view.task_manager);
    if (!task_manager_result) {
        return tl::make_unexpected(task_manager_result.error());
    }
    payloads["task_manager"] = std::move(task_manager_result.value());

    return payloads;
}

tl::expected<void, SerializationError> MasterSnapshotCodec::Decode(
    MasterService* master_service,
    const std::unordered_map<std::string, std::vector<uint8_t>>& payloads)
    const {
    if (master_service == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS, "master_service is null"));
    }

    // 1. Decode metadata (must be first to restore metadata shards)
    auto metadata_it = payloads.find("metadata");
    if (metadata_it == payloads.end()) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'metadata' payload"));
    }
    auto metadata_result = DecodeMetadata(master_service, metadata_it->second);
    if (!metadata_result) {
        return tl::make_unexpected(metadata_result.error());
    }

    // 2. Decode segments
    auto segments_it = payloads.find("segments");
    if (segments_it == payloads.end()) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'segments' payload"));
    }
    auto segments_result = DecodeSegments(master_service, segments_it->second);
    if (!segments_result) {
        return tl::make_unexpected(segments_result.error());
    }

    // 3. Decode task manager
    auto task_manager_it = payloads.find("task_manager");
    if (task_manager_it == payloads.end()) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'task_manager' payload"));
    }
    auto task_manager_result =
        DecodeTaskManager(master_service, task_manager_it->second);
    if (!task_manager_result) {
        return tl::make_unexpected(task_manager_result.error());
    }

    return {};
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeMetadata(
    const MasterService& master_service) const {
    // Delegate to the existing MetadataSerializer for now.
    // This maintains the exact same format as before.
    MasterService::MetadataSerializer serializer(
        const_cast<MasterService*>(&master_service));
    return serializer.Serialize();
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeMetadata(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    // Delegate to the existing MetadataSerializer for now.
    MasterService::MetadataSerializer serializer(master_service);
    return serializer.Deserialize(data);
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeSegments(
    const SegmentManager& segment_manager,
    const NoFSegmentManager& nof_segment_manager) const {
    // Use the existing SegmentSerializer which only handles SegmentManager
    // Note: NoFSegmentManager is not currently serialized in snapshots
    SegmentSerializer serializer(const_cast<SegmentManager*>(&segment_manager));
    return serializer.Serialize();
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeSegments(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    // Access the segment managers from MasterService
    SegmentSerializer serializer(&master_service->segment_manager_);
    return serializer.Deserialize(data);
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeTaskManager(
    const ClientTaskManager& task_manager) const {
    // Use the existing TaskManagerSerializer
    TaskManagerSerializer serializer(
        const_cast<ClientTaskManager*>(&task_manager));
    return serializer.Serialize();
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeTaskManager(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    // Access the task manager from MasterService
    TaskManagerSerializer serializer(&master_service->task_manager_);
    return serializer.Deserialize(data);
}

}  // namespace mooncake::ha
