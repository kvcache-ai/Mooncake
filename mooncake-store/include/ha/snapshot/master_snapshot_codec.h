#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Forward declarations
class MasterService;
class SegmentManager;
class NoFSegmentManager;
class ClientTaskManager;

namespace ha {

/**
 * @brief A read-only view of the live master state for snapshot serialization.
 *
 * This struct holds const references to the live state components that need to
 * be serialized into a master snapshot. It's a half-step DTO approach that
 * avoids copying large state while providing a clean interface for the codec.
 */
struct MasterSnapshotStateView {
    const MasterService& master_service;
    const SegmentManager& segment_manager;
    const NoFSegmentManager& nof_segment_manager;
    const ClientTaskManager& task_manager;

    MasterSnapshotStateView(const MasterService& ms,
                            const SegmentManager& sm,
                            const NoFSegmentManager& nsm,
                            const ClientTaskManager& tm)
        : master_service(ms),
          segment_manager(sm),
          nof_segment_manager(nsm),
          task_manager(tm) {}
};

/**
 * @brief Encodes and decodes master snapshot payloads.
 *
 * This codec handles serialization of the complete master state bundle:
 * - Metadata shards (objects, replicas, tenant state)
 * - Segment manager state
 * - Task manager state
 * - Discarded replicas
 *
 * The current implementation preserves the existing snapshot format exactly
 * to maintain backward compatibility with existing snapshots.
 *
 * Format details:
 * - metadata: msgpack-encoded metadata shards (compressed per-shard with zstd)
 * - segments: msgpack-encoded segment manager state
 * - task_manager: msgpack-encoded task manager state
 * - manifest.txt: format descriptor (e.g., "messagepack|1.0.0|master")
 */
class MasterSnapshotCodec {
   public:
    MasterSnapshotCodec() = default;
    ~MasterSnapshotCodec() = default;

    // Non-copyable, non-movable (contains no state, but enforce ownership semantics)
    MasterSnapshotCodec(const MasterSnapshotCodec&) = delete;
    MasterSnapshotCodec& operator=(const MasterSnapshotCodec&) = delete;
    MasterSnapshotCodec(MasterSnapshotCodec&&) = delete;
    MasterSnapshotCodec& operator=(MasterSnapshotCodec&&) = delete;

    /**
     * @brief Encode master state into serialized buffers.
     *
     * @param state_view Read-only view of the live master state
     * @return Map of payload names to serialized data buffers, or error
     *
     * The returned map contains keys like:
     * - "metadata": serialized metadata shards
     * - "segments": serialized segment manager state
     * - "task_manager": serialized task manager state
     */
    tl::expected<std::unordered_map<std::string, std::vector<uint8_t>>,
                 SerializationError>
    Encode(const MasterSnapshotStateView& state_view) const;

    /**
     * @brief Decode snapshot payloads and restore into master service.
     *
     * @param master_service Target MasterService to restore state into
     * @param payloads Map of payload names to serialized data
     * @return void on success, SerializationError on failure
     */
    tl::expected<void, SerializationError> Decode(
        MasterService* master_service,
        const std::unordered_map<std::string, std::vector<uint8_t>>& payloads)
        const;

    /**
     * @brief Get the manifest content for this codec version.
     *
     * @return Manifest string (e.g., "messagepack|1.0.0|master")
     */
    static std::string GetManifestContent();

   private:
    // Metadata encoding/decoding (delegates to MetadataSerializer for now)
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeMetadata(
        const MasterService& master_service) const;
    tl::expected<void, SerializationError> DecodeMetadata(
        MasterService* master_service, const std::vector<uint8_t>& data) const;

    // Segment encoding/decoding
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeSegments(
        const SegmentManager& segment_manager,
        const NoFSegmentManager& nof_segment_manager) const;
    tl::expected<void, SerializationError> DecodeSegments(
        MasterService* master_service, const std::vector<uint8_t>& data) const;

    // Task manager encoding/decoding
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeTaskManager(
        const ClientTaskManager& task_manager) const;
    tl::expected<void, SerializationError> DecodeTaskManager(
        MasterService* master_service, const std::vector<uint8_t>& data) const;
};

}  // namespace ha
}  // namespace mooncake
