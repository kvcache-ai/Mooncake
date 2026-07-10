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
 * @brief A view of the live master state for snapshot serialization.
 *
 * This struct holds references to the live state components that need to
 * be serialized into a master snapshot. Non-const references are used because
 * the underlying serializers require non-const pointers, avoiding const_cast.
 */
struct MasterSnapshotStateView {
    MasterService& master_service;
    SegmentManager& segment_manager;
    NoFSegmentManager& nof_segment_manager;
    ClientTaskManager& task_manager;

    MasterSnapshotStateView(MasterService& ms, SegmentManager& sm,
                            NoFSegmentManager& nsm, ClientTaskManager& tm)
        : master_service(ms),
          segment_manager(sm),
          nof_segment_manager(nsm),
          task_manager(tm) {}
};

/**
 * @brief Container for serialized master snapshot payloads.
 *
 * This struct provides a type-safe, zero-overhead container for the three
 * serialized payload buffers, eliminating map lookup overhead and potential
 * runtime exceptions from using std::unordered_map.
 */
struct MasterSnapshotPayloads {
    std::vector<uint8_t> metadata;
    std::vector<uint8_t> segments;
    std::vector<uint8_t> task_manager;
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

    // Non-copyable, non-movable (contains no state, but enforce ownership
    // semantics)
    MasterSnapshotCodec(const MasterSnapshotCodec&) = delete;
    MasterSnapshotCodec& operator=(const MasterSnapshotCodec&) = delete;
    MasterSnapshotCodec(MasterSnapshotCodec&&) = delete;
    MasterSnapshotCodec& operator=(MasterSnapshotCodec&&) = delete;

    /**
     * @brief Encode master state into serialized buffers.
     *
     * @param state_view View of the live master state
     * @return Structured payloads containing serialized data, or error
     *
     * The returned struct contains:
     * - metadata: serialized metadata shards
     * - segments: serialized segment manager state
     * - task_manager: serialized task manager state
     */
    tl::expected<MasterSnapshotPayloads, SerializationError> Encode(
        MasterSnapshotStateView& state_view) const;

    /**
     * @brief Decode snapshot payloads and restore into master service.
     *
     * @param master_service Target MasterService to restore state into
     * @param payloads Structured payloads containing serialized data
     * @return void on success, SerializationError on failure
     */
    tl::expected<void, SerializationError> Decode(
        MasterService* master_service,
        const MasterSnapshotPayloads& payloads) const;

    /**
     * @brief Get the manifest content for this codec version.
     *
     * @return Manifest string (e.g., "messagepack|1.0.0|master")
     */
    static std::string GetManifestContent();

   private:
    // Metadata encoding/decoding (delegates to MetadataSerializer for now)
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeMetadata(
        MasterService& master_service) const;
    tl::expected<void, SerializationError> DecodeMetadata(
        MasterService* master_service, const std::vector<uint8_t>& data) const;

    // Segment encoding/decoding
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeSegments(
        SegmentManager& segment_manager,
        NoFSegmentManager& nof_segment_manager) const;
    tl::expected<void, SerializationError> DecodeSegments(
        MasterService* master_service, const std::vector<uint8_t>& data) const;

    // Task manager encoding/decoding
    tl::expected<std::vector<uint8_t>, SerializationError> EncodeTaskManager(
        ClientTaskManager& task_manager) const;
    tl::expected<void, SerializationError> DecodeTaskManager(
        MasterService* master_service, const std::vector<uint8_t>& data) const;
};

}  // namespace ha
}  // namespace mooncake
