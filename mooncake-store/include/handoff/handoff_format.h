#pragma once

// On-disk format of the shutdown handoff snapshot.
//
// A handoff snapshot holds a batch OpLog snapshot (object chunks, segments,
// weight metadata and the manifest that indexes them, see
// ha/snapshot/batch_oplog/) under the root kHandoffSnapshotRoot, plus two
// files of its own:
//
//   handoff_manifest        what only the handoff needs: the batch OpLog
//                           manifest it vouches for (size and CRC), the part
//                           of each segment a remount must match that the
//                           segment record does not carry, object and replica
//                           counts, the next replica id and the codec
//                           fingerprints of the writer. Written last.
//   handoff_quarantine.bin  memory ranges that a writer or reader of the old
//                           master may still use, each with the time the old
//                           master would still have held it back.
//
// Each segment's identity (segment id, base, owner) travels in its
// StandbySegmentInfo record.

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <ylt/struct_pack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha/snapshot/batch_oplog/metadata.h"
#include "metadata_store.h"
#include "segment/status.h"
#include "types.h"
#include "weight_metadata_store.h"

namespace mooncake::handoff {

inline constexpr char kHandoffFormat[] = "mooncake-handoff";
inline constexpr uint32_t kHandoffSchemaVersion = 1;
inline constexpr char kHandoffManifestFile[] = "handoff_manifest";
inline constexpr char kHandoffQuarantineFile[] = "handoff_quarantine.bin";
inline constexpr char kHandoffSnapshotRoot[] = "handoff";

// The part of a memory segment that its StandbySegmentInfo record does not
// carry. A restored segment keeps its status (OK, DRAINING or DRAINED); a
// DRAINING or DRAINED one never becomes allocatable on its own.
struct SegmentAttributes {
    std::string protocol;
    std::string host_id;
    SegmentStatus status{SegmentStatus::OK};
    // The NUMA node and HCAs the store reported at mount, if any. The
    // owner's remount presents them again and must match the restored
    // segment exactly.
    std::optional<SegmentLocality> locality;
};

// A memory segment as the handoff exports and restores it.
struct HandoffSegment {
    StandbySegmentInfo info;  // with segment_id, base and owner_client_id
    SegmentAttributes attributes;

    static HandoffSegment FromSegment(const Segment& segment, const UUID& owner,
                                      SegmentStatus status);
    Segment ToSegment() const;
    UUID Owner() const { return info.owner_client_id.value_or(UUID{0, 0}); }
    uint64_t Base() const { return info.base.value_or(0); }
};

// A file of the snapshot, verified by size and CRC32C.
struct BlobInfo {
    std::string key;
    uint64_t bytes{0};
    uint32_t crc32c{0};
};

struct QuarantineRange {
    uint32_t segment_index{0};
    uint64_t address{0};
    uint64_t size{0};
    // How long the old master would still have held the range back, counted
    // from the export. The restore holds it this long after the restore ends
    // (the gap between export and restore only adds to it).
    uint64_t hold_ms{0};
    YLT_REFL(QuarantineRange, segment_index, address, size, hold_ms);
};

// Fingerprints of the struct_pack layouts the snapshot files were written
// with. A reader built from a different layout rejects the snapshot before
// decoding anything.
struct CodecFingerprint {
    uint32_t object_entry{0};
    uint32_t segments{0};
    uint32_t quarantine{0};
    bool operator==(const CodecFingerprint&) const = default;
};
CodecFingerprint CurrentCodecFingerprint();

struct WriterStats {
    uint64_t objects{0};
    uint64_t replicas{0};
    uint64_t skipped_processing{0};
    uint64_t skipped_no_exportable_replica{0};
    uint64_t dropped_non_memory_replicas{0};
    uint64_t dropped_incomplete_replicas{0};
    uint64_t dropped_unknown_segment_replicas{0};
    uint64_t quarantine_ranges{0};
    uint64_t quarantine_bytes{0};
    // Ranges of discarded replicas the old master still held back, and those
    // that could not be carried (their segment is not exported).
    uint64_t discarded_ranges{0};
    uint64_t dropped_discarded_ranges{0};
    uint64_t total_bytes{0};
    uint64_t workers{0};
    uint64_t duration_ms{0};
    uint64_t peak_rss_kib{0};
};

struct Manifest {
    std::string format{kHandoffFormat};
    uint32_t schema_version{kHandoffSchemaVersion};
    std::string snapshot_id;
    int64_t created_at_ms{0};
    CodecFingerprint codec;
    bool multi_tenant{false};
    uint64_t next_replica_id{0};
    uint64_t object_count{0};
    uint64_t replica_count{0};
    // The batch OpLog manifest of the snapshot.
    BlobInfo batch_manifest;
    // In the order of the segments in the batch OpLog segments file.
    std::vector<SegmentAttributes> segments;
    BlobInfo quarantine;
    uint64_t quarantine_ranges{0};
    WriterStats writer;
};

// Maps a memory descriptor (endpoint, address, size) to the index of the
// segment that contains it. Segment name and transport endpoint are both
// accepted as aliases; several segments may share an alias and are told apart
// by address range.
class SegmentLocator {
   public:
    explicit SegmentLocator(const std::vector<HandoffSegment>& segments);
    std::optional<uint32_t> Find(const std::string& endpoint, uint64_t address,
                                 uint64_t size) const;

   private:
    const std::vector<HandoffSegment>& segments_;
    std::unordered_map<std::string, std::vector<uint32_t>> by_alias_;
};

// Checks the segments of a snapshot: a complete identity, non-empty names,
// a non-zero range, unique ids, no CXL, a status of OK / DRAINING / DRAINED,
// and no overlapping ranges among segments sharing an alias. Returns an
// error string on the first violation.
std::optional<std::string> ValidateSegments(
    const std::vector<HandoffSegment>& segments);

// File layout: "MOONCAKE-HANDOFF <schema> <crc32c> <body bytes>\n<json body>".
std::string EncodeManifest(const Manifest& manifest);
tl::expected<Manifest, std::string> DecodeManifest(std::string_view file);

std::vector<uint8_t> EncodeQuarantine(
    const std::vector<QuarantineRange>& ranges);
tl::expected<std::vector<QuarantineRange>, std::string> DecodeQuarantine(
    const std::vector<uint8_t>& bytes);

// Reads a whole file. Error string on I/O failure, or if the file is larger
// than `max_bytes` (checked before anything is allocated).
tl::expected<std::vector<uint8_t>, std::string> ReadFileBytes(
    const std::string& path,
    uint64_t max_bytes = std::numeric_limits<uint64_t>::max());
// Upper bound for the handoff manifest file.
inline constexpr uint64_t kMaxManifestBytes = 64ULL << 20;
// Reads a file of the snapshot and checks its size and CRC32C.
tl::expected<std::vector<uint8_t>, std::string> ReadVerifiedBlob(
    const std::string& dir, const std::string& key, uint64_t bytes,
    uint32_t crc32c);

// What a restore needs from a snapshot directory before it reads the object
// chunks, read and checked: the manifests and what they vouch for (sizes,
// CRCs, keys, counts), the segments with their identity, the weight
// metadata and the quarantined ranges, each inside its segment.
struct SnapshotContents {
    Manifest manifest;
    ha::BatchOpLogSnapshotManifest batch;
    std::vector<HandoffSegment> segments;
    std::optional<WeightMetadataSnapshot> weight_metadata;
    std::vector<QuarantineRange> quarantine;
};
tl::expected<SnapshotContents, std::string> ReadSnapshot(
    const std::string& dir);

}  // namespace mooncake::handoff
