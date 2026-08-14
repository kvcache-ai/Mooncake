#include "ha/snapshot/batch_oplog/writer.h"

#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "crc32c.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "hot_standby_service.h"

namespace mooncake {

namespace {

std::vector<uint8_t> ToBytes(std::string_view value) {
    return {value.begin(), value.end()};
}

tl::expected<void, std::string> VerifyObject(SnapshotObjectStore& object_store,
                                             const std::string& key,
                                             uint64_t expected_size,
                                             uint32_t expected_crc32c) {
    auto inspection = object_store.InspectObject(key);
    if (!inspection) {
        return tl::make_unexpected("Failed to inspect snapshot object " + key +
                                   ": " + inspection.error());
    }
    if (inspection->stored_size != expected_size) {
        return tl::make_unexpected("Snapshot object size mismatch: " + key);
    }
    if (inspection->crc32c) {
        if (*inspection->crc32c != expected_crc32c) {
            return tl::make_unexpected("Snapshot object CRC32C mismatch: " +
                                       key);
        }
        return {};
    }

    std::vector<uint8_t> downloaded;
    auto download = object_store.DownloadBuffer(key, downloaded);
    if (!download) {
        return tl::make_unexpected("Failed to read back snapshot object " +
                                   key + ": " + download.error());
    }
    if (downloaded.size() != expected_size ||
        Crc32cValue(downloaded.data(), downloaded.size()) != expected_crc32c) {
        return tl::make_unexpected("Snapshot object readback mismatch: " + key);
    }
    return {};
}

}  // namespace

tl::expected<std::string, std::string> BatchOpLogSnapshotWriter::Write(
    HotStandbyService& standby, BatchOpLogSnapshotCapture& capture,
    const std::string& snapshot_root, const std::string& snapshot_id,
    size_t chunk_object_count, int64_t created_at_ms) {
    bool capture_active = true;
    bool candidate_touched = false;
    const std::string prefix =
        ha::BuildBatchOpLogSnapshotArtifactPrefix(snapshot_root, snapshot_id);

    auto end_capture = [&] {
        if (capture_active) {
            standby.EndBatchOpLogSnapshotCapture(capture);
            capture_active = false;
        }
    };
    auto fail =
        [&](std::string error) -> tl::expected<std::string, std::string> {
        end_capture();
        if (candidate_touched) {
            auto cleanup = object_store_.DeleteObjectsWithPrefix(prefix);
            if (!cleanup) {
                LOG(WARNING) << "Failed to clean snapshot candidate " << prefix
                             << ": " << cleanup.error();
            }
        }
        return tl::make_unexpected(std::move(error));
    };

    const std::string expected_id_prefix =
        std::to_string(capture.last_included_batch_id) + "-";
    if (prefix.empty() || !snapshot_id.starts_with(expected_id_prefix) ||
        (capture.last_included_seq == 0) !=
            (capture.last_included_batch_id == 0) ||
        capture.producer_view_version < 0 || chunk_object_count == 0 ||
        created_at_ms < 0) {
        return fail("Invalid batch OpLog snapshot writer arguments");
    }

    std::vector<std::string> existing;
    auto list = object_store_.ListObjectsWithPrefix(prefix, existing);
    if (!list) {
        return fail("Failed to check snapshot candidate prefix: " +
                    list.error());
    }
    if (!existing.empty()) {
        return fail("Snapshot candidate prefix already exists: " + prefix);
    }

    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = snapshot_id;
    manifest.last_included_seq = capture.last_included_seq;
    manifest.last_included_batch_id = capture.last_included_batch_id;
    manifest.producer_view_version = capture.producer_view_version;

    const std::string segments_key =
        ha::BuildBatchOpLogSnapshotSegmentsKey(snapshot_root, snapshot_id);
    auto segments = EncodeBatchOpLogSnapshotSegments(capture.segments);
    manifest.segments = {
        .key = segments_key,
        .stored_size = segments.size(),
        .crc32c = Crc32cValue(segments.data(), segments.size()),
    };
    candidate_touched = true;
    auto upload = object_store_.UploadBuffer(segments_key, segments);
    if (!upload) {
        return fail("Failed to upload snapshot segments: " + upload.error());
    }
    std::vector<uint8_t>().swap(segments);

    uint64_t chunk_index = 0;
    while (!capture.done()) {
        std::vector<StandbyObjectEntry> objects;
        if (!standby.CopyNextBatchOpLogSnapshotChunk(chunk_object_count,
                                                     capture, objects)) {
            return fail("Batch OpLog snapshot capture was cancelled");
        }
        if (objects.empty()) {
            return fail("Batch OpLog snapshot capture returned an empty chunk");
        }

        auto encoded =
            EncodeBatchOpLogSnapshotObjectChunk(chunk_index, objects);
        const std::string key = ha::BuildBatchOpLogSnapshotObjectChunkKey(
            snapshot_root, snapshot_id, chunk_index);
        ha::BatchOpLogSnapshotChunkDescriptor descriptor{
            .chunk_index = chunk_index,
            .key = key,
            .object_count = objects.size(),
            .stored_size = encoded.size(),
            .crc32c = Crc32cValue(encoded.data(), encoded.size()),
        };
        std::vector<StandbyObjectEntry>().swap(objects);

        upload = object_store_.UploadBuffer(key, encoded);
        if (!upload) {
            return fail("Failed to upload snapshot object chunk: " +
                        upload.error());
        }
        manifest.object_chunks.push_back(std::move(descriptor));
        ++chunk_index;
    }
    end_capture();

    auto verify =
        VerifyObject(object_store_, manifest.segments.key,
                     manifest.segments.stored_size, manifest.segments.crc32c);
    if (!verify) {
        return fail(std::move(verify.error()));
    }
    for (const auto& chunk : manifest.object_chunks) {
        verify = VerifyObject(object_store_, chunk.key, chunk.stored_size,
                              chunk.crc32c);
        if (!verify) {
            return fail(std::move(verify.error()));
        }
    }

    const std::string manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey(snapshot_root, snapshot_id);
    const std::string manifest_json =
        ha::EncodeBatchOpLogSnapshotManifest(manifest);
    auto manifest_bytes = ToBytes(manifest_json);
    const uint32_t manifest_crc32c =
        Crc32cValue(manifest_bytes.data(), manifest_bytes.size());
    upload = object_store_.UploadBuffer(manifest_key, manifest_bytes);
    if (!upload) {
        return fail("Failed to upload snapshot manifest: " + upload.error());
    }
    verify = VerifyObject(object_store_, manifest_key, manifest_bytes.size(),
                          manifest_crc32c);
    if (!verify) {
        return fail(std::move(verify.error()));
    }

    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.last_included_seq = capture.last_included_seq;
    descriptor.last_included_batch_id = capture.last_included_batch_id;
    descriptor.producer_view_version = capture.producer_view_version;
    descriptor.manifest_key = manifest_key;
    descriptor.manifest_size = manifest_bytes.size();
    descriptor.manifest_crc32c = manifest_crc32c;
    descriptor.created_at_ms = created_at_ms;

    const std::string descriptor_json =
        ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    auto descriptor_bytes = ToBytes(descriptor_json);
    const std::string descriptor_key =
        ha::BuildBatchOpLogSnapshotDescriptorKey(snapshot_root, snapshot_id);
    upload = object_store_.UploadBuffer(descriptor_key, descriptor_bytes);
    if (!upload) {
        return fail("Failed to upload snapshot descriptor: " + upload.error());
    }
    verify = VerifyObject(
        object_store_, descriptor_key, descriptor_bytes.size(),
        Crc32cValue(descriptor_bytes.data(), descriptor_bytes.size()));
    if (!verify) {
        return fail(std::move(verify.error()));
    }
    return descriptor_json;
}

}  // namespace mooncake
