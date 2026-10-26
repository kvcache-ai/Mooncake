#include "handoff/handoff_format.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <boost/functional/hash.hpp>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "crc32c.h"
#include "ha/snapshot/batch_oplog/codec.h"

namespace mooncake::handoff {

namespace {

constexpr char kMagic[] = "MOONCAKE-HANDOFF";

Json::Value EncodeBlob(const BlobInfo& blob) {
    Json::Value v(Json::objectValue);
    v["key"] = blob.key;
    v["bytes"] = Json::UInt64(blob.bytes);
    v["crc32c"] = Json::UInt(blob.crc32c);
    return v;
}

bool DecodeBlob(const Json::Value& v, BlobInfo& blob) {
    if (!v.isObject() || !v["key"].isString() || !v["bytes"].isUInt64() ||
        !v["crc32c"].isUInt()) {
        return false;
    }
    blob.key = v["key"].asString();
    blob.bytes = v["bytes"].asUInt64();
    blob.crc32c = v["crc32c"].asUInt();
    return !blob.key.empty();
}

bool GetU64(const Json::Value& root, const char* field, uint64_t& out) {
    if (!root.isMember(field) || !root[field].isUInt64()) return false;
    out = root[field].asUInt64();
    return true;
}

bool GetStr(const Json::Value& root, const char* field, std::string& out) {
    if (!root.isMember(field) || !root[field].isString()) return false;
    out = root[field].asString();
    return true;
}

// Segment statuses a snapshot may carry.
const char* RestorableStatusName(SegmentStatus status) {
    switch (status) {
        case SegmentStatus::OK:
            return "OK";
        case SegmentStatus::DRAINING:
            return "DRAINING";
        case SegmentStatus::DRAINED:
            return "DRAINED";
        default:
            return nullptr;
    }
}

bool ParseRestorableStatus(const std::string& name, SegmentStatus& status) {
    for (const auto candidate :
         {SegmentStatus::OK, SegmentStatus::DRAINING, SegmentStatus::DRAINED}) {
        if (name == RestorableStatusName(candidate)) {
            status = candidate;
            return true;
        }
    }
    return false;
}

}  // namespace

HandoffSegment HandoffSegment::FromSegment(const Segment& segment,
                                           const UUID& owner,
                                           SegmentStatus status) {
    HandoffSegment out;
    out.info.segment_name = segment.name;
    out.info.transport_endpoint = segment.te_endpoint;
    out.info.capacity = segment.size;
    out.info.is_memory_segment = true;
    out.info.segment_id = segment.id;
    out.info.base = segment.base;
    out.info.owner_client_id = owner;
    out.attributes.protocol = segment.protocol;
    out.attributes.host_id = segment.host_id;
    out.attributes.status = status;
    if (segment.locality) {
        out.attributes.locality = *segment.locality;
    }
    return out;
}

Segment HandoffSegment::ToSegment() const {
    Segment segment;
    segment.id = info.segment_id.value_or(UUID{0, 0});
    segment.name = info.segment_name;
    segment.base = static_cast<uintptr_t>(Base());
    segment.size = static_cast<size_t>(info.capacity);
    segment.te_endpoint = info.transport_endpoint;
    segment.protocol = attributes.protocol;
    segment.host_id = attributes.host_id;
    if (attributes.locality) {
        segment.locality = *attributes.locality;
    }
    return segment;
}

CodecFingerprint CurrentCodecFingerprint() {
    CodecFingerprint fp;
    fp.object_entry = struct_pack::get_type_code<StandbyObjectEntry>();
    fp.segments = struct_pack::get_type_code<std::vector<StandbySegmentInfo>>();
    fp.quarantine = struct_pack::get_type_code<std::vector<QuarantineRange>>();
    fp.local_disk =
        struct_pack::get_type_code<std::vector<LocalSsdHandoffClient>>();
    return fp;
}

SegmentLocator::SegmentLocator(const std::vector<HandoffSegment>& segments)
    : segments_(segments) {
    for (uint32_t i = 0; i < segments.size(); ++i) {
        const auto& info = segments[i].info;
        by_alias_[info.transport_endpoint].push_back(i);
        if (info.segment_name != info.transport_endpoint) {
            by_alias_[info.segment_name].push_back(i);
        }
    }
}

std::optional<uint32_t> SegmentLocator::Find(const std::string& endpoint,
                                             uint64_t address,
                                             uint64_t size) const {
    const auto it = by_alias_.find(endpoint);
    if (it == by_alias_.end() || size == 0) {
        return std::nullopt;
    }
    for (const uint32_t index : it->second) {
        const uint64_t base = segments_[index].Base();
        const uint64_t capacity = segments_[index].info.capacity;
        if (address >= base && address - base < capacity &&
            size <= capacity - (address - base)) {
            return index;
        }
    }
    return std::nullopt;
}

std::optional<std::string> ValidateSegments(
    const std::vector<HandoffSegment>& segments) {
    std::unordered_set<UUID, boost::hash<UUID>> ids;
    for (const auto& s : segments) {
        const auto& info = s.info;
        if (!info.segment_id || !info.base || !info.owner_client_id ||
            *info.owner_client_id == UUID{0, 0}) {
            return "segment without identity: " + info.segment_name;
        }
        if (info.segment_name.empty() || info.transport_endpoint.empty() ||
            !info.is_memory_segment || *info.base == 0 || info.capacity == 0 ||
            *info.base > UINT64_MAX - info.capacity) {
            return "invalid segment " + info.segment_name;
        }
        if (s.attributes.protocol == "cxl") {
            return "cxl segment not supported: " + info.segment_name;
        }
        if (!ids.insert(*info.segment_id).second) {
            return "duplicate segment id for " + info.segment_name;
        }
        if (RestorableStatusName(s.attributes.status) == nullptr) {
            return "segment status not restorable: " + info.segment_name;
        }
    }
    for (size_t i = 0; i < segments.size(); ++i) {
        for (size_t j = i + 1; j < segments.size(); ++j) {
            const auto& a = segments[i].info;
            const auto& b = segments[j].info;
            const bool share_alias =
                a.transport_endpoint == b.transport_endpoint ||
                a.segment_name == b.segment_name ||
                a.transport_endpoint == b.segment_name ||
                a.segment_name == b.transport_endpoint;
            if (share_alias && *a.base < *b.base + b.capacity &&
                *b.base < *a.base + a.capacity) {
                return "overlapping segments " + a.segment_name + " and " +
                       b.segment_name;
            }
        }
    }
    return std::nullopt;
}

std::string EncodeManifest(const Manifest& m) {
    Json::Value root(Json::objectValue);
    root["format"] = m.format;
    root["schema_version"] = Json::UInt(m.schema_version);
    root["snapshot_id"] = m.snapshot_id;
    root["created_at_ms"] = Json::Int64(m.created_at_ms);
    Json::Value codec(Json::objectValue);
    codec["object_entry"] = Json::UInt(m.codec.object_entry);
    codec["segments"] = Json::UInt(m.codec.segments);
    codec["quarantine"] = Json::UInt(m.codec.quarantine);
    if (m.schema_version >= 2) {
        codec["local_disk"] = Json::UInt(m.codec.local_disk);
    }
    root["codec"] = codec;
    root["multi_tenant"] = m.multi_tenant;
    root["next_replica_id"] = Json::UInt64(m.next_replica_id);
    root["object_count"] = Json::UInt64(m.object_count);
    root["replica_count"] = Json::UInt64(m.replica_count);
    root["batch_manifest"] = EncodeBlob(m.batch_manifest);
    Json::Value segments(Json::arrayValue);
    for (const auto& s : m.segments) {
        Json::Value v(Json::objectValue);
        v["protocol"] = s.protocol;
        v["host_id"] = s.host_id;
        const char* status = RestorableStatusName(s.status);
        v["status"] = status != nullptr ? status : "INVALID";
        if (s.locality) {
            Json::Value locality(Json::objectValue);
            locality["numa_node"] = Json::Int(s.locality->numa_node);
            Json::Value hcas(Json::arrayValue);
            for (const auto& hca : s.locality->hca_names) hcas.append(hca);
            locality["hca_names"] = hcas;
            v["locality"] = locality;
        }
        segments.append(v);
    }
    root["segments"] = segments;
    root["quarantine"] = EncodeBlob(m.quarantine);
    root["quarantine_ranges"] = Json::UInt64(m.quarantine_ranges);
    if (m.schema_version >= 2) {
        root["local_disk"] = EncodeBlob(m.local_disk.value_or(BlobInfo{}));
        root["local_disk_clients"] = Json::UInt64(m.local_disk_clients);
        root["local_disk_replicas"] = Json::UInt64(m.local_disk_replicas);
    }
    Json::Value w(Json::objectValue);
    w["objects"] = Json::UInt64(m.writer.objects);
    w["replicas"] = Json::UInt64(m.writer.replicas);
    w["skipped_processing"] = Json::UInt64(m.writer.skipped_processing);
    w["skipped_no_exportable_replica"] =
        Json::UInt64(m.writer.skipped_no_exportable_replica);
    w["dropped_non_memory_replicas"] =
        Json::UInt64(m.writer.dropped_non_memory_replicas);
    w["dropped_incomplete_replicas"] =
        Json::UInt64(m.writer.dropped_incomplete_replicas);
    w["dropped_unknown_segment_replicas"] =
        Json::UInt64(m.writer.dropped_unknown_segment_replicas);
    w["local_disk_clients"] = Json::UInt64(m.writer.local_disk_clients);
    w["local_disk_replicas"] = Json::UInt64(m.writer.local_disk_replicas);
    w["disk_only_objects"] = Json::UInt64(m.writer.disk_only_objects);
    w["dropped_local_disk_replicas"] =
        Json::UInt64(m.writer.dropped_local_disk_replicas);
    w["quarantine_ranges"] = Json::UInt64(m.writer.quarantine_ranges);
    w["quarantine_bytes"] = Json::UInt64(m.writer.quarantine_bytes);
    w["discarded_ranges"] = Json::UInt64(m.writer.discarded_ranges);
    w["dropped_discarded_ranges"] =
        Json::UInt64(m.writer.dropped_discarded_ranges);
    w["total_bytes"] = Json::UInt64(m.writer.total_bytes);
    w["workers"] = Json::UInt64(m.writer.workers);
    w["duration_ms"] = Json::UInt64(m.writer.duration_ms);
    w["peak_rss_kib"] = Json::UInt64(m.writer.peak_rss_kib);
    root["writer"] = w;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = " ";
    const std::string body = Json::writeString(builder, root);
    const uint32_t crc = Crc32cValue(body.data(), body.size());
    return std::string(kMagic) + " " + std::to_string(m.schema_version) + " " +
           std::to_string(crc) + " " + std::to_string(body.size()) + "\n" +
           body;
}

tl::expected<Manifest, std::string> DecodeManifest(std::string_view file) {
    const size_t newline = file.find('\n');
    if (newline == std::string_view::npos) {
        return tl::make_unexpected("manifest header missing");
    }
    const std::string_view header = file.substr(0, newline);
    const std::string_view body = file.substr(newline + 1);
    // Header: magic schema crc size.
    std::vector<std::string_view> parts;
    size_t pos = 0;
    while (pos <= header.size()) {
        const size_t space = header.find(' ', pos);
        const size_t end =
            space == std::string_view::npos ? header.size() : space;
        parts.push_back(header.substr(pos, end - pos));
        pos = end + 1;
    }
    if (parts.size() != 4 || parts[0] != kMagic) {
        return tl::make_unexpected("manifest magic mismatch");
    }
    uint64_t schema = 0, crc = 0, size = 0;
    auto parse = [](std::string_view s, uint64_t& v) {
        auto r = std::from_chars(s.data(), s.data() + s.size(), v);
        return r.ec == std::errc() && r.ptr == s.data() + s.size();
    };
    if (!parse(parts[1], schema) || !parse(parts[2], crc) ||
        !parse(parts[3], size)) {
        return tl::make_unexpected("manifest header malformed");
    }
    if (schema < kHandoffMinSchemaVersion || schema > kHandoffSchemaVersion) {
        return tl::make_unexpected("unsupported handoff schema " +
                                   std::to_string(schema));
    }
    if (size != body.size()) {
        return tl::make_unexpected("manifest body size mismatch");
    }
    if (Crc32cValue(body.data(), body.size()) != crc) {
        return tl::make_unexpected("manifest crc mismatch");
    }

    Json::CharReaderBuilder builder;
    builder["failIfExtra"] = true;
    builder["rejectDupKeys"] = true;
    builder["strictRoot"] = true;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errors;
    if (!reader->parse(body.data(), body.data() + body.size(), &root,
                       &errors) ||
        !root.isObject()) {
        return tl::make_unexpected("manifest json invalid: " + errors);
    }

    Manifest m;
    uint64_t u = 0;
    if (!GetStr(root, "format", m.format) || m.format != kHandoffFormat) {
        return tl::make_unexpected("manifest format mismatch");
    }
    if (!GetU64(root, "schema_version", u) || u != schema) {
        return tl::make_unexpected("manifest schema mismatch");
    }
    m.schema_version = static_cast<uint32_t>(u);
    const Json::Value& codec = root["codec"];
    if (!GetStr(root, "snapshot_id", m.snapshot_id) ||
        !root["created_at_ms"].isInt64() || !codec.isObject() ||
        !codec["object_entry"].isUInt() || !codec["segments"].isUInt() ||
        !codec["quarantine"].isUInt() || !root["multi_tenant"].isBool() ||
        !GetU64(root, "next_replica_id", m.next_replica_id) ||
        !GetU64(root, "object_count", m.object_count) ||
        !GetU64(root, "replica_count", m.replica_count) ||
        !DecodeBlob(root["batch_manifest"], m.batch_manifest) ||
        !root["segments"].isArray() ||
        !DecodeBlob(root["quarantine"], m.quarantine) ||
        !GetU64(root, "quarantine_ranges", m.quarantine_ranges)) {
        return tl::make_unexpected("manifest field missing or mistyped");
    }
    m.created_at_ms = root["created_at_ms"].asInt64();
    m.codec.object_entry = codec["object_entry"].asUInt();
    m.codec.segments = codec["segments"].asUInt();
    m.codec.quarantine = codec["quarantine"].asUInt();
    m.multi_tenant = root["multi_tenant"].asBool();
    if (m.schema_version >= 2) {
        BlobInfo local_disk;
        if (!codec["local_disk"].isUInt() ||
            !DecodeBlob(root["local_disk"], local_disk) ||
            !GetU64(root, "local_disk_clients", m.local_disk_clients) ||
            !GetU64(root, "local_disk_replicas", m.local_disk_replicas)) {
            return tl::make_unexpected("manifest field missing or mistyped");
        }
        m.codec.local_disk = codec["local_disk"].asUInt();
        m.local_disk = std::move(local_disk);
    } else {
        // Schema 1 has no L4 file; nothing to check its layout against.
        m.codec.local_disk = CurrentCodecFingerprint().local_disk;
    }
    for (const auto& v : root["segments"]) {
        SegmentAttributes s;
        std::string status;
        if (!v.isObject() || !GetStr(v, "protocol", s.protocol) ||
            !GetStr(v, "host_id", s.host_id) || !GetStr(v, "status", status) ||
            !ParseRestorableStatus(status, s.status)) {
            return tl::make_unexpected("manifest segment entry invalid");
        }
        if (v.isMember("locality")) {
            const Json::Value& locality = v["locality"];
            if (!locality.isObject() || !locality["numa_node"].isInt() ||
                locality["numa_node"].asInt() < 0 ||
                !locality["hca_names"].isArray()) {
                return tl::make_unexpected("manifest segment entry invalid");
            }
            SegmentLocality parsed;
            parsed.numa_node = locality["numa_node"].asInt();
            for (const auto& hca : locality["hca_names"]) {
                if (!hca.isString()) {
                    return tl::make_unexpected(
                        "manifest segment entry invalid");
                }
                parsed.hca_names.push_back(hca.asString());
            }
            s.locality = std::move(parsed);
        }
        m.segments.push_back(std::move(s));
    }
    const Json::Value& w = root["writer"];
    if (w.isObject()) {
        GetU64(w, "objects", m.writer.objects);
        GetU64(w, "replicas", m.writer.replicas);
        GetU64(w, "skipped_processing", m.writer.skipped_processing);
        GetU64(w, "skipped_no_exportable_replica",
               m.writer.skipped_no_exportable_replica);
        GetU64(w, "dropped_non_memory_replicas",
               m.writer.dropped_non_memory_replicas);
        GetU64(w, "dropped_incomplete_replicas",
               m.writer.dropped_incomplete_replicas);
        GetU64(w, "dropped_unknown_segment_replicas",
               m.writer.dropped_unknown_segment_replicas);
        GetU64(w, "local_disk_clients", m.writer.local_disk_clients);
        GetU64(w, "local_disk_replicas", m.writer.local_disk_replicas);
        GetU64(w, "disk_only_objects", m.writer.disk_only_objects);
        GetU64(w, "dropped_local_disk_replicas",
               m.writer.dropped_local_disk_replicas);
        GetU64(w, "quarantine_ranges", m.writer.quarantine_ranges);
        GetU64(w, "quarantine_bytes", m.writer.quarantine_bytes);
        GetU64(w, "discarded_ranges", m.writer.discarded_ranges);
        GetU64(w, "dropped_discarded_ranges",
               m.writer.dropped_discarded_ranges);
        GetU64(w, "total_bytes", m.writer.total_bytes);
        GetU64(w, "workers", m.writer.workers);
        GetU64(w, "duration_ms", m.writer.duration_ms);
        GetU64(w, "peak_rss_kib", m.writer.peak_rss_kib);
    }
    return m;
}

std::vector<uint8_t> EncodeLocalDiskClients(
    const std::vector<LocalSsdHandoffClient>& clients) {
    return struct_pack::serialize<std::vector<uint8_t>>(clients);
}

tl::expected<std::vector<LocalSsdHandoffClient>, std::string>
DecodeLocalDiskClients(const std::vector<uint8_t>& bytes) {
    std::vector<LocalSsdHandoffClient> clients;
    if (struct_pack::deserialize_to(clients, bytes) != struct_pack::errc::ok) {
        return tl::make_unexpected("local disk clients decode failed");
    }
    return clients;
}

std::vector<uint8_t> EncodeQuarantine(
    const std::vector<QuarantineRange>& ranges) {
    return struct_pack::serialize<std::vector<uint8_t>>(ranges);
}

tl::expected<std::vector<QuarantineRange>, std::string> DecodeQuarantine(
    const std::vector<uint8_t>& bytes) {
    std::vector<QuarantineRange> ranges;
    if (struct_pack::deserialize_to(ranges, bytes) != struct_pack::errc::ok) {
        return tl::make_unexpected("quarantine decode failed");
    }
    return ranges;
}

tl::expected<std::vector<uint8_t>, std::string> ReadFileBytes(
    const std::string& path, uint64_t max_bytes) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return tl::make_unexpected("open " + path + ": " +
                                   std::strerror(errno));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        const std::string err = std::strerror(errno);
        ::close(fd);
        return tl::make_unexpected("fstat " + path + ": " + err);
    }
    // Checked before allocating: a damaged or replaced file must not make
    // the restore allocate its whole size.
    if (st.st_size < 0 || static_cast<uint64_t>(st.st_size) > max_bytes) {
        ::close(fd);
        return tl::make_unexpected("size mismatch for " + path + ": file has " +
                                   std::to_string(st.st_size) +
                                   " bytes, at most " +
                                   std::to_string(max_bytes) + " expected");
    }
    std::vector<uint8_t> data(static_cast<size_t>(st.st_size));
    size_t done = 0;
    while (done < data.size()) {
        const ssize_t n = ::read(fd, data.data() + done, data.size() - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            const std::string err =
                n == 0 ? "short read" : std::strerror(errno);
            ::close(fd);
            return tl::make_unexpected("read " + path + ": " + err);
        }
        done += static_cast<size_t>(n);
    }
    ::close(fd);
    return data;
}

tl::expected<std::vector<uint8_t>, std::string> ReadVerifiedBlob(
    const std::string& dir, const std::string& key, uint64_t bytes,
    uint32_t crc32c) {
    auto data = ReadFileBytes(dir + "/" + key, bytes);
    if (!data) return data;
    if (data->size() != bytes) {
        return tl::make_unexpected("size mismatch for " + key);
    }
    if (Crc32cValue(data->data(), data->size()) != crc32c) {
        return tl::make_unexpected("crc mismatch for " + key);
    }
    return data;
}

tl::expected<SnapshotContents, std::string> ReadSnapshot(
    const std::string& dir) {
    SnapshotContents contents;
    auto& manifest = contents.manifest;
    {
        auto bytes =
            ReadFileBytes(dir + "/" + kHandoffManifestFile, kMaxManifestBytes);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = DecodeManifest(std::string_view(
            reinterpret_cast<const char*>(bytes->data()), bytes->size()));
        if (!decoded) return tl::make_unexpected(decoded.error());
        manifest = std::move(*decoded);
    }
    if (manifest.codec != CurrentCodecFingerprint()) {
        return tl::make_unexpected("codec fingerprint mismatch");
    }
    const auto read_blob = [&dir](const std::string& key, uint64_t bytes,
                                  uint32_t crc32c) {
        return ReadVerifiedBlob(dir, key, bytes, crc32c);
    };
    // The batch OpLog manifest indexes the segments, weights and chunks.
    auto& batch = contents.batch;
    {
        auto bytes = read_blob(manifest.batch_manifest.key,
                               manifest.batch_manifest.bytes,
                               manifest.batch_manifest.crc32c);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = ha::DecodeBatchOpLogSnapshotManifest(std::string_view(
            reinterpret_cast<const char*>(bytes->data()), bytes->size()));
        if (!decoded) return tl::make_unexpected(decoded.error());
        batch = std::move(*decoded);
    }
    const std::string root(kHandoffSnapshotRoot);
    if (batch.snapshot_id != manifest.snapshot_id ||
        batch.segments.key !=
            ha::BuildBatchOpLogSnapshotSegmentsKey(root, batch.snapshot_id)) {
        return tl::make_unexpected("batch manifest does not match");
    }
    for (size_t i = 0; i < batch.object_chunks.size(); ++i) {
        const auto& chunk = batch.object_chunks[i];
        if (chunk.chunk_index != i ||
            chunk.key != ha::BuildBatchOpLogSnapshotObjectChunkKey(
                             root, batch.snapshot_id, i)) {
            return tl::make_unexpected("chunk index not dense");
        }
    }
    auto& segments = contents.segments;
    {
        auto bytes = read_blob(batch.segments.key, batch.segments.stored_size,
                               batch.segments.crc32c);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = DecodeBatchOpLogSnapshotSegments(*bytes);
        if (!decoded) return tl::make_unexpected(decoded.error());
        if (decoded->size() != manifest.segments.size()) {
            return tl::make_unexpected("segment count mismatch");
        }
        for (size_t i = 0; i < decoded->size(); ++i) {
            segments.push_back(
                {std::move((*decoded)[i]), manifest.segments[i]});
        }
    }
    if (auto invalid = ValidateSegments(segments)) {
        return tl::make_unexpected(*invalid);
    }
    if (batch.weight_metadata) {
        auto bytes = read_blob(batch.weight_metadata->key,
                               batch.weight_metadata->stored_size,
                               batch.weight_metadata->crc32c);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = DecodeBatchOpLogSnapshotWeightMetadata(*bytes);
        if (!decoded) return tl::make_unexpected(decoded.error());
        if (!ValidateWeightMetadataSnapshot(*decoded)) {
            return tl::make_unexpected("invalid weight metadata");
        }
        contents.weight_metadata = std::move(*decoded);
    }
    {
        auto bytes =
            read_blob(manifest.quarantine.key, manifest.quarantine.bytes,
                      manifest.quarantine.crc32c);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = DecodeQuarantine(*bytes);
        if (!decoded) return tl::make_unexpected(decoded.error());
        contents.quarantine = std::move(*decoded);
        if (contents.quarantine.size() != manifest.quarantine_ranges) {
            return tl::make_unexpected("quarantine range count mismatch");
        }
    }
    if (manifest.local_disk) {
        auto bytes =
            read_blob(manifest.local_disk->key, manifest.local_disk->bytes,
                      manifest.local_disk->crc32c);
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto decoded = DecodeLocalDiskClients(*bytes);
        if (!decoded) return tl::make_unexpected(decoded.error());
        contents.local_disk_clients = std::move(*decoded);
        if (contents.local_disk_clients.size() != manifest.local_disk_clients) {
            return tl::make_unexpected("local disk client count mismatch");
        }
    }
    if (manifest.local_disk_replicas > 0 &&
        contents.local_disk_clients.empty()) {
        return tl::make_unexpected("LOCAL_DISK replicas without owners");
    }
    {
        // Each carried registration belongs to the owner of a restored
        // segment (its liveness record is the one the segments get), once.
        std::unordered_map<UUID, bool, boost::hash<UUID>> owners;
        for (const auto& segment : segments) {
            owners.emplace(segment.Owner(), false);
        }
        for (const auto& client : contents.local_disk_clients) {
            const auto it = owners.find(client.client_id);
            if (client.client_id == UUID{0, 0} || it == owners.end() ||
                it->second || client.total_capacity_bytes < 0) {
                return tl::make_unexpected("invalid local disk client entry");
            }
            it->second = true;
        }
    }
    const SegmentLocator locator(segments);
    for (const auto& range : contents.quarantine) {
        if (range.segment_index >= segments.size() ||
            locator.Find(segments[range.segment_index].info.transport_endpoint,
                         range.address, range.size) != range.segment_index) {
            return tl::make_unexpected("quarantine range outside segment");
        }
    }
    return contents;
}

}  // namespace mooncake::handoff
