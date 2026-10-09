// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/buffers.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <sstream>
#include <unordered_set>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent::ub {
namespace {

using json = nlohmann::json;

bool checkedEnd(uint64_t base, uint64_t length, uint64_t& end) {
    if (length > std::numeric_limits<uint64_t>::max() - base) return false;
    end = base + length;
    return true;
}

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 17) ^ (steady >> 11)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

}  // namespace

Status encodeBufferMetadata(const UbBufferMetadata& metadata,
                            std::string& encoded) {
    if (metadata.schema_version != UbBufferMetadata::kSchemaVersion ||
        metadata.generation == 0 || metadata.length == 0 ||
        metadata.segments.empty()) {
        return Status::InvalidArgument("Invalid UB buffer metadata" LOC_MARK);
    }

    uint64_t metadata_end = 0;
    if (!checkedEnd(metadata.base, metadata.length, metadata_end)) {
        return Status::InvalidArgument(
            "UB buffer metadata range overflows" LOC_MARK);
    }
    json segments = json::array();
    std::unordered_set<Topology::NicID> topology_ids;
    for (const auto& segment : metadata.segments) {
        if (segment.topology_id < 0 || segment.device_name.empty() ||
            segment.eid.empty() || segment.descriptor.hex.empty() ||
            segment.descriptor.urma_abi_size == 0 ||
            !topology_ids.insert(segment.topology_id).second) {
            return Status::InvalidArgument(
                "Incomplete UB segment metadata" LOC_MARK);
        }
        segments.push_back(
            {{"topology_id", segment.topology_id},
             {"device_name", segment.device_name},
             {"eid", segment.eid},
             {"eid_index", segment.eid_index},
             {"descriptor",
              {{"schema_version", segment.descriptor.schema_version},
               {"urma_api_version", segment.descriptor.urma_api_version},
               {"urma_abi_size", segment.descriptor.urma_abi_size},
               {"hex", segment.descriptor.hex}}}});
    }
    encoded = json{{"schema_version", metadata.schema_version},
                   {"generation", metadata.generation},
                   {"base", metadata.base},
                   {"length", metadata.length},
                   {"location", metadata.location},
                   {"permission", static_cast<int>(metadata.permission)},
                   {"segments", std::move(segments)}}
                  .dump();
    return Status::OK();
}

Status decodeBufferMetadata(std::string_view encoded,
                            UbBufferMetadata& metadata) {
    try {
        const auto value = json::parse(encoded);
        if (!value.is_object() || !value.contains("schema_version") ||
            !value.contains("generation") || !value.contains("base") ||
            !value.contains("length") || !value.contains("permission") ||
            !value.contains("segments")) {
            return Status::MalformedJson(
                "Missing required UB buffer metadata field" LOC_MARK);
        }

        UbBufferMetadata parsed;
        parsed.schema_version = value.at("schema_version").get<uint32_t>();
        if (parsed.schema_version != UbBufferMetadata::kSchemaVersion) {
            return Status::InvalidMetadataType(
                "Unsupported UB buffer metadata version" LOC_MARK);
        }
        parsed.generation = value.at("generation").get<uint64_t>();
        parsed.base = value.at("base").get<uint64_t>();
        parsed.length = value.at("length").get<uint64_t>();
        parsed.location = value.value("location", "");
        const int permission = value.at("permission").get<int>();
        if (permission < static_cast<int>(kLocalReadWrite) ||
            permission > static_cast<int>(kGlobalReadWrite)) {
            return Status::InvalidMetadataType(
                "Invalid UB buffer permission" LOC_MARK);
        }
        parsed.permission = static_cast<Permission>(permission);
        if (parsed.generation == 0 || parsed.length == 0 ||
            !value.at("segments").is_array() || value.at("segments").empty()) {
            return Status::InvalidMetadataType(
                "Invalid UB buffer metadata values" LOC_MARK);
        }
        uint64_t metadata_end = 0;
        if (!checkedEnd(parsed.base, parsed.length, metadata_end)) {
            return Status::InvalidMetadataType(
                "UB buffer metadata range overflows" LOC_MARK);
        }

        std::unordered_set<Topology::NicID> topology_ids;
        for (const auto& item : value.at("segments")) {
            UbBufferSegmentMetadata segment;
            segment.topology_id = item.at("topology_id").get<int>();
            segment.device_name = item.at("device_name").get<std::string>();
            segment.eid = item.at("eid").get<std::string>();
            segment.eid_index = item.at("eid_index").get<int>();
            const auto& descriptor = item.at("descriptor");
            segment.descriptor.schema_version =
                descriptor.at("schema_version").get<uint32_t>();
            segment.descriptor.urma_api_version =
                descriptor.at("urma_api_version").get<uint32_t>();
            segment.descriptor.urma_abi_size =
                descriptor.at("urma_abi_size").get<uint32_t>();
            segment.descriptor.hex = descriptor.at("hex").get<std::string>();
            if (segment.topology_id < 0 || segment.device_name.empty() ||
                segment.eid.empty() || segment.descriptor.hex.empty() ||
                segment.descriptor.schema_version !=
                    SegmentDescriptor::kSchemaVersion ||
                segment.descriptor.urma_abi_size == 0 ||
                !topology_ids.insert(segment.topology_id).second) {
                return Status::InvalidMetadataType(
                    "Invalid UB segment descriptor envelope" LOC_MARK);
            }
            parsed.segments.push_back(std::move(segment));
        }
        metadata = std::move(parsed);
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::MalformedJson(std::string("Malformed UB metadata: ") +
                                     error.what() + LOC_MARK);
    }
}

UbBufferManager::UbBufferManager(std::shared_ptr<UrmaAdapter> adapter,
                                 std::vector<UbContextPtr> contexts)
    : adapter_(std::move(adapter)), contexts_(std::move(contexts)) {
    next_generation_.store(generationSeed(), std::memory_order_relaxed);
    for (const auto& context : contexts_) {
        if (context) context_by_topology_id_[context->topologyId()] = context;
    }
}

UbBufferManager::~UbBufferManager() { (void)clear(); }

bool UbBufferManager::AddressRange::contains(uint64_t address,
                                             size_t size) const {
    uint64_t this_end = 0;
    uint64_t query_end = 0;
    return checkedEnd(base, length, this_end) &&
           checkedEnd(address, static_cast<uint64_t>(size), query_end) &&
           address >= base && query_end <= this_end;
}

size_t UbBufferManager::ImportKeyHash::operator()(
    const ImportKey& key) const noexcept {
    size_t seed = std::hash<int>{}(key.local_topology_id);
    auto combine = [&seed](size_t value) {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    };
    combine(std::hash<SegmentID>{}(key.remote_segment_id));
    combine(std::hash<int>{}(key.remote_topology_id));
    combine(std::hash<uint64_t>{}(key.buffer_base));
    combine(std::hash<uint64_t>{}(key.generation));
    return seed;
}

uint32_t UbBufferManager::segmentAccess(Permission permission) {
    switch (permission) {
        case kGlobalReadOnly:
            return SEGMENT_ACCESS_READ;
        case kLocalReadWrite:
            return SEGMENT_ACCESS_LOCAL_ONLY;
        case kGlobalReadWrite:
        default:
            return SEGMENT_ACCESS_READ | SEGMENT_ACCESS_WRITE;
    }
}

bool UbBufferManager::permissionAllows(Permission permission,
                                       Request::OpCode opcode) {
    if (permission == kLocalReadWrite) return false;
    return opcode == Request::READ || permission == kGlobalReadWrite;
}

UbContextPtr UbBufferManager::findContext(Topology::NicID topology_id) const {
    auto it = context_by_topology_id_.find(topology_id);
    return it == context_by_topology_id_.end() ? nullptr : it->second;
}

Status UbBufferManager::addBufferInternal(BufferDesc& desc,
                                          const MemoryOptions& options) {
    if (desc.length == 0 || contexts_.empty()) {
        return Status::InvalidArgument(
            "Cannot register an empty UB buffer or without contexts" LOC_MARK);
    }
    AddressRange range{desc.addr, desc.length};
    uint64_t ignored = 0;
    if (!checkedEnd(range.base, range.length, ignored)) {
        return Status::InvalidArgument("UB buffer address overflow" LOC_MARK);
    }

    LocalRecord record;
    record.options = options;
    record.generation =
        next_generation_.fetch_add(1, std::memory_order_relaxed);
    if (record.generation == 0) {
        record.generation =
            next_generation_.fetch_add(1, std::memory_order_relaxed);
    }

    UbBufferMetadata metadata;
    metadata.generation = record.generation;
    metadata.base = desc.addr;
    metadata.length = desc.length;
    metadata.location = desc.location;
    metadata.permission = options.perm;

    SegmentOptions segment_options;
    segment_options.access = segmentAccess(options.perm);
    for (const auto& context : contexts_) {
        if (!context || !context->active()) continue;
        LocalSegmentPtr segment;
        auto status = adapter_->registerLocalSegment(context->handle(),
                                                     desc.addr, desc.length,
                                                     segment_options, segment);
        if (!status.ok()) {
            if (segment) {
                record.segments.emplace(context->topologyId(),
                                        std::move(segment));
            }
            (void)unregisterRecord(record);
            retainPendingRecord(record);
            return status;
        }
        metadata.segments.push_back(UbBufferSegmentMetadata{
            context->topologyId(), context->deviceInfo().native_device_name,
            context->deviceInfo().eid,
            static_cast<int>(context->deviceInfo().eid_index),
            segment->descriptor()});
        record.segments.emplace(context->topologyId(), std::move(segment));
    }
    if (record.segments.empty()) {
        return Status::DeviceNotFound(
            "No active UB context accepted the buffer" LOC_MARK);
    }

    std::string encoded;
    auto status = encodeBufferMetadata(metadata, encoded);
    if (!status.ok()) {
        (void)unregisterRecord(record);
        retainPendingRecord(record);
        return status;
    }

    {
        std::unique_lock<std::shared_mutex> lock(local_mutex_);
        auto next = local_buffers_.lower_bound(range);
        if ((next != local_buffers_.end() &&
             next->first.base < range.base + range.length) ||
            (next != local_buffers_.begin() &&
             std::prev(next)->first.base + std::prev(next)->first.length >
                 range.base)) {
            lock.unlock();
            (void)unregisterRecord(record);
            retainPendingRecord(record);
            return Status::InvalidArgument(
                "Overlapping UB buffer registration" LOC_MARK);
        }
        local_buffers_.emplace(range, std::move(record));
    }
    desc.transport_attrs[TransportType::UB] = std::move(encoded);
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::UB) == desc.transports.end()) {
        desc.transports.push_back(TransportType::UB);
    }
    return Status::OK();
}

Status UbBufferManager::addBuffer(BufferDesc& desc,
                                  const MemoryOptions& options) {
    return addBufferInternal(desc, options);
}

Status UbBufferManager::addBuffers(std::vector<BufferDesc>& descs,
                                   const MemoryOptions& options) {
    std::vector<BufferDesc*> added;
    added.reserve(descs.size());
    for (auto& desc : descs) {
        auto status = addBufferInternal(desc, options);
        if (!status.ok()) {
            for (auto it = added.rbegin(); it != added.rend(); ++it) {
                (void)removeBuffer(**it);
            }
            return status;
        }
        added.push_back(&desc);
    }
    return Status::OK();
}

Status UbBufferManager::unregisterRecord(LocalRecord& record) {
    Status first_error = Status::OK();
    for (auto it = record.segments.begin(); it != record.segments.end();) {
        auto status = adapter_->unregisterLocalSegment(it->second);
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            ++it;
            continue;
        }
        if (it->second) {
            if (first_error.ok()) {
                first_error = Status::InternalError(
                    "URMA adapter retained a local segment after successful "
                    "unregister" LOC_MARK);
            }
            ++it;
            continue;
        }
        it = record.segments.erase(it);
    }
    return first_error;
}

void UbBufferManager::retainPendingRecord(LocalRecord& record) {
    if (record.segments.empty()) return;
    std::unique_lock<std::shared_mutex> lock(local_mutex_);
    for (auto& [_, segment] : record.segments) {
        if (segment) pending_local_segments_.push_back(std::move(segment));
    }
    record.segments.clear();
}

Status UbBufferManager::removeBuffer(BufferDesc& desc) {
    Status status = Status::OK();
    bool removed = false;
    {
        std::unique_lock<std::shared_mutex> lock(local_mutex_);
        auto it = local_buffers_.find(AddressRange{desc.addr, desc.length});
        if (it != local_buffers_.end()) {
            status = unregisterRecord(it->second);
            if (status.ok() && it->second.segments.empty()) {
                local_buffers_.erase(it);
                removed = true;
            }
        } else {
            // Idempotent retry after a previously successful removal.
            removed = true;
        }
    }
    if (!status.ok()) return status;
    if (!removed) {
        return Status::InternalError(
            "UB local record still owns segments after unregister" LOC_MARK);
    }
    desc.transport_attrs.erase(TransportType::UB);
    desc.transports.erase(std::remove(desc.transports.begin(),
                                      desc.transports.end(), TransportType::UB),
                          desc.transports.end());
    return Status::OK();
}

Status UbBufferManager::clear() {
    Status first_error = Status::OK();
    {
        std::unique_lock<std::shared_mutex> lock(local_mutex_);
        for (auto it = local_buffers_.begin(); it != local_buffers_.end();) {
            auto status = unregisterRecord(it->second);
            if (!status.ok() && first_error.ok()) first_error = status;
            if (status.ok() && it->second.segments.empty()) {
                it = local_buffers_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_local_segments_.begin();
             it != pending_local_segments_.end();) {
            auto status = adapter_->unregisterLocalSegment(*it);
            if (!status.ok()) {
                if (first_error.ok()) first_error = status;
                ++it;
            } else if (*it) {
                if (first_error.ok()) {
                    first_error = Status::InternalError(
                        "URMA adapter retained a pending local segment after "
                        "successful unregister" LOC_MARK);
                }
                ++it;
            } else {
                it = pending_local_segments_.erase(it);
            }
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(import_mutex_);
        for (auto it = imports_.begin(); it != imports_.end();) {
            auto status = adapter_->unimportRemoteSegment(it->second);
            if (!status.ok()) {
                if (first_error.ok()) first_error = status;
                ++it;
            } else if (it->second) {
                if (first_error.ok()) {
                    first_error = Status::InternalError(
                        "URMA adapter retained a remote segment after "
                        "successful unimport" LOC_MARK);
                }
                ++it;
            } else {
                it = imports_.erase(it);
            }
        }
        for (auto it = pending_remote_segments_.begin();
             it != pending_remote_segments_.end();) {
            auto status = adapter_->unimportRemoteSegment(*it);
            if (!status.ok()) {
                if (first_error.ok()) first_error = status;
                ++it;
            } else if (*it) {
                if (first_error.ok()) {
                    first_error = Status::InternalError(
                        "URMA adapter retained a pending remote segment after "
                        "successful unimport" LOC_MARK);
                }
                ++it;
            } else {
                it = pending_remote_segments_.erase(it);
            }
        }
    }
    return first_error;
}

Status UbBufferManager::findLocal(uint64_t address, size_t length,
                                  Topology::NicID local_topology_id,
                                  LocalSegmentRef& result) const {
    std::shared_lock<std::shared_mutex> lock(local_mutex_);
    auto it = local_buffers_.upper_bound(
        AddressRange{address, std::numeric_limits<uint64_t>::max()});
    if (it == local_buffers_.begin()) {
        return Status::AddressNotRegistered(
            "Local UB address is not registered" LOC_MARK);
    }
    --it;
    if (!it->first.contains(address, length)) {
        return Status::AddressNotRegistered(
            "Local UB range crosses a registration boundary" LOC_MARK);
    }
    auto segment = it->second.segments.find(local_topology_id);
    if (segment == it->second.segments.end()) {
        return Status::AddressNotRegistered(
            "Local UB buffer is not registered on selected device" LOC_MARK);
    }
    result = LocalSegmentRef{findContext(local_topology_id), segment->second,
                             it->second.generation, it->first.base,
                             it->first.length};
    return Status::OK();
}

Status UbBufferManager::importRemote(SegmentID remote_segment_id,
                                     Topology::NicID local_topology_id,
                                     Topology::NicID remote_topology_id,
                                     const BufferDesc& remote_buffer,
                                     Request::OpCode opcode, uint64_t address,
                                     size_t length,
                                     ImportedSegmentRef& result) {
    auto attr = remote_buffer.transport_attrs.find(TransportType::UB);
    if (attr == remote_buffer.transport_attrs.end()) {
        return Status::NeedsRefreshCache(
            "Remote buffer has no UB metadata" LOC_MARK);
    }
    UbBufferMetadata metadata;
    CHECK_STATUS(decodeBufferMetadata(attr->second, metadata));
    if (metadata.base != remote_buffer.addr ||
        metadata.length != remote_buffer.length) {
        return Status::NeedsRefreshCache(
            "Remote UB metadata does not match BufferDesc" LOC_MARK);
    }
    uint64_t remote_end = 0;
    uint64_t query_end = 0;
    if (!checkedEnd(metadata.base, metadata.length, remote_end) ||
        !checkedEnd(address, length, query_end) || address < metadata.base ||
        query_end > remote_end) {
        return Status::InvalidArgument(
            "Remote UB request is outside the registered buffer" LOC_MARK);
    }
    if (!permissionAllows(metadata.permission, opcode)) {
        return Status::InvalidArgument(
            "Remote UB buffer permission rejects the operation" LOC_MARK);
    }

    auto descriptor = std::find_if(
        metadata.segments.begin(), metadata.segments.end(),
        [remote_topology_id](const UbBufferSegmentMetadata& segment) {
            return segment.topology_id == remote_topology_id;
        });
    if (descriptor == metadata.segments.end()) {
        return Status::NeedsRefreshCache(
            "Remote UB buffer lacks selected device descriptor" LOC_MARK);
    }
    auto context = findContext(local_topology_id);
    if (!context || !context->active()) {
        return Status::DeviceNotFound(
            "Selected local UB context is unavailable" LOC_MARK);
    }

    ImportKey key{local_topology_id, remote_segment_id, remote_topology_id,
                  metadata.base, metadata.generation};
    RemoteSegmentPtr imported;
    {
        // Serialize the provider import with the second cache lookup. This
        // avoids creating a duplicate handle that has no cache key capable of
        // retaining it when an immediate rollback fails.
        std::unique_lock<std::shared_mutex> lock(import_mutex_);
        auto current = imports_.find(key);
        if (current == imports_.end()) {
            SegmentOptions options;
            options.access = segmentAccess(metadata.permission);
            auto import_status = adapter_->importRemoteSegment(
                context->handle(), descriptor->descriptor, options, imported);
            if (!import_status.ok()) {
                if (imported) {
                    (void)adapter_->unimportRemoteSegment(imported);
                    if (imported) {
                        pending_remote_segments_.push_back(std::move(imported));
                    }
                }
                return import_status;
            }
            if (!imported) {
                return Status::InternalError(
                    "URMA adapter returned no remote segment after successful "
                    "import" LOC_MARK);
            }
            current = imports_.emplace(key, imported).first;
        } else {
            imported = current->second;
        }

        for (auto iter = imports_.begin(); iter != imports_.end();) {
            const auto& candidate = iter->first;
            const bool stale =
                candidate.local_topology_id == local_topology_id &&
                candidate.remote_segment_id == remote_segment_id &&
                candidate.remote_topology_id == remote_topology_id &&
                candidate.buffer_base == metadata.base &&
                candidate.generation != metadata.generation;
            if (!stale) {
                ++iter;
                continue;
            }
            auto status = adapter_->unimportRemoteSegment(iter->second);
            if (!status.ok()) {
                // The old generation may still be retained by an in-flight
                // WR. Keep it cached for a later import/clear retry, but do
                // not fail the current request after its new generation was
                // imported successfully.
                ++iter;
            } else if (iter->second) {
                // Treat an adapter that reports success without releasing the
                // handle conservatively: retain ownership and retry later.
                ++iter;
            } else {
                iter = imports_.erase(iter);
            }
        }
    }
    result =
        ImportedSegmentRef{context,       imported,        metadata.generation,
                           metadata.base, metadata.length, remote_topology_id};
    return Status::OK();
}

size_t UbBufferManager::localBufferCount() const {
    std::shared_lock<std::shared_mutex> lock(local_mutex_);
    return local_buffers_.size();
}

size_t UbBufferManager::importedSegmentCount() const {
    std::shared_lock<std::shared_mutex> lock(import_mutex_);
    return imports_.size();
}

}  // namespace mooncake::tent::ub
