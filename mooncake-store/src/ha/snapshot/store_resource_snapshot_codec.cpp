#include "ha/snapshot/store_resource_snapshot_codec.h"

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/functional/hash.hpp>
#include <msgpack.hpp>

#include "ha/snapshot/allocator_snapshot_codec.h"
#include "ha/snapshot/local_ssd_codec.h"
#include "segment/pool.h"
#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"
#include "segment/snapshot_view.h"
#include "serialize/serializer.h"
#include "utils/zstd_util.h"

namespace mooncake::ha {
namespace {

struct DecodedMountedRegion final {
    MountedRegion mounted;
    std::shared_ptr<BufferAllocatorBase> allocator;
};

tl::expected<void, SerializationError> EncodeMountedRegion(
    const MountedRegion& mounted, const BufferAllocatorBase* allocator,
    MsgpackPacker& packer) {
    // Preserved wire shape: [segment_id, segment_name, segment_base,
    // segment_size, te_endpoint, status, has_allocator, allocator, host_id].
    packer.pack_array(9);
    packer.pack(UuidToString(mounted.segment.id));
    packer.pack(mounted.segment.name);
    packer.pack(static_cast<uint64_t>(mounted.segment.base));
    packer.pack(static_cast<uint64_t>(mounted.segment.size));
    packer.pack(mounted.segment.te_endpoint);
    packer.pack(static_cast<int16_t>(mounted.status));
    if (allocator) {
        packer.pack(true);
        auto encoded = AllocatorSnapshotCodec::Encode(*allocator, packer);
        if (!encoded) {
            return tl::make_unexpected(encoded.error());
        }
    } else {
        packer.pack(false);
        packer.pack_nil();
    }
    packer.pack(mounted.segment.host_id);
    return {};
}

tl::expected<DecodedMountedRegion, SerializationError> DecodeMountedRegion(
    const msgpack::object& object) {
    if (object.type != msgpack::type::ARRAY || object.via.array.size < 8) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "snapshot MountedRegion is not an array with at least 8 fields"));
    }

    try {
        const auto* array = object.via.array.ptr;
        UUID id;
        if (!StringToUuid(array[0].as<std::string>(), id)) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "snapshot MountedRegion has invalid UUID"));
        }

        DecodedMountedRegion decoded;
        decoded.mounted.segment.id = id;
        decoded.mounted.segment.name = array[1].as<std::string>();
        decoded.mounted.segment.base =
            static_cast<uintptr_t>(array[2].as<uint64_t>());
        decoded.mounted.segment.size =
            static_cast<size_t>(array[3].as<uint64_t>());
        decoded.mounted.segment.te_endpoint = array[4].as<std::string>();
        decoded.mounted.status =
            static_cast<SegmentStatus>(array[5].as<int16_t>());
        decoded.mounted.kind = RegionKind::HOST_MEMORY;

        if (array[6].as<bool>()) {
            auto allocator = AllocatorSnapshotCodec::Decode(array[7]);
            if (!allocator) {
                return tl::make_unexpected(allocator.error());
            }
            decoded.allocator = std::move(*allocator);
        }
        if (object.via.array.size >= 9) {
            decoded.mounted.segment.host_id = array[8].as<std::string>();
        }
        return decoded;
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            std::string("decode MountedRegion failed: ") + error.what()));
    }
}

tl::expected<std::map<std::string, const msgpack::object*>, SerializationError>
IndexFields(const msgpack::object& object) {
    if (object.type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "snapshot SegmentPool payload is not a map"));
    }
    std::map<std::string, const msgpack::object*> fields;
    for (uint32_t i = 0; i < object.via.map.size; ++i) {
        const auto& key = object.via.map.ptr[i].key;
        if (key.type == msgpack::type::STR) {
            fields.emplace(std::string(key.via.str.ptr, key.via.str.size),
                           &object.via.map.ptr[i].val);
        }
    }
    return fields;
}

}  // namespace

tl::expected<std::vector<uint8_t>, SerializationError>
StoreResourceSnapshotCodec::Encode(
    const SegmentPoolSnapshotView& view,
    const LocalSsdPersistedState& local_ssd_state) {
    const auto allocator_type = view.Resources().GetMemoryAllocatorType();
    if (!allocator_type || *allocator_type != BufferAllocatorType::OFFSET) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::SERIALIZE_UNSUPPORTED,
            "snapshot SegmentPool memory driver is not offset"));
    }
    if (view.Resources().HasKind(RegionKind::CXL)) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::SERIALIZE_UNSUPPORTED,
            "snapshot SegmentPool CXL resource is unsupported"));
    }

    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_map(5);

    packer.pack("ma");
    packer.pack(static_cast<int32_t>(*allocator_type));

    packer.pack("an");
    std::vector<std::string> active_names;
    view.Placement().GetActiveGroupNames(active_names);
    packer.pack_array(active_names.size());
    for (const auto& name : active_names) {
        packer.pack(name);
    }

    packer.pack("ms");
    std::vector<std::pair<UUID, MountedRegion>> mounted_regions;
    view.Catalog().GetMountedRegions(mounted_regions);
    std::sort(
        mounted_regions.begin(), mounted_regions.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    packer.pack_map(mounted_regions.size());
    for (const auto& [id, mounted] : mounted_regions) {
        auto allocator = view.Resources().GetAllocator(id);
        if (!allocator) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::SERIALIZE_FAIL,
                "snapshot mounted region has no driver resource"));
        }
        packer.pack(UuidToString(id));
        auto encoded = EncodeMountedRegion(mounted, allocator.get(), packer);
        if (!encoded) {
            return tl::make_unexpected(encoded.error());
        }
    }

    packer.pack("cs");
    std::vector<std::pair<UUID, std::vector<UUID>>> clients;
    view.Catalog().GetClientRegions(clients);
    std::sort(
        clients.begin(), clients.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    packer.pack_map(clients.size());
    for (const auto& [client, regions] : clients) {
        packer.pack(UuidToString(client));
        packer.pack_array(regions.size());
        for (const auto& id : regions) {
            packer.pack(UuidToString(id));
        }
    }

    packer.pack("ld");
    auto local_ssd = LocalSsdCodec::Encode(local_ssd_state, packer);
    if (!local_ssd) {
        return tl::make_unexpected(local_ssd.error());
    }
    return zstd_compress(reinterpret_cast<const uint8_t*>(buffer.data()),
                         buffer.size(), 3);
}

tl::expected<LocalSsdPersistedState, SerializationError>
StoreResourceSnapshotCodec::Decode(SegmentPool& segment_pool,
                                   const std::vector<uint8_t>& data,
                                   bool account_capacity_metrics) {
    std::vector<uint8_t> decompressed;
    try {
        decompressed = zstd_decompress(data);
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            std::string("decompress SegmentPool snapshot failed: ") +
                error.what()));
    }

    msgpack::object_handle handle;
    try {
        handle =
            msgpack::unpack(reinterpret_cast<const char*>(decompressed.data()),
                            decompressed.size());
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            std::string("unpack SegmentPool snapshot failed: ") +
                error.what()));
    }
    auto fields = IndexFields(handle.get());
    if (!fields) {
        return tl::make_unexpected(fields.error());
    }

    const auto local_ssd_field = fields->find("ld");
    auto local_ssd = LocalSsdCodec::Decode(
        local_ssd_field == fields->end() ? nullptr : local_ssd_field->second);
    if (!local_ssd) {
        return tl::make_unexpected(local_ssd.error());
    }

    const auto memory_allocator_type =
        segment_pool.AcquireReadAccess().Resources().GetMemoryAllocatorType();
    const auto allocator_field = fields->find("ma");
    if (!memory_allocator_type || allocator_field == fields->end() ||
        allocator_field->second->type != msgpack::type::POSITIVE_INTEGER ||
        static_cast<BufferAllocatorType>(
            allocator_field->second->as<int32_t>()) != *memory_allocator_type) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "snapshot SegmentPool allocator type mismatch"));
    }

    std::vector<std::string> active_names;
    const auto names_field = fields->find("an");
    if (names_field != fields->end()) {
        if (names_field->second->type != msgpack::type::ARRAY) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "snapshot active names is not an array"));
        }
        for (uint32_t i = 0; i < names_field->second->via.array.size; ++i) {
            const auto& name = names_field->second->via.array.ptr[i];
            if (name.type != msgpack::type::STR) {
                return tl::make_unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "snapshot active name is not a string"));
            }
            active_names.emplace_back(name.via.str.ptr, name.via.str.size);
        }
    }

    std::unordered_map<UUID, DecodedMountedRegion, boost::hash<UUID>> decoded;
    const auto mounted_field = fields->find("ms");
    if (mounted_field != fields->end()) {
        if (mounted_field->second->type != msgpack::type::MAP) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "snapshot mounted regions is not a map"));
        }
        for (uint32_t i = 0; i < mounted_field->second->via.map.size; ++i) {
            const auto& item = mounted_field->second->via.map.ptr[i];
            if (item.key.type != msgpack::type::STR) {
                return tl::make_unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "snapshot region id is not a string"));
            }
            UUID id;
            if (!StringToUuid(
                    std::string(item.key.via.str.ptr, item.key.via.str.size),
                    id)) {
                return tl::make_unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "snapshot region id is invalid"));
            }
            auto region = DecodeMountedRegion(item.val);
            if (!region || region->mounted.segment.id != id ||
                !region->allocator) {
                return tl::make_unexpected(
                    region ? SerializationError(
                                 ErrorCode::DESERIALIZE_FAIL,
                                 "snapshot region id or allocator mismatch")
                           : region.error());
            }
            const auto& segment = region->mounted.segment;
            if (segment.name.empty() || segment.base == 0 ||
                segment.size == 0 ||
                segment.base >
                    std::numeric_limits<uintptr_t>::max() - segment.size ||
                region->allocator->getSegmentName() != segment.name ||
                region->allocator->getTransportEndpoint() !=
                    segment.te_endpoint ||
                region->allocator->base() != segment.base ||
                region->allocator->capacity() != segment.size) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "snapshot contains invalid adopted resource"));
            }
            if (!decoded.emplace(id, std::move(*region)).second) {
                return tl::make_unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "snapshot contains duplicate region"));
            }
        }
    }

    std::set<UUID> owned_regions;
    const auto clients_field = fields->find("cs");
    if (clients_field != fields->end()) {
        if (clients_field->second->type != msgpack::type::MAP) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "snapshot client regions is not a map"));
        }
        for (uint32_t i = 0; i < clients_field->second->via.map.size; ++i) {
            const auto& item = clients_field->second->via.map.ptr[i];
            if (item.key.type != msgpack::type::STR ||
                item.val.type != msgpack::type::ARRAY) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "snapshot contains invalid client entry"));
            }
            UUID client;
            if (!StringToUuid(
                    std::string(item.key.via.str.ptr, item.key.via.str.size),
                    client)) {
                return tl::make_unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "snapshot contains invalid client id"));
            }
            for (uint32_t j = 0; j < item.val.via.array.size; ++j) {
                const auto& id_object = item.val.via.array.ptr[j];
                UUID id;
                if (id_object.type != msgpack::type::STR ||
                    !StringToUuid(std::string(id_object.via.str.ptr,
                                              id_object.via.str.size),
                                  id) ||
                    !decoded.contains(id) || !owned_regions.insert(id).second) {
                    return tl::make_unexpected(SerializationError(
                        ErrorCode::DESERIALIZE_FAIL,
                        "snapshot contains invalid region ownership"));
                }
                decoded.at(id).mounted.client_id = client;
            }
        }
    }
    if (owned_regions.size() != decoded.size()) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "snapshot region is missing an owner"));
    }

    std::unordered_map<std::string, UUID> owner_by_name;
    std::set<std::string> expected_active_names;
    for (const auto& [_, region] : decoded) {
        auto [owner, inserted] = owner_by_name.emplace(
            region.mounted.segment.name, region.mounted.client_id);
        if (!inserted && owner->second != region.mounted.client_id) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot logical region group has multiple owners"));
        }
        if (region.mounted.status == SegmentStatus::OK) {
            expected_active_names.insert(region.mounted.segment.name);
        }
    }

    std::set<std::string> decoded_active_names;
    for (const auto& name : active_names) {
        if (!decoded_active_names.insert(name).second ||
            !expected_active_names.contains(name)) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot contains invalid active region names"));
        }
    }
    if (decoded_active_names != expected_active_names) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "snapshot active region names do not match mounted regions"));
    }

    std::vector<UUID> order;
    order.reserve(decoded.size());
    for (const auto& name : active_names) {
        for (const auto& [id, region] : decoded) {
            if (region.mounted.status == SegmentStatus::OK &&
                region.mounted.segment.name == name &&
                std::find(order.begin(), order.end(), id) == order.end()) {
                order.push_back(id);
            }
        }
    }
    for (const auto& [id, region] : decoded) {
        (void)region;
        if (std::find(order.begin(), order.end(), id) == order.end()) {
            order.push_back(id);
        }
    }

    auto access = segment_pool.AcquireWriteAccess();
    std::vector<RegionMountTxn> prepared;
    prepared.reserve(order.size());
    for (const auto& id : order) {
        auto& region = decoded.at(id);
        auto next = access.PrepareAdopt(region.mounted, region.allocator,
                                        account_capacity_metrics);
        if (!next) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot SegmentPool failed to adopt allocator"));
        }
        prepared.push_back(std::move(*next));
    }
    // Preparing is transactional and leaves the currently published pool
    // untouched. Once every resource is ready, replacement is no-fail.
    access.Clear();
    for (auto& region : prepared) {
        access.CommitMount(region);
    }
    return std::move(*local_ssd);
}

}  // namespace mooncake::ha
