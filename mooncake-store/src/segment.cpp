#include "segment.h"

#include "master_metric_manager.h"
#include "utils/zstd_util.h"

namespace mooncake {

ErrorCode ScopedSegmentAccess::MountSegment(const Segment& segment,
                                            const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if cxl storage is enable
    if (segment_manager_->enable_cxl_ && segment.protocol == "cxl") {
        LOG(INFO) << "Start Mounting CXL Segment.";
        if (segment_manager_->memory_allocator_ ==
            BufferAllocatorType::CACHELIB) {
            auto allocator = segment_manager_->cxl_global_allocator_;
            if (segment_manager_->cxl_global_allocator_ == nullptr) {
                LOG(ERROR) << "Cxl global allocator has not been initialized.";
                return ErrorCode::INTERNAL_ERROR;
            }
            segment_manager_->allocator_manager_.addAllocator(segment.name,
                                                              allocator);
            segment_manager_->client_segments_[client_id].push_back(segment.id);
            segment_manager_->mounted_segments_[segment.id] = {
                segment, SegmentStatus::OK, allocator};
            segment_manager_->client_by_name_[segment.name] = client_id;

            LOG(INFO) << "[CXL Segment Mounted Successfully] Segment name: "
                      << segment.name
                      << ", Mount size: " << (size / 1024 / 1024 / 1024)
                      << " GB";
            return ErrorCode::OK;
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is invalid";
        return ErrorCode::INVALID_PARAMS;
    }

    if (segment_manager_->memory_allocator_ == BufferAllocatorType::CACHELIB &&
        (buffer % facebook::cachelib::Slab::kSize ||
         size % facebook::cachelib::Slab::kSize)) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize
                   << " as required by Cachelib";
        return ErrorCode::INVALID_PARAMS;
    }

    // Check if segment already exists
    auto exist_segment_it =
        segment_manager_->mounted_segments_.find(segment.id);
    if (exist_segment_it != segment_manager_->mounted_segments_.end()) {
        auto& exist_segment = exist_segment_it->second;
        if (exist_segment.status == SegmentStatus::OK) {
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        } else {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=segment_already_exists_but_not_ok"
                       << ", status=" << exist_segment.status;
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }

    std::shared_ptr<BufferAllocatorBase> allocator;
    // CachelibBufferAllocator may throw an exception if the size or base is
    // invalid for the slab allocator.
    try {
        // Create allocator based on the configured type
        switch (segment_manager_->memory_allocator_) {
            case BufferAllocatorType::CACHELIB:
                allocator = std::make_shared<CachelibBufferAllocator>(
                    segment.name, buffer, size, segment.te_endpoint);
                break;
            case BufferAllocatorType::OFFSET:
                allocator = std::make_shared<OffsetBufferAllocator>(
                    segment.name, buffer, size, segment.te_endpoint);
                break;
            default:
                LOG(ERROR) << "segment_name=" << segment.name
                           << ", error=unknown_memory_allocator="
                           << static_cast<int>(
                                  segment_manager_->memory_allocator_);
                return ErrorCode::INVALID_PARAMS;
        }

        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=failed_to_create_allocator";
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=exception_during_allocator_creation";
        return ErrorCode::INVALID_PARAMS;
    }

    segment_manager_->allocator_manager_.addAllocator(segment.name, allocator);
    segment_manager_->client_segments_[client_id].push_back(segment.id);
    segment_manager_->mounted_segments_[segment.id] = {
        segment, SegmentStatus::OK, std::move(allocator)};
    segment_manager_->client_by_name_[segment.name] = client_id;
    MasterMetricManager::instance().inc_total_mem_capacity(segment.name, size);

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading) {
    auto exist_segment_it =
        segment_manager_->client_local_disk_segment_.find(client_id);
    if (exist_segment_it !=
        segment_manager_->client_local_disk_segment_.end()) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=local_disk_segment_already_exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    segment_manager_->client_local_disk_segment_.emplace(
        client_id, std::make_shared<LocalDiskSegment>(enable_offloading));
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    for (const auto& segment : segments) {
        ErrorCode err = MountSegment(segment, client_id);
        if (err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
            err == ErrorCode::INTERNAL_ERROR) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=fail_to_remount_segment";
            return err;
        } else if (err == ErrorCode::INVALID_PARAMS) {
            // Ignore INVALID_PARAMS. This error cannot be solved by a new
            // remount request.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=invalid_params";
        } else if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            // Segment already exists, no need to remount.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
        } else if (err != ErrorCode::OK) {
            // Ignore other errors. The error may not be solvable by a new
            // remount request.
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=unexpected_error (" << err << ")";
        }
    }

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        LOG(WARNING) << "segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (it->second.status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto& mounted_segment = it->second;
    auto& segment = mounted_segment.segment;
    metrics_dec_capacity = segment.size;

    // Remove the allocator from the segment manager
    std::shared_ptr<BufferAllocatorBase> allocator =
        mounted_segment.buf_allocator;

    // 1. Remove from allocators
    if (!segment_manager_->allocator_manager_.removeAllocator(segment.name,
                                                              allocator)) {
        LOG(ERROR) << "Allocator " << segment.id << " of segment "
                   << segment.name << " not found in allocator manager";
    }

    // 2. Remove from mounted_segment
    mounted_segment.buf_allocator.reset();

    // Set the segment status to UNMOUNTING
    mounted_segment.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity) {
    // Remove from client_segments_
    bool found_in_client_segments = false;
    auto client_it = segment_manager_->client_segments_.find(client_id);
    if (client_it != segment_manager_->client_segments_.end()) {
        auto& segments = client_it->second;
        auto segment_it =
            std::find(segments.begin(), segments.end(), segment_id);
        if (segment_it != segments.end()) {
            segments.erase(segment_it);
            found_in_client_segments = true;
        }
        if (segments.empty()) {
            segment_manager_->client_segments_.erase(client_it);
        }
    }
    if (!found_in_client_segments) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_not_found_in_client_segments";
    }

    // segment_id -> segment_name
    std::string segment_name;
    bool is_cxl = false;
    auto&& segment = segment_manager_->mounted_segments_.find(segment_id);
    if (segment != segment_manager_->mounted_segments_.end()) {
        segment_name = segment->second.segment.name;
        // Also remove from segment_name_client_id_map_
        segment_manager_->client_by_name_.erase(segment_name);
        is_cxl = (segment->second.segment.protocol == "cxl");
    }
    // Remove from mounted_segments_
    segment_manager_->mounted_segments_.erase(segment_id);

    // Decrease the total capacity
    if (!is_cxl) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            segment_name, metrics_dec_capacity);
    }

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto it = segment_manager_->client_segments_.find(client_id);
    if (it == segment_manager_->client_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (auto& segment_id : it->second) {
        auto segment_it = segment_manager_->mounted_segments_.find(segment_id);
        if (segment_it != segment_manager_->mounted_segments_.end()) {
            segments.emplace_back(segment_it->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (auto& segment : segment_manager_->mounted_segments_) {
        if (segment.second.status == SegmentStatus::OK) {
            all_segments.push_back(segment.second.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegments(
    std::vector<std::pair<Segment, UUID>>& all_segments) {
    all_segments.clear();

    for (auto& segment_pair : segment_manager_->mounted_segments_) {
        // Find the client_id for this segment
        UUID client_id = UUID{
            0, 0};  // Default value, indicates no corresponding client found

        // Search for the client corresponding to this segment ID in
        // client_segments_
        for (const auto& client_pair : segment_manager_->client_segments_) {
            const auto& client_id_tmp = client_pair.first;
            const auto& segment_ids = client_pair.second;

            if (std::find(segment_ids.begin(), segment_ids.end(),
                          segment_pair.first) != segment_ids.end()) {
                client_id = client_id_tmp;
                break;
            }
        }

        all_segments.emplace_back(segment_pair.second.segment, client_id);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegmentNames(
    std::vector<std::string>& all_segment_names) {
    all_segment_names.clear();
    for (auto& segment : segment_manager_->mounted_segments_) {
        all_segment_names.push_back(segment.second.segment.name);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::QuerySegments(const std::string& segment,
                                             size_t& used, size_t& capacity) {
    size_t total_used = 0, total_capacity = 0;
    const auto& allocator_manager = segment_manager_->allocator_manager_;
    const auto& allocators = allocator_manager.getAllocators(segment);
    if (allocators != nullptr) {
        for (const auto& allocator : *allocators) {
            total_used += allocator->size();
            total_capacity += allocator->capacity();
        }
    }

    if (total_capacity == 0) {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment
                << ") not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    used = total_used;
    capacity = total_capacity;

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetUnreadySegments(
    std::vector<std::pair<Segment, UUID>>& unready_segments) const {
    unready_segments.clear();

    for (const auto& pair : segment_manager_->mounted_segments_) {
        const auto& mounted_segment = pair.second;
        if (mounted_segment.status != SegmentStatus::OK) {
            // Find the client_id for this segment
            UUID client_id = UUID{
                0,
                0};  // Default value, indicates no corresponding client found

            // Search for the client corresponding to this segment ID in
            // client_segments_
            for (const auto& client_pair : segment_manager_->client_segments_) {
                const auto& client_id_tmp = client_pair.first;
                const auto& segment_ids = client_pair.second;

                if (std::find(segment_ids.begin(), segment_ids.end(),
                              mounted_segment.segment.id) !=
                    segment_ids.end()) {
                    client_id = client_id_tmp;
                    break;
                }
            }

            unready_segments.emplace_back(mounted_segment.segment, client_id);
        }
    }

    return ErrorCode::OK;
}

ErrorCode SegmentView::GetSegment(
    std::shared_ptr<BufferAllocatorBase> allocator, Segment& segment) const {
    // Check input parameters
    if (!allocator) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Iterate through mounted_segments_ to find matching allocator
    for (const auto& [segment_id, mounted_segment] :
         segment_manager_->mounted_segments_) {
        if (mounted_segment.buf_allocator == allocator) {
            segment = mounted_segment.segment;
            return ErrorCode::OK;
        }
    }

    // No matching segment found
    return ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode SegmentView::GetMountedSegment(const UUID& segment_id,
                                         MountedSegment& mountedSegment) const {
    // Check input parameters
    if (segment_id.first == 0 && segment_id.second == 0) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Find the specified segment_id in mounted_segments_
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    // Return the found segment
    mountedSegment = it->second;
    return ErrorCode::OK;
}

tl::expected<std::vector<uint8_t>, SerializationError>
SegmentSerializer::Serialize() {
    if (!segment_manager_) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            "serialize SegmentManager segment_manager_ is null"));
    }

    if (segment_manager_->memory_allocator_ != BufferAllocatorType::OFFSET) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_UNSUPPORTED,
            "serialize SegmentManager memory_allocator_ is not offset"));
    }

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // Create map containing all data
    packer.pack_map(
        5);  // 5 fields: memory_allocator, mounted_segments, client_segments,
             // allocator_names, local_disk_segments

    // Serialize memory_allocator_
    packer.pack("ma");
    packer.pack(static_cast<int32_t>(segment_manager_->memory_allocator_));

    // Serialize allocator_manager_'s names_ order
    packer.pack("an");  // allocator_names
    const auto& names = segment_manager_->allocator_manager_.getNames();
    packer.pack_array(names.size());
    for (const auto& name : names) {
        packer.pack(name);
    }

    // Serialize mounted_segments_
    // Sort UUIDs first to ensure deterministic serialization results
    packer.pack("ms");
    packer.pack_map(segment_manager_->mounted_segments_.size());

    // Collect all segment UUIDs and sort
    std::vector<UUID> sorted_segment_uuids;
    sorted_segment_uuids.reserve(segment_manager_->mounted_segments_.size());
    for (const auto& pair : segment_manager_->mounted_segments_) {
        sorted_segment_uuids.push_back(pair.first);
    }
    std::sort(sorted_segment_uuids.begin(), sorted_segment_uuids.end());

    for (const auto& segment_uuid : sorted_segment_uuids) {
        const auto& mounted_segment =
            segment_manager_->mounted_segments_.at(segment_uuid);
        std::string uuid_str = UuidToString(segment_uuid);
        packer.pack(uuid_str);

        // Serialize MountedSegment object
        auto result =
            Serializer<MountedSegment>::serialize(mounted_segment, packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    // Serialize client_segments_
    // Sort client UUIDs first to ensure deterministic serialization results
    packer.pack("cs");
    packer.pack_map(segment_manager_->client_segments_.size());

    // Collect all client UUIDs and sort
    std::vector<UUID> sorted_client_uuids;
    sorted_client_uuids.reserve(segment_manager_->client_segments_.size());
    for (const auto& pair : segment_manager_->client_segments_) {
        sorted_client_uuids.push_back(pair.first);
    }
    std::sort(sorted_client_uuids.begin(), sorted_client_uuids.end());

    for (const auto& client_uuid : sorted_client_uuids) {
        const auto& segment_ids =
            segment_manager_->client_segments_.at(client_uuid);
        std::string client_uuid_str = UuidToString(client_uuid);
        packer.pack(client_uuid_str);

        // Serialize segment IDs array
        packer.pack_array(segment_ids.size());

        for (const auto& segment_id : segment_ids) {
            std::string segment_uuid_str = UuidToString(segment_id);
            packer.pack(segment_uuid_str);
        }
    }

    // Serialize client_local_disk_segment_
    // Sort client UUIDs first to ensure deterministic serialization results
    packer.pack("ld");  // local_disk_segments
    packer.pack_map(segment_manager_->client_local_disk_segment_.size());

    // Collect all client UUIDs and sort
    std::vector<UUID> sorted_ld_uuids;
    sorted_ld_uuids.reserve(
        segment_manager_->client_local_disk_segment_.size());
    for (const auto& pair : segment_manager_->client_local_disk_segment_) {
        sorted_ld_uuids.push_back(pair.first);
    }
    std::sort(sorted_ld_uuids.begin(), sorted_ld_uuids.end());

    for (const auto& client_uuid : sorted_ld_uuids) {
        const auto& segment =
            segment_manager_->client_local_disk_segment_.at(client_uuid);
        packer.pack(UuidToString(client_uuid));

        // Serialize LocalDiskSegment: [enable_offloading, count, key1, ts1,
        // key2, ts2, ...] Sort keys to ensure determinism
        std::vector<std::string> sorted_keys;
        for (const auto& [key, ts] : segment->offloading_objects) {
            sorted_keys.push_back(key);
        }
        std::sort(sorted_keys.begin(), sorted_keys.end());

        packer.pack_array(2 + sorted_keys.size() * 2);
        packer.pack(segment->enable_offloading);
        packer.pack(static_cast<uint64_t>(sorted_keys.size()));

        for (const auto& key : sorted_keys) {
            packer.pack(key);
            packer.pack(segment->offloading_objects.at(key));
        }
    }

    // Compress entire data
    std::vector<uint8_t> compressed_data = zstd_compress(
        reinterpret_cast<const uint8_t*>(sbuf.data()), sbuf.size(), 3);

    // Return compressed data
    return std::vector<uint8_t>(
        std::make_move_iterator(compressed_data.begin()),
        std::make_move_iterator(compressed_data.end()));
}

tl::expected<void, SerializationError> SegmentSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    // Decompress data
    std::vector<uint8_t> decompressed_data;
    try {
        decompressed_data = zstd_decompress(
            reinterpret_cast<const uint8_t*>(data.data()), data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize SegmentManager failed to "
                               "decompress MessagePack data: " +
                                   std::string(e.what())));
    }

    // Parse MessagePack data
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(
            reinterpret_cast<const char*>(decompressed_data.data()),
            decompressed_data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize SegmentManager failed to unpack MessagePack data: " +
                std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();

    // Check if it's a map
    if (obj.type != msgpack::type::MAP) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize SegmentManager invalid MessagePack "
                               "format: expected map"));
    }

    if (!segment_manager_) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize SegmentManager segment_manager is null"));
    }

    // Clear existing data
    segment_manager_->mounted_segments_.clear();
    segment_manager_->client_segments_.clear();

    // Convert MessagePack map to regular map, use pointers for values to avoid
    // copying
    std::map<std::string, const msgpack::object*> fields_map;
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const msgpack::object& key_obj = obj.via.map.ptr[i].key;
        const msgpack::object& value_obj = obj.via.map.ptr[i].val;

        // Ensure key is a string
        if (key_obj.type == msgpack::type::STR) {
            std::string key(key_obj.via.str.ptr, key_obj.via.str.size);
            fields_map.emplace(std::move(key), &value_obj);
        }
    }

    // Process fields in order
    // 1. First process memory_allocator_
    auto allocator_it = fields_map.find("ma");
    if (allocator_it != fields_map.end()) {
        const msgpack::object* value_obj = allocator_it->second;
        if (value_obj->type != msgpack::type::POSITIVE_INTEGER) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager memory_allocator is not int"));
        }

        auto allocatorType =
            static_cast<BufferAllocatorType>(value_obj->as<int32_t>());

        // Note: Type must match, only OffsetAllocator is supported
        if (allocatorType != segment_manager_->memory_allocator_) {
            LOG(ERROR) << "deserialize memory allocator type doesn't match "
                          "current setting";
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager memory allocator type doesn't "
                "match current setting"));
        }
    }

    // 1.5 Parse allocator_names (saved original names_ order)
    std::vector<std::string> saved_allocator_names;
    auto allocator_names_it = fields_map.find("an");
    if (allocator_names_it != fields_map.end()) {
        const msgpack::object* value_obj = allocator_names_it->second;
        if (value_obj->type != msgpack::type::ARRAY) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager allocator_names is not array"));
        }

        saved_allocator_names.reserve(value_obj->via.array.size);
        for (uint32_t i = 0; i < value_obj->via.array.size; ++i) {
            const msgpack::object& name_obj = value_obj->via.array.ptr[i];
            if (name_obj.type != msgpack::type::STR) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "deserialize SegmentManager allocator_name is not string"));
            }
            saved_allocator_names.emplace_back(name_obj.via.str.ptr,
                                               name_obj.via.str.size);
        }
    }

    // 2. Process mounted_segments_
    auto mounted_segments_it = fields_map.find("ms");
    if (mounted_segments_it != fields_map.end()) {
        const msgpack::object* value_obj = mounted_segments_it->second;
        if (value_obj->type != msgpack::type::MAP) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager mounted_segments is not map"));
        }

        // Iterate through all mounted segments
        for (uint32_t j = 0; j < value_obj->via.map.size; ++j) {
            const msgpack::object& segment_key = value_obj->via.map.ptr[j].key;
            const msgpack::object& segment_value =
                value_obj->via.map.ptr[j].val;

            // Ensure key is a string (UUID)
            if (segment_key.type != msgpack::type::STR) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize SegmentManager "
                                       "mounted_segments key is not string"));
            }

            std::string uuid_str(segment_key.via.str.ptr,
                                 segment_key.via.str.size);
            UUID segment_uuid;
            if (!StringToUuid(uuid_str, segment_uuid)) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize SegmentManager mounted_segments "
                                "uuid {} is invalid",
                                uuid_str)));
            }

            // Deserialize MountedSegment object
            auto result =
                Serializer<MountedSegment>::deserialize(segment_value);
            if (!result) {
                return tl::unexpected(result.error());
            }

            segment_manager_->mounted_segments_.emplace(
                segment_uuid, std::move(result.value()));
        }
    }

    // 3. Process client_segments_
    auto client_segments_it = fields_map.find("cs");
    if (client_segments_it != fields_map.end()) {
        const msgpack::object* value_obj = client_segments_it->second;
        if (value_obj->type != msgpack::type::MAP) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager client_segments is not map"));
        }

        // Iterate through all client segments
        for (uint32_t j = 0; j < value_obj->via.map.size; ++j) {
            const msgpack::object& client_key = value_obj->via.map.ptr[j].key;
            const msgpack::object& client_value = value_obj->via.map.ptr[j].val;

            // Ensure key is a string (UUID)
            if (client_key.type != msgpack::type::STR) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize SegmentManager "
                                       "client_segments key is not string"));
            }

            std::string client_uuid_str(client_key.via.str.ptr,
                                        client_key.via.str.size);
            UUID client_uuid;
            if (!StringToUuid(client_uuid_str, client_uuid)) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize SegmentManager client_segments "
                                "client uuid {} is invalid",
                                client_uuid_str)));
            }

            // Ensure value is an array
            if (client_value.type != msgpack::type::ARRAY) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize SegmentManager "
                                       "client_segments value is not array"));
            }

            // Deserialize segment IDs array
            std::vector<UUID> segment_ids;
            segment_ids.reserve(client_value.via.array.size);

            for (uint32_t k = 0; k < client_value.via.array.size; ++k) {
                const msgpack::object& segment_id_obj =
                    client_value.via.array.ptr[k];

                if (segment_id_obj.type != msgpack::type::STR) {
                    return tl::unexpected(SerializationError(
                        ErrorCode::DESERIALIZE_FAIL,
                        "deserialize SegmentManager segment_id is not string"));
                }

                std::string segment_uuid_str(segment_id_obj.via.str.ptr,
                                             segment_id_obj.via.str.size);
                UUID segment_uuid;
                if (!StringToUuid(segment_uuid_str, segment_uuid)) {
                    return tl::unexpected(SerializationError(
                        ErrorCode::DESERIALIZE_FAIL,
                        fmt::format("deserialize SegmentManager segment_id {} "
                                    "is invalid",
                                    segment_uuid_str)));
                }

                segment_ids.push_back(segment_uuid);
            }

            segment_manager_->client_segments_.emplace(client_uuid,
                                                       std::move(segment_ids));
        }
    }

    // Restore allocator_manager_ based on mounted_segments_ and saved names
    // order
    segment_manager_->allocator_manager_ = AllocatorManager();

    // Add allocators in saved original order
    for (const auto& name : saved_allocator_names) {
        for (auto& pair : segment_manager_->mounted_segments_) {
            MountedSegment& mounted_segment = pair.second;
            if (mounted_segment.segment.name == name &&
                mounted_segment.status == SegmentStatus::OK &&
                mounted_segment.buf_allocator) {
                segment_manager_->allocator_manager_.addAllocator(
                    name, mounted_segment.buf_allocator);
                break;
            }
        }
    }

    // Rebuild client_by_name_ based on client_segments_ and mounted_segments_
    segment_manager_->client_by_name_.clear();
    for (const auto& [client_id, segment_ids] :
         segment_manager_->client_segments_) {
        for (const auto& segment_id : segment_ids) {
            auto it = segment_manager_->mounted_segments_.find(segment_id);
            if (it != segment_manager_->mounted_segments_.end()) {
                segment_manager_->client_by_name_[it->second.segment.name] =
                    client_id;
            }
        }
    }

    // 4. Process client_local_disk_segment_
    segment_manager_->client_local_disk_segment_.clear();
    auto ld_it = fields_map.find("ld");
    if (ld_it != fields_map.end()) {
        const msgpack::object* ld_obj = ld_it->second;
        if (ld_obj->type != msgpack::type::MAP) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "deserialize SegmentManager local_disk_segments is not map"));
        }

        for (uint32_t j = 0; j < ld_obj->via.map.size; ++j) {
            const msgpack::object& client_key = ld_obj->via.map.ptr[j].key;
            const msgpack::object& client_value = ld_obj->via.map.ptr[j].val;

            // Parse client_id
            if (client_key.type != msgpack::type::STR) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize local_disk_segments "
                                       "client key is not string"));
            }

            std::string client_uuid_str(client_key.via.str.ptr,
                                        client_key.via.str.size);
            UUID client_id;
            if (!StringToUuid(client_uuid_str, client_id)) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize local_disk_segments "
                                "client uuid {} is invalid",
                                client_uuid_str)));
            }

            // Parse LocalDiskSegment array: [enable_offloading, count, key1,
            // ts1, ...]
            if (client_value.type != msgpack::type::ARRAY ||
                client_value.via.array.size < 2) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize local_disk_segments "
                                       "value is not valid array"));
            }

            bool enable_offloading = client_value.via.array.ptr[0].as<bool>();
            uint64_t count = client_value.via.array.ptr[1].as<uint64_t>();

            auto segment =
                std::make_shared<LocalDiskSegment>(enable_offloading);

            // Parse offloading_objects
            for (uint64_t k = 0; k < count; ++k) {
                size_t key_idx = 2 + k * 2;
                size_t ts_idx = 2 + k * 2 + 1;
                if (ts_idx >= client_value.via.array.size) {
                    return tl::unexpected(
                        SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                           "deserialize local_disk_segments "
                                           "offloading_objects out of bounds"));
                }

                std::string key(
                    client_value.via.array.ptr[key_idx].via.str.ptr,
                    client_value.via.array.ptr[key_idx].via.str.size);
                int64_t ts = client_value.via.array.ptr[ts_idx].as<int64_t>();
                segment->offloading_objects[key] = ts;
            }

            segment_manager_->client_local_disk_segment_[client_id] =
                std::move(segment);
        }
    }

    return {};
}

void SegmentSerializer::Reset() {
    segment_manager_->mounted_segments_.clear();
    segment_manager_->client_segments_.clear();
    segment_manager_->client_by_name_.clear();
    segment_manager_->client_local_disk_segment_.clear();
    segment_manager_->allocator_manager_ = AllocatorManager();
}

ErrorCode ScopedSegmentAccess::GetClientIdBySegmentName(
    const std::string& segment_name, UUID& client_id) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    if (it == segment_manager_->client_by_name_.end()) {
        LOG(ERROR) << "segment_name=" << segment_name
                   << ", error=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    client_id = it->second;
    return ErrorCode::OK;
}

bool ScopedSegmentAccess::ExistsSegmentName(
    const std::string& segment_name) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    return it != segment_manager_->client_by_name_.end();
}

void SegmentManager::initializeCxlAllocator(const std::string& cxl_path,
                                            const size_t cxl_size) {
    LOG(INFO) << "Init CXL global allocator.";
    LOG(INFO) << "[CXL] create allocator with "
              << "path=" << cxl_path << " base=0x" << std::hex
              << DEFAULT_CXL_BASE << std::dec << " size=" << cxl_size << " ("
              << std::fixed << std::setprecision(2)
              << cxl_size / (1024.0 * 1024 * 1024) << " GB)";

    cxl_global_allocator_ = std::make_shared<CachelibBufferAllocator>(
        cxl_path, DEFAULT_CXL_BASE, cxl_size, cxl_path);
    MasterMetricManager::instance().inc_total_mem_capacity(cxl_path, cxl_size);
}
}  // namespace mooncake