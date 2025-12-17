#include "segment.h"

#include "master_metric_manager.h"
#include "utils/zstd_util.h"

namespace mooncake {

ErrorCode ScopedSegmentAccess::MountSegment(const Segment& segment,
                                            const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

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
    auto&& segment = segment_manager_->mounted_segments_.find(segment_id);
    if (segment != segment_manager_->mounted_segments_.end()) {
        segment_name = segment->second.segment.name;
    }
    // Remove from mounted_segments_
    segment_manager_->mounted_segments_.erase(segment_id);

    // Decrease the total capacity
    MasterMetricManager::instance().dec_total_mem_capacity(
        segment_name, metrics_dec_capacity);

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

ErrorCode ScopedSegmentAccess::GetAllSegments(std::vector<std::pair<Segment, UUID>>& all_segments) {
    all_segments.clear();

    for (auto& segment_pair : segment_manager_->mounted_segments_) {
        // 查找该segment对应的client_id
        UUID client_id = UUID{0, 0};  // 默认值，表示未找到对应的client

        // 在client_segments_中查找该segment ID对应的client
        for (const auto& client_pair : segment_manager_->client_segments_) {
            const auto& client_id_tmp = client_pair.first;
            const auto& segment_ids = client_pair.second;

            if (std::find(segment_ids.begin(), segment_ids.end(), segment_pair.first) !=
                segment_ids.end()) {
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
            // 查找该segment对应的client_id
            UUID client_id = UUID{0, 0};  // 默认值，表示未找到对应的client

            // 在client_segments_中查找该segment ID对应的client
            for (const auto& client_pair : segment_manager_->client_segments_) {
                const auto& client_id_tmp = client_pair.first;
                const auto& segment_ids = client_pair.second;

                if (std::find(segment_ids.begin(), segment_ids.end(), mounted_segment.segment.id) !=
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

ErrorCode SegmentView::GetSegment(std::shared_ptr<BufferAllocatorBase> allocator,
                                  Segment& segment) const {
    // 检查输入参数
    if (!allocator) {
        return ErrorCode::INVALID_PARAMS;
    }

    // 遍历 mounted_segments_ 查找匹配的 allocator
    for (const auto& [segment_id, mounted_segment] : segment_manager_->mounted_segments_) {
        if (mounted_segment.buf_allocator == allocator) {
            segment = mounted_segment.segment;
            return ErrorCode::OK;
        }
    }

    // 未找到匹配的 segment
    return ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode SegmentView::GetMountedSegment(const UUID& segment_id,
                                         MountedSegment& mountedSegment) const {
    // 检查输入参数
    if (segment_id.first == 0 && segment_id.second == 0) {
        return ErrorCode::INVALID_PARAMS;
    }

    // 在 mounted_segments_ 中查找指定的 segment_id
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    // 检查 segment 状态
    //    if (it->second.status != SegmentStatus::OK) {
    //        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    //    }

    // 返回找到的 segment
    mountedSegment = it->second;
    return ErrorCode::OK;
}


tl::expected<std::vector<uint8_t>, SerializationError> SegmentSerializer::Serialize() {
    if (!segment_manager_) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL, "serialize SegmentManager segment_manager_ is null"));
    }

    if (segment_manager_->memory_allocator_ != BufferAllocatorType::OFFSET) {
        return tl::unexpected(
            SerializationError(ErrorCode::SERIALIZE_UNSUPPORTED,
                               "serialize SegmentManager memory_allocator_ is not offset"));
    }

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // 创建包含所有数据的map
    packer.pack_map(3);  // 3个字段: memory_allocator, mounted_segments, client_segments

    // 序列化memory_allocator_
    packer.pack("ma");
    packer.pack(static_cast<int32_t>(segment_manager_->memory_allocator_));

    // 序列化mounted_segments_
    packer.pack("ms");
    packer.pack_map(segment_manager_->mounted_segments_.size());

    for (const auto& pair : segment_manager_->mounted_segments_) {
        std::string uuid_str = UuidToString(pair.first);
        packer.pack(uuid_str);

        // 序列化MountedSegment对象
        auto result = Serializer<MountedSegment>::serialize(pair.second, packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    // 序列化client_segments_
    packer.pack("cs");
    packer.pack_map(segment_manager_->client_segments_.size());

    for (const auto& pair : segment_manager_->client_segments_) {
        std::string client_uuid_str = UuidToString(pair.first);
        packer.pack(client_uuid_str);

        // 序列化segment IDs数组
        const auto& segment_ids = pair.second;
        packer.pack_array(segment_ids.size());

        for (const auto& segment_id : segment_ids) {
            std::string segment_uuid_str = UuidToString(segment_id);
            packer.pack(segment_uuid_str);
        }
    }

    // 对整个数据进行压缩
    std::vector<uint8_t> compressed_data =
        zstd_compress(reinterpret_cast<const uint8_t*>(sbuf.data()), sbuf.size(), 3);

    // 返回压缩后的数据
    return std::vector<uint8_t>(std::make_move_iterator(compressed_data.begin()),
                                std::make_move_iterator(compressed_data.end()));
}

tl::expected<void, SerializationError> SegmentSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    // 解压缩数据
    std::vector<uint8_t> decompressed_data;
    try {
        decompressed_data =
            zstd_decompress(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                      "deserialize SegmentManager failed to "
                                                      "decompress MessagePack data: " +
                                                          std::string(e.what())));
    }

    // 解析MessagePack数据
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(reinterpret_cast<const char*>(decompressed_data.data()),
                             decompressed_data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize SegmentManager failed to unpack MessagePack data: " +
                                   std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();

    // 检查是否为map
    if (obj.type != msgpack::type::MAP) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize SegmentManager invalid MessagePack "
                                                 "format: expected map"));
    }

    if (!segment_manager_) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "deserialize SegmentManager segment_manager is null"));
    }

    // 清空现有数据
    segment_manager_->mounted_segments_.clear();
    segment_manager_->client_segments_.clear();


    // 将MessagePack map转换为普通map，值使用指针避免拷贝
    std::map<std::string, const msgpack::object*> fields_map;
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const msgpack::object& key_obj = obj.via.map.ptr[i].key;
        const msgpack::object& value_obj = obj.via.map.ptr[i].val;

        // 确保键是字符串
        if (key_obj.type == msgpack::type::STR) {
            std::string key(key_obj.via.str.ptr, key_obj.via.str.size);
            fields_map.emplace(std::move(key), &value_obj);
        }
    }

    // 按顺序处理各个字段
    // 1. 先处理memory_allocator_
    auto allocator_it = fields_map.find("ma");
    if (allocator_it != fields_map.end()) {
        const msgpack::object* value_obj = allocator_it->second;
        if (value_obj->type != msgpack::type::POSITIVE_INTEGER) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize SegmentManager memory_allocator is not int"));
        }

        auto allocatorType = static_cast<BufferAllocatorType>(value_obj->as<int32_t>());

        // 注意：校验类型要一致，只能支持OffsetAllocator
        if (allocatorType != segment_manager_->memory_allocator_) {
            LOG(ERROR) << "deserialize memory allocator type doesn't match "
                          "current setting";
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize SegmentManager memory allocator type doesn't "
                                   "match current setting"));
        }
    }

    // 2. 处理mounted_segments_
    auto mounted_segments_it = fields_map.find("ms");
    if (mounted_segments_it != fields_map.end()) {
        const msgpack::object* value_obj = mounted_segments_it->second;
        if (value_obj->type != msgpack::type::MAP) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize SegmentManager mounted_segments is not map"));
        }

        // 遍历所有mounted segments
        for (uint32_t j = 0; j < value_obj->via.map.size; ++j) {
            const msgpack::object& segment_key = value_obj->via.map.ptr[j].key;
            const msgpack::object& segment_value = value_obj->via.map.ptr[j].val;

            // 确保键是字符串(UUID)
            if (segment_key.type != msgpack::type::STR) {
                return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                         "deserialize SegmentManager "
                                                         "mounted_segments key is not string"));
            }

            std::string uuid_str(segment_key.via.str.ptr, segment_key.via.str.size);
            UUID segment_uuid;
            if (!StringToUuid(uuid_str, segment_uuid)) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       fmt::format("deserialize SegmentManager mounted_segments "
                                                   "uuid {} is invalid",
                                                   uuid_str)));
            }

            // 反序列化MountedSegment对象
            auto result = Serializer<MountedSegment>::deserialize(segment_value);
            if (!result) {
                return tl::unexpected(result.error());
            }

            segment_manager_->mounted_segments_.emplace(segment_uuid, std::move(result.value()));
        }
    }

    // 3. 处理client_segments_
    auto client_segments_it = fields_map.find("cs");
    if (client_segments_it != fields_map.end()) {
        const msgpack::object* value_obj = client_segments_it->second;
        if (value_obj->type != msgpack::type::MAP) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize SegmentManager client_segments is not map"));
        }

        // 遍历所有client segments
        for (uint32_t j = 0; j < value_obj->via.map.size; ++j) {
            const msgpack::object& client_key = value_obj->via.map.ptr[j].key;
            const msgpack::object& client_value = value_obj->via.map.ptr[j].val;

            // 确保键是字符串(UUID)
            if (client_key.type != msgpack::type::STR) {
                return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                         "deserialize SegmentManager "
                                                         "client_segments key is not string"));
            }

            std::string client_uuid_str(client_key.via.str.ptr, client_key.via.str.size);
            UUID client_uuid;
            if (!StringToUuid(client_uuid_str, client_uuid)) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       fmt::format("deserialize SegmentManager client_segments "
                                                   "client uuid {} is invalid",
                                                   client_uuid_str)));
            }

            // 确保值是数组
            if (client_value.type != msgpack::type::ARRAY) {
                return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                         "deserialize SegmentManager "
                                                         "client_segments value is not array"));
            }

            // 反序列化segment IDs数组
            std::vector<UUID> segment_ids;
            segment_ids.reserve(client_value.via.array.size);

            for (uint32_t k = 0; k < client_value.via.array.size; ++k) {
                const msgpack::object& segment_id_obj = client_value.via.array.ptr[k];

                if (segment_id_obj.type != msgpack::type::STR) {
                    return tl::unexpected(
                        SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                           "deserialize SegmentManager segment_id is not string"));
                }

                std::string segment_uuid_str(segment_id_obj.via.str.ptr,
                                             segment_id_obj.via.str.size);
                UUID segment_uuid;
                if (!StringToUuid(segment_uuid_str, segment_uuid)) {
                    return tl::unexpected(
                        SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                           fmt::format("deserialize SegmentManager segment_id {} "
                                                       "is invalid",
                                                       segment_uuid_str)));
                }

                segment_ids.push_back(segment_uuid);
            }

            segment_manager_->client_segments_.emplace(client_uuid, std::move(segment_ids));
        }
    }

    // 基于mounted_segments_恢复allocators_by_name_和allocators_
    segment_manager_->allocator_manager_= AllocatorManager();
    for (auto& pair : segment_manager_->mounted_segments_) {
        MountedSegment& mounted_segment = pair.second;
        if (mounted_segment.status == SegmentStatus::OK &&
            mounted_segment.buf_allocator) {
            // 添加到allocator_manager_
            segment_manager_->allocator_manager_.addAllocator(
                mounted_segment.segment.name, mounted_segment.buf_allocator);
        }
    }

    return {};
}


void SegmentSerializer::Reset() {
    segment_manager_->mounted_segments_.clear();
    segment_manager_->client_segments_.clear();
    segment_manager_->allocator_manager_= AllocatorManager();
}

}  // namespace mooncake
