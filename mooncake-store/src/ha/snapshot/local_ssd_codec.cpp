#include "ha/snapshot/local_ssd_codec.h"

#include <fmt/format.h>

#include "tenant_id.h"

namespace mooncake::ha {
namespace {

bool IsMsgpackInteger(const msgpack::object& object) {
    return object.type == msgpack::type::POSITIVE_INTEGER ||
           object.type == msgpack::type::NEGATIVE_INTEGER;
}

SerializationError DecodeError(std::string message) {
    return SerializationError(ErrorCode::DESERIALIZE_FAIL, message);
}

}  // namespace

tl::expected<void, SerializationError> LocalSsdCodec::Encode(
    const LocalSsdPersistedState& state, MsgpackPacker& packer) {
    packer.pack_map(state.size());
    for (const auto& [client_id, client] : state) {
        packer.pack(UuidToString(client_id));
        packer.pack_array(2 + client.pending_offloads.size() * 2 + 1);
        packer.pack(client.enable_offloading);
        packer.pack(static_cast<uint64_t>(client.pending_offloads.size()));
        for (const auto& [encoded_key, task] : client.pending_offloads) {
            packer.pack(encoded_key);
            packer.pack_array(3);
            packer.pack(task.tenant_id);
            packer.pack(task.key);
            packer.pack(task.size);
        }
        packer.pack(client.total_capacity_bytes);
    }
    return {};
}

tl::expected<LocalSsdPersistedState, SerializationError> LocalSsdCodec::Decode(
    const msgpack::object* ld_value) {
    LocalSsdPersistedState state;
    if (ld_value == nullptr) {
        return state;
    }
    if (ld_value->type != msgpack::type::MAP) {
        return tl::unexpected(
            DecodeError("deserialize local_disk_segments is not map"));
    }

    try {
        for (uint32_t i = 0; i < ld_value->via.map.size; ++i) {
            const auto& client_key = ld_value->via.map.ptr[i].key;
            const auto& client_value = ld_value->via.map.ptr[i].val;
            if (client_key.type != msgpack::type::STR) {
                return tl::unexpected(DecodeError(
                    "deserialize local_disk_segments client key is not "
                    "string"));
            }
            std::string client_uuid(client_key.via.str.ptr,
                                    client_key.via.str.size);
            UUID client_id;
            if (!StringToUuid(client_uuid, client_id)) {
                return tl::unexpected(DecodeError(fmt::format(
                    "deserialize local_disk_segments client uuid {} is "
                    "invalid",
                    client_uuid)));
            }
            if (client_value.type != msgpack::type::ARRAY ||
                client_value.via.array.size < 2) {
                return tl::unexpected(DecodeError(
                    "deserialize local_disk_segments value is not valid "
                    "array"));
            }
            if (client_value.via.array.ptr[0].type != msgpack::type::BOOLEAN ||
                !IsMsgpackInteger(client_value.via.array.ptr[1])) {
                return tl::unexpected(DecodeError(
                    "deserialize local_disk_segments header is invalid"));
            }

            LocalSsdPersistedClient client;
            client.enable_offloading = client_value.via.array.ptr[0].as<bool>();
            uint64_t count = client_value.via.array.ptr[1].as<uint64_t>();
            for (uint64_t task_number = 0; task_number < count; ++task_number) {
                size_t key_index = 2 + task_number * 2;
                size_t task_index = key_index + 1;
                if (task_index >= client_value.via.array.size) {
                    return tl::unexpected(DecodeError(
                        "deserialize local_disk_segments offloading_objects "
                        "out of bounds"));
                }
                const auto& key_object = client_value.via.array.ptr[key_index];
                if (key_object.type != msgpack::type::STR) {
                    return tl::unexpected(DecodeError(
                        "deserialize local_disk_segments offloading key is "
                        "not string"));
                }
                std::string encoded_key(key_object.via.str.ptr,
                                        key_object.via.str.size);
                const auto& task_object =
                    client_value.via.array.ptr[task_index];
                OffloadTaskItem task;
                if (task_object.type == msgpack::type::ARRAY &&
                    task_object.via.array.size == 3) {
                    if (task_object.via.array.ptr[0].type !=
                            msgpack::type::STR ||
                        task_object.via.array.ptr[1].type !=
                            msgpack::type::STR ||
                        !IsMsgpackInteger(task_object.via.array.ptr[2])) {
                        return tl::unexpected(DecodeError(
                            "deserialize local_disk_segments offloading task "
                            "is invalid"));
                    }
                    task.tenant_id =
                        task_object.via.array.ptr[0].as<std::string>();
                    task.key = task_object.via.array.ptr[1].as<std::string>();
                    task.size = task_object.via.array.ptr[2].as<int64_t>();
                } else {
                    if (!IsMsgpackInteger(task_object)) {
                        return tl::unexpected(DecodeError(
                            "deserialize local_disk_segments legacy "
                            "offloading size is not integer"));
                    }
                    auto [tenant_id, object_key] =
                        TenantId::ParseScopedKey(encoded_key);
                    task = OffloadTaskItem{.tenant_id = tenant_id.value(),
                                           .key = std::move(object_key),
                                           .size = task_object.as<int64_t>()};
                }
                client.pending_offloads.insert_or_assign(std::move(encoded_key),
                                                         std::move(task));
            }

            const size_t capacity_index = 2 + count * 2;
            if (client_value.via.array.size > capacity_index &&
                IsMsgpackInteger(client_value.via.array.ptr[capacity_index])) {
                client.total_capacity_bytes =
                    client_value.via.array.ptr[capacity_index].as<int64_t>();
            }
            state.insert_or_assign(client_id, std::move(client));
        }
    } catch (const msgpack::type_error& error) {
        return tl::unexpected(DecodeError(fmt::format(
            "deserialize local_disk_segments type error: {}", error.what())));
    } catch (const std::exception& error) {
        return tl::unexpected(DecodeError(fmt::format(
            "deserialize local_disk_segments failed: {}", error.what())));
    }
    return state;
}

}  // namespace mooncake::ha
