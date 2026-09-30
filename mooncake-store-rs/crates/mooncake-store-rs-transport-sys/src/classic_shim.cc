#include "transfer_engine.h"
#include "transfer_engine_c.h"
#include "config.h"

#include <cstring>
#include <memory>
#include <string>

namespace {

int encodeSegmentDescForStore(const mooncake::TransferMetadata::SegmentDesc &desc,
                              Json::Value &segmentJSON) {
    segmentJSON["name"] = desc.name;
    segmentJSON["protocol"] = desc.protocol;
    segmentJSON["tcp_data_port"] = desc.tcp_data_port;
    segmentJSON["timestamp"] = desc.timestamp;

    if (desc.protocol == "rdma" || desc.protocol == "barex" ||
        desc.protocol == "efa") {
        Json::Value devicesJSON(Json::arrayValue);
        for (const auto &device : desc.devices) {
            Json::Value deviceJSON;
            deviceJSON["name"] = device.name;
            deviceJSON["lid"] = device.lid;
            deviceJSON["gid"] = device.gid;
            devicesJSON.append(deviceJSON);
        }
        segmentJSON["devices"] = devicesJSON;

        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            Json::Value rkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.rkey) rkeyJSON.append(entry);
            bufferJSON["rkey"] = rkeyJSON;
            Json::Value lkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.lkey) lkeyJSON.append(entry);
            bufferJSON["lkey"] = lkeyJSON;
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
        segmentJSON["priority_matrix"] = desc.topology.toJson();
        return 0;
    }

    if (desc.protocol == "tcp") {
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
        return 0;
    }

    LOG(ERROR) << "Unsupported segment descriptor for Store-RS classic TE "
               << "export, name " << desc.name << " protocol "
               << desc.protocol;
    return -1;
}

std::shared_ptr<mooncake::TransferMetadata::SegmentDesc>
decodeSegmentDescForStore(Json::Value &segmentJSON,
                          const std::string &segment_name) {
    auto desc = std::make_shared<mooncake::TransferMetadata::SegmentDesc>();
    desc->name = segment_name;
    desc->protocol = segmentJSON["protocol"].asString();
    desc->tcp_data_port = segmentJSON["tcp_data_port"].asInt();
    if (segmentJSON.isMember("timestamp"))
        desc->timestamp = segmentJSON["timestamp"].asString();

    if (desc->protocol == "rdma" || desc->protocol == "barex" ||
        desc->protocol == "efa") {
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            mooncake::TransferMetadata::DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            device.gid = deviceJSON["gid"].asString();
            if (device.name.empty() || device.gid.empty()) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol "
                             << desc->protocol;
                return nullptr;
            }
            desc->devices.push_back(device);
        }

        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            mooncake::TransferMetadata::BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            for (const auto &rkeyJSON : bufferJSON["rkey"])
                buffer.rkey.push_back(rkeyJSON.asUInt());
            for (const auto &lkeyJSON : bufferJSON["lkey"])
                buffer.lkey.push_back(lkeyJSON.asUInt());
            if (buffer.name.empty() || !buffer.addr || !buffer.length ||
                buffer.rkey.empty() ||
                buffer.rkey.size() != buffer.lkey.size()) {
                LOG(WARNING)
                    << "Corrupted segment descriptor, name " << segment_name
                    << " protocol " << desc->protocol << ", " << buffer.name
                    << ", " << buffer.addr << ", " << buffer.length << ", "
                    << buffer.rkey.size() << ", " << buffer.lkey.size();
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }

        int ret = desc->topology.parse(
            segmentJSON["priority_matrix"].toStyledString());
        if (ret) {
            LOG(WARNING) << "Corrupted segment topology, name "
                         << segment_name << " protocol " << desc->protocol;
        }
        return desc;
    }

    if (desc->protocol == "tcp") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            mooncake::TransferMetadata::BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            if (buffer.name.empty() || !buffer.addr || !buffer.length) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol "
                             << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }
        return desc;
    }

    LOG(ERROR) << "Unsupported segment descriptor for Store-RS classic TE "
               << "cache, name " << segment_name << " protocol "
               << desc->protocol;
    return nullptr;
}

uint64_t segmentCacheId(const std::string &segment_name) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : segment_name) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    // classic TE exposes segment handles through the C API as int32_t even
    // though the C++ metadata map uses uint64_t SegmentID. Keep synthetic
    // descriptor IDs inside the positive int32_t range so openSegment() and
    // later descriptor lookups address the same cache entry.
    return (hash % 0x3fffffffULL) + 0x40000000ULL;
}

}  // namespace

extern "C" int mooncake_classic_republish_local_metadata(
    transfer_engine_t engine) {
    if (engine == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto local_desc = metadata->getSegmentDescByID(mooncake::LOCAL_SEGMENT_ID);
    if (!local_desc) {
        return -1;
    }

    auto rpc_desc = metadata->localRpcMeta();
    int rc = metadata->addRpcMetaEntry(local_desc->name, rpc_desc);
    if (rc != 0) {
        return rc;
    }
    return metadata->updateLocalSegmentDesc();
}

extern "C" size_t mooncake_classic_get_local_segment_descriptor_json_size(
    transfer_engine_t engine) {
    if (engine == nullptr) {
        return 0;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return 0;
    }

    auto local_desc = metadata->getSegmentDescByID(mooncake::LOCAL_SEGMENT_ID);
    if (!local_desc) {
        return 0;
    }

    Json::Value segmentJSON;
    if (encodeSegmentDescForStore(*local_desc, segmentJSON) != 0) {
        return 0;
    }
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    auto payload = Json::writeString(writer, segmentJSON);
    return payload.size() + 1;
}

extern "C" int mooncake_classic_get_local_segment_descriptor_json(
    transfer_engine_t engine, char *buf_out, size_t buf_len) {
    if (engine == nullptr || buf_out == nullptr || buf_len == 0) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto local_desc = metadata->getSegmentDescByID(mooncake::LOCAL_SEGMENT_ID);
    if (!local_desc) {
        return -1;
    }

    Json::Value segmentJSON;
    if (encodeSegmentDescForStore(*local_desc, segmentJSON) != 0) {
        return -1;
    }
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    auto payload = Json::writeString(writer, segmentJSON);
    if (payload.size() + 1 > buf_len) {
        return -1;
    }
    std::memcpy(buf_out, payload.c_str(), payload.size() + 1);
    return 0;
}

extern "C" int mooncake_classic_cache_segment_descriptor_json(
    transfer_engine_t engine, const char *segment_name,
    const char *descriptor_json) {
    if (engine == nullptr || segment_name == nullptr ||
        descriptor_json == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    Json::Value segmentJSON;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    std::string payload(descriptor_json);
    if (!reader->parse(payload.data(), payload.data() + payload.size(),
                       &segmentJSON, &errs)) {
        LOG(ERROR) << "Failed to parse Store-RS segment descriptor JSON: "
                   << errs;
        return -1;
    }

    std::string open_name(segment_name);
    auto desc = decodeSegmentDescForStore(segmentJSON, open_name);
    if (!desc) {
        return -1;
    }
    return metadata->addLocalSegment(segmentCacheId(open_name), open_name,
                                     std::move(desc));
}

extern "C" int mooncake_classic_get_batch_transfer_status(
    transfer_engine_t engine, batch_id_t batch_id,
    struct transfer_status *status) {
    if (engine == nullptr || status == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    mooncake::TransferStatus native_status;
    auto result =
        native->getBatchTransferStatus((mooncake::BatchID)batch_id, native_status);
    if (!result.ok()) {
        return static_cast<int>(result.code());
    }

    status->status = static_cast<int>(native_status.s);
    status->transferred_bytes = native_status.transferred_bytes;
    return 0;
}

extern "C" int mooncake_classic_get_segment_first_buffer(
    transfer_engine_t engine, segment_handle_t segment_id, uint64_t *addr_out,
    uint64_t *length_out) {
    if (engine == nullptr || addr_out == nullptr || length_out == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto desc = metadata->getSegmentDescByID(segment_id);
    if (!desc || desc->buffers.empty()) {
        return -1;
    }

    *addr_out = desc->buffers[0].addr;
    *length_out = desc->buffers[0].length;
    return 0;
}

extern "C" int mooncake_classic_get_segment_buffer_count(
    transfer_engine_t engine, segment_handle_t segment_id, size_t *count_out) {
    if (engine == nullptr || count_out == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto desc = metadata->getSegmentDescByID(segment_id);
    if (!desc) {
        return -1;
    }

    *count_out = desc->buffers.size();
    return 0;
}

extern "C" int mooncake_classic_get_segment_buffer(
    transfer_engine_t engine, segment_handle_t segment_id, size_t index,
    uint64_t *addr_out, uint64_t *length_out) {
    if (engine == nullptr || addr_out == nullptr || length_out == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto desc = metadata->getSegmentDescByID(segment_id);
    if (!desc || index >= desc->buffers.size()) {
        return -1;
    }

    *addr_out = desc->buffers[index].addr;
    *length_out = desc->buffers[index].length;
    return 0;
}

extern "C" uint64_t mooncake_classic_get_max_mr_size() {
    return mooncake::globalConfig().max_mr_size;
}
