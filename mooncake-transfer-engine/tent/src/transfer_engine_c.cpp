// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transfer_engine.h"

#include <glog/logging.h>

#include "tent/runtime/transfer_engine_impl.h"

#define CAST(ptr) ((mooncake::tent::TransferEngineImpl*)ptr)
#define CHECK_POINTER(ptr)                       \
    if (!ptr) {                                  \
        LOG(ERROR) << "Invalid argument: " #ptr; \
        return -1;                               \
    }

struct Settings {
    std::string path;
    std::unordered_map<std::string, std::string> attrs;
};

thread_local Settings tl_settings;

void tent_load_config_from_file(const char* path) { tl_settings.path = path; }

void tent_set_config(const char* key, const char* value) {
    tl_settings.attrs[key] = value;
}

tent_engine_t tent_create_engine() {
    auto config = std::make_shared<mooncake::tent::Config>();
    if (!tl_settings.path.empty()) {
        auto status = config->load(tl_settings.path);
        if (!status.ok()) {
            LOG(WARNING) << "tent_create_engine: " << status.ToString()
                         << ", fallback to default config";
        }
    }
    for (auto& attr : tl_settings.attrs) config->set(attr.first, attr.second);
    auto engine = new mooncake::tent::TransferEngineImpl(config);
    return (tent_engine_t)engine;
}

void tent_destroy_engine(tent_engine_t engine) {
    if (engine) {
        delete CAST(engine);
    }
}

int tent_segment_name(tent_engine_t engine, char* buf, size_t buf_len) {
    CHECK_POINTER(engine);
    CHECK_POINTER(buf);
    auto result = CAST(engine)->getSegmentName();
    strncpy(buf, result.c_str(), buf_len);
    return 0;
}

int tent_rpc_server_addr_port(tent_engine_t engine, char* addr_buf,
                              size_t buf_len, uint16_t* port) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr_buf);
    CHECK_POINTER(port);
    auto address = CAST(engine)->getRpcServerAddress();
    strncpy(addr_buf, address.c_str(), buf_len);
    *port = CAST(engine)->getRpcServerPort();
    return 0;
}

int tent_open_segment(tent_engine_t engine, tent_segment_id_t* handle,
                      const char* segment_name) {
    CHECK_POINTER(engine);
    CHECK_POINTER(handle);
    CHECK_POINTER(segment_name);
    auto status = CAST(engine)->openSegment(*handle, segment_name);
    if (!status.ok()) {
        LOG(ERROR) << "tent_open_segment: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_close_segment(tent_engine_t engine, tent_segment_id_t handle) {
    CHECK_POINTER(engine);
    auto status = CAST(engine)->closeSegment(handle);
    if (!status.ok()) {
        LOG(ERROR) << "tent_close_segment: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_get_segment_info(tent_engine_t engine, tent_segment_id_t handle,
                          tent_segment_info_t* info) {
    CHECK_POINTER(engine);
    CHECK_POINTER(info);
    mooncake::tent::SegmentInfo pinfo;
    auto status = CAST(engine)->getSegmentInfo(handle, pinfo);
    if (!status.ok()) {
        LOG(ERROR) << "tent_get_segment_info: " << status.ToString();
        return -1;
    }
    if (pinfo.type == mooncake::tent::SegmentInfo::Memory)
        info->type = TYPE_MEMORY;
    else
        info->type = TYPE_FILE;
    info->buffers = nullptr;
    info->num_buffers = (int)pinfo.buffers.size();
    if (info->num_buffers == 0) return 0;

    info->buffers =
        (tent_buffer_info*)malloc(sizeof(tent_buffer_info) * info->num_buffers);
    if (!info->buffers) {
        LOG(ERROR) << "tent_get_segment_info: out of memory";
        return -1;
    }

    for (int i = 0; i < info->num_buffers; ++i) {
        info->buffers[i].base = pinfo.buffers[i].base;
        info->buffers[i].length = pinfo.buffers[i].length;
        strncpy(info->buffers[i].location, pinfo.buffers[i].location.c_str(),
                63);
    }

    return 0;
}

void tent_free_segment_info(tent_segment_info_t* info) {
    if (info && info->buffers) free(info->buffers);
}

int tent_allocate_memory(tent_engine_t engine, void** addr, size_t size,
                         const char* location) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    CHECK_POINTER(location);
    mooncake::tent::MemoryOptions options;
    if (location) options.location = location;
    auto status = CAST(engine)->allocateLocalMemory(addr, size, options);
    if (!status.ok()) {
        LOG(ERROR) << "tent_allocate_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_free_memory(tent_engine_t engine, void* addr) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->freeLocalMemory(addr);
    if (!status.ok()) {
        LOG(ERROR) << "tent_free_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_register_memory(tent_engine_t engine, void* addr, size_t size) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->registerLocalMemory(addr, size);
    if (!status.ok()) {
        LOG(ERROR) << "tent_register_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_unregister_memory(tent_engine_t engine, void* addr, size_t size) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->unregisterLocalMemory(addr, size);
    if (!status.ok()) {
        LOG(ERROR) << "tent_unregister_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

tent_batch_id_t tent_allocate_batch(tent_engine_t engine, size_t batch_size) {
    CHECK_POINTER(engine);
    return (tent_batch_id_t)CAST(engine)->allocateBatch(batch_size);
}

int tent_free_batch(tent_engine_t engine, tent_batch_id_t batch_id) {
    CHECK_POINTER(engine);
    auto status = CAST(engine)->freeBatch(batch_id);
    if (!status.ok()) {
        LOG(ERROR) << "tent_free_batch: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_submit(tent_engine_t engine, tent_batch_id_t batch_id,
                tent_request_t* entries, size_t count) {
    CHECK_POINTER(engine);
    CHECK_POINTER(entries);
    std::vector<mooncake::tent::Request> req_list;
    req_list.resize(count);
    for (size_t index = 0; index < count; index++) {
        req_list[index].opcode =
            (mooncake::tent::Request::OpCode)entries[index].opcode;
        req_list[index].source = entries[index].source;
        req_list[index].target_id = entries[index].target_id;
        req_list[index].target_offset = entries[index].target_offset;
        req_list[index].length = entries[index].length;
    }
    auto status = CAST(engine)->submitTransfer(batch_id, req_list);
    if (!status.ok()) {
        LOG(ERROR) << "tent_submit: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_submit_notif(tent_engine_t engine, tent_batch_id_t batch_id,
                      tent_request_t* entries, size_t count, const char* name,
                      const char* message) {
    CHECK_POINTER(engine);
    CHECK_POINTER(entries);
    CHECK_POINTER(name);
    CHECK_POINTER(message);
    std::vector<mooncake::tent::Request> req_list;
    req_list.resize(count);
    for (size_t index = 0; index < count; index++) {
        req_list[index].opcode =
            (mooncake::tent::Request::OpCode)entries[index].opcode;
        req_list[index].source = entries[index].source;
        req_list[index].target_id = entries[index].target_id;
        req_list[index].target_offset = entries[index].target_offset;
        req_list[index].length = entries[index].length;
    }
    mooncake::tent::Notification notifi;
    notifi.name = name;
    notifi.msg = message;
    auto status = CAST(engine)->submitTransfer(batch_id, req_list, notifi);
    if (!status.ok()) {
        LOG(ERROR) << "tent_submit_notifi: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_send_notifs(tent_engine_t engine, tent_segment_id_t handle,
                     const char* name, const char* message) {
    CHECK_POINTER(engine);
    CHECK_POINTER(message);
    mooncake::tent::Notification notifi;
    notifi.name = name;
    notifi.msg = message;
    auto status = CAST(engine)->sendNotification(handle, notifi);
    if (!status.ok()) {
        LOG(ERROR) << "tent_send_notifs: " << status.ToString();
        return -1;
    }
    return 0;
}

int tent_recv_notifs(tent_engine_t engine, tent_notifi_info* info) {
    CHECK_POINTER(engine);
    CHECK_POINTER(info);
    std::vector<mooncake::tent::Notification> notify_list;
    auto status = CAST(engine)->receiveNotification(notify_list);
    if (!status.ok()) {
        LOG(ERROR) << "tent_recv_notifs: " << status.ToString();
        return -1;
    }
    info->num_records = (int)notify_list.size();
    if (info->num_records) {
        info->records = (tent_notifi_record*)malloc(sizeof(tent_notifi_record) *
                                                    info->num_records);
        if (!info->records) {
            LOG(ERROR) << "tent_recv_notifs: out of memory";
            return -1;
        }

        for (int i = 0; i < info->num_records; ++i) {
            info->records[i].handle = 0;
            strncpy(info->records[i].name, notify_list[i].name.c_str(), 255);
            strncpy(info->records[i].msg, notify_list[i].msg.c_str(), 4095);
        }
    }
    return 0;
}

void tent_free_notifs(tent_notifi_info* info) {
    if (info && info->records) free(info->records);
}

int tent_task_status(tent_engine_t engine, tent_batch_id_t batch_id,
                     size_t task_id, tent_status_t* xfer_status) {
    CHECK_POINTER(engine);
    CHECK_POINTER(batch_id);
    CHECK_POINTER(xfer_status);
    mooncake::tent::TransferStatus internal_status;
    auto status =
        CAST(engine)->getTransferStatus(batch_id, task_id, internal_status);
    if (!status.ok()) {
        LOG(ERROR) << "tent_overall_status: " << status.ToString();
        return -1;
    }
    xfer_status->status = (int)internal_status.s;
    xfer_status->transferred_bytes = internal_status.transferred_bytes;
    return 0;
}

int tent_overall_status(tent_engine_t engine, tent_batch_id_t batch_id,
                        tent_status_t* xfer_status) {
    CHECK_POINTER(engine);
    CHECK_POINTER(batch_id);
    CHECK_POINTER(xfer_status);
    mooncake::tent::TransferStatus internal_status;
    auto status = CAST(engine)->getTransferStatus(batch_id, internal_status);
    if (!status.ok()) {
        LOG(ERROR) << "tent_overall_status: " << status.ToString();
        return -1;
    }
    xfer_status->status = (int)internal_status.s;
    xfer_status->transferred_bytes = internal_status.transferred_bytes;
    return 0;
}
