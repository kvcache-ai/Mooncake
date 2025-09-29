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

#include "v1/transfer_engine_c.h"

#include <glog/logging.h>

#include "v1/runtime/transfer_engine_impl.h"

#define CAST(ptr) ((mooncake::v1::TransferEngineImpl *)ptr)
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

void mc_load_config_from_file(const char *path) { tl_settings.path = path; }

void mc_set_config(const char *key, const char *value) {
    tl_settings.attrs[key] = value;
}

mc_engine_t mc_create_engine() {
    auto config = std::make_shared<mooncake::v1::ConfigManager>();
    if (!tl_settings.path.empty()) {
        auto status = config->loadConfigContent(tl_settings.path);
        if (!status.ok()) {
            LOG(WARNING) << "mc_create_engine: " << status.ToString()
                         << ", fallback to default config";
        }
    }
    for (auto &attr : tl_settings.attrs) config->set(attr.first, attr.second);
    auto engine = new mooncake::v1::TransferEngineImpl(config);
    return (mc_engine_t)engine;
}

void mc_destroy_engine(mc_engine_t engine) {
    if (engine) {
        delete CAST(engine);
    }
}

int mc_segment_name(mc_engine_t engine, char *buf, size_t buf_len) {
    CHECK_POINTER(engine);
    CHECK_POINTER(buf);
    auto result = CAST(engine)->getSegmentName();
    strncpy(buf, result.c_str(), buf_len);
    return 0;
}

int mc_rpc_server_addr_port(mc_engine_t engine, char *addr_buf, size_t buf_len,
                            uint16_t *port) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr_buf);
    CHECK_POINTER(port);
    auto address = CAST(engine)->getRpcServerAddress();
    strncpy(addr_buf, address.c_str(), buf_len);
    *port = CAST(engine)->getRpcServerPort();
    return 0;
}

int mc_open_segment(mc_engine_t engine, mc_segment_id_t *handle,
                    const char *segment_name) {
    CHECK_POINTER(engine);
    CHECK_POINTER(handle);
    CHECK_POINTER(segment_name);
    auto status = CAST(engine)->openSegment(*handle, segment_name);
    if (!status.ok()) {
        LOG(ERROR) << "mc_open_segment: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_close_segment(mc_engine_t engine, mc_segment_id_t handle) {
    CHECK_POINTER(engine);
    auto status = CAST(engine)->closeSegment(handle);
    if (!status.ok()) {
        LOG(ERROR) << "mc_close_segment: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_get_segment_info(mc_engine_t engine, mc_segment_id_t handle,
                        mc_segment_info_t *info) {
    CHECK_POINTER(engine);
    CHECK_POINTER(info);
    mooncake::v1::SegmentInfo pinfo;
    auto status = CAST(engine)->getSegmentInfo(handle, pinfo);
    if (!status.ok()) {
        LOG(ERROR) << "mc_get_segment_info: " << status.ToString();
        return -1;
    }
    if (pinfo.type == mooncake::v1::SegmentInfo::Memory)
        info->type = TYPE_MEMORY;
    else
        info->type = TYPE_FILE;
    info->buffers = nullptr;
    info->num_buffers = (int)pinfo.buffers.size();
    if (info->num_buffers == 0) return 0;

    info->buffers =
        (mc_buffer_info *)malloc(sizeof(mc_buffer_info) * info->num_buffers);
    if (!info->buffers) {
        LOG(ERROR) << "mc_get_segment_info: out of memory";
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

void mc_free_segment_info(mc_segment_info_t *info) {
    if (info && info->buffers) free(info->buffers);
}

int mc_allocate_memory(mc_engine_t engine, void **addr, size_t size,
                       const char *location) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    CHECK_POINTER(location);
    mooncake::v1::MemoryOptions options;
    if (location) options.location = location;
    auto status = CAST(engine)->allocateLocalMemory(addr, size, options);
    if (!status.ok()) {
        LOG(ERROR) << "mc_allocate_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_free_memory(mc_engine_t engine, void *addr) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->freeLocalMemory(addr);
    if (!status.ok()) {
        LOG(ERROR) << "mc_free_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_register_memory(mc_engine_t engine, void *addr, size_t size) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->registerLocalMemory(addr, size);
    if (!status.ok()) {
        LOG(ERROR) << "mc_register_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_unregister_memory(mc_engine_t engine, void *addr, size_t size) {
    CHECK_POINTER(engine);
    CHECK_POINTER(addr);
    auto status = CAST(engine)->unregisterLocalMemory(addr, size);
    if (!status.ok()) {
        LOG(ERROR) << "mc_unregister_memory: " << status.ToString();
        return -1;
    }
    return 0;
}

mc_batch_id_t mc_allocate_batch(mc_engine_t engine, size_t batch_size) {
    CHECK_POINTER(engine);
    return (mc_batch_id_t)CAST(engine)->allocateBatch(batch_size);
}

int mc_free_batch(mc_engine_t engine, mc_batch_id_t batch_id) {
    CHECK_POINTER(engine);
    auto status = CAST(engine)->freeBatch(batch_id);
    if (!status.ok()) {
        LOG(ERROR) << "mc_free_batch: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_submit(mc_engine_t engine, mc_batch_id_t batch_id, mc_request_t *entries,
              size_t count) {
    CHECK_POINTER(engine);
    CHECK_POINTER(entries);
    std::vector<mooncake::v1::Request> req_list;
    req_list.resize(count);
    for (size_t index = 0; index < count; index++) {
        req_list[index].opcode =
            (mooncake::v1::Request::OpCode)entries[index].opcode;
        req_list[index].source = entries[index].source;
        req_list[index].target_id = entries[index].target_id;
        req_list[index].target_offset = entries[index].target_offset;
        req_list[index].length = entries[index].length;
    }
    auto status = CAST(engine)->submitTransfer(batch_id, req_list);
    if (!status.ok()) {
        LOG(ERROR) << "mc_submit: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_send_notifs(mc_engine_t engine, mc_segment_id_t handle,
                   const char *message) {
    CHECK_POINTER(engine);
    CHECK_POINTER(message);
    auto status = CAST(engine)->sendNotification(handle, message);
    if (!status.ok()) {
        LOG(ERROR) << "mc_send_notifs: " << status.ToString();
        return -1;
    }
    return 0;
}

int mc_recv_notifs(mc_engine_t engine, mc_notifi_info *info) {
    CHECK_POINTER(engine);
    CHECK_POINTER(info);
    std::vector<mooncake::v1::Notification> notify_list;
    auto status = CAST(engine)->receiveNotification(notify_list);
    if (!status.ok()) {
        LOG(ERROR) << "mc_recv_notifs: " << status.ToString();
        return -1;
    }
    info->num_records = (int)notify_list.size();
    if (info->num_records) {
        info->records = (mc_notifi_record *)malloc(sizeof(mc_notifi_record) *
                                                   info->num_records);
        if (!info->records) {
            LOG(ERROR) << "mc_recv_notifs: out of memory";
            return -1;
        }

        for (int i = 0; i < info->num_records; ++i) {
            info->records[i].handle = 0;
            strncpy(info->records[i].content, notify_list[i].c_str(), 4095);
        }
    }
    return 0;
}

void mc_free_notifs(mc_notifi_info *info) {
    if (info && info->records) free(info->records);
}

int mc_task_status(mc_engine_t engine, mc_batch_id_t batch_id, size_t task_id,
                   mc_status_t *xfer_status) {
    CHECK_POINTER(engine);
    CHECK_POINTER(batch_id);
    CHECK_POINTER(xfer_status);
    mooncake::v1::TransferStatus internal_status;
    auto status =
        CAST(engine)->getTransferStatus(batch_id, task_id, internal_status);
    if (!status.ok()) {
        LOG(ERROR) << "mc_overall_status: " << status.ToString();
        return -1;
    }
    xfer_status->status = (int)internal_status.s;
    xfer_status->transferred_bytes = internal_status.transferred_bytes;
    return 0;
}

int mc_overall_status(mc_engine_t engine, mc_batch_id_t batch_id,
                      mc_status_t *xfer_status) {
    CHECK_POINTER(engine);
    CHECK_POINTER(batch_id);
    CHECK_POINTER(xfer_status);
    mooncake::v1::TransferStatus internal_status;
    auto status = CAST(engine)->getTransferStatus(batch_id, internal_status);
    if (!status.ok()) {
        LOG(ERROR) << "mc_overall_status: " << status.ToString();
        return -1;
    }
    xfer_status->status = (int)internal_status.s;
    xfer_status->transferred_bytes = internal_status.transferred_bytes;
    return 0;
}
