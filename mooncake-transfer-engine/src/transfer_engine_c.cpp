// Copyright 2024 KVCache.AI
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

#include "transfer_engine_c.h"

#include <cstdint>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "v1/transfer_engine.h"

using namespace mooncake;

static bool g_enable_v1 = (getenv("MC_USE_TEV1") != nullptr);
#define CAST(ptr) ((mooncake::v1::TransferEngine *)ptr)

std::pair<std::string, std::string> parseConnectionString(
    const std::string &conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else if (conn_string == P2PHANDSHAKE) {
        proto = "p2p";
        domain = "";
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

transfer_engine_t createTransferEngine(const char *metadata_conn_string,
                                       const char *local_server_name,
                                       const char *ip_or_host_name,
                                       uint64_t rpc_port, int auto_discover) {
    if (g_enable_v1) {
        auto conn_string = parseConnectionString(metadata_conn_string);
        auto config = std::make_shared<mooncake::v1::ConfigManager>();
        config->set("local_segment_name", local_server_name);
        config->set("metadata_type", conn_string.first);
        config->set("metadata_servers", conn_string.second);
        auto engine = new mooncake::v1::TransferEngine(config);
        if (!engine->available()) return nullptr;
        return (transfer_engine_t)engine;
    }
    TransferEngine *native = new TransferEngine(auto_discover);
    int ret = native->init(metadata_conn_string, local_server_name,
                           ip_or_host_name, rpc_port);
    if (ret) {
        delete native;
        return nullptr;
    }
    return (transfer_engine_t)native;
}

int getLocalIpAndPort(transfer_engine_t engine, char *buf_out, size_t buf_len) {
    if (g_enable_v1) {
        auto address = CAST(engine)->getRpcServerAddress() + ":" +
                       std::to_string(CAST(engine)->getRpcServerPort());
        strncpy(buf_out, address.c_str(), buf_len);
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    auto str = native->getLocalIpAndPort();
    strncpy(buf_out, str.c_str(), buf_len);
    return 0;
}

transport_t installTransport(transfer_engine_t engine, const char *proto,
                             void **args) {
    if (g_enable_v1) return 0;
    TransferEngine *native = (TransferEngine *)engine;
    return (transport_t)native->installTransport(proto, args);
}

int uninstallTransport(transfer_engine_t engine, const char *proto) {
    if (g_enable_v1) return 0;
    TransferEngine *native = (TransferEngine *)engine;
    return native->uninstallTransport(proto);
}

void destroyTransferEngine(transfer_engine_t engine) {
    if (g_enable_v1) {
        delete CAST(engine);
        return;
    }
    TransferEngine *native = (TransferEngine *)engine;
    delete native;
}

segment_id_t openSegment(transfer_engine_t engine, const char *segment_name) {
    if (g_enable_v1) {
        SegmentID handle;
        auto status = CAST(engine)->openSegment(handle, segment_name);
        if (!status.ok()) {
            LOG(ERROR) << "openSegment: " << status.ToString();
            return -1;
        }
        return (segment_id_t)handle;
    }
    TransferEngine *native = (TransferEngine *)engine;
    return native->openSegment(segment_name);
}

segment_id_t openSegmentNoCache(transfer_engine_t engine,
                                const char *segment_name) {
    if (g_enable_v1) {
        SegmentID handle;
        auto status = CAST(engine)->openSegment(handle, segment_name);
        if (!status.ok()) {
            LOG(ERROR) << "openSegment: " << status.ToString();
            return -1;
        }
        return (segment_id_t)handle;
    }
    TransferEngine *native = (TransferEngine *)engine;
    int rc = native->syncSegmentCache(segment_name);
    if (rc) return rc;
    return native->openSegment(segment_name);
}

int closeSegment(transfer_engine_t engine, segment_id_t segment_id) {
    if (g_enable_v1) {
        auto status = CAST(engine)->closeSegment(segment_id);
        if (!status.ok()) {
            LOG(ERROR) << "mc_close_segment: " << status.ToString();
            return -1;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    return native->closeSegment(segment_id);
}

int removeLocalSegment(transfer_engine_t engine, const char *segment_name) {
    if (g_enable_v1) return 0;
    TransferEngine *native = (TransferEngine *)engine;
    return native->removeLocalSegment(segment_name);
}

int registerLocalMemory(transfer_engine_t engine, void *addr, size_t length,
                        const char *location, int remote_accessible) {
    if (g_enable_v1) {
        auto status = CAST(engine)->registerLocalMemory(addr, length);
        if (!status.ok()) {
            LOG(ERROR) << "mc_register_memory: " << status.ToString();
            return -1;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    return native->registerLocalMemory(addr, length, location,
                                       remote_accessible, true);
}

int unregisterLocalMemory(transfer_engine_t engine, void *addr) {
    if (g_enable_v1) {
        auto status = CAST(engine)->unregisterLocalMemory(addr, 0);
        if (!status.ok()) {
            LOG(ERROR) << "mc_unregister_memory: " << status.ToString();
            return -1;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    return native->unregisterLocalMemory(addr);
}

int registerLocalMemoryBatch(transfer_engine_t engine,
                             buffer_entry_t *buffer_list, size_t buffer_len,
                             const char *location) {
    if (g_enable_v1) {
        for (size_t i = 0; i < buffer_len; ++i) {
            int rc = registerLocalMemory(engine, buffer_list[i].addr,
                                         buffer_list[i].length, location, true);
            if (rc) return rc;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    std::vector<BufferEntry> native_buffer_list;
    for (size_t i = 0; i < buffer_len; ++i) {
        BufferEntry entry;
        entry.addr = buffer_list[i].addr;
        entry.length = buffer_list[i].length;
        native_buffer_list.push_back(entry);
    }
    return native->registerLocalMemoryBatch(native_buffer_list, location);
}

int unregisterLocalMemoryBatch(transfer_engine_t engine, void **addr_list,
                               size_t addr_len) {
    if (g_enable_v1) {
        for (size_t i = 0; i < addr_len; ++i) {
            int rc = unregisterLocalMemory(engine, addr_list[i]);
            if (rc) return rc;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    std::vector<void *> native_addr_list;
    for (size_t i = 0; i < addr_len; ++i)
        native_addr_list.push_back(addr_list[i]);
    return native->unregisterLocalMemoryBatch(native_addr_list);
}

batch_id_t allocateBatchID(transfer_engine_t engine, size_t batch_size) {
    if (g_enable_v1) {
        return (batch_id_t)CAST(engine)->allocateBatch(batch_size);
    }
    TransferEngine *native = (TransferEngine *)engine;
    return (batch_id_t)native->allocateBatchID(batch_size);
}

int submitTransfer(transfer_engine_t engine, batch_id_t batch_id,
                   struct transfer_request *entries, size_t count) {
    if (g_enable_v1) {
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
    TransferEngine *native = (TransferEngine *)engine;
    std::vector<Transport::TransferRequest> native_entries;
    native_entries.resize(count);
    for (size_t index = 0; index < count; index++) {
        native_entries[index].opcode =
            (Transport::TransferRequest::OpCode)entries[index].opcode;
        native_entries[index].source = entries[index].source;
        native_entries[index].target_id = entries[index].target_id;
        native_entries[index].target_offset = entries[index].target_offset;
        native_entries[index].length = entries[index].length;
    }
    Status s =
        native->submitTransfer((Transport::BatchID)batch_id, native_entries);
    return (int)s.code();
}

int submitTransferWithNotify(transfer_engine_t engine, batch_id_t batch_id,
                             struct transfer_request *entries, size_t count,
                             notify_msg_t notify_msg) {
    if (g_enable_v1) return -1;
    uint64_t target_id = entries[0].target_id;
    int rc = submitTransfer(engine, batch_id, entries, count);
    if (rc) {
        return rc;
    }
    // notify
    TransferEngine *native = (TransferEngine *)engine;
    TransferMetadata::NotifyDesc notify;
    notify.name = notify_msg.name;
    notify.notify_msg = notify_msg.msg;
    return native->sendNotifyByID((SegmentID)target_id, notify);
}

int getTransferStatus(transfer_engine_t engine, batch_id_t batch_id,
                      size_t task_id, struct transfer_status *status) {
    if (g_enable_v1) {
        mooncake::v1::TransferStatus internal_status;
        auto ret =
            CAST(engine)->getTransferStatus(batch_id, task_id, internal_status);
        if (!ret.ok()) {
            LOG(ERROR) << "mc_overall_status: " << ret.ToString();
            return -1;
        }
        status->status = (int)internal_status.s;
        status->transferred_bytes = internal_status.transferred_bytes;
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    Transport::TransferStatus native_status;
    Status s = native->getTransferStatus((Transport::BatchID)batch_id, task_id,
                                         native_status);
    if (s.ok()) {
        status->status = (int)native_status.s;
        status->transferred_bytes = native_status.transferred_bytes;
    }
    return (int)s.code();
}

notify_msg_t *getNotifsFromEngine(transfer_engine_t engine, int *size) {
    if (g_enable_v1) return nullptr;
    TransferEngine *native = (TransferEngine *)engine;
    std::vector<TransferMetadata::NotifyDesc> notifies_desc;
    native->getNotifies(notifies_desc);
    *size = notifies_desc.size();
    notify_msg_t *notifies =
        (notify_msg_t *)malloc(*size * sizeof(notify_msg_t));
    for (int i = 0; i < *size; i++) {
        notifies[i].name = const_cast<char *>(notifies_desc[i].name.c_str());
        notifies[i].msg =
            const_cast<char *>(notifies_desc[i].notify_msg.c_str());
    }
    return notifies;
}

int genNotifyInEngine(transfer_engine_t engine, uint64_t target_id,
                      notify_msg_t notify_msg) {
    TransferEngine *native = (TransferEngine *)engine;
    TransferMetadata::NotifyDesc notify;
    notify.name.assign(notify_msg.name);
    notify.notify_msg.assign(notify_msg.msg);
    return native->sendNotifyByID(target_id, notify);
}

int freeBatchID(transfer_engine_t engine, batch_id_t batch_id) {
    if (g_enable_v1) {
        auto status = CAST(engine)->freeBatch(batch_id);
        if (!status.ok()) {
            LOG(ERROR) << "mc_free_batch: " << status.ToString();
            return -1;
        }
        return 0;
    }
    TransferEngine *native = (TransferEngine *)engine;
    Status s = native->freeBatchID(batch_id);
    return (int)s.code();
}

int syncSegmentCache(transfer_engine_t engine) {
    if (g_enable_v1) return 0;
    TransferEngine *native = (TransferEngine *)engine;
    return native->syncSegmentCache();
}
