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

using namespace mooncake;

transfer_engine_t createTransferEngine(const char *metadata_conn_string,
                                       const char *local_server_name,
                                       const char *ip_or_host_name,
                                       uint64_t rpc_port,
                                       int auto_discover) {
    TransferEngine *native = new TransferEngine(auto_discover);
    int ret = native->init(metadata_conn_string, local_server_name,
                           ip_or_host_name, rpc_port);
    if (ret) {
        delete native;
        return nullptr;
    }
    return (transfer_engine_t)native;
}

transport_t installTransport(transfer_engine_t engine, const char *proto,
                             void **args) {
    TransferEngine *native = (TransferEngine *)engine;
    return (transport_t)native->installTransport(proto, args);
}

int uninstallTransport(transfer_engine_t engine, const char *proto) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->uninstallTransport(proto);
}

void destroyTransferEngine(transfer_engine_t engine) {
    TransferEngine *native = (TransferEngine *)engine;
    delete native;
}

segment_id_t openSegment(transfer_engine_t engine, const char *segment_name) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->openSegment(segment_name);
}

segment_id_t openSegmentNoCache(transfer_engine_t engine, const char *segment_name) {
    TransferEngine *native = (TransferEngine *)engine;
    int rc = native->syncSegmentCache(segment_name);
    if (rc) return rc;
    return native->openSegment(segment_name);
}

int closeSegment(transfer_engine_t engine, segment_id_t segment_id) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->closeSegment(segment_id);
}

int registerLocalMemory(transfer_engine_t engine, void *addr, size_t length,
                        const char *location, int remote_accessible) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->registerLocalMemory(addr, length, location,
                                       remote_accessible, true);
}

int unregisterLocalMemory(transfer_engine_t engine, void *addr) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->unregisterLocalMemory(addr);
}

int registerLocalMemoryBatch(transfer_engine_t engine,
                             buffer_entry_t *buffer_list, size_t buffer_len,
                             const char *location) {
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
    TransferEngine *native = (TransferEngine *)engine;
    std::vector<void *> native_addr_list;
    for (size_t i = 0; i < addr_len; ++i)
        native_addr_list.push_back(addr_list[i]);
    return native->unregisterLocalMemoryBatch(native_addr_list);
}

batch_id_t allocateBatchID(transfer_engine_t engine, size_t batch_size) {
    TransferEngine *native = (TransferEngine *)engine;
    return (batch_id_t)native->allocateBatchID(batch_size);
}

int submitTransfer(transfer_engine_t engine, batch_id_t batch_id,
                   struct transfer_request *entries,
                   size_t count) {
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

int getTransferStatus(transfer_engine_t engine,
                      batch_id_t batch_id, size_t task_id,
                      struct transfer_status *status) {
    TransferEngine *native = (TransferEngine *)engine;
    Transport::TransferStatus native_status;
    Status s = native->getTransferStatus((Transport::BatchID)batch_id,
                                                    task_id, native_status);
    if (s.ok()) {
        status->status = (int)native_status.s;
        status->transferred_bytes = native_status.transferred_bytes;
    }
    return (int)s.code();
}

int freeBatchID(transfer_engine_t engine, batch_id_t batch_id) {
    TransferEngine *native = (TransferEngine *)engine;
    Status s = native->freeBatchID(batch_id);
    return (int)s.code();
}

int syncSegmentCache(transfer_engine_t engine) {
    TransferEngine *native = (TransferEngine *)engine;
    return native->syncSegmentCache();
}
