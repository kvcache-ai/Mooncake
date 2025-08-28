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

#ifndef TRANSFER_ENGINE_C
#define TRANSFER_ENGINE_C

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define segment_handle_t int32_t
#define segment_id_t int32_t
#define batch_id_t uint64_t
#define LOCAL_SEGMENT (0)
#define INVALID_BATCH UINT64_MAX

#define OPCODE_READ (0)
#define OPCODE_WRITE (1)

struct transfer_request {
    int opcode;
    void *source;
    segment_id_t target_id;
    uint64_t target_offset;
    uint64_t length;
};

typedef struct transfer_request transfer_request_t;

struct notify_msg {
    char *name;
    char *msg;
};

typedef struct notify_msg notify_msg_t;

#define STATUS_WAITING (0)
#define STATUS_PENDING (1)
#define STATUS_INVALID (2)
#define STATUS_CANCELED (3)
#define STATUS_COMPLETED (4)
#define STATUS_TIMEOUT (5)
#define STATUS_FAILED (6)

struct transfer_status {
    int status;
    uint64_t transferred_bytes;
};

struct segment_desc {
    int type;  // RDMA / NVMeoF
    union {
        struct {
            void *addr;
            uint64_t size;
            const char *location;
        } rdma;
        struct {
            const char *file_path;
            const char *subsystem_name;
            const char *proto;
            const char *ip;
            uint64_t port;
            // maybe more needed for mount
        } nvmeof;
    } desc_;
};

typedef struct transfer_status transfer_status_t;

struct buffer_entry {
    void *addr;
    size_t length;
};
typedef struct buffer_entry buffer_entry_t;

typedef struct segment_desc segment_desc_t;
typedef void *transfer_engine_t;
typedef void *transport_t;

/*
 * All memory pointed to by the "char *" parameters will not be used
 * after the C function returns.
 * This means that the caller can free the memory pointed to by "char *"
 * parameters, after the call is completed.
 * All the C functions here follow this convention.
 */

transfer_engine_t createTransferEngine(const char *metadata_conn_string,
                                       const char *local_server_name,
                                       const char *ip_or_host_name,
                                       uint64_t rpc_port, int auto_discover);

int getLocalIpAndPort(transfer_engine_t engine, char *buf_out, size_t buf_len);

transport_t installTransport(transfer_engine_t engine, const char *proto,
                             void **args);

int uninstallTransport(transfer_engine_t engine, const char *proto);

segment_id_t openSegment(transfer_engine_t engine, const char *segment_name);

segment_id_t openSegmentNoCache(transfer_engine_t engine,
                                const char *segment_name);

int closeSegment(transfer_engine_t engine, segment_id_t segment_id);

int removeLocalSegment(transfer_engine_t engine, const char *segment_name);

void destroyTransferEngine(transfer_engine_t engine);

int registerLocalMemory(transfer_engine_t engine, void *addr, size_t length,
                        const char *location, int remote_accessible);

int unregisterLocalMemory(transfer_engine_t engine, void *addr);

int registerLocalMemoryBatch(transfer_engine_t engine,
                             buffer_entry_t *buffer_list, size_t buffer_len,
                             const char *location);

int unregisterLocalMemoryBatch(transfer_engine_t engine, void **addr_list,
                               size_t addr_len);

batch_id_t allocateBatchID(transfer_engine_t engine, size_t batch_size);

int submitTransfer(transfer_engine_t engine, batch_id_t batch_id,
                   struct transfer_request *entries, size_t count);

int submitTransferWithNotify(transfer_engine_t engine, batch_id_t batch_id,
                             struct transfer_request *entries, size_t count,
                             notify_msg_t notify_msg);

notify_msg_t *getNotifsFromEngine(transfer_engine_t engine, int *size);

int freeNotifsMsgBuf(notify_msg_t *msg, int size);

int genNotifyInEngine(transfer_engine_t engine, uint64_t target_id,
                      notify_msg_t notify_msg);

int getTransferStatus(transfer_engine_t engine, batch_id_t batch_id,
                      size_t task_id, struct transfer_status *status);

int freeBatchID(transfer_engine_t engine, batch_id_t batch_id);

int syncSegmentCache(transfer_engine_t engine);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TRANSFER_ENGINE_C
