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

#ifndef TRANSFER_ENGINE_INL
#define TRANSFER_ENGINE_INL

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define mc_batch_id_t uint64_t
#define mc_segment_id_t uint64_t

#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif

#define OPCODE_READ (0)
#define OPCODE_WRITE (1)

struct mc_request {
    int opcode;
    void *source;
    mc_segment_id_t target_id;
    uint64_t target_offset;
    uint64_t length;
};

typedef struct mc_request mc_request_t;

#define STATUS_WAITING (0)
#define STATUS_PENDING (1)
#define STATUS_INVALID (2)
#define STATUS_CANCELED (3)
#define STATUS_COMPLETED (4)
#define STATUS_TIMEOUT (5)
#define STATUS_FAILED (6)

struct mc_status {
    int status;
    uint64_t transferred_bytes;
};

typedef struct mc_status mc_status_t;

typedef void *mc_engine_t;

struct mc_buffer_info {
    uint64_t base, length;
    char location[64];
};

#define TYPE_MEMORY (0)
#define TYPE_FILE (1)

struct mc_segment_info {
    int type;
    int num_buffers;
    struct mc_buffer_info *buffers;
};

typedef struct mc_segment_info mc_segment_info_t;

struct mc_notifi_record {
    mc_segment_id_t handle;
    char content[4096];
};

struct mc_notifi_info {
    int num_records;
    struct mc_notifi_record *records;
};

void mc_load_config_from_file(const char *path);

void mc_set_config(const char *key, const char *value);

mc_engine_t mc_create_engine();

void mc_destroy_engine(mc_engine_t engine);

int mc_segment_name(mc_engine_t engine, char *buf, size_t buf_len);

int mc_rpc_server_addr_port(mc_engine_t engine, char *addr_buf, size_t buf_len,
                            uint16_t *port);

int mc_open_segment(mc_engine_t engine, mc_segment_id_t *handle,
                    const char *segment_name);

int mc_close_segment(mc_engine_t engine, mc_segment_id_t handle);

int mc_get_segment_info(mc_engine_t engine, mc_segment_id_t handle,
                        mc_segment_info_t *info);

void mc_free_segment_info(mc_segment_info_t *info);

int mc_allocate_memory(mc_engine_t engine, void **addr, size_t size,
                       const char *location);

int mc_free_memory(mc_engine_t engine, void *addr);

int mc_register_memory(mc_engine_t engine, void *addr, size_t size);

int mc_unregister_memory(mc_engine_t engine, void *addr, size_t size);

mc_batch_id_t mc_allocate_batch(mc_engine_t engine, size_t batch_size);

int mc_free_batch(mc_engine_t engine, mc_batch_id_t batch_id);

int mc_submit(mc_engine_t engine, mc_batch_id_t batch_id, mc_request_t *entries,
              size_t count);

int mc_send_notifs(mc_engine_t engine, mc_segment_id_t handle,
                   const char *message);

int mc_recv_notifs(mc_engine_t engine, mc_notifi_info *info);

void mc_free_notifs(mc_notifi_info *info);

int mc_task_status(mc_engine_t engine, mc_batch_id_t batch_id, size_t task_id,
                   mc_status_t *status);

int mc_overall_status(mc_engine_t engine, mc_batch_id_t batch_id,
                      mc_status_t *status);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TRANSFER_ENGINE_INL
