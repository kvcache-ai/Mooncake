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

#ifndef TENT_API_H
#define TENT_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define tent_batch_id_t uint64_t
#define tent_segment_id_t uint64_t

#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif

#define OPCODE_READ (0)
#define OPCODE_WRITE (1)

struct tent_request {
    int opcode;
    void* source;
    tent_segment_id_t target_id;
    uint64_t target_offset;
    uint64_t length;
};

typedef struct tent_request tent_request_t;

#define STATUS_WAITING (0)
#define STATUS_PENDING (1)
#define STATUS_INVALID (2)
#define STATUS_CANCELED (3)
#define STATUS_COMPLETED (4)
#define STATUS_TIMEOUT (5)
#define STATUS_FAILED (6)

struct tent_status {
    int status;
    uint64_t transferred_bytes;
};

typedef struct tent_status tent_status_t;

typedef void* tent_engine_t;

struct tent_buffer_info {
    uint64_t base, length;
    char location[64];
};

#define TYPE_MEMORY (0)
#define TYPE_FILE (1)

struct tent_segment_info {
    int type;
    int num_buffers;
    struct tent_buffer_info* buffers;
};

typedef struct tent_segment_info tent_segment_info_t;

struct tent_notifi_record {
    tent_segment_id_t handle;
    char name[256];
    char msg[4096];
};

typedef struct tent_notifi_record tent_notifi_record_t;

struct tent_notifi_info {
    int num_records;
    struct tent_notifi_record* records;
};

typedef struct tent_notifi_info tent_notifi_info;

#define PERM_LOCAL_READ_WRITE (0)
#define PERM_GLOBAL_READ_ONLY (1)
#define PERM_GLOBAL_READ_WRITE (2)

#define TRANSPORT_RDMA (0)
#define TRANSPORT_MNNVL (1)
#define TRANSPORT_SHM (2)
#define TRANSPORT_NVLINK (3)
#define TRANSPORT_GDS (4)
#define TRANSPORT_IOURING (5)
#define TRANSPORT_TCP (6)
#define TRANSPORT_ASCEND_DIRECT (7)
#define TRANSPORT_UNSPEC (8)

struct tent_memory_options {
    char location[64];
    int permission;      /* PERM_LOCAL_READ_WRITE, etc. */
    int transport_type;  /* TRANSPORT_RDMA, etc. */
    char shm_path[256];
    size_t shm_offset;
    int internal;        /* 0 = false, nonzero = true */
};

typedef struct tent_memory_options tent_memory_options_t;

void tent_load_config_from_file(const char* path);

void tent_set_config(const char* key, const char* value);

tent_engine_t tent_create_engine();

void tent_destroy_engine(tent_engine_t engine);

int tent_segment_name(tent_engine_t engine, char* buf, size_t buf_len);

int tent_rpc_server_addr_port(tent_engine_t engine, char* addr_buf,
                              size_t buf_len, uint16_t* port);

int tent_open_segment(tent_engine_t engine, tent_segment_id_t* handle,
                      const char* segment_name);

int tent_close_segment(tent_engine_t engine, tent_segment_id_t handle);

int tent_get_segment_info(tent_engine_t engine, tent_segment_id_t handle,
                          tent_segment_info_t* info);

void tent_free_segment_info(tent_segment_info_t* info);

int tent_allocate_memory(tent_engine_t engine, void** addr, size_t size,
                         const char* location);

int tent_free_memory(tent_engine_t engine, void* addr);

int tent_register_memory(tent_engine_t engine, void* addr, size_t size);

int tent_unregister_memory(tent_engine_t engine, void* addr, size_t size);

tent_batch_id_t tent_allocate_batch(tent_engine_t engine, size_t batch_size);

int tent_free_batch(tent_engine_t engine, tent_batch_id_t batch_id);

int tent_submit(tent_engine_t engine, tent_batch_id_t batch_id,
                tent_request_t* entries, size_t count);

int tent_submit_notif(tent_engine_t engine, tent_batch_id_t batch_id,
                      tent_request_t* entries, size_t count, const char* name,
                      const char* message);

int tent_send_notifs(tent_engine_t engine, tent_segment_id_t handle,
                     const char* name, const char* message);

int tent_recv_notifs(tent_engine_t engine, tent_notifi_info* info);

void tent_free_notifs(tent_notifi_info* info);

int tent_task_status(tent_engine_t engine, tent_batch_id_t batch_id,
                     size_t task_id, tent_status_t* status);

int tent_overall_status(tent_engine_t engine, tent_batch_id_t batch_id,
                        tent_status_t* status);

int tent_available(tent_engine_t engine);

int tent_register_memory_with_perm(tent_engine_t engine, void* addr,
                                   size_t size, int permission);

int tent_register_memory_batch(tent_engine_t engine, void** addrs,
                               size_t* sizes, size_t count, int permission);

int tent_unregister_memory_batch(tent_engine_t engine, void** addrs,
                                 size_t* sizes, size_t count);

int tent_allocate_memory_ex(tent_engine_t engine, void** addr, size_t size,
                            tent_memory_options_t* opts);

int tent_register_memory_ex(tent_engine_t engine, void* addr, size_t size,
                            tent_memory_options_t* opts);

int tent_register_memory_batch_ex(tent_engine_t engine, void** addrs,
                                  size_t* sizes, size_t count,
                                  tent_memory_options_t* opts);

int tent_task_status_list(tent_engine_t engine, tent_batch_id_t batch_id,
                          tent_status_t* statuses, size_t* count);

#ifdef __cplusplus
}
#endif  // __cplusplus

#ifdef __cplusplus
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {
class TransferEngineImpl;
class Config;
class TransferEngine {
   public:
    TransferEngine();

    TransferEngine(const std::string config_path);

    TransferEngine(std::shared_ptr<Config> config);

    ~TransferEngine();

    TransferEngine(const TransferEngine&) = delete;

    TransferEngine& operator=(const TransferEngine&) = delete;

   public:
    bool available() const;

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

   public:
    Status exportLocalSegment(std::string& shared_handle);

    Status importRemoteSegment(SegmentID& handle,
                               const std::string& shared_handle);

    Status openSegment(SegmentID& handle, const std::string& segment_name);

    Status closeSegment(SegmentID handle);

    Status getSegmentInfo(SegmentID handle, SegmentInfo& info);

   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               Location location = kWildcardLocation);

    Status freeLocalMemory(void* addr);

    Status registerLocalMemory(void* addr, size_t size,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(void* addr, size_t size = 0);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(std::vector<void*> addr_list,
                                 std::vector<size_t> size_list = {});

    // advanced buffer allocate function
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options);

    // advanced buffer register function
    Status registerLocalMemory(void* addr, size_t size, MemoryOptions& options);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               MemoryOptions& options);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list,
                          const Notification& notifi);

    Status sendNotification(SegmentID target_id, const Notification& notifi);

    Status receiveNotification(std::vector<Notification>& notifi_list);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus>& status_list);

    Status getTransferStatus(BatchID batch_id, TransferStatus& overall_status);

   private:
    std::unique_ptr<TransferEngineImpl> impl_;
};
}  // namespace tent
}  // namespace mooncake
#endif

#endif