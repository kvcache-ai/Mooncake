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

#ifndef MOONCAKE_CLIENT_C
#define MOONCAKE_CLIENT_C

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// types.h 中的 ErrorCode Code
typedef int32_t ErrorCode_t;

#define MOONCAKE_ERROR_OK ((ErrorCode_t)0)
#define MOONCAKE_ERROR_INTERNAL_ERROR ((ErrorCode_t)-1)
#define MOONCAKE_ERROR_BUFFER_OVERFLOW ((ErrorCode_t)-10)
#define MOONCAKE_ERROR_SHARD_INDEX_OUT_OF_RANGE ((ErrorCode_t)-100)
#define MOONCAKE_ERROR_SEGMENT_NOT_FOUND ((ErrorCode_t)-101)
#define MOONCAKE_ERROR_SEGMENT_ALREADY_EXISTS ((ErrorCode_t)-102)
#define MOONCAKE_ERROR_NO_AVAILABLE_HANDLE ((ErrorCode_t)-200)
#define MOONCAKE_ERROR_INVALID_VERSION ((ErrorCode_t)-300)
#define MOONCAKE_ERROR_INVALID_KEY ((ErrorCode_t)-400)
#define MOONCAKE_ERROR_WRITE_FAIL ((ErrorCode_t)-500)
#define MOONCAKE_ERROR_INVALID_PARAMS ((ErrorCode_t)-600)
#define MOONCAKE_ERROR_INVALID_WRITE ((ErrorCode_t)-700)
#define MOONCAKE_ERROR_INVALID_READ ((ErrorCode_t)-701)
#define MOONCAKE_ERROR_INVALID_REPLICA ((ErrorCode_t)-702)
#define MOONCAKE_ERROR_REPLICA_IS_NOT_READY ((ErrorCode_t)-703)
#define MOONCAKE_ERROR_OBJECT_NOT_FOUND ((ErrorCode_t)-704)
#define MOONCAKE_ERROR_OBJECT_ALREADY_EXISTS ((ErrorCode_t)-705)
#define MOONCAKE_ERROR_OBJECT_HAS_LEASE ((ErrorCode_t)-706)
#define MOONCAKE_ERROR_TRANSFER_FAIL ((ErrorCode_t)-800)
#define MOONCAKE_ERROR_RPC_FAIL ((ErrorCode_t)-900)
#define MOONCAKE_ERROR_ETCD_OPERATION_ERROR ((ErrorCode_t)-1000)
#define MOONCAKE_ERROR_ETCD_KEY_NOT_EXIST ((ErrorCode_t)-1001)
#define MOONCAKE_ERROR_ETCD_TRANSACTION_FAIL ((ErrorCode_t)-1002)
#define MOONCAKE_ERROR_ETCD_CTX_CANCELLED ((ErrorCode_t)-1003)
#define MOONCAKE_ERROR_UNAVAILABLE_IN_CURRENT_STATUS ((ErrorCode_t)-1010)
#define MOONCAKE_ERROR_UNAVAILABLE_IN_CURRENT_MODE ((ErrorCode_t)-1011)
#define MOONCAKE_ERROR_FILE_NOT_FOUND ((ErrorCode_t)-1100)
#define MOONCAKE_ERROR_FILE_OPEN_FAIL ((ErrorCode_t)-1101)
#define MOONCAKE_ERROR_FILE_READ_FAIL ((ErrorCode_t)-1102)
#define MOONCAKE_ERROR_FILE_WRITE_FAIL ((ErrorCode_t)-1103)
#define MOONCAKE_ERROR_FILE_INVALID_BUFFER ((ErrorCode_t)-1104)
#define MOONCAKE_ERROR_FILE_LOCK_FAIL ((ErrorCode_t)-1105)
#define MOONCAKE_ERROR_FILE_INVALID_HANDLE ((ErrorCode_t)-1106)

typedef struct {
    void* ptr = NULL;
    size_t size = 0;
} Slice_t;

typedef struct {
    size_t replica_num;
    const char* preferred_segment;
} ReplicateConfig_t;

typedef void *client_t;

client_t mooncake_client_create(
    const char* local_hostname,
    const char* metadata_connstring,
    const char* protocol,
    const char* rdma_devices,
    const char* master_server_entry);

ErrorCode_t mooncake_client_register_local_memory(
    client_t client,
    void* addr,
    size_t length,
    const char* location,
    bool remote_accessible,
    bool update_metadata);

ErrorCode_t mooncake_client_unregister_local_memory(
    client_t client,
    void* addr,
    bool update_metadata);

ErrorCode_t mooncake_client_mount_segment(
    client_t client,
    size_t size);

ErrorCode_t mooncake_client_get(
    client_t client, 
    const char* key, 
    Slice_t* slices, 
    size_t slices_count);

ErrorCode_t mooncake_client_put(
    client_t client, 
    const char* key, 
    Slice_t* slices, 
    size_t slices_count,
    const ReplicateConfig_t config);

ErrorCode_t mooncake_client_isexist(
    client_t client,
    const char* key);

ErrorCode_t mooncake_client_remove(
    client_t client,
    const char* key);

void mooncake_client_destroy(client_t client);

uint64_t mooncake_max_slice_size();

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // MOONCAKE_CLIENT_C
