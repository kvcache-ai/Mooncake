// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TEST_MOCK_ACL_H
#define TENT_TEST_MOCK_ACL_H

#ifndef TENT_ACL_TEST_ONLY
#error "These ACL declarations are for isolated test targets only"
#endif

#include <stddef.h>
#include <stdint.h>

// Test-only declarations for the APIs used by MemFabric and AscendPlatform.
// These headers must never be used with the real ACL runtime.
#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t aclError;
typedef void* aclrtContext;

#define ACL_ERROR_NONE 0
#define ACL_SUCCESS 0

typedef enum {
    ACL_MEM_MALLOC_HUGE_FIRST = 0,
    ACL_MEM_MALLOC_HUGE_ONLY = 1,
    ACL_MEM_MALLOC_NORMAL_ONLY = 2,
} aclrtMemMallocPolicy;

typedef enum {
    ACL_MEM_LOCATION_TYPE_HOST = 0,
    ACL_MEM_LOCATION_TYPE_DEVICE = 1,
} aclrtMemLocationType;

typedef struct {
    int32_t id;
    aclrtMemLocationType type;
} aclrtMemLocation;

typedef struct {
    aclrtMemLocation location;
    uint32_t pageSize;
    uint32_t rsv[4];
} aclrtPtrAttributes;

typedef enum { ACL_MEMCPY_DEFAULT = 4 } aclrtMemcpyKind;

aclError aclrtGetDeviceCount(uint32_t* count);
aclError aclrtGetDevice(int32_t* deviceId);
aclError aclrtSetDevice(int32_t deviceId);
aclError aclrtGetCurrentContext(aclrtContext* context);
aclError aclrtSetCurrentContext(aclrtContext context);
aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes);
aclError aclrtMalloc(void** devPtr, size_t size, aclrtMemMallocPolicy policy);
aclError aclrtFree(void* devPtr);
aclError aclrtMallocHost(void** hostPtr, size_t size);
aclError aclrtFreeHost(void* hostPtr);
aclError aclrtMemcpy(void* dst, size_t destMax, const void* src, size_t count,
                     aclrtMemcpyKind kind);

#ifdef __cplusplus
}
#endif

#endif  // TENT_TEST_MOCK_ACL_H
