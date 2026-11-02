#ifndef MOCK_ACL_ACL_H_
#define MOCK_ACL_ACL_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t aclError;

#define ACL_ERROR_NONE 0
#define ACL_SUCCESS 0

typedef void *aclrtContext;

typedef enum {
    ACL_MEM_MALLOC_HUGE_FIRST = 0,
    ACL_MEM_MALLOC_HUGE_ONLY = 1,
    ACL_MEM_MALLOC_NORMAL = 2,
} aclrtMemMallocPolicy;

typedef enum {
    ACL_MEM_LOCATION_TYPE_HOST = 0,
    ACL_MEM_LOCATION_TYPE_DEVICE = 1,
    ACL_MEM_LOCATION_TYPE_HOST_NUMA = 2,
} aclrtMemLocationType;

typedef struct {
    struct {
        uint32_t id;
        aclrtMemLocationType type;
    } location;
} aclrtPtrAttributes;

typedef void *aclrtDrvMemHandle;

typedef enum {
    ACL_MEM_HANDLE_TYPE_NONE = 0,
    ACL_MEM_HANDLE_TYPE_RESERVE = 1,
} aclrtMemHandleType;

typedef enum {
    ACL_MEM_ALLOCATION_TYPE_PINNED = 0,
    ACL_MEM_ALLOCATION_TYPE_MAX = 1,
} aclrtMemAllocationType;

typedef enum {
    ACL_MEM_P2P_HUGE = 0,
    ACL_MEM_P2P_NORMAL = 1,
    ACL_MEM_P2P_HUGE_2M = 2,
    ACL_MEM_P2P_HUGE_1G = 3,
    ACL_MEM_P2P_HUGE1G = 3,
} aclrtMemAttr;

typedef struct {
    aclrtMemHandleType handleType;
    aclrtMemAllocationType allocationType;
    aclrtMemAttr memAttr;
    struct {
        uint32_t id;
        aclrtMemLocationType type;
    } location;
} aclrtPhysicalMemProp;

typedef enum {
    ACL_MEMCPY_HOST_TO_HOST = 0,
    ACL_MEMCPY_HOST_TO_DEVICE = 1,
    ACL_MEMCPY_DEVICE_TO_HOST = 2,
    ACL_MEMCPY_DEVICE_TO_DEVICE = 3,
    ACL_MEMCPY_DEFAULT = 0,
} aclrtMemcpyKind;

aclError aclrtGetDevice(int32_t *deviceId);
aclError aclrtGetDeviceCount(uint32_t *count);
aclError aclrtSetDevice(int32_t deviceId);
aclError aclrtGetCurrentContext(aclrtContext *context);
aclError aclrtSetCurrentContext(aclrtContext context);
aclError aclrtPointerGetAttributes(const void *ptr,
                                   aclrtPtrAttributes *attributes);
aclError aclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy);
aclError aclrtFree(void *devPtr);
aclError aclrtMallocHost(void **hostPtr, size_t size);
aclError aclrtFreeHost(void *hostPtr);
aclError aclrtGetPhyDevIdByLogicDevId(int32_t logicDevId, int32_t *phyDevId);
aclError aclrtMallocPhysical(aclrtDrvMemHandle *handle, size_t size,
                             const aclrtPhysicalMemProp *prop, uint64_t flags);
aclError aclrtFreePhysical(aclrtDrvMemHandle handle);
aclError aclrtReserveMemAddress(void **va, size_t size, uint64_t alignSize,
                                void *base, size_t count);
aclError aclrtMapMem(void *va, size_t size, uint64_t offset,
                     aclrtDrvMemHandle handle, uint64_t flags);
aclError aclrtUnmapMem(void *va);
aclError aclrtReleaseMemAddress(void *va);
aclError aclrtMemcpy(void *dst, size_t dpMax, const void *src, size_t spMax,
                     aclrtMemcpyKind kind);
const char *aclGetRecentErrMsg(void);

#ifdef __cplusplus
}
#endif

#endif  // MOCK_ACL_ACL_H_
