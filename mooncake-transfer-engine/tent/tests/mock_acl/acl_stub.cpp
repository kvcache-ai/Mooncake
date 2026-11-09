// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

// Host-only ACL implementation embedded in transfer_engine for mock CI.
// The MemFabric transport unit test supplies its own device-aware mocks.
#include <acl/acl.h>

#include <cstdlib>
#include <cstring>

namespace {
constexpr aclError kUnsupported = 1;
constexpr aclError kInvalidArgument = 2;
}  // namespace

extern "C" {

aclError aclrtGetDevice(int32_t* device_id) {
    if (device_id) *device_id = -1;
    return kUnsupported;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    if (!ptr || !attributes) return kInvalidArgument;
    *attributes = {};
    attributes->location.type = ACL_MEM_LOCATION_TYPE_HOST;
    return ACL_SUCCESS;
}

aclError aclrtMalloc(void** ptr, size_t, aclrtMemMallocPolicy) {
    if (ptr) *ptr = nullptr;
    return kUnsupported;
}

aclError aclrtFree(void*) { return kUnsupported; }

aclError aclrtMallocHost(void** ptr, size_t size) {
    if (!ptr || !size) return kInvalidArgument;
    *ptr = std::malloc(size);
    return *ptr ? ACL_SUCCESS : kInvalidArgument;
}

aclError aclrtFreeHost(void* ptr) {
    std::free(ptr);
    return ACL_SUCCESS;
}

aclError aclrtMemcpy(void* dst, size_t capacity, const void* src, size_t count,
                     aclrtMemcpyKind kind) {
    if (kind != ACL_MEMCPY_HOST_TO_HOST && kind != ACL_MEMCPY_DEFAULT)
        return kUnsupported;
    if (count > capacity || (count && (!dst || !src))) return kInvalidArgument;
    if (count) std::memmove(dst, src, count);
    return ACL_SUCCESS;
}

}  // extern "C"
