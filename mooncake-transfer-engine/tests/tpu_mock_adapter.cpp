// Mock TPU adapter shared library for tpu.h shim unit tests.
// Simulates a single-device TPU using CPU memory so tests can run without
// any real TPU hardware or vendor SDK.  The shared library is loaded at
// runtime by tpu_adapter_mock_test via MC_TPU_ADAPTER_LIB.

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ---- device-allocation registry ------------------------------------------

#define MAX_DEVICE_ALLOCS 64

static struct {
    void *ptr;
    size_t size;
} g_dev_allocs[MAX_DEVICE_ALLOCS];

static int g_dev_alloc_count = 0;
static int g_current_device = 0;

static void register_alloc(void *ptr, size_t size) {
    if (g_dev_alloc_count < MAX_DEVICE_ALLOCS) {
        g_dev_allocs[g_dev_alloc_count].ptr = ptr;
        g_dev_allocs[g_dev_alloc_count].size = size;
        ++g_dev_alloc_count;
    }
}

static void unregister_alloc(void *ptr) {
    for (int i = 0; i < g_dev_alloc_count; ++i) {
        if (g_dev_allocs[i].ptr == ptr) {
            g_dev_allocs[i] = g_dev_allocs[g_dev_alloc_count - 1];
            --g_dev_alloc_count;
            return;
        }
    }
}

static int lookup_alloc(const void *ptr, size_t *out_size) {
    for (int i = 0; i < g_dev_alloc_count; ++i) {
        if (g_dev_allocs[i].ptr == ptr) {
            if (out_size) *out_size = g_dev_allocs[i].size;
            return 1;
        }
    }
    return 0;
}

// ---- mc_tpu_pointer_attributes_t -----------------------------------------
// Redeclare here so the mock doesn't depend on tpu.h.
typedef struct {
    int type;
    int device;
    size_t size;
} mc_tpu_pointer_attributes_t;

// ---- exported adapter symbols --------------------------------------------

extern "C" {

int mc_tpu_adapter_init(uint32_t abi_version) {
    // Accept ABI version 1; fail everything else.
    return (abi_version == 1) ? 0 : 1;
}

const char *mc_tpu_adapter_get_error_string(int error) {
    return (error == 0) ? "mock success" : "mock error";
}

int mc_tpu_adapter_get_device_count(int *count) {
    *count = 1;
    return 0;
}

int mc_tpu_adapter_get_device(int *device) {
    *device = g_current_device;
    return 0;
}

int mc_tpu_adapter_set_device(int device) {
    g_current_device = device;
    return 0;
}

int mc_tpu_adapter_device_get_pci_bus_id(char *pci_bus_id, int len,
                                         int device) {
    snprintf(pci_bus_id, (size_t)len, "0000:00:0%d.0", device);
    return 0;
}

int mc_tpu_adapter_pointer_get_attributes(mc_tpu_pointer_attributes_t *attr,
                                          const void *ptr) {
    size_t sz = 0;
    if (lookup_alloc(ptr, &sz)) {
        attr->type = 2; /* cudaMemoryTypeDevice */
        attr->device = g_current_device;
        attr->size = sz;
    } else {
        attr->type = 1; /* cudaMemoryTypeHost */
        attr->device = -1;
        attr->size = 0;
    }
    return 0;
}

int mc_tpu_adapter_malloc(void **ptr, size_t bytes) {
    *ptr = malloc(bytes);
    if (!*ptr) return 1;
    register_alloc(*ptr, bytes);
    return 0;
}

int mc_tpu_adapter_free(void *ptr) {
    unregister_alloc(ptr);
    free(ptr);
    return 0;
}

// malloc_host / free_host intentionally omitted to test the std::malloc
// fallback path in the shim.

int mc_tpu_adapter_memcpy(void *dst, const void *src, size_t bytes, int kind) {
    (void)kind;
    memcpy(dst, src, bytes);
    return 0;
}

// memcpy_async intentionally omitted to test the synchronous fallback.

int mc_tpu_adapter_memset(void *ptr, int value, size_t bytes) {
    memset(ptr, value, bytes);
    return 0;
}

// stream_create / stream_destroy / stream_synchronize intentionally omitted
// to test the "return success with nullptr" fallback path.

int mc_tpu_adapter_device_can_access_peer(int *can_access, int device,
                                          int peer_device) {
    *can_access = (device == peer_device) ? 1 : 0;
    return 0;
}

int mc_tpu_adapter_device_get_attribute(int *value, int attribute,
                                        int device) {
    (void)device;
    // Attribute 1 == CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED in Mooncake's
    // internal mapping; report it as supported.
    *value = (attribute == 1) ? 1 : 0;
    return 0;
}

int mc_tpu_adapter_mem_get_handle_for_address_range(void *handle,
                                                    uintptr_t ptr, size_t size,
                                                    int handle_type,
                                                    unsigned long long flags) {
    (void)ptr;
    (void)size;
    (void)handle_type;
    (void)flags;
    if (!handle) return 1;
    *static_cast<int *>(handle) = 42; /* fake DMA-BUF fd */
    return 0;
}

} // extern "C"
