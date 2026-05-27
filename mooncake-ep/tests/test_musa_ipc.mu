// test_musa_ipc.mu — Test MUSA IPC handle exchange + P2P write
// Build: mcc test_musa_ipc.mu -o test_musa_ipc -lmusart

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <musa_runtime.h>

#define CHECK_MUSA(call) do { \
    musaError_t err = (call); \
    if (err != musaSuccess) { \
        fprintf(stderr, "MUSA error at %s:%d: %s\n", __FILE__, __LINE__, \
                musaGetErrorString(err)); \
        exit(1); \
    } \
} while(0)

// Simple kernel that writes to a buffer
__global__ void write_kernel(int* buf, int value, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        buf[idx] = value + idx;
    }
}

int test_same_process_ipc(void) {
    printf("============================================================\n");
    printf("Test: Same-process IPC handle exchange + P2P write\n");
    printf("============================================================\n");

    int device_count = 0;
    CHECK_MUSA(musaGetDeviceCount(&device_count));
    printf("Number of MUSA devices: %d\n", device_count);
    if (device_count < 2) {
        printf("SKIP: Need at least 2 MUSA devices\n");
        return 0;
    }

    // Step 1: Allocate on device 1
    CHECK_MUSA(musaSetDevice(1));
    int* buf1 = NULL;
    CHECK_MUSA(musaMalloc(&buf1, 1024 * sizeof(int)));
    CHECK_MUSA(musaMemset(buf1, 0, 1024 * sizeof(int)));
    printf("[Dev 1] Allocated buffer at %p\n", buf1);

    // Step 2: Get IPC handle from device 1
    musaIpcMemHandle_t ipc_handle;
    CHECK_MUSA(musaIpcGetMemHandle(&ipc_handle, buf1));
    printf("[Dev 1] Got IPC handle\n");

    // Step 3: Switch to device 0 and enable peer access
    CHECK_MUSA(musaSetDevice(0));

    int can_access = 0;
    CHECK_MUSA(musaDeviceCanAccessPeer(&can_access, 0, 1));
    printf("[Dev 0] can_access_peer(0,1) = %d\n", can_access);

    if (can_access) {
        musaError_t err = musaDeviceEnablePeerAccess(1, 0);
        if (err == musaErrorPeerAccessAlreadyEnabled) {
            musaGetLastError();
            printf("[Dev 0] Peer access already enabled\n");
        } else {
            CHECK_MUSA(err);
            printf("[Dev 0] Enabled peer access\n");
        }
    }

    // Step 4: Open IPC handle on device 0
    void* peer_ptr = NULL;
    CHECK_MUSA(musaIpcOpenMemHandle(&peer_ptr, ipc_handle,
                                     musaIpcMemLazyEnablePeerAccess));
    printf("[Dev 0] Opened IPC handle: peer_ptr = %p\n", peer_ptr);

    // Step 5: Write to peer memory from device 0 via musaMemcpy
    int host_data[1024];
    for (int i = 0; i < 1024; i++) host_data[i] = 12345 + i;
    CHECK_MUSA(musaMemcpy(peer_ptr, host_data, 1024 * sizeof(int),
                          musaMemcpyHostToDevice));
    printf("[Dev 0] musaMemcpy to peer ptr succeeded\n");

    CHECK_MUSA(musaDeviceSynchronize());

    // Step 6: Verify on device 1
    CHECK_MUSA(musaSetDevice(1));
    CHECK_MUSA(musaDeviceSynchronize());

    int verify[1024];
    CHECK_MUSA(musaMemcpy(verify, buf1, 1024 * sizeof(int),
                          musaMemcpyDeviceToHost));

    int success = (verify[0] == 12345 && verify[1] == 12346 && verify[999] == 13344);
    printf("[Dev 1] verify[0]=%d (expected 12345), verify[1]=%d (expected 12346), "
           "verify[999]=%d (expected 13344)\n", verify[0], verify[1], verify[999]);

    // Cleanup
    CHECK_MUSA(musaSetDevice(0));
    CHECK_MUSA(musaIpcCloseMemHandle(peer_ptr));
    CHECK_MUSA(musaSetDevice(1));
    CHECK_MUSA(musaFree(buf1));

    printf("Same-process IPC test: %s\n", success ? "PASS" : "FAIL");
    return success;
}

int test_kernel_p2p_write(void) {
    printf("============================================================\n");
    printf("Test: Kernel P2P write to IPC-mapped peer memory\n");
    printf("============================================================\n");

    int device_count = 0;
    CHECK_MUSA(musaGetDeviceCount(&device_count));
    if (device_count < 2) {
        printf("SKIP: Need at least 2 MUSA devices\n");
        return 0;
    }

    // Allocate on device 1
    CHECK_MUSA(musaSetDevice(1));
    int* buf1 = NULL;
    CHECK_MUSA(musaMalloc(&buf1, 1024 * sizeof(int)));
    CHECK_MUSA(musaMemset(buf1, 0, 1024 * sizeof(int)));
    printf("[Dev 1] Allocated buffer at %p\n", buf1);

    // Get IPC handle
    musaIpcMemHandle_t ipc_handle;
    CHECK_MUSA(musaIpcGetMemHandle(&ipc_handle, buf1));
    printf("[Dev 1] Got IPC handle\n");

    // Switch to device 0
    CHECK_MUSA(musaSetDevice(0));

    // Enable peer access
    int can_access = 0;
    CHECK_MUSA(musaDeviceCanAccessPeer(&can_access, 0, 1));
    if (can_access) {
        musaError_t err = musaDeviceEnablePeerAccess(1, 0);
        if (err == musaErrorPeerAccessAlreadyEnabled) musaGetLastError();
    }

    // Open IPC handle
    void* peer_ptr = NULL;
    CHECK_MUSA(musaIpcOpenMemHandle(&peer_ptr, ipc_handle,
                                     musaIpcMemLazyEnablePeerAccess));
    printf("[Dev 0] Opened IPC handle: peer_ptr = %p\n", peer_ptr);

    // Write to peer memory using a kernel on device 0
    write_kernel<<<1, 1024>>>((int*)peer_ptr, 55555, 1024);
    CHECK_MUSA(musaDeviceSynchronize());
    printf("[Dev 0] Kernel write to peer ptr succeeded\n");

    // Verify on device 1
    CHECK_MUSA(musaSetDevice(1));
    CHECK_MUSA(musaDeviceSynchronize());

    int verify[1024];
    CHECK_MUSA(musaMemcpy(verify, buf1, 1024 * sizeof(int),
                          musaMemcpyDeviceToHost));

    int success = (verify[0] == 55555 && verify[1] == 55556 && verify[999] == 56554);
    printf("[Dev 1] verify[0]=%d (expected 55555), verify[1]=%d (expected 55556), "
           "verify[999]=%d (expected 56554)\n", verify[0], verify[1], verify[999]);

    // Cleanup
    CHECK_MUSA(musaSetDevice(0));
    CHECK_MUSA(musaIpcCloseMemHandle(peer_ptr));
    CHECK_MUSA(musaSetDevice(1));
    CHECK_MUSA(musaFree(buf1));

    printf("Kernel P2P write test: %s\n", success ? "PASS" : "FAIL");
    return success;
}

int main(int argc, char** argv) {
    int results[2] = {0};

    results[0] = test_same_process_ipc();
    results[1] = test_kernel_p2p_write();

    printf("\n============================================================\n");
    printf("SUMMARY\n");
    printf("============================================================\n");
    printf("  same_process_ipc: %s\n", results[0] ? "PASS" : "FAIL");
    printf("  kernel_p2p_write: %s\n", results[1] ? "PASS" : "FAIL");

    return (results[0] && results[1]) ? 0 : 1;
}
