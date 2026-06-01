// Minimal test: Can MUSA device kernels write to peer-mapped memory via IPC?
//
// Compile on MT server:
//   mcc -o test_musa_p2p test_musa_p2p.mu -lmusa_runtime
//
// Run:
//   ./test_musa_p2p
//
// Expected output on CUDA: all tests pass
// Expected output on MUSA: Test 1 (host memcpy) passes, Test 2 (kernel write) may fail

#include <musa_runtime.h>
#include <stdio.h>
#include <string.h>

// Simple kernel: write a single int to peer memory
__global__ void kernel_write_int(int* peer_ptr, int value) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        *peer_ptr = value;
    }
}

// Kernel: write int4 via volatile field-by-field (same as mc_st_na on MUSA)
__global__ void kernel_write_int4(int* peer_ptr, int x, int y, int z, int w) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        volatile int* vp = peer_ptr;
        vp[0] = x;
        vp[1] = y;
        vp[2] = z;
        vp[3] = w;
    }
}

// Kernel: write int via volatile + threadfence_system (same as mc_st_release on MUSA)
__global__ void kernel_write_release(int* peer_ptr, int value) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        *reinterpret_cast<volatile int*>(peer_ptr) = value;
        __threadfence_system();
    }
}

#define CHECK_MUSA(call)                                               \
    do {                                                               \
        musaError_t err = (call);                                      \
        if (err != musaSuccess) {                                      \
            printf("  FAIL: %s => %s (%s:%d)\n", #call,               \
                   musaGetErrorString(err), __FILE__, __LINE__);       \
            return false;                                              \
        }                                                              \
    } while (0)

bool test_musa_p2p() {
    int device_count = 0;
    musaGetDeviceCount(&device_count);
    printf("MUSA device count: %d\n", device_count);
    if (device_count < 2) {
        printf("Need at least 2 devices, skipping\n");
        return true;
    }

    // Check and enable peer access
    int can_access_01 = 0, can_access_10 = 0;
    musaDeviceCanAccessPeer(&can_access_01, 0, 1);
    musaDeviceCanAccessPeer(&can_access_10, 1, 0);
    printf("Peer access: dev0->dev1=%d, dev1->dev0=%d\n", can_access_01, can_access_10);

    if (!can_access_01 || !can_access_10) {
        printf("P2P not available, skipping\n");
        return true;
    }

    musaSetDevice(0);
    musaDeviceEnablePeerAccess(1, 0);
    musaSetDevice(1);
    musaDeviceEnablePeerAccess(0, 0);
    // Ignore "already enabled" errors
    musaGetLastError();

    // Allocate buffer on device 1
    musaSetDevice(1);
    int* buf1 = nullptr;
    CHECK_MUSA(musaMalloc(&buf1, 256));
    CHECK_MUSA(musaMemset(buf1, 0, 256));

    // Get IPC handle from device 1
    musaIpcMemHandle_t handle1;
    CHECK_MUSA(musaIpcGetMemHandle(&handle1, buf1));

    // Open IPC handle on device 0
    musaSetDevice(0);
    int* peer_ptr = nullptr;
    musaError_t err = musaIpcOpenMemHandle((void**)&peer_ptr, handle1,
                                            musaIpcMemLazyEnablePeerAccess);
    if (err != musaSuccess) {
        printf("musaIpcOpenMemHandle failed: %s\n", musaGetErrorString(err));
        return false;
    }
    printf("IPC handle opened on dev0: peer_ptr=%p (dev1 buf=%p)\n", peer_ptr, buf1);

    // Also allocate buffer on device 0 and open on device 1 (bidirectional test)
    musaSetDevice(0);
    int* buf0 = nullptr;
    CHECK_MUSA(musaMalloc(&buf0, 256));
    CHECK_MUSA(musaMemset(buf0, 0, 256));

    musaIpcMemHandle_t handle0;
    CHECK_MUSA(musaIpcGetMemHandle(&handle0, buf0));

    musaSetDevice(1);
    int* peer_ptr1 = nullptr;
    err = musaIpcOpenMemHandle((void**)&peer_ptr1, handle0,
                                musaIpcMemLazyEnablePeerAccess);
    if (err != musaSuccess) {
        printf("musaIpcOpenMemHandle (dev1->dev0) failed: %s\n", musaGetErrorString(err));
    } else {
        printf("IPC handle opened on dev1: peer_ptr=%p (dev0 buf=%p)\n", peer_ptr1, buf0);
    }

    bool all_ok = true;

    // ===== Test 1: Host-side memcpy (should work) =====
    printf("\n--- Test 1: Host-side musaMemcpy to peer memory ---\n");
    {
        int test_val = 0xDEADBEEF;
        musaSetDevice(0);
        CHECK_MUSA(musaMemcpy(peer_ptr, &test_val, sizeof(int), musaMemcpyHostToDevice));

        // Read back from device 1
        musaSetDevice(1);
        int readback = 0;
        CHECK_MUSA(musaMemcpy(&readback, buf1, sizeof(int), musaMemcpyDeviceToHost));
        printf("  Host memcpy: wrote 0x%X, read back 0x%X %s\n",
               test_val, readback, readback == test_val ? "OK" : "MISMATCH");
        if (readback != test_val) all_ok = false;
    }

    // ===== Test 2: Device kernel write (simple int store) =====
    printf("\n--- Test 2: Device kernel write (simple int store) ---\n");
    {
        // Clear the buffer first
        musaSetDevice(1);
        CHECK_MUSA(musaMemset(buf1, 0, 256));

        // Launch kernel on device 0 to write to peer memory
        musaSetDevice(0);
        int kernel_val = 0xCAFECAFE;
        kernel_write_int<<<1, 1>>>(peer_ptr, kernel_val);
        err = musaGetLastError();
        if (err != musaSuccess) {
            printf("  Kernel launch failed: %s\n", musaGetErrorString(err));
            all_ok = false;
        } else {
            err = musaDeviceSynchronize();
            if (err != musaSuccess) {
                printf("  Kernel execution failed: %s\n", musaGetErrorString(err));
                all_ok = false;
            } else {
                // Read back from device 1
                musaSetDevice(1);
                int readback = 0;
                CHECK_MUSA(musaMemcpy(&readback, buf1, sizeof(int), musaMemcpyDeviceToHost));
                printf("  Kernel write: wrote 0x%X, read back 0x%X %s\n",
                       kernel_val, readback, readback == kernel_val ? "OK" : "MISMATCH");
                if (readback != kernel_val) all_ok = false;
            }
        }
    }

    // ===== Test 3: Device kernel write (int4 volatile, like mc_st_na) =====
    printf("\n--- Test 3: Device kernel write (int4 volatile field-by-field) ---\n");
    {
        musaSetDevice(1);
        CHECK_MUSA(musaMemset(buf1, 0, 256));

        musaSetDevice(0);
        kernel_write_int4<<<1, 1>>>(peer_ptr, 0x11111111, 0x22222222, 0x33333333, 0x44444444);
        err = musaGetLastError();
        if (err != musaSuccess) {
            printf("  Kernel launch failed: %s\n", musaGetErrorString(err));
            all_ok = false;
        } else {
            err = musaDeviceSynchronize();
            if (err != musaSuccess) {
                printf("  Kernel execution failed: %s\n", musaGetErrorString(err));
                all_ok = false;
            } else {
                musaSetDevice(1);
                int readback[4] = {};
                CHECK_MUSA(musaMemcpy(readback, buf1, sizeof(int) * 4, musaMemcpyDeviceToHost));
                printf("  int4 write: wrote [0x%X,0x%X,0x%X,0x%X], read [0x%X,0x%X,0x%X,0x%X] %s\n",
                       0x11111111, 0x22222222, 0x33333333, 0x44444444,
                       readback[0], readback[1], readback[2], readback[3],
                       (readback[0] == 0x11111111 && readback[1] == 0x22222222 &&
                        readback[2] == 0x33333333 && readback[3] == 0x44444444) ? "OK" : "MISMATCH");
                if (readback[0] != 0x11111111 || readback[1] != 0x22222222 ||
                    readback[2] != 0x33333333 || readback[3] != 0x44444444)
                    all_ok = false;
            }
        }
    }

    // ===== Test 4: Device kernel write (release store, like mc_st_release) =====
    printf("\n--- Test 4: Device kernel write (volatile + threadfence_system) ---\n");
    {
        musaSetDevice(1);
        CHECK_MUSA(musaMemset(buf1, 0, 256));

        musaSetDevice(0);
        kernel_write_release<<<1, 1>>>(peer_ptr, 0xBEEFCAFE);
        err = musaGetLastError();
        if (err != musaSuccess) {
            printf("  Kernel launch failed: %s\n", musaGetErrorString(err));
            all_ok = false;
        } else {
            err = musaDeviceSynchronize();
            if (err != musaSuccess) {
                printf("  Kernel execution failed: %s\n", musaGetErrorString(err));
                all_ok = false;
            } else {
                musaSetDevice(1);
                int readback = 0;
                CHECK_MUSA(musaMemcpy(&readback, buf1, sizeof(int), musaMemcpyDeviceToHost));
                printf("  Release write: wrote 0x%X, read back 0x%X %s\n",
                       0xBEEFCAFE, readback, readback == 0xBEEFCAFE ? "OK" : "MISMATCH");
                if (readback != 0xBEEFCAFE) all_ok = false;
            }
        }
    }

    // ===== Test 5: musaMemcpyPeer (potential fallback) =====
    printf("\n--- Test 5: musaMemcpyPeer (host-initiated peer copy) ---\n");
    {
        // Write test value to device 0's local buffer
        musaSetDevice(0);
        int test_val = 0xF00DCAFE;
        CHECK_MUSA(musaMemcpy(buf0, &test_val, sizeof(int), musaMemcpyHostToDevice));

        // Clear device 1's buffer
        musaSetDevice(1);
        CHECK_MUSA(musaMemset(buf1, 0, 256));

        // Copy from device 0 to device 1 using musaMemcpyPeer
        musaSetDevice(0);
        err = musaMemcpyPeer(buf1, 1, buf0, 0, sizeof(int));
        if (err != musaSuccess) {
            printf("  musaMemcpyPeer failed: %s\n", musaGetErrorString(err));
            // Try musaMemcpyPeerAsync
            musaStream_t stream;
            musaStreamCreate(&stream);
            err = musaMemcpyPeerAsync(buf1, 1, buf0, 0, sizeof(int), stream);
            if (err != musaSuccess) {
                printf("  musaMemcpyPeerAsync also failed: %s\n", musaGetErrorString(err));
            } else {
                musaStreamSynchronize(stream);
                musaSetDevice(1);
                int readback = 0;
                musaMemcpy(&readback, buf1, sizeof(int), musaMemcpyDeviceToHost);
                printf("  musaMemcpyPeerAsync: wrote 0x%X, read back 0x%X %s\n",
                       test_val, readback, readback == test_val ? "OK" : "MISMATCH");
                if (readback != test_val) all_ok = false;
                musaStreamDestroy(stream);
            }
        } else {
            musaSetDevice(1);
            int readback = 0;
            musaMemcpy(&readback, buf1, sizeof(int), musaMemcpyDeviceToHost);
            printf("  musaMemcpyPeer: wrote 0x%X, read back 0x%X %s\n",
                   test_val, readback, readback == test_val ? "OK" : "MISMATCH");
            if (readback != test_val) all_ok = false;
        }
    }

    // ===== Test 6: Reverse direction (dev1 kernel writes to dev0) =====
    printf("\n--- Test 6: Reverse direction (dev1 kernel -> dev0 buffer) ---\n");
    if (peer_ptr1) {
        musaSetDevice(0);
        CHECK_MUSA(musaMemset(buf0, 0, 256));

        musaSetDevice(1);
        kernel_write_int<<<1, 1>>>(peer_ptr1, 0xABCD1234);
        err = musaGetLastError();
        if (err != musaSuccess) {
            printf("  Kernel launch failed: %s\n", musaGetErrorString(err));
            all_ok = false;
        } else {
            err = musaDeviceSynchronize();
            if (err != musaSuccess) {
                printf("  Kernel execution failed: %s\n", musaGetErrorString(err));
                all_ok = false;
            } else {
                musaSetDevice(0);
                int readback = 0;
                CHECK_MUSA(musaMemcpy(&readback, buf0, sizeof(int), musaMemcpyDeviceToHost));
                printf("  Reverse kernel write: wrote 0x%X, read back 0x%X %s\n",
                       0xABCD1234, readback, readback == 0xABCD1234 ? "OK" : "MISMATCH");
                if (readback != 0xABCD1234) all_ok = false;
            }
        }
    }

    // Cleanup
    musaSetDevice(0);
    musaIpcCloseMemHandle(peer_ptr);
    musaFree(buf0);
    if (peer_ptr1) musaIpcCloseMemHandle(peer_ptr1);
    musaSetDevice(1);
    musaFree(buf1);

    printf("\n=== RESULT: %s ===\n", all_ok ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
    return all_ok;
}

int main() {
    bool ok = test_musa_p2p();
    return ok ? 0 : 1;
}
