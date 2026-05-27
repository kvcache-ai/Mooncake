// test_musa_ipc_crossproc.mu — Test MUSA IPC across forked processes
// Build: mcc test_musa_ipc_crossproc.mu -o test_musa_ipc_crossproc -lmusart

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <musa_runtime.h>

#define CHECK_MUSA(call) do { \
    musaError_t err = (call); \
    if (err != musaSuccess) { \
        fprintf(stderr, "[Rank %d] MUSA error at %s:%d: %s\n", rank, __FILE__, __LINE__, \
                musaGetErrorString(err)); \
        exit(1); \
    } \
} while(0)

// Shared memory for IPC handle exchange
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct {
    musaIpcMemHandle_t handle[2];
    int ready[2];
    int verify[2][4];  // rank -> verify[0..3]
} SharedData;

__global__ void write_kernel(int* buf, int value, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        buf[idx] = value + idx;
    }
}

int main(int argc, char** argv) {
    printf("============================================================\n");
    printf("Test: Cross-process IPC handle exchange + P2P write\n");
    printf("============================================================\n");

    int device_count = 0;
    musaError_t err = musaGetDeviceCount(&device_count);
    if (err != musaSuccess || device_count < 2) {
        printf("SKIP: Need at least 2 MUSA devices (found %d)\n", device_count);
        return 0;
    }

    // Create shared memory for IPC handle exchange
    int shm_fd = shm_open("/musa_ipc_test", O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) { perror("shm_open"); return 1; }
    ftruncate(shm_fd, sizeof(SharedData));
    SharedData* shared = (SharedData*)mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE,
                              MAP_SHARED, shm_fd, 0);
    memset(shared, 0, sizeof(SharedData));

    pid_t pid = fork();
    int rank = (pid == 0) ? 1 : 0;
    int peer_rank = 1 - rank;

    // Each rank uses its own GPU
    int device = rank;
    CHECK_MUSA(musaSetDevice(device));

    // Allocate buffer
    int* buf = NULL;
    CHECK_MUSA(musaMalloc(&buf, 1024 * sizeof(int)));
    CHECK_MUSA(musaMemset(buf, 0, 1024 * sizeof(int)));
    printf("[Rank %d] Allocated buffer at %p on device %d\n", rank, buf, device);

    // Get IPC handle
    CHECK_MUSA(musaIpcGetMemHandle(&shared->handle[rank], buf));
    printf("[Rank %d] Got IPC handle\n", rank);

    // Signal that our handle is ready
    __sync_fetch_and_add(&shared->ready[rank], 1);

    // Wait for peer's handle
    while (shared->ready[peer_rank] == 0) { usleep(1000); }
    printf("[Rank %d] Peer handle ready\n", rank);

    // Enable peer access (peer device ordinal = peer_rank since each rank
    // maps to a GPU with the same ID)
    int can_access = 0;
    CHECK_MUSA(musaDeviceCanAccessPeer(&can_access, device, peer_rank));
    if (can_access) {
        musaError_t peer_err = musaDeviceEnablePeerAccess(peer_rank, 0);
        if (peer_err == musaErrorPeerAccessAlreadyEnabled) {
            musaGetLastError();
        } else if (peer_err != musaSuccess) {
            fprintf(stderr, "[Rank %d] musaDeviceEnablePeerAccess failed: %s\n",
                    rank, musaGetErrorString(peer_err));
        } else {
            printf("[Rank %d] Enabled peer access to device %d\n", rank, peer_rank);
        }
    }

    // Open peer's IPC handle
    void* peer_ptr = NULL;
    CHECK_MUSA(musaIpcOpenMemHandle(&peer_ptr, shared->handle[peer_rank],
                                     musaIpcMemLazyEnablePeerAccess));
    printf("[Rank %d] Opened peer IPC handle: peer_ptr = %p\n", rank, peer_ptr);

    // Write to peer's buffer using a kernel
    int magic = 10000 + rank;
    write_kernel<<<1, 1024>>>((int*)peer_ptr, magic, 1024);
    CHECK_MUSA(musaDeviceSynchronize());
    printf("[Rank %d] Kernel write to peer completed\n", rank);

    // Wait for peer to finish writing
    usleep(500000);  // 500ms
    CHECK_MUSA(musaDeviceSynchronize());

    // Read our own buffer (should have been written by peer)
    int verify[4];
    CHECK_MUSA(musaMemcpy(verify, buf, 4 * sizeof(int), musaMemcpyDeviceToHost));
    int peer_magic = 10000 + peer_rank;
    printf("[Rank %d] verify[0]=%d (expected %d), verify[1]=%d (expected %d)\n",
           rank, verify[0], peer_magic, verify[1], peer_magic + 1);

    // Store results in shared memory
    memcpy(shared->verify[rank], verify, 4 * sizeof(int));

    // Cleanup
    CHECK_MUSA(musaIpcCloseMemHandle(peer_ptr));
    CHECK_MUSA(musaFree(buf));

    if (pid == 0) {
        // Child exits
        _exit(0);
    } else {
        // Parent waits for child
        int status;
        waitpid(pid, &status, 0);

        // Check results
        int success = 1;
        for (int r = 0; r < 2; r++) {
            int expected_magic = 10000 + (1 - r);
            if (shared->verify[r][0] != expected_magic ||
                shared->verify[r][1] != expected_magic + 1) {
                printf("[Rank %d] MISMATCH: got (%d, %d), expected (%d, %d)\n",
                       r, shared->verify[r][0], shared->verify[r][1],
                       expected_magic, expected_magic + 1);
                success = 0;
            }
        }

        printf("Cross-process IPC test: %s\n", success ? "PASS" : "FAIL");

        // Cleanup shared memory
        munmap(shared, sizeof(SharedData));
        close(shm_fd);
        shm_unlink("/musa_ipc_test");

        return success ? 0 : 1;
    }
}
