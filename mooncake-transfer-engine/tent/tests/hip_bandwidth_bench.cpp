// Copyright 2025 KVCache.AI
//
// Standalone intra-node HIP bandwidth benchmark.
// Measures H2D, D2H, D2D (same GPU), and GPU-to-GPU (P2P via XGMI)
// bandwidth across a sweep of transfer sizes.
//
// Build:
//   hipcc -O3 -o hip_bandwidth_bench hip_bandwidth_bench.cpp
//
// Run:
//   ./hip_bandwidth_bench              # all GPUs, all directions
//   ./hip_bandwidth_bench 0 1          # GPU 0 as src, GPU 1 as dst

#include <hip/hip_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <vector>

#define CHECK_HIP(call)                                                  \
    do {                                                                 \
        hipError_t err = (call);                                         \
        if (err != hipSuccess) {                                         \
            fprintf(stderr, "HIP error %s:%d: %s\n", __FILE__, __LINE__, \
                    hipGetErrorString(err));                             \
            exit(EXIT_FAILURE);                                          \
        }                                                                \
    } while (0)

static const size_t kWarmupIter = 5;
static const size_t kBenchIter = 20;

// Returns elapsed microseconds for `iters` copies of `bytes`.
static double benchCopy(void* dst, const void* src, size_t bytes,
                        hipMemcpyKind kind, hipStream_t stream, size_t iters) {
    // warmup
    for (size_t i = 0; i < kWarmupIter; i++)
        CHECK_HIP(hipMemcpyAsync(dst, src, bytes, kind, stream));
    CHECK_HIP(hipStreamSynchronize(stream));

    hipEvent_t start, stop;
    CHECK_HIP(hipEventCreate(&start));
    CHECK_HIP(hipEventCreate(&stop));

    CHECK_HIP(hipEventRecord(start, stream));
    for (size_t i = 0; i < iters; i++)
        CHECK_HIP(hipMemcpyAsync(dst, src, bytes, kind, stream));
    CHECK_HIP(hipEventRecord(stop, stream));
    CHECK_HIP(hipStreamSynchronize(stream));

    float ms = 0;
    CHECK_HIP(hipEventElapsedTime(&ms, start, stop));
    CHECK_HIP(hipEventDestroy(start));
    CHECK_HIP(hipEventDestroy(stop));
    return (double)ms * 1000.0;  // → microseconds
}

static void printHeader() {
    printf("\n%-18s  %10s  %12s  %12s\n", "Size", "Iters", "BW (GB/s)",
           "Lat (us)");
    printf("%s\n", std::string(58, '-').c_str());
}

static void runBench(const char* label, void* dst, const void* src,
                     size_t max_bytes, hipMemcpyKind kind, hipStream_t stream) {
    printf("\n[%s]\n", label);
    printHeader();

    for (size_t bytes = 4096; bytes <= max_bytes; bytes *= 4) {
        size_t iters = std::max<size_t>(kBenchIter, 200 * 1024 * 1024 / bytes);
        double us = benchCopy(dst, src, bytes, kind, stream, iters);
        double bw = (double)bytes * iters / (us / 1e6) / 1e9;  // GB/s
        double lat = us / iters;
        printf("%-18zu  %10zu  %12.3f  %12.2f\n", bytes, iters, bw, lat);
    }
}

int main(int argc, char** argv) {
    int gpu_count = 0;
    CHECK_HIP(hipGetDeviceCount(&gpu_count));
    printf("Detected %d AMD GPU(s)\n", gpu_count);

    for (int g = 0; g < gpu_count; g++) {
        hipDeviceProp_t prop;
        CHECK_HIP(hipGetDeviceProperties(&prop, g));
        printf("  GPU %d: %s  VRAM %.0f GiB  PCIe %04x:%02x:%02x.0\n", g,
               prop.name,
               (double)prop.totalGlobalMem / (1024.0 * 1024.0 * 1024.0),
               prop.pciDomainID, prop.pciBusID, prop.pciDeviceID);
    }

    int src_gpu = (argc >= 2) ? atoi(argv[1]) : 0;
    int dst_gpu = (argc >= 3) ? atoi(argv[2]) : (gpu_count > 1 ? 1 : 0);

    const size_t kMaxBytes = 4ULL * 1024 * 1024 * 1024;  // 4 GiB
    const size_t kHostBytes = 512ULL * 1024 * 1024;      // 512 MiB host

    // --- allocate host pinned ---
    void* h_src = nullptr;
    void* h_dst = nullptr;
    CHECK_HIP(hipHostMalloc(&h_src, kHostBytes, hipHostMallocDefault));
    CHECK_HIP(hipHostMalloc(&h_dst, kHostBytes, hipHostMallocDefault));
    memset(h_src, 0xAA, kHostBytes);

    // --- allocate device buffers on src_gpu ---
    CHECK_HIP(hipSetDevice(src_gpu));
    void* d_src = nullptr;
    CHECK_HIP(hipMalloc(&d_src, kMaxBytes));
    CHECK_HIP(hipMemset(d_src, 0xBB, kMaxBytes));

    hipStream_t stream_src;
    CHECK_HIP(hipStreamCreate(&stream_src));

    // --- allocate device buffer on dst_gpu ---
    void* d_dst = nullptr;
    hipStream_t stream_dst;
    if (dst_gpu != src_gpu) {
        CHECK_HIP(hipSetDevice(dst_gpu));
        CHECK_HIP(hipMalloc(&d_dst, kMaxBytes));
        CHECK_HIP(hipMemset(d_dst, 0xCC, kMaxBytes));
        CHECK_HIP(hipStreamCreate(&stream_dst));
    } else {
        d_dst = (char*)d_src + kMaxBytes / 2;
        stream_dst = stream_src;
    }

    // ----------------------------------------------------------------
    printf("\n=== H2D: Host → GPU %d ===\n", src_gpu);
    CHECK_HIP(hipSetDevice(src_gpu));
    runBench("H2D", d_src, h_src, kHostBytes, hipMemcpyHostToDevice,
             stream_src);

    // ----------------------------------------------------------------
    printf("\n=== D2H: GPU %d → Host ===\n", src_gpu);
    runBench("D2H", h_dst, d_src, kHostBytes, hipMemcpyDeviceToHost,
             stream_src);

    // ----------------------------------------------------------------
    printf("\n=== D2D (intra-GPU %d): same-device copy ===\n", src_gpu);
    runBench("D2D intra", (char*)d_src + kMaxBytes / 2, d_src, kMaxBytes / 2,
             hipMemcpyDeviceToDevice, stream_src);

    // ----------------------------------------------------------------
    if (dst_gpu != src_gpu) {
        // Check P2P access
        int can_access = 0;
        hipDeviceCanAccessPeer(&can_access, src_gpu, dst_gpu);
        if (can_access) {
            CHECK_HIP(hipSetDevice(src_gpu));
            CHECK_HIP(hipDeviceEnablePeerAccess(dst_gpu, 0));
        }

        char label[64];
        snprintf(label, sizeof(label), "GPU %d → GPU %d (P2P %s)", src_gpu,
                 dst_gpu, can_access ? "enabled" : "disabled");
        printf("\n=== %s ===\n", label);

        CHECK_HIP(hipSetDevice(src_gpu));
        runBench(label, d_dst, d_src, kMaxBytes, hipMemcpyDeviceToDevice,
                 stream_src);

        // Reverse direction
        snprintf(label, sizeof(label), "GPU %d → GPU %d (reverse)", dst_gpu,
                 src_gpu);
        printf("\n=== %s ===\n", label);
        CHECK_HIP(hipSetDevice(dst_gpu));
        runBench(label, d_src, d_dst, kMaxBytes, hipMemcpyDeviceToDevice,
                 stream_dst);
    }

    // ----------------------------------------------------------------
    // All-pairs GPU-to-GPU if no specific args
    if (argc < 3 && gpu_count > 1) {
        printf("\n=== All-pairs GPU P2P bandwidth (large block = 1GiB) ===\n");
        printf("%-8s  %-8s  %12s\n", "Src", "Dst", "BW (GB/s)");
        printf("%s\n", std::string(32, '-').c_str());

        std::vector<void*> dbufs(gpu_count, nullptr);
        for (int g = 0; g < gpu_count; g++) {
            CHECK_HIP(hipSetDevice(g));
            CHECK_HIP(hipMalloc(&dbufs[g], 1ULL * 1024 * 1024 * 1024));
            CHECK_HIP(hipMemset(dbufs[g], g, 1ULL * 1024 * 1024 * 1024));
        }

        const size_t kP2PBytes = 1ULL * 1024 * 1024 * 1024;
        for (int s = 0; s < gpu_count; s++) {
            for (int d = 0; d < gpu_count; d++) {
                if (s == d) {
                    printf("GPU%-5d  GPU%-5d  %12s\n", s, d, "-");
                    continue;
                }
                CHECK_HIP(hipSetDevice(s));
                int ca = 0;
                hipDeviceCanAccessPeer(&ca, s, d);
                if (ca) hipDeviceEnablePeerAccess(d, 0);

                hipStream_t st;
                CHECK_HIP(hipStreamCreate(&st));
                double us = benchCopy(dbufs[d], dbufs[s], kP2PBytes,
                                      hipMemcpyDeviceToDevice, st, kBenchIter);
                double bw = (double)kP2PBytes * kBenchIter / (us / 1e6) / 1e9;
                printf("GPU%-5d  GPU%-5d  %12.2f\n", s, d, bw);
                CHECK_HIP(hipStreamDestroy(st));
            }
        }

        for (int g = 0; g < gpu_count; g++) {
            CHECK_HIP(hipSetDevice(g));
            CHECK_HIP(hipFree(dbufs[g]));
        }
    }

    // cleanup
    CHECK_HIP(hipSetDevice(src_gpu));
    CHECK_HIP(hipFree(d_src));
    CHECK_HIP(hipStreamDestroy(stream_src));
    if (dst_gpu != src_gpu) {
        CHECK_HIP(hipSetDevice(dst_gpu));
        CHECK_HIP(hipFree(d_dst));
        CHECK_HIP(hipStreamDestroy(stream_dst));
    }
    CHECK_HIP(hipHostFree(h_src));
    CHECK_HIP(hipHostFree(h_dst));

    printf("\nDone.\n");
    return 0;
}
