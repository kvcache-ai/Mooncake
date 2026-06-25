#ifndef MOONCAKE_WORKER_KERNELS_CUH
#define MOONCAKE_WORKER_KERNELS_CUH

// Include the main worker header for struct definitions (Task, SegmentInfo,
// TransferGroupMeta). When compiled by mcc (__MUSA__ defined), the torch-
// dependent parts are guarded out, making this safe for the MUSA compiler.
#include <mooncake_worker.cuh>

namespace mooncake {

// Kernel function declarations — guarded so g++ doesn't see __global__
// which it can't parse. Parameters use plain C++ types (int instead of
// c10d::OpType / c10d::ReduceOp::RedOpType) so that mcc can compile them
// without torch headers.
#if defined(__CUDACC__) || defined(__MUSA__)
__global__ void enqueueTaskKernel(int opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId);

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks, int op,
                             bool* activeRanks);
#endif

// Host-callable kernel launch wrappers (compiled by mcc/nvcc, callable from
// g++) g++ cannot compile <<<>>> syntax, so these wrappers are compiled by the
// GPU compiler and provide plain C++ functions that the host code can call.
// Both CUDA and MUSA use cudaStream_t in the declaration: on MUSA,
// cuda_alike.h typedefs cudaStream_t to musaStream_t.

void launchEnqueueTaskKernel(int opType, size_t tensorSize,
                             int64_t broadcastRoot, int bufferOffset,
                             uint64_t submitSequence, void* meta, Task* tasks,
                             int numRanks, const bool* activeRanks,
                             int* activeRanksTensor, size_t taskId,
                             cudaStream_t stream);

void launchReduceKernel_uint8(uint8_t* dst, const uint8_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_int8(int8_t* dst, const int8_t* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);
void launchReduceKernel_int16(int16_t* dst, const int16_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_int32(int* dst, const int* src, size_t numElements,
                              size_t numRanks, int op, bool* activeRanks,
                              cudaStream_t stream);
void launchReduceKernel_int64(int64_t* dst, const int64_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_float(float* dst, const float* src, size_t numElements,
                              size_t numRanks, int op, bool* activeRanks,
                              cudaStream_t stream);
void launchReduceKernel_double(double* dst, const double* src,
                               size_t numElements, size_t numRanks, int op,
                               bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_bool(bool* dst, const bool* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);
void launchReduceKernel_bf16(void* dst, const void* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);

void launchP2pReduceScatterSlottedKernel_uint8(
    uint8_t* output, const uint8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_int8(
    int8_t* output, const int8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_int16(
    int16_t* output, const int16_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_int32(
    int* output, const int* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_int64(
    int64_t* output, const int64_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_float(
    float* output, const float* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_double(
    double* output, const double* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_bool(
    bool* output, const bool* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);

#define DECL_P2P_RS_GRAPH(suffix, type)                                      \
void launchP2pReduceScatterSlottedGraphKernel_##suffix(                      \
    type* output, const type* input, void* local_recv_base, void** peer_ptrs, \
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,\
    int rank, int numRanks, const uint32_t* baseSequenceSlot,                \
    cudaStream_t stream);

DECL_P2P_RS_GRAPH(uint8, uint8_t)
DECL_P2P_RS_GRAPH(int8, int8_t)
DECL_P2P_RS_GRAPH(int16, int16_t)
DECL_P2P_RS_GRAPH(int32, int)
DECL_P2P_RS_GRAPH(int64, int64_t)
DECL_P2P_RS_GRAPH(float, float)
DECL_P2P_RS_GRAPH(double, double)
DECL_P2P_RS_GRAPH(bool, bool)

#undef DECL_P2P_RS_GRAPH

void launchP2pReduceScatterSlottedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    cudaStream_t stream);

void launchP2pReduceScatterSlottedChunkedKernel_uint8(
    uint8_t* output, const uint8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_int8(
    int8_t* output, const int8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_int16(
    int16_t* output, const int16_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_int32(
    int* output, const int* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_int64(
    int64_t* output, const int64_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_float(
    float* output, const float* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_double(
    double* output, const double* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_bool(
    bool* output, const bool* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);
void launchP2pReduceScatterSlottedChunkedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);

#define DECL_P2P_RS_CHUNKED_GRAPH(suffix, type)                              \
void launchP2pReduceScatterSlottedChunkedGraphKernel_##suffix(               \
    type* output, const type* input, void* local_recv_base, void** peer_ptrs, \
    int32_t* available, size_t fullNumElements, size_t chunkElements,         \
    size_t slotStrideBytes, int slots, int rank, int numRanks,                \
    const uint32_t* baseSequenceSlot, cudaStream_t stream);

DECL_P2P_RS_CHUNKED_GRAPH(uint8, uint8_t)
DECL_P2P_RS_CHUNKED_GRAPH(int8, int8_t)
DECL_P2P_RS_CHUNKED_GRAPH(int16, int16_t)
DECL_P2P_RS_CHUNKED_GRAPH(int32, int)
DECL_P2P_RS_CHUNKED_GRAPH(int64, int64_t)
DECL_P2P_RS_CHUNKED_GRAPH(float, float)
DECL_P2P_RS_CHUNKED_GRAPH(double, double)
DECL_P2P_RS_CHUNKED_GRAPH(bool, bool)

#undef DECL_P2P_RS_CHUNKED_GRAPH

void launchP2pReduceScatterSlottedChunkedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream);

void launchP2pAllReduceSlottedKernel_uint8(
    uint8_t* output, const uint8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_int8(
    int8_t* output, const int8_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_int16(
    int16_t* output, const int16_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_int32(
    int* output, const int* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_int64(
    int64_t* output, const int64_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_float(
    float* output, const float* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_double(
    double* output, const double* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_bool(
    bool* output, const bool* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);
void launchP2pAllReduceSlottedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);

void preloadReduceKernels();

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_KERNELS_CUH
