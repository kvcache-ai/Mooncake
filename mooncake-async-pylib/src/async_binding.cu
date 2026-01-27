#include <torch/extension.h>
#include <c10/cuda/CUDAStream.h>
#include <cuda_runtime.h>
#include <vector>
#include <iostream>
// #include <sstream>
#include <chrono>

// Mooncake headers
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

// Context for the host callback
struct TransferOnCudaContext {
    TransferEngine* engine;
    Transport::BatchID batch_id;
    std::vector<Transport::TransferRequest> requests;
    uint64_t total_bytes;
    bool is_write;  // 新增：记录是读还是写
    std::chrono::high_resolution_clock::time_point start_time;
};

// Callback function executed by the CUDA driver
void CUDART_CB submit_callback(void* data) {
    auto* ctx = reinterpret_cast<TransferOnCudaContext*>(data);
    
    // 在真正开始传输前记录时间
    ctx->start_time = std::chrono::high_resolution_clock::now();

    // 1. Submit the transfer
    auto status = ctx->engine->submitTransfer(ctx->batch_id, ctx->requests);
    
    if (!status.ok()) {
        std::cerr << "[Mooncake Async] Submit failed in callback: " << status.ToString() << std::endl;
        ctx->engine->freeBatchID(ctx->batch_id);
        delete ctx;
        return;
    }

    // 2. Poll for completion
    Transport::TransferStatus t_status;
    while (true) {
        auto ret = ctx->engine->getBatchTransferStatus(ctx->batch_id, t_status);
        if (!ret.ok()) {
            std::cerr << "[Mooncake Async] Failed to get status" << std::endl;
            break;
        }
        
        if (t_status.s == Transport::TransferStatusEnum::COMPLETED) {
            break;
        } else if (t_status.s == Transport::TransferStatusEnum::FAILED) {
            std::cerr << "[Mooncake Async] Transfer failed" << std::endl;
            break;
        } else if (t_status.s == Transport::TransferStatusEnum::TIMEOUT) {
            std::cerr << "[Mooncake Async] Transfer timeout" << std::endl;
            break;
        }
    }
    
    // 3. Cleanup
    ctx->engine->freeBatchID(ctx->batch_id);
    
    // 计算实际耗时和带宽（可选，用于日志）
    auto end_time = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end_time - ctx->start_time).count();
    double bandwidth_gbps = (ctx->total_bytes * 8.0) / (duration_ms / 1000.0) / 1e9;
    
    std::cout << "[Mooncake Async] " 
              << (ctx->is_write ? "Write" : "Read") << " "
              << ctx->total_bytes << " bytes in " 
              << duration_ms << " ms, "
              << "Bandwidth: " << bandwidth_gbps << " Gbps" << std::endl;
    
    delete ctx;
}

void transfer_on_cuda(
    uint64_t engine_ptr,
    std::string target_hostname,
    uint64_t source_addr,
    uint64_t target_addr,
    size_t length,
    bool is_write
) {
    auto engine = reinterpret_cast<TransferEngine*>(engine_ptr);
    if (!engine) {
        throw std::runtime_error("Invalid engine pointer");
    }

    // 1. Open Segment
    auto segment_id = engine->openSegment(target_hostname);
    if (segment_id == (Transport::SegmentHandle)-1) {
        throw std::runtime_error("Failed to open segment for " + target_hostname);
    }

    // 2. Allocate Batch
    auto batch_id = engine->allocateBatchID(1);

    // 3. Prepare Request
    std::vector<Transport::TransferRequest> requests(1);
    requests[0].opcode = is_write ? Transport::TransferRequest::WRITE : Transport::TransferRequest::READ;
    requests[0].source = (void*)source_addr;
    requests[0].target_id = segment_id;
    requests[0].target_offset = target_addr;
    requests[0].length = length;
    
    // Get current CUDA stream from PyTorch
    cudaStream_t stream = c10::cuda::getCurrentCUDAStream().stream();

    // 4. Launch Host Callback
    // 新增：将 is_write 传入 context
    auto* ctx = new TransferOnCudaContext{engine, batch_id, requests, length, is_write};
    
    cudaError_t err = cudaLaunchHostFunc(stream, submit_callback, ctx);
    if (err != cudaSuccess) {
        delete ctx;
        engine->freeBatchID(batch_id);
        throw std::runtime_error("cudaLaunchHostFunc failed: " + std::string(cudaGetErrorString(err)));
    }
}

// Batched version
void batch_transfer_on_cuda(
    uint64_t engine_ptr,
    std::string target_hostname,
    std::vector<uint64_t> source_addrs,
    std::vector<uint64_t> target_addrs,
    std::vector<size_t> lengths,
    bool is_write
) {
    auto engine = reinterpret_cast<TransferEngine*>(engine_ptr);
    if (!engine) throw std::runtime_error("Invalid engine pointer");
    
    size_t batch_size = source_addrs.size();
    if (batch_size == 0) return;
    if (target_addrs.size() != batch_size || lengths.size() != batch_size) {
        throw std::runtime_error("Input lists must have the same length");
    }

    // 1. Open Segment
    auto segment_id = engine->openSegment(target_hostname);
    if (segment_id == (Transport::SegmentHandle)-1) {
        throw std::runtime_error("Failed to open segment for " + target_hostname);
    }

    // 2. Allocate Batch
    auto batch_id = engine->allocateBatchID(batch_size);

    // 3. Prepare Requests
    std::vector<Transport::TransferRequest> requests(batch_size);
    uint64_t total_bytes = 0;
    
    for (size_t i = 0; i < batch_size; ++i) {
        requests[i].opcode = is_write ? Transport::TransferRequest::WRITE : Transport::TransferRequest::READ;
        requests[i].source = (void*)source_addrs[i];
        requests[i].target_id = segment_id;
        requests[i].target_offset = target_addrs[i];
        requests[i].length = lengths[i];
        total_bytes += lengths[i];
    }
    
    // Get current CUDA stream from PyTorch
    cudaStream_t stream = c10::cuda::getCurrentCUDAStream().stream();

    // 4. Launch Host Callback
    // 新增：将 is_write 传入 context
    auto* ctx = new TransferOnCudaContext{engine, batch_id, requests, total_bytes, is_write};
    
    cudaError_t err = cudaLaunchHostFunc(stream, submit_callback, ctx);
    if (err != cudaSuccess) {
        delete ctx;
        engine->freeBatchID(batch_id);
        throw std::runtime_error("cudaLaunchHostFunc failed: " + std::string(cudaGetErrorString(err)));
    }
}


void transfer_on_cuda_read(uint64_t engine_ptr, std::string target_hostname, uint64_t local_addr, uint64_t remote_addr, size_t length) {
    transfer_on_cuda(engine_ptr, target_hostname, local_addr, remote_addr, length, false);
}

void transfer_on_cuda_write(uint64_t engine_ptr, std::string target_hostname, uint64_t local_addr, uint64_t remote_addr, size_t length) {
    transfer_on_cuda(engine_ptr, target_hostname, local_addr, remote_addr, length, true);
}

void batch_transfer_on_cuda_read(uint64_t engine_ptr, std::string target_hostname, std::vector<uint64_t> local_addrs, std::vector<uint64_t> remote_addrs, std::vector<size_t> lengths) {
    batch_transfer_on_cuda(engine_ptr, target_hostname, local_addrs, remote_addrs, lengths, false);
}

void batch_transfer_on_cuda_write(uint64_t engine_ptr, std::string target_hostname, std::vector<uint64_t> local_addrs, std::vector<uint64_t> remote_addrs, std::vector<size_t> lengths) {
    batch_transfer_on_cuda(engine_ptr, target_hostname, local_addrs, remote_addrs, lengths, true);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("transfer_on_cuda_read", &transfer_on_cuda_read, "Async Read via Mooncake on CUDA Stream");
    m.def("transfer_on_cuda_write", &transfer_on_cuda_write, "Async Write via Mooncake on CUDA Stream");
    m.def("batch_transfer_on_cuda_read", &batch_transfer_on_cuda_read, "Batched Async Read via Mooncake on CUDA Stream");
    m.def("batch_transfer_on_cuda_write", &batch_transfer_on_cuda_write, "Batched Async Write via Mooncake on CUDA Stream");
}
