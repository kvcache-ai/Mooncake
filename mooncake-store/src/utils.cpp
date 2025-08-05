#include "utils.h"

#include <cuda_runtime.h>
#include <Slab.h>
#include <glog/logging.h>
#include <iostream>
#include <unordered_map>

namespace mooncake {

static std::unordered_map<void *, void *> g_cuda_aligned_allocations;
static std::mutex g_cuda_alloc_mutex;

void *allocate_buffer_allocator_memory(size_t total_size) {
    const size_t alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

void *allocate_vram_buffer_allocator_memory(size_t total_size) {
    const size_t alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }

    size_t alloc_size = total_size + alignment - 1 + sizeof(void *);
    void *raw_ptr = nullptr;
    cudaError_t err = cudaMalloc(&raw_ptr, alloc_size);
    if (err != cudaSuccess) {
        return nullptr;
    }
    uintptr_t start_ptr_int =
        reinterpret_cast<uintptr_t>(raw_ptr) + sizeof(void *);
    uintptr_t aligned_ptr_int =
        (start_ptr_int + alignment - 1) & ~(alignment - 1);
    void *aligned_ptr = reinterpret_cast<void *>(aligned_ptr_int);
    {
        std::lock_guard<std::mutex> lock(g_cuda_alloc_mutex);
        g_cuda_aligned_allocations[aligned_ptr] = raw_ptr;
    }
    std::cout << "alloc good" << std::endl;

    // Allocate aligned memory
    return aligned_ptr;
}

void cudaAlignedFree(void *ptr) {
    if (ptr == nullptr) {
        return;
    }

    void *raw_ptr = nullptr;

    {
        std::lock_guard<std::mutex> lock(g_cuda_alloc_mutex);
        auto it = g_cuda_aligned_allocations.find(ptr);
        if (it == g_cuda_aligned_allocations.end()) {
            return;
        }
        raw_ptr = it->second;
        g_cuda_aligned_allocations.erase(it);
    }

    cudaFree(raw_ptr);
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

static std::string loadNicPriorityMatrix(const std::string &device_name) {
    auto device_names = formatDeviceNames(device_name);
    return "{\"cpu:0\": [[" + device_names + "], []]}";
}

void **rdma_args(const std::string &device_name) {
    static auto nic_priority_matrix = loadNicPriorityMatrix(device_name);
    void **args = (void **)malloc(2 * sizeof(void *));
    args[0] = (void *)nic_priority_matrix.c_str();
    args[1] = nullptr;
    return args;
}

}  // namespace mooncake
