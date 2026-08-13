#pragma once

#include <memory>
#include <mutex>

#include "nof/dma_buffer_allocator.h"
#include "nof/nvmeof_initiator.h"

namespace mooncake {

// Concrete SPDK-backed initiator. All SPDK headers and types are confined
// to the .cpp (pimpl). Construction is cheap; the SPDK environment is
// acquired lazily on first OpenSegment / ProbeSegment.
class SpdkInitiator : public NVMeoFInitiator {
   public:
    SpdkInitiator();
    ~SpdkInitiator() override;

    SpdkInitiator(const SpdkInitiator&) = delete;
    SpdkInitiator& operator=(const SpdkInitiator&) = delete;

    NofSegmentHandle* OpenSegment(const std::string& transport_str) override;
    bool ProbeSegment(const std::string& transport_str, uint32_t timeout_ms,
                      std::string* error_reason) override;
    uint32_t GetBlockSize(const NofSegmentHandle* handle) override;
    int SubmitIO(NofSegmentHandle* handle, void* buffer, uint64_t byte_offset,
                 uint64_t byte_length, NofIOOp op,
                 NofIOAdaptor* adaptor) override;
    int64_t PollCompletion(NofSegmentHandle* handle,
                           uint32_t max_completions) override;
    ErrorCode RegisterMemory(void* ptr, size_t size) override;
    ErrorCode UnregisterMemory(void* ptr) override;
    NofCapabilities GetCapabilities() const override;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// SPDK hugepage-pool DMA allocator (spdk_zmalloc / spdk_free). The SPDK
// environment is acquired lazily on the first Alloc and kept alive by an
// internal env-guard reference, so buffers stay valid as long as the
// allocator object lives.
class SpdkDmaAllocator : public DmaBufferAllocator {
   public:
    SpdkDmaAllocator();
    ~SpdkDmaAllocator() override;

    void* Alloc(size_t size, size_t align, int socket_id = -1) override;
    void Free(void* ptr) override;

   private:
    // Type-erased SpdkEnvGuard reference; keeps the SPDK env alive.
    std::shared_ptr<void> env_guard_;
    // Guards lazy env acquisition: the Python-ABI path
    // (hugepage_memory_alloc) can call Alloc concurrently (评审 #4).
    std::mutex env_mutex_;
};

}  // namespace mooncake
