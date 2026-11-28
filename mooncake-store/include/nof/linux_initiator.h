#pragma once

#include <memory>

#include "nof/nvmeof_initiator.h"

namespace mooncake {

class LinuxNvmeInitiator final : public NVMeoFInitiator {
   public:
    LinuxNvmeInitiator();
    ~LinuxNvmeInitiator() override;
    LinuxNvmeInitiator(const LinuxNvmeInitiator&) = delete;
    LinuxNvmeInitiator& operator=(const LinuxNvmeInitiator&) = delete;

    NofSegmentHandle* OpenSegment(const std::string& endpoint) override;
    bool ProbeSegment(const std::string& endpoint, uint32_t timeout_ms,
                      std::string* error_reason) override;
    uint32_t GetBlockSize(const NofSegmentHandle* handle) override;
    int SubmitIO(NofSegmentHandle* handle, void* buffer, uint64_t byte_offset,
                 uint64_t byte_length, NofIOOp op,
                 NofIOAdaptor* adaptor) override;
    int64_t PollCompletion(NofSegmentHandle* handle,
                           uint32_t max_completions) override;
    ErrorCode RegisterMemory(void*, size_t) override { return ErrorCode::OK; }
    ErrorCode UnregisterMemory(void*) override { return ErrorCode::OK; }
    NofCapabilities GetCapabilities() const override;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
