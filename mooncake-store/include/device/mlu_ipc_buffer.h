#pragma once

#include <memory>

#include "device/mlu_ipc_buffer_handle.h"

namespace mooncake::device {

// Keep this export alive, and keep the allocation unchanged, through the RPC.
// Destruction revokes late imports and waits for existing importers to close,
// including after a lost RPC reply. It may outlive the ordinary RPC deadline.
// Forcefully terminating the exporter while it is borrowed is unsupported.
class MluIpcBufferExport {
   public:
    MluIpcBufferExport();
    ~MluIpcBufferExport();
    MluIpcBufferExport(MluIpcBufferExport &&) noexcept;
    MluIpcBufferExport &operator=(MluIpcBufferExport &&) noexcept;
    MluIpcBufferExport(const MluIpcBufferExport &) = delete;
    MluIpcBufferExport &operator=(const MluIpcBufferExport &) = delete;

    static tl::expected<MluIpcBufferExport, ErrorCode> Create(const void *ptr,
                                                              size_t size);
    const MluIpcBufferHandle &handle() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class MluIpcBufferMapping {
   public:
    MluIpcBufferMapping();
    ~MluIpcBufferMapping();
    MluIpcBufferMapping(MluIpcBufferMapping &&) noexcept;
    MluIpcBufferMapping &operator=(MluIpcBufferMapping &&) noexcept;
    MluIpcBufferMapping(const MluIpcBufferMapping &) = delete;
    MluIpcBufferMapping &operator=(const MluIpcBufferMapping &) = delete;

    static tl::expected<MluIpcBufferMapping, ErrorCode> Open(
        const MluIpcBufferHandle &handle);
    void *ptr() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::device
