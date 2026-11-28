// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>

#include "ssd_read_pipeline.h"

namespace mooncake::ssd_offload {

struct ReadResult {
    size_t bytes{0};
    Error error{Error::kOk};
    int sys_errno{0};
};

class ReadSource {
   public:
    virtual ~ReadSource() = default;
    virtual uint64_t file_bytes() const noexcept = 0;
    // Synchronous, position-independent read. When this returns, no I/O
    // can still write into the supplied buffer, including on failure.
    virtual ReadResult ReadAt(std::span<std::byte> buffer,
                              uint64_t file_offset) noexcept = 0;
    // Borrowed fd, pinned by the source lifetime. Async drivers must retain
    // this source until every submitted read has actually completed.
    virtual std::optional<int> native_read_fd() const noexcept { return {}; }
};

struct ReadSourceResult {
    Error error;
    std::shared_ptr<ReadSource> source;
    int sys_errno{0};
};

// Opens a regular file read-only (non-blocking open cleared after S_ISREG
// validation) and verifies it is at least min_file_bytes long. The optional
// lifetime token pins backend metadata/file lifetime through the last source
// reference.
ReadSourceResult OpenPosixReadSource(const std::string& path,
                                     uint64_t min_file_bytes,
                                     std::shared_ptr<void> lifetime = {});

}  // namespace mooncake::ssd_offload
