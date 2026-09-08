// Copyright 2026 KVCache.AI
// StorageFile backed by a byte-addressable, mmap-able device.

#pragma once

#include <memory>
#include <string>

#include "file_interface.h"

namespace mooncake {

/**
 * @class DaxFile
 * @brief StorageFile over an mmap(MAP_SHARED) of a byte-addressable device.
 *
 * Intended for Linux device-DAX character devices (/dev/daxX.Y backed by
 * PMEM or CXL-attached memory), fsdax files, or any regular file. Device-DAX
 * nodes reject read/write syscalls, so every I/O is a memcpy through the
 * mapping. By default the mapping is treated as fast volatile memory:
 * datasync() is an msync(), which does not flush CPU caches to persistent
 * media. With flush_cpu_cache (x86-64 only), every vector_write also writes
 * its cache lines back (CLWB, else CLFLUSHOPT, else CLFLUSH) and fences, so a
 * record is power-fail durable on PMEM by the time the write returns.
 */
class DaxFile : public StorageFile {
   public:
    /**
     * @brief Opens `path` O_RDWR and maps its first `size` bytes MAP_SHARED.
     *
     * Regular files smaller than `size` are grown with ftruncate so fsdax
     * files and test fixtures work. Character devices report st_size == 0
     * and are mapped as-is; device-DAX requires `size` to be a multiple of
     * the device alignment (typically 2 MiB) or mmap fails with EINVAL.
     * `flush_cpu_cache` fails with INVALID_PARAMS off x86-64. Takes an
     * exclusive flock on `path` for the file's lifetime and fails with
     * FILE_OPEN_FAIL if another open file description already holds it.
     */
    static tl::expected<std::shared_ptr<DaxFile>, ErrorCode> Open(
        const std::string &path, size_t size, bool flush_cpu_cache = false);

    ~DaxFile() override;

    tl::expected<void, ErrorCode> datasync() override;

    // Sequential I/O is not offered: the only consumer
    // (OffsetAllocatorStorageBackend) addresses records by offset.
    tl::expected<size_t, ErrorCode> write(const std::string &buffer,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> write(std::span<const char> data,
                                          size_t length) override;
    tl::expected<size_t, ErrorCode> read(std::string &buffer,
                                         size_t length) override;

    tl::expected<size_t, ErrorCode> vector_write(const iovec *iov, int iovcnt,
                                                 off_t offset) override;
    tl::expected<size_t, ErrorCode> vector_read(const iovec *iov, int iovcnt,
                                                off_t offset) override;

    void *base() const { return base_; }
    size_t size() const { return size_; }

   private:
    DaxFile(const std::string &path, int fd, void *base, size_t size,
            bool flush_cpu_cache);

    bool InRange(off_t offset, size_t length) const;

    void *base_;
    size_t size_;
    bool flush_cpu_cache_;
};

}  // namespace mooncake
