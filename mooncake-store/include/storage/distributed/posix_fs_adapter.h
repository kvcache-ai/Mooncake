#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "storage/distributed/fs_adapter.h"

namespace mooncake {

class PosixFsAdapter : public FileSystemAdapter {
   public:
    tl::expected<size_t, ErrorCode> WriteFile(
        const std::string& path, std::span<const char> data) override;

    tl::expected<size_t, ErrorCode> ReadFile(const std::string& path, void* buf,
                                             size_t len) override;

    tl::expected<size_t, ErrorCode> VectorWriteFile(const std::string& path,
                                                    const iovec* iov,
                                                    int iovcnt,
                                                    off_t offset) override;

    tl::expected<size_t, ErrorCode> VectorReadFile(const std::string& path,
                                                   const iovec* iov, int iovcnt,
                                                   off_t offset) override;

    tl::expected<void, ErrorCode> DeleteFile(const std::string& path) override;

    tl::expected<bool, ErrorCode> FileExists(const std::string& path) override;

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) override;

    tl::expected<int, ErrorCode> OpenFile(const std::string& path) override;

    tl::expected<void, ErrorCode> CloseFile(int fd) override;

    tl::expected<void, ErrorCode> PreallocateFile(const std::string& path,
                                                  uint64_t size) override;

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override;

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override;

    tl::expected<int, ErrorCode> OpenFileDirect(
        const std::string& path) override;

    tl::expected<size_t, ErrorCode> DirectReadAt(int fd, iovec* iov, int iovcnt,
                                                 int64_t offset) override;

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;

    tl::expected<void, ErrorCode> Shutdown() override;

    const char* GetName() const override { return "posix"; }

   protected:
    // Split out for focused tests and to keep the zero-copy and bounce-buffer
    // paths explicit. DirectReadAt selects the first only when the file offset,
    // every non-empty iovec address, and every iovec length are 4K-aligned.
    virtual tl::expected<size_t, ErrorCode> DirectReadAtAligned(int fd,
                                                                iovec* iov,
                                                                int iovcnt,
                                                                int64_t offset);

    virtual tl::expected<size_t, ErrorCode> DirectReadAtStaged(int fd,
                                                               iovec* iov,
                                                               int iovcnt,
                                                               int64_t offset);

   private:
    // Unaligned O_DIRECT reads stage through a contiguous bounce buffer.
    // Allocating one bounce per key on the 128-way batch path thrashes the
    // allocator, so buffers are pooled and reused across reads.
    struct DirectStaging {
        DirectStaging() = default;
        // Frees the posix_memalign'd buffer.
        ~DirectStaging();
        DirectStaging(const DirectStaging&) = delete;
        DirectStaging& operator=(const DirectStaging&) = delete;

        void* ptr = nullptr;  // 4K-aligned, from posix_memalign
        size_t capacity = 0;  // allocated bytes
        bool in_use = false;
    };

    // Borrow a staging buffer with room for at least `size` bytes. Grows the
    // pool on demand up to a cap; returns nullptr when the cap is reached and
    // every slot is busy, in which case the caller falls back to a transient
    // allocation.
    DirectStaging* AcquireDirectStaging(size_t size);

    // Return a borrowed buffer to the pool.
    void ReleaseDirectStaging(DirectStaging* slot);

    std::string mount_path_;

    std::mutex direct_staging_mutex_;
    std::vector<std::unique_ptr<DirectStaging>> direct_staging_pool_;
};

}  // namespace mooncake
