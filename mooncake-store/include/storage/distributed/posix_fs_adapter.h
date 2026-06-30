#pragma once

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

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;

    tl::expected<void, ErrorCode> Shutdown() override;

    const char* GetName() const override { return "posix"; }

   private:
    std::string mount_path_;
};

}  // namespace mooncake
