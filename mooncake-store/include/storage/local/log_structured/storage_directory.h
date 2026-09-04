#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

struct StorageIdentity {
    uint64_t high{0};
    uint64_t low{0};

    bool operator==(const StorageIdentity&) const = default;
};

enum class StorageDirectoryError {
    kInvalidArgument,
    kIoError,
    kAlreadyMounted,
    kUnrecognizedFormat,
    kCorruptIdentity,
};

class StorageDirectory {
   public:
    static tl::expected<std::unique_ptr<StorageDirectory>,
                        StorageDirectoryError>
    Open(std::string root_path);

    ~StorageDirectory();

    StorageDirectory(const StorageDirectory&) = delete;
    StorageDirectory& operator=(const StorageDirectory&) = delete;

    const std::string& root_path() const { return root_path_; }
    const StorageIdentity& identity() const { return identity_; }

   private:
    StorageDirectory(std::string root_path, int lock_fd,
                     StorageIdentity identity);

    std::string root_path_;
    int lock_fd_{-1};
    StorageIdentity identity_;
};

}  // namespace mooncake::logstructured
