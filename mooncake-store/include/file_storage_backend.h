#pragma once

#include "storage_backend.h"
#include <string>
#include <vector>
#include <mutex>
#include "types.h"

namespace mooncake {

class FileStorageBackend : public StorageBackend {
 public:
  explicit FileStorageBackend(const std::string& root_dir);
  ErrorCode Write(const ObjectKey& key, const std::vector<Slice>& slices) override;
  ErrorCode Read(const ObjectKey& key, std::vector<Slice>& slices) override;

 private:
  std::string SanitizeKey(const ObjectKey& key) const;
  std::string ResolvePath(const ObjectKey& key) const;

  std::string root_dir_;
};

}  // namespace mooncake
