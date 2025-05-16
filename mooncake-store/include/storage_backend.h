#pragma once

#include <vector>
#include <string>
#include "types.h"

namespace mooncake {

static const std::string kDefaultStorageRootPath = "";
class StorageBackend {
 public:
  virtual ~StorageBackend() = default;

  virtual ErrorCode Write(const ObjectKey& key, const std::vector<Slice>& slices) = 0;
  virtual ErrorCode Read(const ObjectKey& key, std::vector<Slice>& slices) = 0;
};

}  // namespace mooncake
