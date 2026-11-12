#pragma once

#include <string>

namespace mooncake {

class FileUtil {
   public:
    static std::string ResolvePathFromKey(const std::string& key,
                                          const std::string& root_dir,
                                          const std::string& fsdir);
};

}  // namespace mooncake
