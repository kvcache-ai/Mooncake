#include "common/file_util.h"

#include <filesystem>
#include <functional>
#include <string_view>

namespace mooncake {
namespace {

std::string SanitizeKey(const std::string& key) {
    // Set of invalid filesystem characters to be replaced
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());

    for (char c : key) {
        // Replace invalid characters with underscore
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c);
    }
    return sanitized_key;
}

}  // namespace

std::string FileUtil::ResolvePathFromKey(const std::string& key,
                                       const std::string& root_dir,
                                       const std::string& fsdir) {
    const size_t hash = std::hash<std::string>{}(key);
    const char dir1 = static_cast<char>('a' + (hash & 0x0F));
    const char dir2 = static_cast<char>('a' + ((hash >> 4) & 0x0F));
    const std::filesystem::path dir_path =
        std::filesystem::path(std::string(1, dir1)) / std::string(1, dir2);
    return (std::filesystem::path(root_dir) / fsdir / dir_path /
            SanitizeKey(key))
        .lexically_normal()
        .string();
}

}  // namespace mooncake
