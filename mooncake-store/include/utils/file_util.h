#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <filesystem>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

namespace fs = std::filesystem;

class FileUtil {
   public:
    /**
     * @brief Save string content to file
     * @param content Content to save
     * @param file_path File path
     * @return tl::expected<void, std::string> Success or failure result
     */
    static tl::expected<void, std::string> SaveStringToFile(
        const std::string& content, const std::string& file_path);

    /**
     * @brief Save binary data to file
     * @param data Binary data to save
     * @param file_path File path
     * @return On success returns empty value, on failure returns error message
     */
    static tl::expected<void, std::string> SaveBinaryToFile(
        const std::vector<uint8_t>& data, const std::string& file_path);

    /**
     * @brief Ensure directory exists, create if not
     * @param dir_path Directory path
     * @return Returns true if creation succeeded or directory already exists,
     * false otherwise
     */
    static tl::expected<void, std::string> EnsureDirExists(
        const std::string& dir_path);
};
}  // namespace mooncake
