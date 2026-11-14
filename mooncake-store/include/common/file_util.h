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
    // Snapshot birth times before sorting, oldest known first. Failed metadata
    // queries and filesystems without birth times sort last, by path. Entries
    // are retained so callers can separately check whether they are readable.
    static void SortByCreationTime(std::vector<fs::directory_entry>& files);

    static std::string ResolvePathFromKey(const std::string& key,
                                          const std::string& root_dir,
                                          const std::string& fsdir);

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
