#include "utils/file_util.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>

namespace mooncake {
namespace {

constexpr std::size_t kDefaultChunkSize = 64ull * 1024 * 1024;  // 64 MB

tl::expected<void, std::string> PrepareOutputFile(const std::string &file_path,
                                                  std::ofstream &file_stream) {
    auto dir_result = FileUtil::EnsureDirExists(
        std::filesystem::path(file_path).parent_path().string());
    if (!dir_result) {
        return tl::make_unexpected(dir_result.error());
    }

    file_stream.open(file_path, std::ios::binary | std::ios::trunc);
    if (!file_stream.is_open()) {
        return tl::make_unexpected("Failed to open file for writing: " +
                                   file_path);
    }

    return {};
}

tl::expected<void, std::string> WriteChunks(std::ofstream &file,
                                            const char *data, std::size_t size,
                                            const std::string &file_path) {
    constexpr std::streamsize kStreamMax =
        std::numeric_limits<std::streamsize>::max();  // usually large
                                                      // (LLONG_MAX)
    const std::size_t chunk_size = static_cast<std::size_t>(
        std::min<std::streamsize>(kDefaultChunkSize, kStreamMax));

    std::size_t offset = 0;
    while (offset < size) {
        const std::size_t remaining = size - offset;
        const std::size_t current_chunk = std::min(remaining, chunk_size);
        file.write(data + offset, static_cast<std::streamsize>(current_chunk));
        if (!file) {
            return tl::make_unexpected("Failed while writing to file: " +
                                       file_path);
        }
        offset += current_chunk;
    }
    return {};
}

}  // namespace

tl::expected<void, std::string> FileUtil::SaveStringToFile(
    const std::string &content, const std::string &file_path) {
    try {
        std::ofstream file;
        if (auto status = PrepareOutputFile(file_path, file); !status) {
            return status;
        }

        if (auto status = WriteChunks(file, content.data(),
                                      static_cast<std::size_t>(content.size()),
                                      file_path);
            !status) {
            return status;
        }

        file.close();
        return {};
    } catch (const std::exception &e) {
        return tl::make_unexpected(
            "Exception occurred while saving string to file: " + file_path +
            ", error: " + e.what());
    }
}

tl::expected<void, std::string> FileUtil::SaveBinaryToFile(
    const std::vector<uint8_t> &data, const std::string &file_path) {
    try {
        std::ofstream file;
        if (auto status = PrepareOutputFile(file_path, file); !status) {
            return status;
        }

        const char *buffer = reinterpret_cast<const char *>(data.data());
        if (auto status = WriteChunks(
                file, buffer, static_cast<std::size_t>(data.size()), file_path);
            !status) {
            return status;
        }

        file.close();
        return {};
    } catch (const std::exception &e) {
        return tl::make_unexpected(
            "Exception occurred while saving binary data to file: " +
            file_path + ", error: " + e.what());
    }
}

tl::expected<void, std::string> FileUtil::EnsureDirExists(
    const std::string &dir_path) {
    try {
        if (dir_path.empty()) {
            return {};  // Current directory, no need to create
        }

        std::filesystem::path dir(dir_path);
        if (std::filesystem::exists(dir)) {
            if (std::filesystem::is_directory(dir)) {
                return {};  // Directory already exists
            } else {
                return tl::make_unexpected(
                    "Path exists but is not a directory: " + dir_path);
            }
        }

        if (!std::filesystem::create_directories(dir)) {
            return tl::make_unexpected("Failed to create directory: " +
                                       dir_path);
        }

        return {};  // Success
    } catch (const std::exception &e) {
        return tl::make_unexpected(
            "Exception occurred while ensuring directory exists: " + dir_path +
            ", error: " + e.what());
    }
}
}  // namespace mooncake
