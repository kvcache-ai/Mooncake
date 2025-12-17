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
     * @brief 保存字符串内容到文件
     * @param content 要保存的内容
     * @param file_path 文件路径
     * @return tl::expected<void, std::string> 成功或失败的结果
     */
    static tl::expected<void, std::string> SaveStringToFile(const std::string& content,
                                                            const std::string& file_path);

    /**
     * @brief 将二进制数据保存到文件
     * @param data 要保存的二进制数据
     * @param file_path 文件路径
     * @return 成功返回空值，失败返回错误信息
     */
    static tl::expected<void, std::string> SaveBinaryToFile(const std::vector<uint8_t>& data,
                                                            const std::string& file_path);

    /**
     * @brief 确保目录存在，如果不存在则创建
     * @param dir_path 目录路径
     * @return 创建成功或目录已存在返回true，否则返回false
     */
    static tl::expected<void, std::string> EnsureDirExists(const std::string& dir_path);
};
}  // namespace mooncake
