#pragma once

#include <cstdint>
#include <string_view>

namespace mooncake {

/**
 * @brief 类型转换工具类，提供字符串与基本类型之间的转换功能
 * 可用于存放各类字符串、数据类型转换操作
 */
class TypeUtil {
 public:
  /**
   * @brief 将字符串解析为布尔值
   * @param value 要解析的字符串（支持 "true"/"false", "1"/"0", "yes"/"no"，不区分大小写）
   * @param out 输出参数，解析成功时存储解析结果
   * @return 解析成功返回 true，否则返回 false
   */
  static bool ParseBool(std::string_view value, bool& out);

  /**
   * @brief 将字符串解析为 uint64_t
   * @param value 要解析的字符串
   * @param out 输出参数，解析成功时存储解析结果
   * @return 解析成功返回 true，否则返回 false
   */
  static bool ParseUint64(std::string_view value, uint64_t& out);

  /**
   * @brief 将字符串解析为 int64_t
   * @param value 要解析的字符串
   * @param out 输出参数，解析成功时存储解析结果
   * @return 解析成功返回 true，否则返回 false
   */
  static bool ParseInt64(std::string_view value, int64_t& out);
};

}  // namespace mooncake

