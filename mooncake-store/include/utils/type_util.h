#pragma once

#include <cstdint>
#include <string_view>

namespace mooncake {

/**
 * @brief Type conversion utility class providing conversion between strings and
 * basic types Can be used for various string and data type conversion
 * operations
 */
class TypeUtil {
   public:
    /**
     * @brief Parse string to boolean
     * @param value String to parse (supports "true"/"false", "1"/"0",
     * "yes"/"no", case-insensitive)
     * @param out Output parameter, stores the result on success
     * @return Returns true on success, false otherwise
     */
    static bool ParseBool(std::string_view value, bool& out);

    /**
     * @brief Parse string to uint64_t
     * @param value String to parse
     * @param out Output parameter, stores the result on success
     * @return Returns true on success, false otherwise
     */
    static bool ParseUint64(std::string_view value, uint64_t& out);

    /**
     * @brief Parse string to int64_t
     * @param value String to parse
     * @param out Output parameter, stores the result on success
     * @return Returns true on success, false otherwise
     */
    static bool ParseInt64(std::string_view value, int64_t& out);
};

}  // namespace mooncake
