// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SERIALIZATION_H_
#define SERIALIZATION_H_

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Convert a hexadecimal character to its integer value
 *
 * @param c Hexadecimal character ('0'-'9', 'A'-'F', 'a'-'f')
 * @return Integer value (0-15)
 * @throws std::invalid_argument if character is not a valid hex digit
 */
inline int hexCharToValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    throw std::invalid_argument("Invalid hexadecimal character");
}

/**
 * @brief Serialize binary data to hexadecimal string representation
 *
 * This function converts binary data (e.g., IPC memory handles) into a
 * hexadecimal string for transmission or storage.
 *
 * @param data Pointer to binary data
 * @param length Length of data in bytes
 * @return Hexadecimal string representation (2 characters per byte)
 * @throws std::invalid_argument if data pointer is null
 */
inline std::string serializeBinaryData(const void *data, size_t length) {
    if (!data) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    std::string hexString;
    hexString.reserve(length * 2);

    const auto *byteData = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < length; ++i) {
        hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
        hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
    }

    return hexString;
}

/**
 * @brief Deserialize hexadecimal string back to binary data
 *
 * This function converts a hexadecimal string back into binary data,
 * typically for reconstructing IPC memory handles.
 *
 * @param hexString Hexadecimal string to deserialize
 * @param buffer Output buffer to store binary data
 * @throws std::invalid_argument if input string length is not even
 */
inline void deserializeBinaryData(const std::string &hexString,
                                  std::vector<unsigned char> &buffer) {
    if (hexString.length() % 2 != 0) {
        throw std::invalid_argument("Input string length must be even");
    }

    buffer.clear();
    buffer.reserve(hexString.length() / 2);

    for (size_t i = 0; i < hexString.length(); i += 2) {
        int high = hexCharToValue(hexString[i]);
        int low = hexCharToValue(hexString[i + 1]);
        buffer.push_back(static_cast<unsigned char>((high << 4) | low));
    }
}

}  // namespace mooncake

#endif  // SERIALIZATION_H_
