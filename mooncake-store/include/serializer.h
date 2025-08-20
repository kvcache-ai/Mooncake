#pragma once

#include <memory>
#include <string>
#include <cstring>
#include <cstdint>
#include <glog/logging.h>

#include "types.h"

namespace mooncake {

/**
 * @brief Serialization Framework Usage Guide
 *
 * To implement serialization for your class, you need to provide two methods:
 *
 * 1. **serialize_to()** - Template method that works with both
 * SerializeSizeCounter and SerializeWriter
 * 2. **deserialize_from()** - Static template method that reconstructs the
 * object
 *
 * Example implementation:
 * @code
 * class MyClass {
 * public:
 *     // Serialization method (works with both counter and writer)
 *     template <typename T>
 *     void serialize_to(T& serializer) const {
 *         serializer.write(&member1, sizeof(member1));
 *         serializer.write(&member2, sizeof(member2));
 *         // ... serialize other members
 *     }
 *
 *     // Deserialization method
 *     template <typename T>
 *     static std::shared_ptr<MyClass> deserialize_from(T& serializer) {
 *         try {
 *             auto obj = std::make_shared<MyClass>();
 *             serializer.read(&obj->member1, sizeof(obj->member1));
 *             serializer.read(&obj->member2, sizeof(obj->member2));
 *             // ... deserialize other members
 *             return obj;
 *         } catch (const std::exception& e) {
 *             return nullptr;
 *         }
 *     }
 *
 * private:
 *     int member1;
 *     double member2;
 * };
 * @endcode
 *
 * Usage:
 * @code
 * MyClass obj;
 * std::vector<SerializedByte> buffer;
 * serialize_to(obj, buffer);                    // Serialize
 * auto restored = deserialize_from<MyClass>(buffer);  // Deserialize
 * @endcode
 */

/**
 * @brief A utility class for calculating the size of serialized data without
 * actually writing it.
 *
 * This class is used in the first pass of serialization to determine the exact
 * buffer size needed for storing the serialized data. It implements the same
 * interface as SerializeWriter but only accumulates the size without performing
 * any actual memory writes.
 *
 */
class SerializeSizeCounter {
   public:
    SerializeSizeCounter() = default;

    ~SerializeSizeCounter() = default;

    /**
     * @brief Simulates writing data by adding its size to the total count
     *
     * This method doesn't actually write data but accumulates the size that
     * would be written. It's used to calculate the total buffer size needed for
     * serialization.
     *
     * @param data Pointer to the data that would be written
     * @param data_size Size of the data in bytes
     */
    void write(const void* data, const size_t data_size) {
        if (has_error_) {
            return;
        }
        if (data == nullptr) {
            set_error("data is nullptr");
            return;
        }
        size_ += data_size;
    }

    /**
     * @brief Returns the total accumulated size of all data that would be
     * serialized
     *
     * @return Total size in bytes needed for the serialized data
     */
    size_t get_size() const { return size_; }

    /**
     * @brief Sets an error state with a descriptive message
     *
     * @param error Error message describing what went wrong
     */
    void set_error(const char* error) {
        has_error_ = true;
        error_ = error;
    }

    /**
     * @brief Checks if an error occurred during size calculation
     *
     * @return true if an error occurred, false otherwise
     */
    bool has_error() const { return has_error_; }

    /**
     * @brief Returns the error message if an error occurred
     *
     * @return Error message string, empty if no error occurred
     */
    const std::string& get_error() const { return error_; }

   private:
    size_t size_{0};  ///< Accumulated size of all data that would be serialized
    bool has_error_{false};  ///< Flag indicating if an error occurred during
                             ///< size calculation
    std::string error_;      ///< Error message describing what went wrong
};

/**
 * @brief A class for writing serialized data to a pre-allocated buffer.
 *
 * This class handles the actual writing of serialized data to memory. It
 implements the same interface as
 * SerializeSizeCounter but performs actual memory writes instead of just
 counting.
 *
 * The class is typically used in the second pass of serialization after
 * SerializeSizeCounter has determined the required buffer size.

 */
class SerializeWriter {
   public:
    /**
     * @brief Constructs a SerializeWriter with a target buffer
     *
     * @param buffer Pointer to the pre-allocated buffer where data will be
     * written
     * @param size Total size of the buffer in bytes
     */
    SerializeWriter(void* buffer, const size_t size)
        : buffer_(buffer), size_(size), offset_(0), has_error_(false) {}

    ~SerializeWriter() = default;

    /**
     * @brief Writes data to the buffer at the current offset position
     *
     * This method copies the specified data to the buffer starting at the
     * current offset.
     *
     * @param data Pointer to the data to be written
     * @param data_size Size of the data in bytes
     */
    void write(const void* data, const size_t data_size) {
        if (has_error_) {
            return;
        }
        if (data == nullptr) {
            set_error("null_pointer_data");
            return;
        }
        if (data_size + offset_ > size_) {
            set_error("buffer_overflow");
            return;
        }
        std::memcpy(static_cast<uint8_t*>(buffer_) + offset_, data, data_size);
        offset_ += data_size;
    }

    /**
     * @brief Sets an error state with a descriptive message
     *
     * Once an error is set, all subsequent write operations will be ignored
     * until the error is checked and handled by the caller.
     *
     * @param error Error message describing what went wrong
     */
    void set_error(const char* error) {
        has_error_ = true;
        error_ = error;
    }

    /**
     * @brief Checks if an error occurred during writing
     *
     * @return true if an error occurred, false otherwise
     */
    bool has_error() const { return has_error_; }

    /**
     * @brief Returns the error message if an error occurred
     *
     * @return Error message string, empty if no error occurred
     */
    const std::string& get_error() const { return error_; }

    /**
     * @brief Checks if the buffer has been completely filled
     *
     * This is useful for validation to ensure that the expected amount
     * of data was written to the buffer.
     *
     * @return true if the buffer is full (offset equals buffer size), false
     * otherwise
     */
    bool finish_write() const { return offset_ >= size_; }

   private:
    void* buffer_;    ///< Pointer to the target buffer for writing data
    size_t size_;     ///< Total size of the buffer in bytes
    size_t offset_;   ///< Current write position within the buffer
    bool has_error_;  ///< Flag indicating if an error occurred during writing
    std::string error_;  ///< Error message describing what went wrong
};

/**
 * @brief A class for reading serialized data from a buffer during
 * deserialization.
 *
 * This class handles the reading of serialized data from a memory buffer. It
 * maintains an internal offset to track the current read position and provides
 * bounds checking to prevent buffer overflows. It throws an exception during
 * the deserialization process if any error occurs.
 */
class SerializerReader {
   public:
    /**
     * @brief Constructs a SerializerReader with a source buffer
     * @param buffer Pointer to the buffer containing serialized data to be read
     * @param size Total size of the buffer in bytes
     */
    SerializerReader(const void* buffer, const size_t size)
        : buffer_(buffer), size_(size), offset_(0) {}

    ~SerializerReader() = default;

    /**
     * @brief Reads data from the buffer at the current offset position
     * @param data Pointer to the destination where data will be copied
     * @param data_size Size of the data to read in bytes
     * @throws std::runtime_error if the read operation would exceed buffer
     * bounds
     */
    void read(void* data, const size_t data_size) {
        if (offset_ + data_size > size_) {
            throw std::runtime_error("buffer_overflow");
        }
        std::memcpy(data, static_cast<const uint8_t*>(buffer_) + offset_,
                    data_size);
        offset_ += data_size;
    }

    /**
     * @brief Checks if all data in the buffer has been read
     *
     * This is useful for validation to ensure that the entire buffer was
     * consumed during deserialization, which helps detect data corruption or
     * incomplete deserialization.
     *
     * @return true if all data has been read (offset equals buffer size), false
     * otherwise
     */
    bool finish_read() const { return offset_ == size_; }

   private:
    const void*
        buffer_;   ///< Pointer to the source buffer containing serialized data
    size_t size_;  ///< Total size of the buffer in bytes
    size_t offset_;  ///< Current read position within the buffer
};

/**
 * @brief Serializes an object to a byte buffer using two-pass approach.
 * @tparam T Type that implements serialize_to(SerializeSizeCounter&) and
 * serialize_to(SerializeWriter&)
 * @param target Object to serialize (must not be null)
 * @param buffer Output buffer for serialized data
 * @return ErrorCode indicating success or failure
 */
template <typename T>
[[nodiscard]] ErrorCode serialize_to_internal(
    const T* target, std::vector<SerializedByte>& buffer) {
    if (target == nullptr) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Get the size of the serialized data
    SerializeSizeCounter counter;
    target->serialize_to(counter);
    if (counter.has_error()) {
        LOG(ERROR) << "Serializing failed, error=" << counter.get_error();
        return ErrorCode::INTERNAL_ERROR;
    }

    // Serialize the data to the buffer
    buffer.clear();
    buffer.resize(counter.get_size());
    SerializeWriter writer(buffer.data(), buffer.size());
    target->serialize_to(writer);

    // Check if the serialization failed
    if (writer.has_error()) {
        LOG(ERROR) << "Serializing failed, error=" << writer.get_error();
        return ErrorCode::INTERNAL_ERROR;
    }
    if (!writer.finish_write()) {
        LOG(ERROR) << "Serializing failed, error=wrong_data_size";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

template <typename T>
[[nodiscard]] ErrorCode serialize_to(const T& target,
                                     std::vector<SerializedByte>& buffer) {
    return serialize_to_internal(std::addressof(target), buffer);
}

template <typename T>
[[nodiscard]] ErrorCode serialize_to(const std::shared_ptr<T>& target,
                                     std::vector<SerializedByte>& buffer) {
    return serialize_to_internal(target.get(), buffer);
}

/**
 * @brief Deserializes an object from a byte buffer.
 * @tparam T Type that implements static deserialize_from(SerializerReader&)
 * @param buffer Buffer containing serialized data
 * @return Shared pointer to deserialized object, or nullptr on failure
 */
template <typename T>
[[nodiscard]] std::shared_ptr<T> deserialize_from(
    const std::vector<SerializedByte>& buffer) {
    try {
        // Deserialize the object
        SerializerReader reader(buffer.data(), buffer.size());
        auto ret = T::deserialize_from(reader);

        // Check if the deserialization failed
        if (ret == nullptr) {
            LOG(ERROR) << "Deserializing failed, error=return_nullptr";
            return nullptr;
        }
        if (!reader.finish_read()) {
            LOG(ERROR) << "Deserializing failed, error=wrong_data_size";
            return nullptr;
        }
        return ret;
    } catch (const std::exception& e) {
        // The deserialization method may throw an exception if it fails.
        LOG(ERROR) << "Deserializing failed, error=" << e.what();
        return nullptr;
    }
}

}  // namespace mooncake