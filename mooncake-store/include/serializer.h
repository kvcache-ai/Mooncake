#pragma once

#include <memory>
#include <string>
#include <cstring>
#include <cstdint>
#include <glog/logging.h>

#include "offset_allocator/offset_allocator.hpp"
#include "types.h"

namespace mooncake {

class SerializeSizeCounter {
   public:
    SerializeSizeCounter() = default;
    ~SerializeSizeCounter() = default;

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
    size_t get_size() const { return size_; }
    void set_error(const char* error) {
        has_error_ = true;
        error_ = error;
    }
    bool has_error() const { return has_error_; }
    const std::string& get_error() const { return error_; }

   private:
    size_t size_{0};
    bool has_error_{false};
    std::string error_;
};

class SerializeWriter {
     public:
     SerializeWriter(void* buffer, const size_t size) : buffer_(buffer), size_(size), offset_(0), has_error_(false) {}
     ~SerializeWriter() = default;
 
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
     void set_error(const char* error) {
         has_error_ = true;
         error_ = error;
     }
     bool has_error() const { return has_error_; }
     const std::string& get_error() const { return error_; }
     bool is_full() const { return offset_ >= size_; }
 
   private:
    void* buffer_;
    size_t size_;
    size_t offset_;
    bool has_error_;
    std::string error_;
};

class SerializerReader {
   public:
    SerializerReader(const void* buffer, const size_t size) : buffer_(buffer), size_(size), offset_(0) {}
    ~SerializerReader() = default;

    void read(void* data, const size_t data_size) {
        if (offset_ + data_size > size_) {
            throw std::runtime_error("buffer_overflow");
        }
        std::memcpy(data, static_cast<const uint8_t*>(buffer_) + offset_, data_size);
        offset_ += data_size;
    }
    bool is_empty() const { return offset_ == size_; }

   private:
    const void* buffer_;
    size_t size_;
    size_t offset_;
};

template <typename T>
[[nodiscard]] ErrorCode serialize_to(std::shared_ptr<T>& target,
                                     std::vector<SerializedByte>& buffer) {
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
    if (writer.has_error()) {
        LOG(ERROR) << "Serializing failed, error=" << writer.get_error();
        return ErrorCode::INTERNAL_ERROR;
    }
    if (!writer.is_full()) {
        LOG(ERROR) << "Serializing failed, error=wrong_data_size";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

template <typename T>
[[nodiscard]] std::shared_ptr<T> deserialize_from(
    const std::vector<SerializedByte>& buffer) {
    try {
        SerializerReader reader(buffer.data(), buffer.size());
        auto ret = T::deserialize_from(reader);
        if (ret == nullptr) {
            LOG(ERROR) << "Deserializing failed, error=return_nullptr";
            return nullptr;
        }
        if (!reader.is_empty()) {
            LOG(ERROR) << "Deserializing failed, error=wrong_data_size";
            return nullptr;
        }
        return ret;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Deserializing failed, error=" << e.what();
        return nullptr;
    }
}

}  // namespace mooncake