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
//
// The design of this code is adapted from the RocksDB project with some
// modifications.
// https://github.com/facebook/rocksdb/blob/main/include/rocksdb/status.h

#ifndef TENT_STATUS_H
#define TENT_STATUS_H

#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#define TYPE_CHECK(err)                                                  \
    [[nodiscard]] bool Is##err() const { return Code::k##err == code_; } \
    static Status err(std::string_view msg) {                            \
        return Status(Code::k##err, msg);                                \
    }

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define LOC_MARK "\n    Raised at " __FILE__ ":" TOSTRING(__LINE__)

#define CHECK_STATUS(call)               \
    do {                                 \
        Status status = call;            \
        if (!status.ok()) return status; \
    } while (0)

#ifdef USE_CUDA
#define CHECK_CUDA(call)                                                      \
    do {                                                                      \
        auto err = call;                                                      \
        if (err != cudaSuccess)                                               \
            return Status::InternalError(std::string(#call) + ": " +          \
                                         cudaGetErrorString(err) + LOC_MARK); \
    } while (0)

#define CHECK_CU(call)                                                        \
    do {                                                                      \
        auto err = call;                                                      \
        if (err != CUDA_SUCCESS) {                                            \
            return Status::InternalError(std::string(#call) + ": cuResult " + \
                                         std::to_string(err) + LOC_MARK);     \
        }                                                                     \
    } while (0)
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
#define CHECK_ASCEND(call)                                                \
    do {                                                                  \
        aclError err = call;                                              \
        if (err != ACL_SUCCESS)                                           \
            return Status::InternalError(std::string(#call) + " ret : " + \
                                         std::to_string(err) + LOC_MARK); \
    } while (0)
#endif

namespace mooncake {
namespace tent {
class Status final {
   public:
    // The code of the status.
    enum class Code : uint16_t {
        kOk = 0,

        kInvalidArgument = 1,
        kTooManyRequests = 2,
        kAddressNotRegistered = 3,
        kDeviceNotFound = 4,
        kInvalidEntry = 5,
        kInvalidMetadataType = 6,

        kRdmaError = 100,
        kCudaError = 101,
        kMetadataError = 131,
        kRpcServiceError = 132,
        kMalformedJson = 133,
        kInternalError = 199,

        kNotImplemented = 200,
        kMaxCode
    };

    // Builds an OK Status.
    Status() = default;

    ~Status() { delete[] message_; }

    // Constructs a Status object containing a status code and message.
    // If 'code == Code::kOk', 'msg' is ignored and an object identical to
    // an OK status is constructed.
    Status(Code code, std::string_view message);

    Status(const Status& s);
    Status& operator=(const Status& s);
    Status(Status&& s);
    Status& operator=(Status&& s);

    bool operator==(const Status& s) const;
    bool operator!=(const Status& s) const;

    // Returns the stored status code.
    Code code() const { return code_; }

    // Return the error message (if any).
    std::string_view message() const {
        if (message_) {
            return message_;
        } else {
            return std::string_view();
        }
    }

    // Returns true if the Status is OK.
    [[nodiscard]] bool ok() const { return Code::kOk == code_; }

    // Return a status of an appropriate type.
    static Status OK() { return Status(); }

    TYPE_CHECK(InvalidArgument);
    TYPE_CHECK(TooManyRequests);
    TYPE_CHECK(AddressNotRegistered);
    TYPE_CHECK(DeviceNotFound);
    TYPE_CHECK(InvalidEntry);
    TYPE_CHECK(InvalidMetadataType);

    TYPE_CHECK(RdmaError);
    TYPE_CHECK(CudaError);
    TYPE_CHECK(MetadataError);
    TYPE_CHECK(RpcServiceError);
    TYPE_CHECK(MalformedJson);
    TYPE_CHECK(InternalError);

    TYPE_CHECK(NotImplemented);

    // Return a combination of the error code name and message.
    std::string ToString() const;

    // Return a human-readable name of the 'code'.
    static std::string_view CodeToString(Code code);

   private:
    // Return a copy of the message 'msg'.
    static const char* CopyMessage(const char* msg);

    // The code of the status.
    Code code_ = Code::kOk;
    // The error message of the status. Refer to the Status definition in
    // RocksDB, we don't use 'std::string' type message but 'const char*'
    // type one for the performance considerations. A memory allocation in
    // the std::string construction could be avoid for the most cases that
    // the Status is OK. And the total size of 'message_' is only 8 bytes on
    // a x86-64 platform, while the size of a uninitialized strings with SSO
    // (Small String Optimization) will be 24 to 32 bytes big, excluding the
    // dynamically allocated memory.
    const char* message_ = nullptr;
};

inline Status::Status(const Status& s) : code_(s.code_) {
    message_ = (s.message_ == nullptr) ? nullptr : CopyMessage(s.message_);
}

inline Status& Status::operator=(const Status& s) {
    if (this != &s) {
        code_ = s.code_;
        delete[] message_;
        message_ = (s.message_ == nullptr) ? nullptr : CopyMessage(s.message_);
    }
    return *this;
}

inline Status::Status(Status&& s) : Status() { *this = std::move(s); }

inline Status& Status::operator=(Status&& s) {
    if (this != &s) {
        code_ = std::move(s.code_);
        s.code_ = Code::kOk;
        delete[] message_;
        message_ = nullptr;
        std::swap(message_, s.message_);
    }
    return *this;
}

// Prints a human-readable representation name of the 'code' to 'os'.
std::ostream& operator<<(std::ostream& os, Status::Code code);

// Prints a human-readable representation of 's' to 'os'.
std::ostream& operator<<(std::ostream& os, const Status& s);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_STATUS_H
