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

#ifndef STATUS_H
#define STATUS_H

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"

namespace mooncake {

class Status final {
 public:
  // The code of the status.
  enum class Code : uint16_t {
    kOk = 0,
    kInvalidArgument = 1,
    kTooManyRequests = 2,
    kAddressNotRegistered = 3,
    kBatchBusy = 4,
    kDeviceNotFound = 6,
    kAddressOverlapped = 7,
    kDns = 101,
    kSocket = 102,
    kMalformedJson = 103,
    kRejectHandshake = 104,
    kMetadata = 200,
    kEndpoint = 201,
    kContext = 202,
    kNuma = 300,
    kClock = 301,
    kMemory = 302,
    kNotImplmented = 999,
    kMaxCode
  };

  // Builds an OK Status.
  Status() = default;

  ~Status() { delete[] message_; }

  // Constructs a Status object containing a status code and message.
  // If 'code == Code::kOk', 'msg' is ignored and an object identical to an OK
  // status is constructed.
  Status(Code code, absl::string_view message);

  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s);
  Status& operator=(Status&& s);

  // Returns the stored status code.
  Code code() const { return code_; }

  // Return the error message (if any).
  absl::string_view message() const {
    if (message_) {
      return message_;
    } else {
      return absl::string_view();
    }
  }

  // Returns true if the Status is OK.
  ABSL_MUST_USE_RESULT bool ok() const { return Code::kOk == code_; }

  // Returns true iff the status indicates an InvalidArgument error.
  ABSL_MUST_USE_RESULT bool IsInvalidArgument() const {
    return Code::kInvalidArgument == code_;
  }

  // Returns true iff the status indicates a TooManyRequests error.
  ABSL_MUST_USE_RESULT bool IsTooManyRequests() const {
    return Code::kTooManyRequests == code_;
  }

  // Returns true iff the status indicates an AddressNotRegistered error.
  ABSL_MUST_USE_RESULT bool IsAddressNotRegistered() const {
    return Code::kAddressNotRegistered == code_;
  }

  // Returns true iff the status indicates a BatchBusy error.
  ABSL_MUST_USE_RESULT bool IsBatchBusy() const {
    return Code::kBatchBusy == code_;
  }

  // Returns true iff the status indicates an DeviceNotFound error.
  ABSL_MUST_USE_RESULT bool IsDeviceNotFound() const {
    return Code::kDeviceNotFound == code_;
  }

  // Returns true iff the status indicates an AddressOverlapped error.
  ABSL_MUST_USE_RESULT bool IsAddressOverlapped() const {
    return Code::kAddressOverlapped == code_;
  }

  // Returns true iff the status indicates a dns error.
  ABSL_MUST_USE_RESULT bool IsDns() const {
    return Code::kDns == code_;
  }

  // Returns true iff the status indicates an Socket error.
  ABSL_MUST_USE_RESULT bool IsSocket() const {
    return Code::kSocket == code_;
  }

  // Returns true iff the status indicates a MalformedJson error.
  ABSL_MUST_USE_RESULT bool IsMalformedJson() const {
    return Code::kMalformedJson == code_;
  }

  // Returns true iff the status indicates a RejectHandshake error.
  ABSL_MUST_USE_RESULT bool IsRejectHandshake() const {
    return Code::kRejectHandshake == code_;
  }

  // Returns true iff the status indicates a Metadata error.
  ABSL_MUST_USE_RESULT bool IsMetadata() const {
    return Code::kMetadata == code_;
  }

  // Returns true iff the status indicates an Endpoint error.
  ABSL_MUST_USE_RESULT bool IsEndpoint() const {
    return Code::kEndpoint == code_;
  }

  // Returns true iff the status indicates a Context error.
  ABSL_MUST_USE_RESULT bool IsContext() const {
    return Code::kContext == code_;
  }

  // Returns true iff the status indicates a Numa error.
  ABSL_MUST_USE_RESULT bool IsNuma() const {
    return Code::kNuma == code_;
  }

  // Returns true iff the status indicates a Clock error.
  ABSL_MUST_USE_RESULT bool IsClock() const {
    return Code::kClock == code_;
  }

  // Returns true iff the status indicates a Memory error.
  ABSL_MUST_USE_RESULT bool IsMemory() const {
    return Code::kMemory == code_;
  }

  // Returns true iff the status indicates a NotImplmented error.
  ABSL_MUST_USE_RESULT bool IsNotImplmented() const {
    return Code::kNotImplmented == code_;
  }

  // Return a combination of the error code name and message.
  std::string ToString() const;

  bool operator==(const Status& s) const;
  bool operator!=(const Status& s) const;

  // Return a status of an appropriate type.
  static Status OK() { return Status(); }
  static Status InvalidArgument(absl::string_view msg) {
    return Status(Code::kInvalidArgument, msg);
  }
  static Status TooManyRequests(absl::string_view msg) {
    return Status(Code::kTooManyRequests, msg);
  }
  static Status AddressNotRegistered(absl::string_view msg) {
    return Status(Code::kAddressNotRegistered, msg);
  }
  static Status BatchBusy(absl::string_view msg) {
    return Status(Code::kBatchBusy, msg);
  }
  static Status DeviceNotFound(absl::string_view msg) {
    return Status(Code::kDeviceNotFound, msg);
  }
  static Status AddressOverlapped(absl::string_view msg) {
    return Status(Code::kAddressOverlapped, msg);
  }
  static Status Dns(absl::string_view msg) {
    return Status(Code::kDns, msg);
  }
  static Status Socket(absl::string_view msg) {
    return Status(Code::kSocket, msg);
  }
  static Status MalformedJson(absl::string_view msg) {
    return Status(Code::kMalformedJson, msg);
  }
  static Status RejectHandshake(absl::string_view msg) {
    return Status(Code::kRejectHandshake, msg);
  }
  static Status Metadata(absl::string_view msg) {
    return Status(Code::kMetadata, msg);
  }
  static Status Endpoint(absl::string_view msg) {
    return Status(Code::kEndpoint, msg);
  }
  static Status Context(absl::string_view msg) {
    return Status(Code::kContext, msg);
  }
  static Status Numa(absl::string_view msg) {
    return Status(Code::kNuma, msg);
  }
  static Status Clock(absl::string_view msg) {
    return Status(Code::kClock, msg);
  }
  static Status Memory(absl::string_view msg) {
    return Status(Code::kMemory, msg);
  }
  static Status NotImplmented(absl::string_view msg) {
    return Status(Code::kNotImplmented, msg);
  }

  // Return a human-readable name of the 'code'.
  static std::string_view CodeToString(Code code);

 private:
  // Return a copy of the message 'msg'.
  static const char* CopyMessage(const char* msg);

  // The code of the status.
  Code code_ = Code::kOk;
  // The error message of the status. Refer to the Status definition in RocksDB,
  // we don't use 'std::string' type message but 'const char*' type one for the
  // performance considerations. A memory allocation in the std::string
  // construction could be avoid for the most cases that the Status is OK. And
  // the total size of 'message_' is only 8 bytes on a x86-64 platform, while
  // the size of a uninitialized strings with SSO (Small String Optimization)
  // will be 24 to 32 bytes big, excluding the dynamically allocated memory.
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

}  // namespace mooncake

#endif  // STATUS_H
