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
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kUnknown = 6,
    kInternalError = 7,
    kAlreadyExists = 8,
    kFailedPrecondition = 9,
    kBusy = 10,
    kTimedOut = 11,
    kUnavailable = 12,
    kShutdown = 13,
    kMaxCode
  };

  // The sub code of the status.
  enum class SubCode : uint16_t {
    kNone = 0,
    kMetaStoreFailure = 1,
    kRpcConnTimedOut = 2,
    kSegmentIdMismatch = 3,
    kMaxSubCode
  };

  // Builds an OK Status.
  Status() = default;

  ~Status() { delete[] message_; }

  // Constructs a Status object containing a status code and message.
  // If 'code == Code::kOk', 'msg' is ignored and an object identical to an OK
  // status is constructed.
  Status(Code code, absl::string_view message);
  // Constructs a Status object containing a status code, a subcode and a
  // message. If 'code == Code::kOk', 'subcode' and 'message' are ignored and
  // an object identical to an OK status is constructed.
  Status(Code code, SubCode subcode, absl::string_view message);

  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s);
  Status& operator=(Status&& s);

  // Returns the stored status code.
  Code code() const { return code_; }

  // Returns the stored status sub code.
  SubCode subcode() const { return subcode_; }

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

  // Returns true if the subcode of the status is set.
  ABSL_MUST_USE_RESULT bool HasSubCode() const {
    return SubCode::kNone != subcode_;
  }

  // Returns true iff the status indicates a NotFound error.
  ABSL_MUST_USE_RESULT bool IsNotFound() const {
    return Code::kNotFound == code_;
  }

  // Returns true iff the status indicates a Corruption error.
  ABSL_MUST_USE_RESULT bool IsCorruption() const {
    return Code::kCorruption == code_;
  }

  // Returns true iff the status indicates a NotSupported error.
  ABSL_MUST_USE_RESULT bool IsNotSupported() const {
    return Code::kNotSupported == code_;
  }

  // Returns true iff the status indicates an InvalidArgument error.
  ABSL_MUST_USE_RESULT bool IsInvalidArgument() const {
    return Code::kInvalidArgument == code_;
  }

  // Returns true iff the status indicates an IOError.
  ABSL_MUST_USE_RESULT bool IsIOError() const {
    return Code::kIOError == code_;
  }

  // Returns true iff the status indicates an Unknown error.
  ABSL_MUST_USE_RESULT bool IsUnknown() const {
    return Code::kUnknown == code_;
  }

  // Returns true iff the status indicates an internal error.
  ABSL_MUST_USE_RESULT bool IsInternalError() const {
    return Code::kInternalError == code_;
  }

  // Returns true iff the status indicates an AlreadyExists error.
  ABSL_MUST_USE_RESULT bool IsAlreadyExists() const {
    return Code::kAlreadyExists == code_;
  }

  // Returns true iff the status indicates a FailedPrecondition error.
  ABSL_MUST_USE_RESULT bool IsFailedPrecondition() const {
    return Code::kFailedPrecondition == code_;
  }

  // Returns true iff the status indicates a Busy error.
  ABSL_MUST_USE_RESULT bool IsBusy() const { return Code::kBusy == code_; }

  // Returns true iff the status indicates a TimedOut error.
  ABSL_MUST_USE_RESULT bool IsTimedOut() const {
    return Code::kTimedOut == code_;
  }

  // Returns true iff the status indicates a Unavailable error.
  ABSL_MUST_USE_RESULT bool IsUnavailable() const {
    return Code::kUnavailable == code_;
  }

  // Returns true iff the status indicates a Shutdown error.
  ABSL_MUST_USE_RESULT bool IsShutdown() const {
    return Code::kShutdown == code_;
  }

  // Returns true iff the status indicates a MetaStoreFailure error.
  ABSL_MUST_USE_RESULT bool IsMetaStoreFailure() const {
    return IsInternalError() && SubCode::kMetaStoreFailure == subcode_;
  }

  // Returns true iff the status indicates a RpcConnTimedOut error.
  ABSL_MUST_USE_RESULT bool IsRpcConnTimedOut() const {
    return IsInternalError() && SubCode::kRpcConnTimedOut == subcode_;
  }

  // Returns true iff the status indicates a SegmentIdMismatch error.
  ABSL_MUST_USE_RESULT bool IsSegmentIdMismatch() const {
    return IsFailedPrecondition() && SubCode::kSegmentIdMismatch == subcode_;
  }

  // Return a combination of the error code name and message.
  std::string ToString() const;

  bool operator==(const Status& s) const;
  bool operator!=(const Status& s) const;

  // Return a status of an appropriate type.
  static Status OK() { return Status(); }
  static Status NotFound(absl::string_view msg) {
    return Status(Code::kNotFound, msg);
  }
  static Status Corruption(absl::string_view msg) {
    return Status(Code::kCorruption, msg);
  }
  static Status NotSupported(absl::string_view msg) {
    return Status(Code::kNotSupported, msg);
  }
  static Status InvalidArgument(absl::string_view msg) {
    return Status(Code::kInvalidArgument, msg);
  }
  static Status IOError(absl::string_view msg) {
    return Status(Code::kIOError, msg);
  }
  static Status Unknown(absl::string_view msg) {
    return Status(Code::kUnknown, msg);
  }
  static Status InternalError(absl::string_view msg) {
    return Status(Code::kInternalError, msg);
  }
  static Status AlreadyExists(absl::string_view msg) {
    return Status(Code::kAlreadyExists, msg);
  }
  static Status FailedPrecondition(absl::string_view msg) {
    return Status(Code::kFailedPrecondition, msg);
  }
  static Status Busy(absl::string_view msg) { return Status(Code::kBusy, msg); }
  static Status TimedOut(absl::string_view msg) {
    return Status(Code::kTimedOut, msg);
  }
  static Status Unavailable(absl::string_view msg) {
    return Status(Code::kUnavailable, msg);
  }
  static Status Shutdown(absl::string_view msg) {
    return Status(Code::kShutdown, msg);
  }
  // Sub status.
  static Status MetaStoreFailure(absl::string_view msg) {
    return Status(Code::kInternalError, SubCode::kMetaStoreFailure, msg);
  }
  static Status RpcConnTimedOut(absl::string_view msg) {
    return Status(Code::kInternalError, SubCode::kRpcConnTimedOut, msg);
  }
  static Status SegmentIdMismatch(absl::string_view msg) {
    return Status(Code::kFailedPrecondition, SubCode::kSegmentIdMismatch, msg);
  }

  // Return a human-readable name of the 'code'.
  static std::string_view CodeToString(Code code);
  // Return a human-readable name of the 'subcode'.
  static std::string_view SubCodeToString(SubCode subcode);

 private:
  // Return a copy of the message 'msg'.
  static const char* CopyMessage(const char* msg);

  // The code of the status.
  Code code_ = Code::kOk;
  // The sub code of the status.
  SubCode subcode_ = SubCode::kNone;
  // The error message of the status. Refer to the Status definition in RocksDB,
  // we don't use 'std::string' type message but 'const char*' type one for the
  // performance considerations. A memory allocation in the std::string
  // construction could be avoid for the most cases that the Status is OK. And
  // the total size of 'message_' is only 8 bytes on a x86-64 platform, while
  // the size of a uninitialized strings with SSO (Small String Optimization)
  // will be 24 to 32 bytes big, excluding the dynamically allocated memory.
  const char* message_ = nullptr;
};

inline Status::Status(const Status& s) : code_(s.code_), subcode_(s.subcode_) {
  message_ = (s.message_ == nullptr) ? nullptr : CopyMessage(s.message_);
}

inline Status& Status::operator=(const Status& s) {
  if (this != &s) {
    code_ = s.code_;
    subcode_ = s.subcode_;
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
    subcode_ = std::move(s.subcode_);
    s.subcode_ = SubCode::kNone;
    delete[] message_;
    message_ = nullptr;
    std::swap(message_, s.message_);
  }
  return *this;
}

// Prints a human-readable representation name of the 'code' to 'os'.
std::ostream& operator<<(std::ostream& os, Status::Code code);

// Prints a human-readable representation name of the 'subcode' to 'os'.
std::ostream& operator<<(std::ostream& os, Status::SubCode subcode);

// Prints a human-readable representation of 's' to 'os'.
std::ostream& operator<<(std::ostream& os, const Status& s);

}  // namespace mooncake

#endif  // STATUS_H
