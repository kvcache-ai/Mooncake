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
// https://github.com/facebook/rocksdb/blob/main/util/status.cc

#include "common/base/status.h"

#include <cstring>

#include "absl/strings/str_cat.h"
#include "glog/logging.h"

namespace mooncake {

Status::Status(Status::Code code, absl::string_view message)
    : Status(code, /*subcode=*/SubCode::kNone, message) {}

Status::Status(Status::Code code, SubCode subcode, absl::string_view message)
    : code_(code) {
  if (code != Code::kOk) {
    subcode_ = subcode;
    // Only store the message when it is not empty.
    if (!message.empty()) {
      const size_t len = message.size();
      // +1 for null terminator
      char* const result = new char[len + 1];
      memcpy(result, message.data(), len);
      result[len] = '\0';
      message_ = result;
    }
  }
}

std::string Status::ToString() const {
  if (ok()) {
    return "OK";
  } else if (HasSubCode()) {
    return absl::StrCat(CodeToString(code()), "/", SubCodeToString(subcode()),
                        ": ", message());
  } else {
    return absl::StrCat(CodeToString(code()), ": ", message());
  }
}

std::string_view Status::CodeToString(Status::Code code) {
  switch (code) {
    case Code::kOk:
      return "OK";
    case Code::kNotFound:
      return "NotFound";
    case Code::kCorruption:
      return "Corruption";
    case Code::kNotSupported:
      return "NotSupported";
    case Code::kInvalidArgument:
      return "InvalidArgument";
    case Code::kIOError:
      return "IOError";
    case Code::kUnknown:
      return "Unknown";
    case Code::kInternalError:
      return "InternalError";
    case Code::kAlreadyExists:
      return "AlreadyExists";
    case Code::kFailedPrecondition:
      return "FailedPrecondition";
    case Code::kBusy:
      return "Busy";
    case Code::kTimedOut:
      return "TimedOut";
    case Code::kUnavailable:
      return "Unavailable";
    case Code::kShutdown:
      return "Shutdown";
    default:
      LOG(ERROR) << "Unknown code: " << static_cast<uint16_t>(code);
      return absl::StrCat(code);
  }
}

std::string_view Status::SubCodeToString(Status::SubCode subcode) {
  switch (subcode) {
    case SubCode::kNone:
      return "None";
    case SubCode::kMetaStoreFailure:
      return "DataStoreFailure";
    case SubCode::kRpcConnTimedOut:
      return "RpcConnTimedOut";
    case SubCode::kSegmentIdMismatch:
      return "SegmentIdMismatch";
    default:
      LOG(ERROR) << "Unknown subcode: " << static_cast<uint16_t>(subcode);
      return absl::StrCat(subcode);
  }
}

const char* Status::CopyMessage(const char* msg) {
  // +1 for the null terminator
  const size_t len = std::strlen(msg) + 1;
  return std::strncpy(new char[len], msg, len);
}

bool Status::operator==(const Status& s) const {
  // Compare the code.
  if (code_ != s.code_) {
    return false;
  }
  // Compare the subcode.
  if (subcode_ != s.subcode_) {
    return false;
  }
  // Compare the message content.
  if (message_ == nullptr && s.message_ == nullptr) {
    return true;
  }
  if (message_ != nullptr && s.message_ != nullptr) {
    return strcmp(message_, s.message_) == 0;
  }
  return false;
}

bool Status::operator!=(const Status& s) const { return !(*this == s); }

std::ostream& operator<<(std::ostream& os, Status::Code code) {
  return os << Status::CodeToString(code);
}

std::ostream& operator<<(std::ostream& os, Status::SubCode subcode) {
  return os << Status::SubCodeToString(subcode);
}

std::ostream& operator<<(std::ostream& os, const Status& s) {
  return os << s.ToString();
}

}  // namespace mooncake
