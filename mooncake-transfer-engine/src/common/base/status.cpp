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

#include "glog/logging.h"

namespace mooncake {

Status::Status(Status::Code code, std::string_view message) : code_(code) {
    if (code != Code::kOk) {
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
    } else {
        return std::string(CodeToString(code())) + ": " +
               std::string(message());
    }
}

std::string_view Status::CodeToString(Status::Code code) {
    switch (code) {
        case Code::kOk:
            return "OK";
        case Code::kInvalidArgument:
            return "InvalidArgument";
        case Code::kTooManyRequests:
            return "TooManyRequests";
        case Code::kAddressNotRegistered:
            return "AddressNotRegistered";
        case Code::kBatchBusy:
            return "BatchBusy";
        case Code::kDeviceNotFound:
            return "DeviceNotFound";
        case Code::kAddressOverlapped:
            return "AddressOverlapped";
        case Code::kDns:
            return "Dns";
        case Code::kSocket:
            return "Socket";
        case Code::kMalformedJson:
            return "MalformedJson";
        case Code::kRejectHandshake:
            return "RejectHandshake";
        case Code::kMetadata:
            return "Metadata";
        case Code::kEndpoint:
            return "Endpoint";
        case Code::kContext:
            return "Context";
        case Code::kNuma:
            return "Numa";
        case Code::kClock:
            return "Clock";
        case Code::kMemory:
            return "Memory";
        case Code::kNotImplemented:
            return "NotImplemented";
        case Code::kNotSupportedTransport:
            return "NotSupportedTransport";
        default:
            LOG(ERROR) << "Unknown code: " << static_cast<uint16_t>(code);
            return "UnknownCode";
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

std::ostream& operator<<(std::ostream& os, const Status& s) {
    return os << s.ToString();
}

}  // namespace mooncake
