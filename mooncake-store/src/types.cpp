#include "types.h"

namespace mooncake {

const std::string& toString(ErrorCode errorCode) noexcept {
    static const std::unordered_map<ErrorCode, std::string> errorCodeMap = {
        {ErrorCode::OK, "OK"},
        {ErrorCode::BUFFER_OVERFLOW, "BUFFER_OVERFLOW"},
        {ErrorCode::SHARD_INDEX_OUT_OF_RANGE, "SHARD_INDEX_OUT_OF_RANGE"},
        {ErrorCode::AVAILABLE_SEGMENT_EMPTY, "AVAILABLE_SEGMENT_EMPTY"},
        {ErrorCode::NO_AVAILABLE_HANDLE, "NO_AVAILABLE_HANDLE"},
        {ErrorCode::INVALID_VERSION, "INVALID_VERSION"},
        {ErrorCode::INVALID_KEY, "INVALID_KEY"},
        {ErrorCode::WRITE_FAIL, "WRITE_FAIL"},
        {ErrorCode::INVALID_PARAMS, "INVALID_PARAMS"},
        {ErrorCode::INVALID_WRITE, "INVALID_WRITE"},
        {ErrorCode::INVALID_READ, "INVALID_READ"},
        {ErrorCode::INVALID_REPLICA, "INVALID_REPLICA"},
        {ErrorCode::TRANSFER_FAIL, "TRANSFER_FAIL"},
    };

    auto it = errorCodeMap.find(errorCode);
    static const std::string unknownError = "UNKNOWN_ERROR";
    return (it != errorCodeMap.end()) ? it->second : unknownError;
}

int32_t toInt(ErrorCode errorCode) noexcept {
    return static_cast<int32_t>(errorCode);
}

ErrorCode fromInt(int32_t errorCode) noexcept {
    return static_cast<ErrorCode>(errorCode);
}

}  // namespace mooncake
