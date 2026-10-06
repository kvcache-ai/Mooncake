#include "conductor/client/types.h"

namespace mooncake::conductor {

const char* ErrorCodeName(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::OK:
            return "OK";
        case ErrorCode::INTERNAL_ERROR:
            return "INTERNAL_ERROR";
        case ErrorCode::INVALID_PARAMS:
            return "INVALID_PARAMS";
        case ErrorCode::RPC_FAIL:
            return "RPC_FAIL";
        case ErrorCode::RPC_TIMEOUT:
            return "RPC_TIMEOUT";
        case ErrorCode::CONDUCTOR_UNAVAILABLE:
            return "CONDUCTOR_UNAVAILABLE";
        case ErrorCode::SERVICE_NOT_FOUND:
            return "SERVICE_NOT_FOUND";
    }
    return "UNKNOWN";
}

}  // namespace mooncake::conductor
