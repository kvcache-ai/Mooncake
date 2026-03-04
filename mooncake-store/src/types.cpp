#include "types.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace mooncake {

const std::string& toString(ErrorCode errorCode) noexcept {
    static const std::unordered_map<ErrorCode, std::string> errorCodeMap = {
        {ErrorCode::OK, "OK"},
        {ErrorCode::INTERNAL_ERROR, "INTERNAL_ERROR"},
        {ErrorCode::BUFFER_OVERFLOW, "BUFFER_OVERFLOW"},
        {ErrorCode::SHARD_INDEX_OUT_OF_RANGE, "SHARD_INDEX_OUT_OF_RANGE"},
        {ErrorCode::SEGMENT_NOT_FOUND, "SEGMENT_NOT_FOUND"},
        {ErrorCode::SEGMENT_ALREADY_EXISTS, "SEGMENT_ALREADY_EXISTS"},
        {ErrorCode::CLIENT_NOT_FOUND, "CLIENT_NOT_FOUND"},
        {ErrorCode::NO_AVAILABLE_HANDLE, "NO_AVAILABLE_HANDLE"},
        {ErrorCode::INVALID_VERSION, "INVALID_VERSION"},
        {ErrorCode::INVALID_KEY, "INVALID_KEY"},
        {ErrorCode::WRITE_FAIL, "WRITE_FAIL"},
        {ErrorCode::INVALID_PARAMS, "INVALID_PARAMS"},
        {ErrorCode::ILLEGAL_CLIENT, "ILLEGAL_CLIENT"},
        {ErrorCode::INVALID_WRITE, "INVALID_WRITE"},
        {ErrorCode::INVALID_READ, "INVALID_READ"},
        {ErrorCode::INVALID_REPLICA, "INVALID_REPLICA"},
        {ErrorCode::REPLICA_IS_NOT_READY, "REPLICA_IS_NOT_READY"},
        {ErrorCode::OBJECT_NOT_FOUND, "OBJECT_NOT_FOUND"},
        {ErrorCode::OBJECT_ALREADY_EXISTS, "OBJECT_ALREADY_EXISTS"},
        {ErrorCode::OBJECT_HAS_LEASE, "OBJECT_HAS_LEASE"},
        {ErrorCode::LEASE_EXPIRED, "LEASE_EXPIRED"},
        {ErrorCode::TRANSFER_FAIL, "TRANSFER_FAIL"},
        {ErrorCode::RPC_FAIL, "RPC_FAIL"},
        {ErrorCode::ETCD_OPERATION_ERROR, "ETCD_OPERATION_ERROR"},
        {ErrorCode::ETCD_KEY_NOT_EXIST, "ETCD_KEY_NOT_EXIST"},
        {ErrorCode::ETCD_TRANSACTION_FAIL, "ETCD_TRANSACTION_FAIL"},
        {ErrorCode::ETCD_CTX_CANCELLED, "ETCD_CTX_CANCELLED"},
        {ErrorCode::CLIENT_ALREADY_EXISTS, "CLIENT_ALREADY_EXISTS"},
        {ErrorCode::CLIENT_UNHEALTHY, "CLIENT_UNHEALTHY"},
        {ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
         "UNAVAILABLE_IN_CURRENT_STATUS"},
        {ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, "UNAVAILABLE_IN_CURRENT_MODE"},
        {ErrorCode::FILE_NOT_FOUND, "FILE_NOT_FOUND"},
        {ErrorCode::FILE_OPEN_FAIL, "FILE_OPEN_FAIL"},
        {ErrorCode::FILE_READ_FAIL, "FILE_READ_FAIL"},
        {ErrorCode::FILE_WRITE_FAIL, "FILE_WRITE_FAIL"},
        {ErrorCode::FILE_INVALID_BUFFER, "FILE_INVALID_BUFFER"},
        {ErrorCode::FILE_LOCK_FAIL, "FILE_LOCK_FAIL"},
        {ErrorCode::FILE_INVALID_HANDLE, "FILE_INVALID_HANDLE"},
        {ErrorCode::BUCKET_NOT_FOUND, "BUCKET_NOT_FOUND"},
        {ErrorCode::BUCKET_ALREADY_EXISTS, "BUCKET_ALREADY_EXISTS"},
        {ErrorCode::KEYS_EXCEED_BUCKET_LIMIT, "KEYS_EXCEED_BUCKET_LIMIT"},
        {ErrorCode::KEYS_ULTRA_LIMIT, "KEYS_ULTRA_LIMIT"},
        {ErrorCode::UNABLE_OFFLOAD, "UNABLE_OFFLOAD"},
        {ErrorCode::UNABLE_OFFLOADING, "UNABLE_OFFLOADING"}};

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

UUID generate_uuid() {
    UUID pair_uuid;
    boost::uuids::random_generator gen;
    boost::uuids::uuid uuid = gen();
    std::memcpy(&pair_uuid.first, uuid.data, sizeof(uint64_t));
    std::memcpy(&pair_uuid.second, uuid.data + sizeof(uint64_t),
                sizeof(uint64_t));
    return pair_uuid;
}

}  // namespace mooncake
