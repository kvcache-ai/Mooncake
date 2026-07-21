#include "environ.h"
#include <cerrno>
#include <climits>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <thread>

namespace mooncake {

namespace {

constexpr char kRpcClientIoThreadsEnv[] = "MC_RPC_CLIENT_IO_THREADS";
constexpr char kStoreRpcClientIoThreadsEnv[] = "MC_STORE_RPC_CLIENT_IO_THREADS";
constexpr char kTransferEngineRpcClientIoThreadsEnv[] =
    "MC_TE_RPC_CLIENT_IO_THREADS";
constexpr uint32_t kDefaultRpcClientIoThreads = 16;

class OsEnvironSource final : public EnvironSource {
   public:
    const char* Get(const char* name) const override {
        return std::getenv(name);
    }
};

const EnvironSource& GetOsEnvironSource() {
    static const OsEnvironSource source;
    return source;
}

int ReadInt(const EnvironSource& source, const char* name, int default_value) {
    const char* val = source.Get(name);
    if (val) {
        char* endptr = nullptr;
        errno = 0;
        long result = std::strtol(val, &endptr, 10);
        if (endptr == val || *endptr != '\0' || errno == ERANGE ||
            result < INT_MIN || result > INT_MAX) {
            std::cerr << "[Mooncake] Warning: invalid value '" << val
                      << "' for env " << name << ", using default "
                      << default_value << std::endl;
            return default_value;
        }
        return static_cast<int>(result);
    }
    return default_value;
}

int64_t ReadInt64(const EnvironSource& source, const char* name,
                  int64_t default_value) {
    const char* val = source.Get(name);
    if (val) {
        char* endptr = nullptr;
        errno = 0;
        long long result = std::strtoll(val, &endptr, 10);
        if (endptr == val || *endptr != '\0' || errno == ERANGE) {
            std::cerr << "[Mooncake] Warning: invalid value '" << val
                      << "' for env " << name << ", using default "
                      << default_value << std::endl;
            return default_value;
        }
        return static_cast<int64_t>(result);
    }
    return default_value;
}

size_t ReadSizeT(const EnvironSource& source, const char* name,
                 size_t default_value) {
    const char* val = source.Get(name);
    if (val) {
        char* endptr = nullptr;
        errno = 0;
        long long result = std::strtoll(val, &endptr, 10);
        if (endptr == val || *endptr != '\0' || errno == ERANGE || result < 0 ||
            static_cast<unsigned long long>(result) > SIZE_MAX) {
            std::cerr << "[Mooncake] Warning: invalid value '" << val
                      << "' for env " << name << ", using default "
                      << default_value << std::endl;
            return default_value;
        }
        return static_cast<size_t>(result);
    }
    return default_value;
}

bool ReadBool(const EnvironSource& source, const char* name,
              bool default_value) {
    const char* val = source.Get(name);
    if (val) {
        std::string s(val);
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return s == "1" || s == "true" || s == "on" || s == "yes";
    }
    return default_value;
}

std::string ReadString(const EnvironSource& source, const char* name,
                       const std::string& default_value) {
    const char* val = source.Get(name);
    return val ? std::string(val) : default_value;
}

uint32_t ResolveRpcClientIoThreads(const EnvironSource& source,
                                   const char* env_name, uint32_t fallback) {
    const int configured =
        ReadInt(source, env_name, static_cast<int>(fallback));
    return configured > 0 ? static_cast<uint32_t>(configured) : fallback;
}

}  // namespace

Environ& Environ::Get() {
    static Environ instance(
        GetOsEnvironSource(),
        static_cast<uint32_t>(std::thread::hardware_concurrency()));
    return instance;
}

int Environ::GetInt(const char* name, int default_value) {
    return ReadInt(GetOsEnvironSource(), name, default_value);
}

int64_t Environ::GetInt64(const char* name, int64_t default_value) {
    return ReadInt64(GetOsEnvironSource(), name, default_value);
}

size_t Environ::GetSizeT(const char* name, size_t default_value) {
    return ReadSizeT(GetOsEnvironSource(), name, default_value);
}

bool Environ::GetBool(const char* name, bool default_value) {
    return ReadBool(GetOsEnvironSource(), name, default_value);
}

std::string Environ::GetString(const char* name,
                               const std::string& default_value) {
    return ReadString(GetOsEnvironSource(), name, default_value);
}

Environ::Environ(const EnvironSource& source, uint32_t hardware_threads) {
    const uint32_t default_rpc_client_io_threads = std::min(
        kDefaultRpcClientIoThreads, std::max(uint32_t{1}, hardware_threads));
    rpc_client_io_threads_ = ResolveRpcClientIoThreads(
        source, kRpcClientIoThreadsEnv, default_rpc_client_io_threads);
    store_rpc_client_io_threads_ = ResolveRpcClientIoThreads(
        source, kStoreRpcClientIoThreadsEnv, rpc_client_io_threads_);
    transfer_engine_rpc_client_io_threads_ = ResolveRpcClientIoThreads(
        source, kTransferEngineRpcClientIoThreadsEnv, rpc_client_io_threads_);

    num_cq_per_ctx_ = ReadInt(source, "MC_NUM_CQ_PER_CTX", 1);
    num_comp_channels_per_ctx_ =
        ReadInt(source, "MC_NUM_COMP_CHANNELS_PER_CTX", 1);
    ib_port_ = ReadInt(source, "MC_IB_PORT", 1);
    ib_tc_ = ReadInt(source, "MC_IB_TC", -1);
    ib_pci_relaxed_ordering_ = ReadInt(source, "MC_IB_PCI_RELAXED_ORDERING", 0);
    gid_index_ = ReadInt(source, "MC_GID_INDEX", 3);
    max_cqe_per_ctx_ = ReadInt(source, "MC_MAX_CQE_PER_CTX", 4096);
    max_ep_per_ctx_ = ReadInt(source, "MC_MAX_EP_PER_CTX", 65536);
    num_qp_per_ep_ = ReadInt(source, "MC_NUM_QP_PER_EP", 2);
    max_sge_ = ReadInt(source, "MC_MAX_SGE", 4);
    max_wr_ = ReadInt(source, "MC_MAX_WR", 256);
    max_inline_ = ReadInt(source, "MC_MAX_INLINE", 64);
    mtu_ = ReadInt(source, "MC_MTU", 4096);
    workers_per_ctx_ = ReadInt(source, "MC_WORKERS_PER_CTX", 2);
    slice_size_ = ReadSizeT(source, "MC_SLICE_SIZE", 65536);
    retry_cnt_ = ReadInt(source, "MC_RETRY_CNT", 9);
    log_level_ = ReadString(source, "MC_LOG_LEVEL", "INFO");
    disable_metacache_ = ReadBool(source, "MC_DISABLE_METACACHE", false);
    handshake_listen_backlog_ =
        ReadInt(source, "MC_HANDSHAKE_LISTEN_BACKLOG", 128);
    handshake_max_length_ = ReadInt(source, "MC_HANDSHAKE_MAX_LENGTH", 1048576);
    log_dir_ = ReadString(source, "MC_LOG_DIR", "");
    redis_password_ = ReadString(source, "MC_REDIS_PASSWORD", "");
    redis_db_index_ = ReadInt(source, "MC_REDIS_DB_INDEX", 0);
    fragment_ratio_ = ReadInt(source, "MC_FRAGMENT_RATIO", 4);
    enable_dest_device_affinity_ =
        ReadBool(source, "MC_ENABLE_DEST_DEVICE_AFFINITY", false);
    use_ipv6_ = ReadBool(source, "MC_USE_IPV6", false);
    min_rpc_port_ = ReadInt(source, "MC_MIN_RPC_PORT",
                            ReadInt(source, "MC_MIN_PRC_PORT", 15000));
    max_rpc_port_ = ReadInt(source, "MC_MAX_RPC_PORT",
                            ReadInt(source, "MC_MAX_PRC_PORT", 17000));
    enable_parallel_reg_mr_ = ReadInt(source, "MC_ENABLE_PARALLEL_REG_MR", -1);
    endpoint_store_type_ =
        ReadString(source, "MC_ENDPOINT_STORE_TYPE", "SIEVE");
    force_tcp_ = ReadBool(source, "MC_FORCE_TCP", false);
    force_hca_ = ReadBool(source, "MC_FORCE_HCA", false);
    force_mnnvl_ = ReadBool(source, "MC_FORCE_MNNVL", false);
    intra_nvlink_ = ReadBool(source, "MC_INTRA_NVLINK", false);
    path_roundrobin_ = ReadBool(source, "MC_PATH_ROUNDROBIN", false);
    with_nvidia_peermem_ = ReadBool(source, "WITH_NVIDIA_PEERMEM", true);
    efa_cq_threads_ = ReadInt(source, "MC_EFA_CQ_THREADS", 1);

    // AWS / S3 client configuration (consumed by s3_helper.cpp)
    aws_region_ = ReadString(source, "MOONCAKE_AWS_REGION", "");
    aws_s3_endpoint_ = ReadString(source, "MOONCAKE_AWS_S3_ENDPOINT", "");
    aws_bucket_name_ = ReadString(source, "MOONCAKE_AWS_BUCKET_NAME", "");
    aws_access_key_id_ = ReadString(source, "MOONCAKE_AWS_ACCESS_KEY_ID", "");
    aws_secret_access_key_ =
        ReadString(source, "MOONCAKE_AWS_SECRET_ACCESS_KEY", "");
    aws_use_virtual_addressing_ =
        ReadBool(source, "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", true);
    aws_use_https_ = ReadBool(source, "MOONCAKE_AWS_USE_HTTPS", true);
    // Empty string preserves "unset" semantics — s3_helper keeps the AWS SDK
    // default in that case rather than forcing a value.
    aws_request_checksum_calculation_ =
        ReadString(source, "MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "");
    aws_response_checksum_validation_ =
        ReadString(source, "MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "");
    aws_connect_timeout_ms_ =
        ReadInt64(source, "MOONCAKE_AWS_CONNECT_TIMEOUT_MS", 10000);
    aws_request_timeout_ms_ =
        ReadInt64(source, "MOONCAKE_AWS_REQUEST_TIMEOUT_MS", 30000);
}

}  // namespace mooncake
