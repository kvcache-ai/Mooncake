#include "environ.h"
#include <cerrno>
#include <climits>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <limits>

namespace mooncake {

namespace {

constexpr char kRpcClientIoThreadsEnv[] = "MC_RPC_CLIENT_IO_THREADS";
constexpr unsigned kDefaultRpcClientIoThreads = 16;

unsigned GetRpcClientIoThreadsFor(const char* component_env,
                                  unsigned hardware_threads) {
    const size_t default_value = std::min<size_t>(
        kDefaultRpcClientIoThreads, std::max<size_t>(1, hardware_threads));
    const size_t common_value =
        Environ::GetSizeT(kRpcClientIoThreadsEnv, default_value);
    const size_t common_fallback =
        common_value == 0 || common_value > std::numeric_limits<unsigned>::max()
            ? default_value
            : common_value;
    if (common_fallback != common_value) {
        std::cerr << "[Mooncake] Warning: invalid value '" << common_value
                  << "' for env " << kRpcClientIoThreadsEnv
                  << ", using default " << default_value << std::endl;
    }

    const size_t configured =
        component_env == nullptr
            ? common_fallback
            : Environ::GetSizeT(component_env, common_fallback);
    if (configured == 0 || configured > std::numeric_limits<unsigned>::max()) {
        std::cerr << "[Mooncake] Warning: invalid value '" << configured
                  << "' for env " << component_env << ", using fallback "
                  << common_fallback << std::endl;
        return static_cast<unsigned>(common_fallback);
    }
    return static_cast<unsigned>(configured);
}

}  // namespace

Environ& Environ::Get() {
    static Environ instance;
    return instance;
}

int Environ::GetInt(const char* name, int default_value) {
    const char* val = std::getenv(name);
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

int64_t Environ::GetInt64(const char* name, int64_t default_value) {
    const char* val = std::getenv(name);
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

size_t Environ::GetSizeT(const char* name, size_t default_value) {
    const char* val = std::getenv(name);
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

bool Environ::GetBool(const char* name, bool default_value) {
    const char* val = std::getenv(name);
    if (val) {
        std::string s(val);
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return s == "1" || s == "true" || s == "on" || s == "yes";
    }
    return default_value;
}

std::string Environ::GetString(const char* name,
                               const std::string& default_value) {
    const char* val = std::getenv(name);
    if (val) {
        return std::string(val);
    }
    return default_value;
}

unsigned Environ::GetRpcClientIoThreads(unsigned hardware_threads) {
    return GetRpcClientIoThreadsFor(nullptr, hardware_threads);
}

unsigned Environ::GetComponentRpcClientIoThreads(const char* component_env,
                                                 unsigned hardware_threads) {
    return GetRpcClientIoThreadsFor(component_env, hardware_threads);
}

Environ::Environ() {
    num_cq_per_ctx_ = GetInt("MC_NUM_CQ_PER_CTX", 1);
    num_comp_channels_per_ctx_ = GetInt("MC_NUM_COMP_CHANNELS_PER_CTX", 1);
    ib_port_ = GetInt("MC_IB_PORT", 1);
    ib_tc_ = GetInt("MC_IB_TC", -1);
    ib_pci_relaxed_ordering_ = GetInt("MC_IB_PCI_RELAXED_ORDERING", 0);
    gid_index_ = GetInt("MC_GID_INDEX", 3);
    max_cqe_per_ctx_ = GetInt("MC_MAX_CQE_PER_CTX", 4096);
    max_ep_per_ctx_ = GetInt("MC_MAX_EP_PER_CTX", 65536);
    num_qp_per_ep_ = GetInt("MC_NUM_QP_PER_EP", 2);
    max_sge_ = GetInt("MC_MAX_SGE", 4);
    max_wr_ = GetInt("MC_MAX_WR", 256);
    max_inline_ = GetInt("MC_MAX_INLINE", 64);
    mtu_ = GetInt("MC_MTU", 4096);
    workers_per_ctx_ = GetInt("MC_WORKERS_PER_CTX", 2);
    slice_size_ = GetSizeT("MC_SLICE_SIZE", 65536);
    retry_cnt_ = GetInt("MC_RETRY_CNT", 9);
    log_level_ = GetString("MC_LOG_LEVEL", "INFO");
    disable_metacache_ = GetBool("MC_DISABLE_METACACHE", false);
    handshake_listen_backlog_ = GetInt("MC_HANDSHAKE_LISTEN_BACKLOG", 128);
    handshake_max_length_ = GetInt("MC_HANDSHAKE_MAX_LENGTH", 1048576);
    log_dir_ = GetString("MC_LOG_DIR", "");
    redis_password_ = GetString("MC_REDIS_PASSWORD", "");
    redis_db_index_ = GetInt("MC_REDIS_DB_INDEX", 0);
    fragment_ratio_ = GetInt("MC_FRAGMENT_RATIO", 4);
    enable_dest_device_affinity_ =
        GetBool("MC_ENABLE_DEST_DEVICE_AFFINITY", false);
    use_ipv6_ = GetBool("MC_USE_IPV6", false);
    min_rpc_port_ = GetInt("MC_MIN_RPC_PORT", GetInt("MC_MIN_PRC_PORT", 15000));
    max_rpc_port_ = GetInt("MC_MAX_RPC_PORT", GetInt("MC_MAX_PRC_PORT", 17000));
    enable_parallel_reg_mr_ = GetInt("MC_ENABLE_PARALLEL_REG_MR", -1);
    endpoint_store_type_ = GetString("MC_ENDPOINT_STORE_TYPE", "SIEVE");
    force_tcp_ = GetBool("MC_FORCE_TCP", false);
    force_hca_ = GetBool("MC_FORCE_HCA", false);
    force_mnnvl_ = GetBool("MC_FORCE_MNNVL", false);
    intra_nvlink_ = GetBool("MC_INTRA_NVLINK", false);
    path_roundrobin_ = GetBool("MC_PATH_ROUNDROBIN", false);
    with_nvidia_peermem_ = GetBool("WITH_NVIDIA_PEERMEM", true);
    efa_cq_threads_ = GetInt("MC_EFA_CQ_THREADS", 1);

    // AWS / S3 client configuration (consumed by s3_helper.cpp)
    aws_region_ = GetString("MOONCAKE_AWS_REGION", "");
    aws_s3_endpoint_ = GetString("MOONCAKE_AWS_S3_ENDPOINT", "");
    aws_bucket_name_ = GetString("MOONCAKE_AWS_BUCKET_NAME", "");
    aws_access_key_id_ = GetString("MOONCAKE_AWS_ACCESS_KEY_ID", "");
    aws_secret_access_key_ = GetString("MOONCAKE_AWS_SECRET_ACCESS_KEY", "");
    aws_use_virtual_addressing_ =
        GetBool("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", true);
    aws_use_https_ = GetBool("MOONCAKE_AWS_USE_HTTPS", true);
    // Empty string preserves "unset" semantics — s3_helper keeps the AWS SDK
    // default in that case rather than forcing a value.
    aws_request_checksum_calculation_ =
        GetString("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "");
    aws_response_checksum_validation_ =
        GetString("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "");
    aws_connect_timeout_ms_ =
        GetInt64("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", 10000);
    aws_request_timeout_ms_ =
        GetInt64("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", 30000);
}

}  // namespace mooncake
