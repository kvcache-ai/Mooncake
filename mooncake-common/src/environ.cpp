#include "environ.h"
#include <cstring>
#include <algorithm>

namespace mooncake {

Environ& Environ::Get() {
    static Environ instance;
    return instance;
}

int Environ::GetInt(const char* name, int default_value) {
    const char* val = std::getenv(name);
    if (val) {
        return std::atoi(val);
    }
    return default_value;
}

size_t Environ::GetSizeT(const char* name, size_t default_value) {
    const char* val = std::getenv(name);
    if (val) {
        return static_cast<size_t>(std::strtoull(val, nullptr, 10));
    }
    return default_value;
}

bool Environ::GetBool(const char* name, bool default_value) {
    const char* val = std::getenv(name);
    if (val) {
        std::string s(val);
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
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
    min_prc_port_ = GetInt("MC_MIN_PRC_PORT", 15000);
    max_prc_port_ = GetInt("MC_MAX_PRC_PORT", 17000);
    enable_parallel_reg_mr_ = GetInt("MC_ENABLE_PARALLEL_REG_MR", -1);
    endpoint_store_type_ = GetString("MC_ENDPOINT_STORE_TYPE", "SIEVE");
    force_tcp_ = GetBool("MC_FORCE_TCP", false);
    force_hca_ = GetBool("MC_FORCE_HCA", false);
    force_mnnvl_ = GetBool("MC_FORCE_MNNVL", false);
    intra_nvlink_ = GetBool("MC_INTRA_NVLINK", false);
    path_roundrobin_ = GetBool("MC_PATH_ROUNDROBIN", false);
}

}  // namespace mooncake
