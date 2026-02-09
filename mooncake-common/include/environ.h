#pragma once

#include <string>
#include <cstdint>
#include <cstdlib>

namespace mooncake {

class Environ {
   public:
    // Singleton access
    static Environ& Get();

    // Getters for Environment Variables
    int GetNumCqPerCtx() const { return num_cq_per_ctx_; }
    int GetNumCompChannelsPerCtx() const { return num_comp_channels_per_ctx_; }
    int GetIbPort() const { return ib_port_; }
    int GetIbTc() const { return ib_tc_; }
    int GetIbPciRelaxedOrdering() const { return ib_pci_relaxed_ordering_; }
    int GetGidIndex() const { return gid_index_; }
    int GetMaxCqePerCtx() const { return max_cqe_per_ctx_; }
    int GetMaxEpPerCtx() const { return max_ep_per_ctx_; }
    int GetNumQpPerEp() const { return num_qp_per_ep_; }
    int GetMaxSge() const { return max_sge_; }
    int GetMaxWr() const { return max_wr_; }
    int GetMaxInline() const { return max_inline_; }
    int GetMtu() const { return mtu_; }
    int GetWorkersPerCtx() const { return workers_per_ctx_; }
    size_t GetSliceSize() const { return slice_size_; }
    int GetRetryCnt() const { return retry_cnt_; }
    std::string GetLogLevel() const { return log_level_; }
    bool GetDisableMetacache() const { return disable_metacache_; }
    int GetHandshakeListenBacklog() const { return handshake_listen_backlog_; }
    int GetHandshakeMaxLength() const { return handshake_max_length_; }
    std::string GetLogDir() const { return log_dir_; }
    std::string GetRedisPassword() const { return redis_password_; }
    int GetRedisDbIndex() const { return redis_db_index_; }
    int GetFragmentRatio() const { return fragment_ratio_; }
    bool GetEnableDestDeviceAffinity() const {
        return enable_dest_device_affinity_;
    }
    bool GetUseIpv6() const { return use_ipv6_; }
    int GetMinPrcPort() const { return min_prc_port_; }
    int GetMaxPrcPort() const { return max_prc_port_; }
    int GetEnableParallelRegMr() const { return enable_parallel_reg_mr_; }
    std::string GetEndpointStoreType() const { return endpoint_store_type_; }
    bool GetForceTcp() const { return force_tcp_; }
    bool GetForceHca() const { return force_hca_; }
    bool GetForceMnnvl() const { return force_mnnvl_; }
    bool GetIntraNvlink() const { return intra_nvlink_; }
    bool GetPathRoundrobin() const { return path_roundrobin_; }

   private:
    Environ();

    // Helper method to get int from env
    static int GetInt(const char* name, int default_value);
    // Helper method to get size_t from env
    static size_t GetSizeT(const char* name, size_t default_value);
    // Helper method to get bool from env (checks for "1", "true", "TRUE")
    static bool GetBool(const char* name, bool default_value);
    // Helper method to get string from env
    static std::string GetString(const char* name,
                                 const std::string& default_value);

    // Member variables
    int num_cq_per_ctx_;
    int num_comp_channels_per_ctx_;
    int ib_port_;
    int ib_tc_;
    int ib_pci_relaxed_ordering_;
    int gid_index_;
    int max_cqe_per_ctx_;
    int max_ep_per_ctx_;
    int num_qp_per_ep_;
    int max_sge_;
    int max_wr_;
    int max_inline_;
    int mtu_;
    int workers_per_ctx_;
    size_t slice_size_;
    int retry_cnt_;
    std::string log_level_;
    bool disable_metacache_;
    int handshake_listen_backlog_;
    int handshake_max_length_;
    std::string log_dir_;
    std::string redis_password_;
    int redis_db_index_;
    int fragment_ratio_;
    bool enable_dest_device_affinity_;
    bool use_ipv6_;
    int min_prc_port_;
    int max_prc_port_;
    int enable_parallel_reg_mr_;
    std::string endpoint_store_type_;
    bool force_tcp_;
    bool force_hca_;
    bool force_mnnvl_;
    bool intra_nvlink_;
    bool path_roundrobin_;
};

}  // namespace mooncake
