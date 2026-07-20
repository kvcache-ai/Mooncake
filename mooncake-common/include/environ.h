#pragma once

#include <string>
#include <cstdint>
#include <cstdlib>
#include <thread>

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
    int GetMinRpcPort() const { return min_rpc_port_; }
    int GetMaxRpcPort() const { return max_rpc_port_; }
    int GetEnableParallelRegMr() const { return enable_parallel_reg_mr_; }
    std::string GetEndpointStoreType() const { return endpoint_store_type_; }
    bool GetForceTcp() const { return force_tcp_; }
    bool GetForceHca() const { return force_hca_; }
    bool GetForceMnnvl() const { return force_mnnvl_; }
    bool GetIntraNvlink() const { return intra_nvlink_; }
    bool GetPathRoundrobin() const { return path_roundrobin_; }
    bool GetWithNvidiaPeermem() const { return with_nvidia_peermem_; }
    int GetEfaCqThreads() const { return efa_cq_threads_; }

    // AWS / S3 client configuration
    std::string GetAwsRegion() const { return aws_region_; }
    std::string GetAwsS3Endpoint() const { return aws_s3_endpoint_; }
    std::string GetAwsBucketName() const { return aws_bucket_name_; }
    std::string GetAwsAccessKeyId() const { return aws_access_key_id_; }
    std::string GetAwsSecretAccessKey() const { return aws_secret_access_key_; }
    bool GetAwsUseVirtualAddressing() const {
        return aws_use_virtual_addressing_;
    }
    bool GetAwsUseHttps() const { return aws_use_https_; }
    // Empty string means "unset" — s3_helper keeps the AWS SDK default in
    // that case. Parsing to AWS enums is done by the consumer.
    std::string GetAwsRequestChecksumCalculation() const {
        return aws_request_checksum_calculation_;
    }
    std::string GetAwsResponseChecksumValidation() const {
        return aws_response_checksum_validation_;
    }
    int64_t GetAwsConnectTimeoutMs() const { return aws_connect_timeout_ms_; }
    int64_t GetAwsRequestTimeoutMs() const { return aws_request_timeout_ms_; }

    // Helper method to get int from env
    static int GetInt(const char* name, int default_value);
    static int64_t GetInt64(const char* name, int64_t default_value);
    // Helper method to get size_t from env
    static size_t GetSizeT(const char* name, size_t default_value);
    // Helper method to get bool from env (checks for "1", "true", "TRUE")
    static bool GetBool(const char* name, bool default_value);
    // Helper method to get string from env
    static std::string GetString(const char* name,
                                 const std::string& default_value);
    // RPC client I/O contexts use one thread per context. Invalid values and
    // zero fall back to the online hardware thread count, with a minimum of 1.
    static unsigned GetRpcClientIoThreads(
        unsigned hardware_threads = std::thread::hardware_concurrency());

   private:
    Environ();

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
    int min_rpc_port_;
    int max_rpc_port_;
    int enable_parallel_reg_mr_;
    std::string endpoint_store_type_;
    bool force_tcp_;
    bool force_hca_;
    bool force_mnnvl_;
    bool intra_nvlink_;
    bool path_roundrobin_;
    bool with_nvidia_peermem_;
    int efa_cq_threads_;

    // AWS / S3 client configuration
    std::string aws_region_;
    std::string aws_s3_endpoint_;
    std::string aws_bucket_name_;
    std::string aws_access_key_id_;
    std::string aws_secret_access_key_;
    bool aws_use_virtual_addressing_;
    bool aws_use_https_;
    std::string aws_request_checksum_calculation_;
    std::string aws_response_checksum_validation_;
    int64_t aws_connect_timeout_ms_;
    int64_t aws_request_timeout_ms_;
};

}  // namespace mooncake
