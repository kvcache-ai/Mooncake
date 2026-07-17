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
    int GetPkeyIndex() const { return GetInt("MC_PKEY_INDEX", -1); }
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
    uint64_t GetMaxMrSize() const { return GetSizeT("MC_MAX_MR_SIZE", 0); }
    int GetAutoGidMaxRetries() const {
        return GetInt("MC_AUTO_GID_MAX_RETRIES", 2);
    }
    std::string GetTeMetadataRefreshIntervalSeconds() const {
        return GetString("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS", "");
    }
    std::string GetLogLevel() const { return log_level_; }
    bool GetDisableMetacache() const { return disable_metacache_; }
    int GetHandshakeListenBacklog() const { return handshake_listen_backlog_; }
    int GetHandshakeMaxLength() const { return handshake_max_length_; }
    int GetHandshakeConnectTimeout() const {
        return GetInt("MC_HANDSHAKE_CONNECT_TIMEOUT", 5);
    }
    std::string GetLogDir() const { return log_dir_; }
    std::string GetRedisUsername() const { return redis_username_; }
    std::string GetRedisPassword() const { return redis_password_; }
    int GetRedisDbIndex() const;
    std::string GetRedisDbIndexRaw() const {
        return GetString("MC_REDIS_DB_INDEX", "");
    }
    int GetFragmentRatio() const { return fragment_ratio_; }
    bool GetEnableDestDeviceAffinity() const {
        return enable_dest_device_affinity_;
    }
    std::string GetEnableHcaPeerAffinity() const {
        return GetString("MC_ENABLE_HCA_PEER_AFFINITY", "");
    }
    std::string GetNicPeerAffinity() const {
        return GetString("MC_NIC_PEER_AFFINITY", "");
    }
    std::string GetLogRdmaSliceAffinity() const {
        return GetString("MC_LOG_RDMA_SLICE_AFFINITY", "");
    }
    std::string GetTrackRdmaPostedSlices() const {
        return GetString("MC_TRACK_RDMA_POSTED_SLICES", "");
    }
    bool GetUseIpv6() const { return use_ipv6_; }
    int GetMinRpcPort() const { return min_rpc_port_; }
    int GetMaxRpcPort() const { return max_rpc_port_; }
    int GetEnableParallelRegMr() const { return enable_parallel_reg_mr_; }
    std::string GetEndpointStoreType() const { return endpoint_store_type_; }
    bool GetForceTcp() const { return force_tcp_; }
    bool GetForceHca() const { return force_hca_; }
    bool GetForceMnnvl() const { return force_mnnvl_; }
    bool GetUseBarex() const { return GetBool("USE_BAREX", false); }
    bool GetMacaHostTransport() const {
        return IsSet("MC_MACA_HOST_TRANSPORT");
    }
    bool GetDisableHip() const { return IsSet("MC_DISABLE_HIP"); }
    bool GetDisableMaca() const { return IsSet("MC_DISABLE_MACA"); }
    bool GetIntraNvlink() const { return intra_nvlink_; }
    bool GetPathRoundrobin() const { return path_roundrobin_; }
    int GetIbServiceLevel() const { return GetInt("MC_IB_SL", -1); }
    std::string GetMlx5QpUdpSports() const {
        return GetString("MC_MLX5_QP_UDP_SPORTS", "");
    }
    std::string GetMlx5QpLagPortBalance() const {
        return GetString("MC_MLX5_QP_LAG_PORT_BALANCE", "");
    }
    std::string GetCustomTopoJson() const { return custom_topo_json_; }
    std::string GetCxlDevPath() const {
        return GetString("MC_CXL_DEV_PATH", cxl_dev_path_);
    }
    std::string GetCxlDevSize() const {
        return GetString("MC_CXL_DEV_SIZE", cxl_dev_size_);
    }
    bool GetDisableGpuDirectRdma() const { return disable_gpu_direct_rdma_; }
    bool GetEnableMnnvl() const { return enable_mnnvl_; }
    int GetHandshakePort() const { return handshake_port_; }
    int GetHipNumEvents() const { return hip_num_events_; }
    int GetHipNumStreams() const { return hip_num_streams_; }
    bool GetIntranodeNvlink() const { return intranode_nvlink_; }
    bool GetLegacyRpcPortBinding() const { return legacy_rpc_port_binding_; }
    std::string GetMetadataClusterId() const { return metadata_cluster_id_; }
    size_t GetMinRegSize() const { return min_reg_size_; }
    std::string GetMsAutoDisc() const {
        return GetString("MC_MS_AUTO_DISC", ms_auto_disc_);
    }
    std::string GetMsFilters() const {
        return GetString("MC_MS_FILTERS", ms_filters_);
    }
    std::string GetRpcProtocol() const { return rpc_protocol_; }
    std::string GetRpcTimeoutMs() const {
        return GetString("MC_RPC_TIMEOUT_MS", "");
    }
    std::string GetRpcConnectTimeoutMs() const {
        return GetString("MC_RPC_CONNECT_TIMEOUT_MS", "");
    }
    std::string GetMooncakeTeMetadataServer() const {
        return GetString("MOONCAKE_TE_META_DATA_SERVER", "");
    }
    std::string GetMooncakeConfigPath() const {
        return GetString("MOONCAKE_CONFIG_PATH", "");
    }
    std::string GetPodName() const { return GetString("POD_NAME", ""); }
    std::string GetPodNamespace() const {
        return GetString("POD_NAMESPACE", "");
    }
    int GetSliceTimeout() const { return slice_timeout_; }
    std::string GetStoreClientMetric() const {
        return GetString("MC_STORE_CLIENT_METRIC", "");
    }
    int GetStoreClientMetricInterval() const {
        return GetInt("MC_STORE_CLIENT_METRIC_INTERVAL", 0);
    }
    bool GetStoreClientMetricBandwidth() const {
        return GetBool("MC_STORE_CLIENT_METRIC_BANDWIDTH", true);
    }
    std::string GetStoreClusterId() const { return store_cluster_id_; }
    std::string GetStoreHugepageSize() const {
        return GetString("MC_STORE_HUGEPAGE_SIZE", store_hugepage_size_);
    }
    std::string GetStoreMemcpy() const {
        return GetString("MC_STORE_MEMCPY", store_memcpy_);
    }
    bool IsStoreMemcpySet() const { return IsSet("MC_STORE_MEMCPY"); }
    std::string GetStoreUseHugepage() const {
        return GetString("MC_STORE_USE_HUGEPAGE", store_use_hugepage_);
    }
    bool GetStoreUseHugepageEnabled() const {
        return IsSet("MC_STORE_USE_HUGEPAGE");
    }
    std::string GetTcpBindAddress() const { return tcp_bind_address_; }
    std::string GetRdmaBindAddress() const {
        return GetString("MC_RDMA_BIND_ADDRESS", "");
    }
    std::string GetDisableHipDmabuf() const {
        return GetString("MOONCAKE_DISABLE_HIP_DMABUF", "");
    }
    std::string GetTeFilters() const { return GetString("MC_TE_FILTERS", ""); }
    std::string GetMacaCallerSync() const {
        return GetString("MC_MACA_CALLER_SYNC", "");
    }
    std::string GetMacaCopyApi() const {
        return GetString("MC_MACA_COPY_API", "");
    }
    std::string GetMacaBatchflagMinBytes() const {
        return GetString("MC_MACA_BATCHFLAG_MIN_BYTES", "");
    }
    bool GetUseUbshmemIpc() const {
        return GetBool("MC_USE_UBSHMEM_IPC", false);
    }
    std::string GetUbshmemStreamsPerTransfer() const {
        return GetString("MC_UBSHMEM_STREAMS_PER_TRANSFER", "");
    }
    std::string GetUbshmemMaxStreams() const {
        return GetString("MC_UBSHMEM_MAX_STREAMS", "");
    }
    std::string GetUbshmemThreadPoolSize() const {
        return GetString("MC_UBSHMEM_THREAD_POOL_SIZE", "");
    }
    std::string GetUbshmemGetStreamTimeout() const {
        return GetString("MC_UBSHMEM_GET_STREAM_TIMEOUT", "");
    }
    std::string GetTcpSliceSize() const {
        return GetString("MC_TCP_SLICE_SIZE", "");
    }
    std::string GetTcpProto() const {
        return GetString("MC_TCP_PROTO", tcp_proto_);
    }
    std::string GetTcpStatusTimeoutSec() const {
        return GetString("MC_TCP_STATUS_TIMEOUT_SEC", tcp_status_timeout_sec_);
    }
    std::string GetTcpEnableConnectionPool() const {
        return GetString("MC_TCP_ENABLE_CONNECTION_POOL",
                         tcp_enable_connection_pool_);
    }
    std::string GetTangrtLibDir() const {
        return GetString("MC_TANGRT_LIB_DIR", "");
    }
    std::string GetTangrtRoot() const {
        return GetString("MC_TANGRT_ROOT", "/usr/local/tangrt");
    }
    std::string GetTangrtLibArch() const {
        return GetString("MC_TANGRT_LIB_ARCH", "linux-x86_64");
    }
    std::string GetTpuPjrtLib() const {
        return GetString("MC_TPU_PJRT_LIB", "libmooncake_tpu_pjrt.so");
    }
    std::string GetEpActiveQpsPerRank() const {
        return GetString("MOONCAKE_EP_ACTIVE_QPS_PER_RANK", "");
    }
    std::string GetEpDeviceFilter() const {
        return GetString("MOONCAKE_EP_DEVICE_FILTER", "");
    }
    std::string GetEpMacaP2pPairs() const {
        return GetString("MOONCAKE_EP_MACA_P2P_PAIRS", "");
    }
    std::string GetTeMetric() const { return te_metric_; }
    int GetTeMetricIntervalSeconds() const {
        return te_metric_interval_seconds_;
    }
    std::string GetTentConf() const { return GetString("MC_TENT_CONF", ""); }
    int GetTransferTimeout() const { return transfer_timeout_; }
    bool GetUseHipIpc() const { return GetBool("MC_USE_HIP_IPC", true); }
    bool GetUseNvlinkIpc() const { return GetBool("MC_USE_NVLINK_IPC", false); }
    // Returns true only when MC_USE_NVLINK_IPC is explicitly set to "0",
    // which opts in to fabric-memory mode on MNNVL clusters.
    bool GetNvlinkFabricMemEnabled() const {
        return GetString("MC_USE_NVLINK_IPC", "") == "0";
    }
    // HIP fabric memory follows the legacy transport matrix: either explicitly
    // disable HIP IPC, or explicitly opt into NVLink fabric memory with "0".
    bool GetHipFabricMemEnabled() const {
        return !GetUseHipIpc() || GetNvlinkFabricMemEnabled();
    }
    bool GetUseTent() const { return use_tent_; }
    bool GetUseTev1() const { return use_tev1_; }
    std::string GetYltLogLevel() const { return ylt_log_level_; }
    bool GetWithNvidiaPeermem() const { return with_nvidia_peermem_; }
    int GetEfaCqThreads() const { return efa_cq_threads_; }

    // These settings are intentionally read on every call. Tests and clients
    // may update them between independent runtime instances in one process.
    std::string GetStoreLocalHotCacheSize() const {
        return GetString("MC_STORE_LOCAL_HOT_CACHE_SIZE", "");
    }
    std::string GetStoreLocalHotBlockSize() const {
        return GetString("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "");
    }
    std::string GetStoreLocalHotCacheUseShm() const {
        return GetString("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "");
    }
    std::string GetStoreLocalHotAdmissionThreshold() const {
        return GetString("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", "");
    }
    std::string GetStoreReplicaScoring() const {
        return GetString("MC_STORE_REPLICA_SCORING", "");
    }
    bool GetMasterServiceSnapshotTestSkipCleanup() const {
        return IsSet("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
    }
    bool GetNofDebug() const { return GetBool("MC_NOF_DEBUG", false); }
    int GetNofDebugIntervalMs() const {
        int value = GetInt("MC_NOF_DEBUG_INTERVAL_MS", 1000);
        return value > 0 ? value : 1000;
    }
    int GetNofSubmitChunkBytes() const {
        int value = GetInt("MC_NOF_SUBMIT_CHUNK_BYTES", 1 << 17);
        return value > 0 ? value : 1 << 17;
    }
    int GetNofInflightBytesLimit() const {
        int value = GetInt("MC_NOF_INFLIGHT_BYTES_LIMIT", 1 << 25);
        return value > 0 ? value : 1 << 25;
    }
    int GetNofWorkers() const {
        int value = GetInt("MC_NOF_WORKERS", 4);
        return value > 0 ? value : 4;
    }
    std::string GetNofTrtype() const {
        return GetString("MC_NOF_TRTYPE", "RDMA");
    }
    std::string GetStoreNumaSocketId() const {
        return GetString("MC_STORE_NUMA_SOCKET_ID", "");
    }

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
    static bool IsSet(const char* name);

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
    std::string redis_username_;
    std::string redis_password_;
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
    std::string custom_topo_json_;
    std::string cxl_dev_path_;
    std::string cxl_dev_size_;
    bool disable_gpu_direct_rdma_;
    bool enable_mnnvl_;
    int handshake_port_;
    int hip_num_events_;
    int hip_num_streams_;
    bool intranode_nvlink_;
    bool legacy_rpc_port_binding_;
    std::string metadata_cluster_id_;
    size_t min_reg_size_;
    std::string ms_auto_disc_;
    std::string ms_filters_;
    std::string rpc_protocol_;
    int slice_timeout_;
    std::string store_cluster_id_;
    std::string store_hugepage_size_;
    std::string store_memcpy_;
    std::string store_use_hugepage_;
    std::string tcp_bind_address_;
    std::string tcp_proto_;
    std::string tcp_status_timeout_sec_;
    std::string tcp_enable_connection_pool_;
    std::string te_metric_;
    int te_metric_interval_seconds_;
    int transfer_timeout_;
    bool use_tent_;
    bool use_tev1_;
    std::string ylt_log_level_;
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
