#include <async_simple/coro/SyncAwait.h>
#include <csignal>
#include <ylt/easylog/record.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <algorithm>
#include <limits>
#include <string_view>

#include "real_client.h"
#include "dummy_client.h"
#include "common/client_buffer_allocation.h"
#include "uds_transport.h"
#include "common/result.h"
#include "common/scoped_vlog_timer.h"
#include "rpc_types.h"
#include "types.h"
#include "default_config.h"
#include "config.h"
#include "master_client.h"
#include "device/cuda_ipc_buffer.h"
#include "device/accelerator_registry.h"
#ifdef USE_ASCEND_DIRECT
#include "acl/acl_rt.h"
#include "ascend_allocator.h"
#endif

namespace {
std::vector<int> session_results(
    tl::expected<std::vector<int>, mooncake::ErrorCode> result,
    size_t expected_size) {
    if (!result) {
        return std::vector<int>(expected_size, mooncake::toInt(result.error()));
    }
    if (result->size() != expected_size) {
        LOG(ERROR) << "Dummy session RPC response size mismatch: expected "
                   << expected_size << ", got " << result->size();
        return std::vector<int>(expected_size,
                                mooncake::toInt(mooncake::ErrorCode::RPC_FAIL));
    }
    return std::move(*result);
}

size_t sum_value_sizes(const std::vector<std::span<const char>>& values) {
    size_t total = 0;
    for (const auto& value : values) {
        total += value.size_bytes();
    }
    return total;
}

size_t sum_sizes(const std::vector<size_t>& sizes) {
    size_t total = 0;
    for (size_t size : sizes) {
        total += size;
    }
    return total;
}

size_t sum_successful_sizes(const std::vector<int>& results,
                            const std::vector<size_t>& sizes) {
    size_t total = 0;
    for (size_t i = 0; i < results.size() && i < sizes.size(); ++i) {
        if (results[i] == 0) {
            total += sizes[i];
        }
    }
    return total;
}

size_t sum_successful_nested_sizes(
    const std::vector<int>& results,
    const std::vector<std::vector<size_t>>& nested_sizes) {
    size_t total = 0;
    for (size_t i = 0; i < results.size() && i < nested_sizes.size(); ++i) {
        if (results[i] == 0) {
            total += sum_sizes(nested_sizes[i]);
        }
    }
    return total;
}

size_t sum_successful_cuda_ipc_sizes(
    const std::vector<int>& results,
    const std::vector<mooncake::CudaIpcWriteRequest>& requests) {
    size_t total = 0;
    for (size_t i = 0; i < results.size() && i < requests.size(); ++i) {
        if (results[i] == 0) {
            total += requests[i].metadata.size + requests[i].payload.size;
        }
    }
    return total;
}

std::optional<std::vector<mooncake::CudaIpcBufferHandle>>
try_export_cuda_ipc_buffers(const std::vector<void*>& buffers,
                            const std::vector<size_t>& sizes) {
    if (buffers.size() != sizes.size() || buffers.empty()) return std::nullopt;

    std::vector<mooncake::CudaIpcBufferHandle> payloads;
    payloads.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        auto payload =
            mooncake::device::ExportCudaIpcBuffer(buffers[i], sizes[i]);
        if (!payload) return std::nullopt;
        payloads.push_back(*payload);
    }
    return payloads;
}

size_t sum_positive_results(const std::vector<int64_t>& results) {
    size_t total = 0;
    for (int64_t result : results) {
        if (result > 0) {
            total += static_cast<size_t>(result);
        }
    }
    return total;
}

size_t sum_positive_results(const std::vector<int>& results) {
    size_t total = 0;
    for (int result : results) {
        if (result > 0) {
            total += static_cast<size_t>(result);
        }
    }
    return total;
}

size_t sum_positive_ranges(
    const std::vector<std::vector<std::vector<int64_t>>>& results) {
    size_t total = 0;
    for (const auto& key_rows : results) {
        for (const auto& row : key_rows) {
            total += sum_positive_results(row);
        }
    }
    return total;
}

template <typename Result = int64_t, typename T>
std::vector<Result> expected_results_to_py(
    const std::vector<tl::expected<T, mooncake::ErrorCode>>& internal_results) {
    std::vector<Result> results;
    results.reserve(internal_results.size());
    for (const auto& result : internal_results) {
        results.push_back(static_cast<Result>(mooncake::to_py_ret(result)));
    }
    return results;
}

std::vector<uint64_t> void_ptrs_to_u64(const std::vector<void*>& ptrs) {
    std::vector<uint64_t> out;
    out.reserve(ptrs.size());
    for (void* p : ptrs) {
        out.push_back(reinterpret_cast<uint64_t>(p));
    }
    return out;
}

mooncake::RpcClientPool::PoolConfig dummy_rpc_pool_config() {
    mooncake::RpcClientPool::PoolConfig config;
    mooncake::detail::ApplyRpcTimeoutOverrides(
        config.client_config, mooncake::RpcTimeoutConfig::FromEnvironment());
    return config;
}

}  // namespace

namespace mooncake {

template <auto ServiceMethod>
using RpcMethod =
    std::integral_constant<decltype(ServiceMethod), ServiceMethod>;

template <auto ServiceMethod>
constexpr bool can_invoke_when_disconnected() {
    using Method = RpcMethod<ServiceMethod>;
    return std::is_same_v<Method, RpcMethod<&RealClient::ping>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::ping_persistent_dummy>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::service_ready_internal>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::is_shm_mapped_internal>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::ascend_shm_internal>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::ascend_ipc_shm_internal>> ||
           std::is_same_v<Method, RpcMethod<&RealClient::unmap_shm_internal>> ||
           std::is_same_v<Method,
                          RpcMethod<&RealClient::ascend_unmap_shm_internal>>;
}

static bool cuda_buffer_in_use(int rc) {
    return rc == toInt(ErrorCode::CUDA_BUFFER_IN_USE);
}

template <class T>
static bool cuda_buffer_in_use(const tl::expected<T, ErrorCode>& result) {
    return !result && result.error() == ErrorCode::CUDA_BUFFER_IN_USE;
}

template <class T>
static bool cuda_buffer_in_use(const std::vector<T>& results) {
    return std::any_of(results.begin(), results.end(), [](const auto& item) {
        return cuda_buffer_in_use(item);
    });
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> DummyClient::invoke_rpc(Args&&... args) {
    RpcDrainGuard::ScopedCall inflight(rpc_drain_);
    if (!inflight.ok()) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto pool = client_accessor_.GetClientPool();
    if (!pool) return tl::unexpected(ErrorCode::RPC_FAIL);

    if constexpr (!can_invoke_when_disconnected<ServiceMethod>()) {
        if (!connected_.load()) {
            LOG(ERROR) << "Dummy Client not connected";
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
    }

    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Dummy Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>> DummyClient::invoke_batch_rpc(
    size_t input_size, Args&&... args) {
    return invoke_batch_rpc<ServiceMethod, ResultType>(
        std::span<const PreparedBuffer>{}, input_size,
        std::forward<Args>(args)...);
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>> DummyClient::invoke_batch_rpc(
    std::span<const PreparedBuffer> buffers, size_t input_size,
    Args&&... args) {
    using Results = std::vector<tl::expected<ResultType, ErrorCode>>;
    auto response =
        invoke_rpc<ServiceMethod, Results>(std::forward<Args>(args)...);
    if (response &&
        (!cuda_ipc_persistent_registration_ || !cuda_buffer_in_use(*response)))
        return std::move(*response);
    const auto error =
        response ? ErrorCode::CUDA_BUFFER_IN_USE : response.error();
    return Results(input_size,
                   tl::unexpected(buffer_rpc_error(error, buffers)));
}

DummyClient::DummyClient(std::optional<bool> cuda_ipc_persistent_registration)
    : cuda_ipc_persistent_registration_([cuda_ipc_persistent_registration] {
          if (cuda_ipc_persistent_registration.has_value()) {
              return *cuda_ipc_persistent_registration;
          }
          const char* value =
              std::getenv("MC_CUDA_IPC_PERSISTENT_REGISTRATION");
          return value != nullptr && std::string_view(value) == "1";
      }()),
      client_accessor_(GetStoreRpcClientIoContextPool(),
                       dummy_rpc_pool_config()),
      client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels({{"client_mode", "dummy"}}),
                                    false)) {
    // Initialize logging severity (leave as before)
    mooncake::init_ylt_log_level();
}

DummyClient::~DummyClient() {
    // Teardown-order invariant (review #3943): an in-flight RPC never touches
    // the members tearDownAll() resets. The RPC path holds a ScopedCall
    // across the whole round trip and otherwise touches only the client pool
    // (kept alive process-wide by the registry), atomic flags like
    // connected_, by-value arguments, and the drain's shared State.
    // shm_helper_, the registered-buffer tables, the local/hot-cache mappings
    // and the ping thread are only ever accessed synchronously on caller
    // threads, outside the drain's scope; a caller racing ~DummyClient is a
    // usage-level data race no teardown order can fix.
    //
    // The reverse order is impossible: unregister_shm() is itself an RPC and
    // the ping thread is only joined inside tearDownAll(), so draining first
    // would reject the unmap and spin the reconnection loop for the whole
    // wait (#3943 review).
    tearDownAll();
    // Never release the pool under a suspended request coroutine (#3909).
    if (!rpc_drain_.drain_for(std::chrono::seconds(30))) {
        LOG(ERROR) << "DummyClient teardown: RPCs still in flight after 30s "
                      "drain; releasing the pool regardless";
    }
}

void DummyClient::ObserveTransferMetric(TransferOperationKind kind,
                                        const char* op_name, size_t bytes,
                                        uint64_t latency_us, bool batch) {
    if (!metrics_) {
        return;
    }
    metrics_->ObserveTransferOperation(kind, op_name, bytes, latency_us);
    if (kind == TransferOperationKind::kRead) {
        metrics_->transfer_metric.total_read_bytes.inc(bytes);
        if (batch) {
            metrics_->transfer_metric.batch_get_latency_us.observe(latency_us);
        } else {
            metrics_->transfer_metric.get_latency_us.observe(latency_us);
        }
    } else {
        metrics_->transfer_metric.total_write_bytes.inc(bytes);
        if (batch) {
            metrics_->transfer_metric.batch_put_latency_us.observe(latency_us);
        } else {
            metrics_->transfer_metric.put_latency_us.observe(latency_us);
        }
    }
}

ErrorCode DummyClient::connect(const std::string& server_address) {
    ScopedVLogTimer timer(1, "DummyClient::Connect");
    timer.LogRequest("real_client_addr=", server_address);

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ != server_address) {
        client_accessor_.GetOrCreateClientPool(server_address);
        client_addr_param_ = server_address;
    }
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result = invoke_rpc<&RealClient::service_ready_internal, void>();
    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        return result.error();
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    remote_cleanup_pending_ = true;
    connected_.store(true);
    return ErrorCode::OK;
}

int DummyClient::register_ascend_shm(const ShmHelper::ShmSegment* shm,
                                     bool is_local) {
#ifdef USE_ASCEND_DIRECT
    const auto dummy_base_addr = reinterpret_cast<uint64_t>(shm->base_addr);
    auto mapped_result = invoke_rpc<&RealClient::is_shm_mapped_internal, bool>(
        dummy_base_addr, client_id_);
    if (!mapped_result.has_value()) {
        LOG(WARNING) << "Failed to query real-side shared memory mapping, addr="
                     << shm->base_addr;
        return -1;
    }
    if (mapped_result.value()) {
        LOG(INFO) << "Real-side shared memory mapping already exists, addr="
                  << shm->base_addr << ", size=" << shm->size;
        return 0;
    }

    // Detect memory type: device memory uses IPC sharing
    aclrtPtrAttributes attributes;
    auto ret = aclrtPointerGetAttributes(shm->base_addr, &attributes);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get pointer attributes, ret=" << ret;
        return -1;
    }
    // all device mem shared by ipc
    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        constexpr size_t kIPCKeyLen = 65;
        char ipc_key[kIPCKeyLen] = {0};
        ret = aclrtIpcMemGetExportKey(
            shm->base_addr, shm->size, ipc_key, kIPCKeyLen,
            ACL_RT_IPC_MEM_EXPORT_FLAG_DISABLE_PID_VALIDATION);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtIpcMemGetExportKey failed, ret=" << ret
                       << ", errmsg: " << aclGetRecentErrMsg();
            return -1;
        }

        std::string ipc_key_bytes(ipc_key, kIPCKeyLen);
        auto map_ret = invoke_rpc<&RealClient::ascend_ipc_shm_internal, void>(
            dummy_base_addr, shm->size, is_local, ipc_key_bytes, device_id_,
            client_id_);
        if (!map_ret.has_value()) {
            LOG(ERROR) << "Failed to map IPC buffer on real side";
            return -1;
        }
        LOG(INFO) << "Registered device memory via IPC, addr=" << shm->base_addr
                  << ", size=" << shm->size << ", device_id=" << device_id_;
        return 0;
    }
    if (!globalConfig().ascend_use_fabric_mem) {
        // Host: memfd + mmap (ShmHelper); register with Real like non-agent GPU
        // path.
        if (shm->fd < 0) {
            LOG(ERROR)
                << "Host POSIX shared memory requires memfd-backed allocation "
                   "(use ShmHelper::allocate / alloc_from_mem_pool)";
            return -1;
        }
        return register_shm_via_ipc(shm, is_local);
    }

    // Fabric host mem shared by vmm
    aclrtDrvMemHandle physical_handle =
        ascend_get_physical_handle_from_va(shm->base_addr);
    if (physical_handle == nullptr) {
        LOG(ERROR) << "Failed to get physical handle for va (memory must be "
                      "allocated via ascend_allocate_vmm_memory_direct)";
        return -1;
    }

    aclrtMemFabricHandle export_handle = {};
    ret = aclrtMemExportToShareableHandleV2(
        physical_handle, ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION,
        ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, &export_handle);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to export shareable handle, ret=" << ret;
        return -1;
    }

    std::string handle_bytes(reinterpret_cast<char*>(&export_handle),
                             sizeof(export_handle));
    auto map_ret = invoke_rpc<&RealClient::ascend_shm_internal, void>(
        dummy_base_addr, shm->size, is_local, handle_bytes, device_id_,
        client_id_);
    if (!map_ret.has_value()) {
        LOG(ERROR) << "Failed to map VMM buffer on real side";
        return -1;
    }
    LOG(INFO) << "Registered memory suc, addr=" << shm->base_addr
              << ", size=" << shm->size << ", device_id=" << device_id_;
#endif
    return 0;
}

int DummyClient::map_shm(const ShmHelper::ShmSegment* shm, bool local) {
    return globalConfig().ascend_agent_mode ? register_ascend_shm(shm, local)
                                            : register_shm_via_ipc(shm, local);
}

int DummyClient::register_shm_via_ipc(const ShmHelper::ShmSegment* shm,
                                      bool is_local) {
    if (shm->fd < 0) {
        LOG(ERROR) << "Invalid shm_fd during IPC registration";
        return -1;
    }

    UdsConnector connector(ipc_socket_path_);
    LOG(INFO) << "Connecting to IPC socket: " << ipc_socket_path_;
    auto connection_result = connector.connect();
    if (!connection_result) {
        LOG(ERROR) << "Failed to connect IPC socket '" << ipc_socket_path_
                   << "': " << connection_result.error();
        // This is expected if RealClient is down
        return -1;
    }
    auto connection = std::move(connection_result.value());

    // Send request type first
    IpcRequestType type = IPC_SHM_REGISTER;
    if (connection->sendRaw(&type, sizeof(type)) < 0) {
        LOG(ERROR) << "Failed to send IPC request type: " << strerror(errno);
        return -1;
    }

    ShmRegisterRequest req;
    req.client_id_first = client_id_.first;
    req.client_id_second = client_id_.second;
    req.dummy_base_addr = reinterpret_cast<uintptr_t>(shm->base_addr);
    req.shm_size = shm->size;
    req.device_id = globalConfig().ascend_agent_mode ? device_id_
                                                     : kInvalidPhysicalDeviceId;
    req.is_local_buffer = is_local;

    if (connection->sendFd(shm->fd, &req, sizeof(req)) < 0) {
        LOG(ERROR) << "Failed to send FD to RealClient: " << strerror(errno);
        return -1;
    }

    int status = -1;
    if (connection->recvRaw(&status, sizeof(status)) < 0) {
        LOG(ERROR) << "Failed to receive response from RealClient";
        return -1;
    }

    if (status != 0) {
        LOG(ERROR) << "RealClient failed to map shared memory, error code: "
                   << status;
        return -1;
    }

    LOG(INFO) << "Successfully registered SHM via IPC, base: "
              << shm->base_addr;
    return 0;
}

int DummyClient::setup_dummy(size_t mem_pool_size, size_t local_buffer_size,
                             const std::string& server_address,
                             const std::string& ipc_socket_path) {
    if (cuda_transfer_state_ != CudaTransferState::kActive)
        return toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    const char* use_fabric_mem_env = getEnvWithFallback(
        "MC_ASCEND_ENABLE_USE_FABRIC_MEM", "ASCEND_ENABLE_USE_FABRIC_MEM");
    if (use_fabric_mem_env) {
        std::string val(use_fabric_mem_env);
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (val == "1" || val == "true") {
            globalConfig().ascend_use_fabric_mem = true;
        }
    }

    void* base_addr = nullptr;
    ErrorCode err = connect(server_address);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to real client";
        return -1;
    }

#ifdef USE_ASCEND_DIRECT
    // just set to true when USE_ASCEND_DIRECT
    globalConfig().ascend_agent_mode = true;
    int32_t logic_dev = 0;
    auto acl_ret = aclrtGetDevice(&logic_dev);
    if (acl_ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get current device, ret=" << acl_ret;
        return -1;
    }
    acl_ret = aclrtGetPhyDevIdByLogicDevId(logic_dev, &device_id_);
    if (acl_ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to get physical device id, ret=" << acl_ret
                   << ", errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    LOG(INFO) << "Setup dummy: logic_dev=" << logic_dev
              << " physical_dev=" << device_id_;
#endif

    ipc_socket_path_ = ipc_socket_path;
    shm_helper_ = ShmHelper::getInstance();
    if (cuda_ipc_persistent_registration_) {
        auto result = ping_daemon(true);
        if (!result || result->client_status == ClientStatus::NEED_REMOUNT) {
            LOG(ERROR) << "Failed to establish persistent DummyClient context";
            return -1;
        }
    }
    if (local_buffer_size > 0) {
        try {
            base_addr = shm_helper_->allocate(local_buffer_size);
            local_buffer_base_ = base_addr;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to allocate shared memory: " << e.what();
            return -1;
        }
        // Attempt registration for the primary segment
        auto local_buffer_shm = shm_helper_->get_shm(base_addr);
        if (!local_buffer_shm) {
            LOG(ERROR) << "Failed to get shm segment for base address";
            shm_helper_->free(base_addr);
            local_buffer_base_ = nullptr;
            return -1;
        }

        if (map_shm(local_buffer_shm.get(), true) != 0) {
            shm_helper_->free(base_addr);
            local_buffer_base_ = nullptr;
            return -1;
        }
        registered_shms_.emplace(base_addr, std::move(local_buffer_shm));

        // Best-effort: request hot cache shm from real client
        if (request_hot_cache_fd() != 0) {
            LOG(INFO)
                << "Hot cache shm not available (real client may not have it)";
        }
    }

    ping_running_ = true;
    ping_thread_ = std::thread([this]() mutable { this->ping_thread_main(); });
    return 0;
}

int DummyClient::tearDownAll() {
    // Reconnection iterates SHM and CUDA registrations. Join it before
    // unregistering buffers or releasing their mappings and FDs.
    ping_running_ = false;
    if (ping_thread_.joinable()) {
        ping_thread_.join();
    }

    int cleanup_status = 0;
    {
        std::lock_guard<std::mutex> lock(registered_cuda_buffers_mutex_);
        if (cuda_transfer_pending() || !registered_cuda_buffers_.empty() ||
            remote_cleanup_pending_)
            cleanup_status = unmap_client_locked();
        if (cleanup_status == 0) remote_cleanup_pending_ = false;
        connected_.store(false);
    }
    if (local_buffer_base_) {
        std::lock_guard lock(shm_mutex_);
        if (registered_shms_.contains(local_buffer_base_)) {
            LOG(ERROR)
                << "Retaining dummy local shared memory after failed unmap";
        } else if (shm_helper_ && shm_helper_->get_shm(local_buffer_base_) &&
                   shm_helper_->free(local_buffer_base_) != 0) {
            LOG(ERROR) << "Failed to free dummy local shared memory";
        } else {
            local_buffer_base_ = nullptr;
        }
    }

    // Cleanup hot cache shm mapping
    if (hot_cache_base_) {
        munmap(hot_cache_base_, hot_cache_size_);
        hot_cache_base_ = nullptr;
        hot_cache_size_ = 0;
    }
    if (hot_cache_fd_ >= 0) {
        close(hot_cache_fd_);
        hot_cache_fd_ = -1;
    }

    last_ping_healthy_.store(false);
    std::lock_guard<std::mutex> lock(registered_external_buffers_mutex_);
    registered_external_buffers_.clear();
#if defined(USE_ASCEND_DIRECT)
    {
        std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
        registered_device_buffers_.clear();
    }
#endif
    return cleanup_status;
}

std::optional<BufferHandle> DummyClient::allocate_client_buffer(size_t size) {
    auto result = invoke_rpc<&RealClient::allocate_buffer_dummy,
                             std::tuple<uint64_t, size_t>>(size, client_id_);
    if (!result.has_value()) {
        return std::nullopt;
    }

    auto [dummy_addr, allocated_size] = result.value();
    void* local_ptr = reinterpret_cast<void*>(dummy_addr);
    auto release = [this, dummy_addr]() {
        (void)invoke_rpc<&RealClient::release_buffer_dummy, void>(dummy_addr,
                                                                  client_id_);
    };
    return std::make_optional<BufferHandle>(local_ptr, allocated_size,
                                            std::move(release));
}

bool DummyClient::is_dummy_shm_buffer(void* buffer, size_t size,
                                      bool* uses_pool) const {
    if (!buffer || !shm_helper_) return false;
    auto shm = shm_helper_->get_shm(buffer);
    if (!shm || is_device_buffer(buffer)) return false;
    std::lock_guard lock(shm_mutex_);
    auto it = registered_shms_.find(shm->base_addr);
    if (it == registered_shms_.end() || it->second != shm) return false;
    if (uses_pool && shm->base_addr == local_buffer_base_) *uses_pool = true;
    const auto offset = reinterpret_cast<uintptr_t>(buffer) -
                        reinterpret_cast<uintptr_t>(shm->base_addr);
    return size <= shm->size - offset;
}

bool DummyClient::is_device_buffer(void* buffer) const {
    return device::GetAcceleratorRegistry()
               .RuntimeAccelerators()
               .FindDeviceForPointer(buffer) != nullptr;
}

std::optional<size_t> DummyClient::external_buffer_remaining(
    void* buffer, bool* direct_cuda) const {
    const uintptr_t address = reinterpret_cast<uintptr_t>(buffer);
    {
        std::lock_guard<std::mutex> lock(registered_external_buffers_mutex_);
        for (const auto& [base, registration] : registered_external_buffers_) {
            if (address >= base && address - base <= registration.size) {
                return registration.size - (address - base);
            }
        }
    }

    if (auto remaining = registered_cuda_buffer_remaining(buffer)) {
        if (direct_cuda) *direct_cuda = true;
        return remaining;
    }

#if defined(USE_ASCEND_DIRECT)
    // Ascend device allocations are registered through register_ascend_shm()
    // and tracked separately so they can be restored after reconnect.  Treat
    // those mappings as registered external buffers for all transfer paths.
    {
        std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
        for (const auto& [base, registration] : registered_device_buffers_) {
            if (address >= base && address - base <= registration.size) {
                return registration.size - (address - base);
            }
        }
    }
#endif

    return std::nullopt;
}

bool DummyClient::is_registered_buffer(void* buffer, size_t size,
                                       bool* direct_cuda) const {
    if (buffer == nullptr) return size == 0;
    auto remaining = external_buffer_remaining(buffer, direct_cuda);
    return remaining.has_value() && size <= *remaining;
}

bool DummyClient::can_export_cuda_buffers(
    const std::vector<void*>& buffers, const std::vector<size_t>& sizes) const {
    if (cuda_ipc_persistent_registration_ || buffers.size() != sizes.size())
        return false;
    for (size_t i = 0; i < buffers.size(); ++i)
        if (!is_registered_buffer(buffers[i], sizes[i])) return false;
    return true;
}

DummyClient::BufferRegistrationAction DummyClient::retain_buffer_registration(
    BufferRegistrationMap& registrations, uintptr_t base, size_t size) {
    const uintptr_t end = base + size;
    for (const auto& [other_base, registration] : registrations) {
        const uintptr_t other_end = other_base + registration.size;
        if (base < other_end && other_base < end &&
            (base != other_base || size != registration.size)) {
            return BufferRegistrationAction::kReject;
        }
    }

    auto [it, inserted] =
        registrations.emplace(base, ExternalBufferRegistration{size, 1});
    if (inserted) return BufferRegistrationAction::kFirst;
    if (it->second.references == std::numeric_limits<size_t>::max()) {
        return BufferRegistrationAction::kReject;
    }
    ++it->second.references;
    return BufferRegistrationAction::kRetained;
}

DummyClient::BufferReleaseAction DummyClient::release_buffer_registration(
    BufferRegistrationMap& registrations, uintptr_t base) {
    auto it = registrations.find(base);
    if (it == registrations.end()) return BufferReleaseAction::kReject;
    if (it->second.references == 1) return BufferReleaseAction::kFinal;
    --it->second.references;
    return BufferReleaseAction::kRetained;
}

int DummyClient::register_external_buffer(void* buffer, size_t size) {
    if (buffer == nullptr || size == 0) return -1;

    const uintptr_t base = reinterpret_cast<uintptr_t>(buffer);
    if (size > std::numeric_limits<uintptr_t>::max() - base) return -1;

    std::lock_guard<std::mutex> lock(registered_external_buffers_mutex_);
    if (retain_buffer_registration(registered_external_buffers_, base, size) ==
        BufferRegistrationAction::kReject) {
        LOG(ERROR) << "Invalid or overlapping external buffer registration";
        return -1;
    }
    return 0;
}

int DummyClient::unregister_external_buffer(void* buffer) {
    if (buffer == nullptr) return -1;

    std::lock_guard<std::mutex> lock(registered_external_buffers_mutex_);
    const uintptr_t base = reinterpret_cast<uintptr_t>(buffer);
    const auto action =
        release_buffer_registration(registered_external_buffers_, base);
    if (action == BufferReleaseAction::kReject) return -1;
    if (action == BufferReleaseAction::kFinal)
        registered_external_buffers_.erase(base);
    return 0;
}

std::optional<size_t> DummyClient::registered_cuda_buffer_remaining(
    void* buffer) const {
    if (!cuda_ipc_persistent_registration_) return std::nullopt;
    const auto address = reinterpret_cast<uintptr_t>(buffer);
    std::lock_guard<std::mutex> lock(registered_cuda_buffers_mutex_);
    if (!connected_.load()) return std::nullopt;
    for (const auto& [base, registration] : registered_cuda_buffers_) {
        if (registration.state == CudaRegistrationState::kRegistered &&
            address >= base && address - base < registration.size) {
            return registration.size - (address - base);
        }
    }
    return std::nullopt;
}

DummyClient::CudaBufferIpcResult DummyClient::cuda_buffer_ipc_request(
    IpcRequestType type, uint64_t dummy_addr, const CudaIpcBufferHandle& handle,
    int fd) {
    RpcDrainGuard::ScopedCall inflight(rpc_drain_);
    if (!inflight.ok()) return {toInt(ErrorCode::RPC_FAIL), false};

    UdsConnector connector(ipc_socket_path_);
    auto connection_result = connector.connect();
    if (!connection_result) {
        LOG(ERROR) << "Failed to connect CUDA registration IPC socket: "
                   << connection_result.error();
        return {toInt(ErrorCode::RPC_FAIL), false};
    }
    auto connection = std::move(*connection_result);
    if (auto timeout = connection->setRecvTimeout(std::chrono::seconds(30));
        !timeout) {
        LOG(ERROR) << "Failed to set CUDA registration response timeout: "
                   << timeout.error();
        return {toInt(ErrorCode::RPC_FAIL), false};
    }
    if (connection->sendRaw(&type, sizeof(type)) < 0) {
        LOG(ERROR) << "Failed to send CUDA registration IPC request type";
        return {toInt(ErrorCode::RPC_FAIL), false};
    }
    CudaRegisterRequest request{};
    request.client_id_first = client_id_.first;
    request.client_id_second = client_id_.second;
    request.dummy_addr = dummy_addr;
    request.handle = handle;
    const int sent = fd >= 0 ? connection->sendFd(fd, &request, sizeof(request))
                             : connection->sendRaw(&request, sizeof(request));
    if (sent < 0) {
        LOG(ERROR) << "Failed to send CUDA registration IPC request";
        return {toInt(ErrorCode::RPC_FAIL), false};
    }
    int status = toInt(ErrorCode::RPC_FAIL);
    if (connection->recvRaw(&status, sizeof(status)) < 0) {
        LOG(ERROR) << "Failed to receive CUDA registration response";
        return {toInt(ErrorCode::RPC_FAIL), true};
    }
    return {status, true};
}

DummyClient::CudaBufferIpcResult DummyClient::register_cuda_via_ipc(
    uint64_t dummy_addr, const CudaIpcBufferHandle& handle) {
    auto dmabuf = device::ExportCudaIpcDmabuf(
        handle, reinterpret_cast<void*>(dummy_addr));
    if (!dmabuf) return {toInt(dmabuf.error()), false};
    auto result =
        cuda_buffer_ipc_request(IPC_CUDA_REGISTER, dummy_addr, handle, *dmabuf);
    close(*dmabuf);
    return result;
}

ErrorCode DummyClient::drain_failed_cuda_transfer() {
    std::lock_guard lock(registered_cuda_buffers_mutex_);
    if (cuda_transfer_state_ == CudaTransferState::kDrained)
        return ErrorCode::RPC_FAIL;
    cuda_transfer_state_ = CudaTransferState::kPending;
    ping_running_ = false;
    connected_.store(false);
    return unmap_client_locked() == 0 ? ErrorCode::RPC_FAIL
                                      : ErrorCode::CUDA_BUFFER_IN_USE;
}

ErrorCode DummyClient::buffer_rpc_error(
    ErrorCode error, std::span<const PreparedBuffer> buffers) {
    if (error == ErrorCode::CUDA_BUFFER_IN_USE ||
        ((error == ErrorCode::RPC_FAIL || error == ErrorCode::RPC_TIMEOUT) &&
         std::any_of(buffers.begin(), buffers.end(),
                     [](const auto& item) { return item.direct_cuda; }))) {
        return drain_failed_cuda_transfer();
    }
    return error;
}

int DummyClient::register_cuda_buffer(void* buffer, size_t size) {
    const auto base = reinterpret_cast<uintptr_t>(buffer);
    if (size == 0 || size > std::numeric_limits<uintptr_t>::max() - base) {
        return toInt(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> lock(registered_cuda_buffers_mutex_);
    if (cuda_transfer_pending()) return toInt(ErrorCode::CUDA_BUFFER_IN_USE);
    if (cuda_transfer_state_ == CudaTransferState::kDrained)
        return toInt(ErrorCode::RPC_FAIL);
    if (!connected_.load()) return toInt(ErrorCode::RPC_FAIL);
    auto it = registered_cuda_buffers_.find(base);
    if (it != registered_cuda_buffers_.end() && it->second.references == 0) {
        // A failed initial request may have installed the MR. Retire it before
        // exporting the allocation currently occupying this address.
        auto result = cuda_buffer_ipc_request(IPC_CUDA_UNREGISTER, base,
                                              it->second.handle);
        if (result.status != 0) return result.status;
        registered_cuda_buffers_.erase(it);
        it = registered_cuda_buffers_.end();
    }
    if (it != registered_cuda_buffers_.end()) {
        if (it->second.size != size ||
            it->second.state == CudaRegistrationState::kReleasing ||
            it->second.references == std::numeric_limits<size_t>::max()) {
            return toInt(ErrorCode::INVALID_PARAMS);
        }
        if (it->second.state == CudaRegistrationState::kRegistered) {
            ++it->second.references;
            return 0;
        }
    } else {
        for (const auto& [other_base, registration] :
             registered_cuda_buffers_) {
            if (base < other_base + registration.size &&
                other_base < base + size) {
                return toInt(ErrorCode::INVALID_PARAMS);
            }
        }
        auto handle = device::ExportCudaIpcBuffer(buffer, size);
        if (!handle) return toInt(handle.error());
        it = registered_cuda_buffers_
                 .emplace(base, CudaBufferRegistration{size, *handle})
                 .first;
    }
    auto result = register_cuda_via_ipc(base, it->second.handle);
    if (result.status == 0) {
        it->second.state = CudaRegistrationState::kRegistered;
        ++it->second.references;
    } else if (!result.sent && it->second.references == 0) {
        registered_cuda_buffers_.erase(it);
    }
    return result.status;
}

std::optional<int> DummyClient::unregister_cuda_buffer(void* buffer) {
    const auto base = reinterpret_cast<uintptr_t>(buffer);
    std::lock_guard<std::mutex> lock(registered_cuda_buffers_mutex_);
    auto it = registered_cuda_buffers_.find(base);
    if (it == registered_cuda_buffers_.end()) return std::nullopt;
    if (cuda_transfer_state_ == CudaTransferState::kPending)
        return toInt(ErrorCode::CUDA_BUFFER_IN_USE);
    if (it->second.references > 1) {
        --it->second.references;
        return 0;
    }
    // The daemon may release the MR even when we lose its reply. Keep the
    // handle for a retry and stop exposing this address to transfers/replay.
    it->second.state = CudaRegistrationState::kReleasing;
    auto result =
        cuda_buffer_ipc_request(IPC_CUDA_UNREGISTER, base, it->second.handle);
    if (result.status == 0) registered_cuda_buffers_.erase(it);
    return result.status;
}

int DummyClient::reregister_cuda_buffers() {
    std::lock_guard<std::mutex> lock(registered_cuda_buffers_mutex_);
    if (cuda_transfer_state_ != CudaTransferState::kActive)
        return toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto last_ping = std::chrono::steady_clock::now();
    for (auto& [base, registration] : registered_cuda_buffers_) {
        if (registration.references == 0 ||
            registration.state == CudaRegistrationState::kReleasing) {
            continue;
        }
        registration.state = CudaRegistrationState::kUnconfirmed;
        auto result = register_cuda_via_ipc(base, registration.handle);
        if (result.status != 0) return result.status;
        registration.state = CudaRegistrationState::kRegistered;
        // Replay runs on the heartbeat thread. Renew the context while a
        // large pool's multiple registrations are being restored.
        if (std::chrono::steady_clock::now() - last_ping >=
            std::chrono::seconds(DEFAULT_CLIENT_LIVE_TTL_SEC / 2)) {
            auto ping = ping_daemon();
            if (!ping || ping->client_status != ClientStatus::OK)
                return toInt(ErrorCode::RPC_FAIL);
            last_ping = std::chrono::steady_clock::now();
        }
    }
    return 0;
}

#if defined(USE_ASCEND_DIRECT)
std::optional<size_t> DummyClient::registered_ascend_buffer_remaining(
    void* buffer) const {
    const uintptr_t address = reinterpret_cast<uintptr_t>(buffer);
    std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
    for (const auto& [base, registration] : registered_device_buffers_) {
        if (address >= base && address - base <= registration.size) {
            return registration.size - (address - base);
        }
    }
    return std::nullopt;
}
#endif

std::optional<DummyClient::PreparedBuffer> DummyClient::prepare_buffer(
    void* buffer, size_t size, bool copy_to_staging) {
    if (buffer == nullptr && size != 0) return std::nullopt;
    if (buffer == nullptr) {
        return PreparedBuffer{nullptr, nullptr, 0, nullptr};
    }
    if (auto remaining = registered_cuda_buffer_remaining(buffer)) {
        if (size > *remaining) return std::nullopt;
        return PreparedBuffer{buffer, buffer, size, nullptr, true};
    }
#if defined(USE_ASCEND_DIRECT)
    if (auto remaining = registered_ascend_buffer_remaining(buffer)) {
        if (size > *remaining) return std::nullopt;
        return PreparedBuffer{buffer, buffer, size, nullptr};
    }
#endif
    if (is_dummy_shm_buffer(buffer, size)) {
        return PreparedBuffer{buffer, buffer, size, nullptr};
    }
    auto remaining = external_buffer_remaining(buffer);
    if (!remaining.has_value() || size > *remaining) return std::nullopt;

    auto allocation = allocate_client_buffer(std::max<size_t>(size, 1));
    if (!allocation) return std::nullopt;
    auto staging = std::make_unique<BufferHandle>(std::move(*allocation));
    if (copy_to_staging) {
        const auto& runtime =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        if (!runtime.CopyToHost(staging->ptr(), buffer, size))
            return std::nullopt;
    }
    return PreparedBuffer{buffer, staging->ptr(), size, std::move(staging)};
}

std::optional<DummyClient::PreparedBuffer>
DummyClient::prepare_ranged_read_buffer(
    void* buffer, std::vector<std::vector<size_t>>& dst_offsets,
    const std::vector<std::vector<size_t>>& sizes) {
    if (buffer == nullptr) {
        return PreparedBuffer{nullptr, nullptr, 0, nullptr};
    }
    if (auto remaining = registered_cuda_buffer_remaining(buffer)) {
        return prepare_buffer(buffer, *remaining, false);
    }
#if defined(USE_ASCEND_DIRECT)
    if (auto remaining = registered_ascend_buffer_remaining(buffer)) {
        return prepare_buffer(buffer, *remaining, false);
    }
#endif
    if (auto remaining = external_buffer_remaining(buffer)) {
        const size_t capacity = *remaining;
        size_t staging_size = 0;
        const size_t row_count = std::min(dst_offsets.size(), sizes.size());
        for (size_t i = 0; i < row_count; ++i) {
            const size_t fragment_count =
                std::min(dst_offsets[i].size(), sizes[i].size());
            for (size_t j = 0; j < fragment_count; ++j) {
                const size_t offset = dst_offsets[i][j];
                const size_t fragment_size = sizes[i][j];
                if (offset > capacity || fragment_size > capacity - offset) {
                    // Keep invalid destinations invalid after compaction, even
                    // for zero-length fragments. RealClient reports each error.
                    dst_offsets[i][j] = std::numeric_limits<size_t>::max();
                    continue;
                }
                if (fragment_size >
                    std::numeric_limits<size_t>::max() - staging_size) {
                    return std::nullopt;
                }
                dst_offsets[i][j] = staging_size;
                staging_size += fragment_size;
            }
        }
        // Reserve only fragment bytes, not the unused prefix or sparse gaps.
        // The original destination capacity remains the copy-back bound.
        auto allocation =
            allocate_client_buffer(std::max<size_t>(staging_size, 1));
        if (!allocation) return std::nullopt;
        auto staging = std::make_unique<BufferHandle>(std::move(*allocation));
        return PreparedBuffer{buffer, staging->ptr(), capacity,
                              std::move(staging)};
    }
    if (shm_helper_ == nullptr) return std::nullopt;
    auto shm = shm_helper_->get_shm(buffer);
    if (shm == nullptr) return std::nullopt;

    const uintptr_t base = reinterpret_cast<uintptr_t>(shm->base_addr);
    const uintptr_t address = reinterpret_cast<uintptr_t>(buffer);
    if (address < base || address - base > shm->size) return std::nullopt;
    return prepare_buffer(buffer, shm->size - (address - base), false);
}

bool DummyClient::copy_from_staging(const PreparedBuffer& buffer, size_t size,
                                    size_t offset,
                                    size_t staging_offset) const {
    if (buffer.staging == nullptr || size == 0) return true;
    if (offset > buffer.size || size > buffer.size - offset ||
        staging_offset > buffer.staging->size() ||
        size > buffer.staging->size() - staging_offset)
        return false;
    const auto& runtime =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    auto* destination = static_cast<uint8_t*>(buffer.original) + offset;
    auto* source =
        static_cast<uint8_t*>(buffer.staging->ptr()) + staging_offset;
    return runtime.CopyFromHost(destination, source, size);
}

bool DummyClient::prepare_buffers(const std::vector<void*>& buffers,
                                  const std::vector<size_t>& sizes, bool write,
                                  std::vector<uint64_t>& addresses,
                                  std::vector<PreparedBuffer>& prepared) {
    if (buffers.size() != sizes.size()) return false;
    addresses.reserve(buffers.size());
    prepared.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        auto item = prepare_buffer(buffers[i], sizes[i], write);
        if (!item) return false;
        addresses.push_back(reinterpret_cast<uint64_t>(item->dummy));
        prepared.push_back(std::move(*item));
    }
    return true;
}

std::optional<DummyClient::PreparedMultiBuffers>
DummyClient::prepare_multi_buffers(
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes, bool write) {
    if (all_buffers.size() != all_sizes.size()) return std::nullopt;
    PreparedMultiBuffers prepared;
    prepared.dummy_buffers.resize(all_buffers.size());
    size_t total_buffers = 0;
    for (const auto& buffers : all_buffers) total_buffers += buffers.size();
    prepared.buffers.reserve(total_buffers);
    for (size_t i = 0; i < all_buffers.size(); ++i) {
        if (!prepare_buffers(all_buffers[i], all_sizes[i], write,
                             prepared.dummy_buffers[i], prepared.buffers))
            return std::nullopt;
    }
    return prepared;
}

int64_t DummyClient::unregister_shm() {
    std::lock_guard lock(registered_cuda_buffers_mutex_);
    return unmap_client_locked();
}

// The daemon's exclusive unmap waits for active handlers and removes the
// mappings used to translate requests that have not started yet.
int DummyClient::unmap_client_locked() {
    std::lock_guard lock(shm_mutex_);
    if (cuda_transfer_pending() || !registered_cuda_buffers_.empty()) {
        for (auto& [address, registration] : registered_cuda_buffers_)
            registration.state = CudaRegistrationState::kReleasing;
        const int rc =
            cuda_buffer_ipc_request(IPC_CUDA_UNMAP_CLIENT, 0, {}).status;
        if (rc != 0)
            return cuda_transfer_pending()
                       ? toInt(ErrorCode::CUDA_BUFFER_IN_USE)
                       : rc;
        registered_cuda_buffers_.clear();
        if (cuda_transfer_pending()) {
            cuda_transfer_state_ = CudaTransferState::kDrained;
            remote_cleanup_pending_ = false;
        }
    } else {
        auto result =
            globalConfig().ascend_agent_mode
                ? invoke_rpc<&RealClient::ascend_unmap_shm_internal, void>(
                      client_id_)
                : invoke_rpc<&RealClient::unmap_shm_internal, void>(client_id_);
        if (!result) return toInt(result.error());
    }
    registered_shms_.clear();
    std::erase_if(pending_session_staging_, [this](void* address) {
        return shm_helper_->free(address) == 0;
    });
    return 0;
}

#if defined(USE_ASCEND_DIRECT)
int DummyClient::register_device_buffer_for_reconnect(void* buffer,
                                                      size_t size) {
    const auto buffer_addr = reinterpret_cast<uintptr_t>(buffer);
    if (size == 0 ||
        size > std::numeric_limits<uintptr_t>::max() - buffer_addr) {
        LOG(ERROR) << "Device buffer registration overflows address range";
        return -1;
    }

    std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
    const auto action = retain_buffer_registration(registered_device_buffers_,
                                                   buffer_addr, size);
    if (action == BufferRegistrationAction::kReject) {
        LOG(ERROR) << "Invalid or overlapping device buffer registration, "
                   << "buffer=" << buffer << ", size=" << size;
        return -1;
    }
    if (action == BufferRegistrationAction::kRetained) return 0;

    ShmHelper::ShmSegment shm{};
    shm.base_addr = buffer;
    shm.size = size;
    if (register_ascend_shm(&shm, false) != 0) {
        registered_device_buffers_.erase(buffer_addr);
        LOG(ERROR) << "Failed to register device buffer, buffer=" << buffer
                   << ", size=" << size;
        return -1;
    }

    return 0;
}

int DummyClient::unregister_device_buffer_for_reconnect(void* buffer) {
    const auto buffer_addr = reinterpret_cast<uintptr_t>(buffer);
    std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
    const auto action =
        release_buffer_registration(registered_device_buffers_, buffer_addr);
    if (action == BufferReleaseAction::kReject) {
        LOG(ERROR) << "Device buffer is not registered with RealClient";
        return -1;
    }
    if (action == BufferReleaseAction::kRetained) return 0;

    auto ret = invoke_rpc<&RealClient::unregister_shm_buffer_internal, void>(
        static_cast<uint64_t>(buffer_addr), client_id_);
    // Same contract as unregister_buffer: the receiver removes the mapping on
    // INTERNAL_ERROR too (unmapped, or quarantined when the SPDK or transfer
    // engine unregister fails), and INVALID_PARAMS means the server has no such
    // mapping, so drop the local bookkeeping then; keep it on RPC_FAIL /
    // RPC_TIMEOUT where the server may not have run (see unregister_buffer).
    // No extra locking here: the function-scope lock above is already held.
    if (ret.has_value() ||
        (!ret.has_value() && (ret.error() == ErrorCode::INTERNAL_ERROR ||
                              ret.error() == ErrorCode::INVALID_PARAMS))) {
        registered_device_buffers_.erase(buffer_addr);
    }
    return to_py_ret(ret);
}

int DummyClient::reregister_fabric_buffers() {
    std::lock_guard<std::mutex> registration_lock(
        external_fabric_registration_mutex_);
    std::lock_guard<std::mutex> lock(registered_external_buffers_mutex_);
    for (const auto& [buffer_addr, registration] :
         registered_external_buffers_) {
        ShmHelper::ShmSegment shm{};
        shm.base_addr = reinterpret_cast<void*>(buffer_addr);
        shm.size = registration.size;
        if (register_ascend_shm(&shm, false) != 0) return -1;
    }
    return 0;
}

int DummyClient::reregister_device_buffers() {
    std::lock_guard<std::mutex> lock(registered_device_buffers_mutex_);
    for (const auto& [buffer_addr, registration] : registered_device_buffers_) {
        ShmHelper::ShmSegment shm{};
        shm.base_addr = reinterpret_cast<void*>(buffer_addr);
        shm.size = registration.size;
        if (register_ascend_shm(&shm, false) != 0) return -1;
    }
    return 0;
}
#endif

// Dummy only register buffer within the shared memory region
int DummyClient::register_buffer(void* buffer, size_t size) {
    if (buffer == nullptr || size == 0) {
        LOG(ERROR) << "Invalid buffer pointer";
        return -1;
    }
#if defined(USE_ASCEND_DIRECT)
    if (globalConfig().ascend_agent_mode) {
        aclrtPtrAttributes attributes{};
        auto acl_ret = aclrtPointerGetAttributes(buffer, &attributes);
        if (acl_ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to get pointer attributes, ret=" << acl_ret;
            return -1;
        }
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
            return register_device_buffer_for_reconnect(buffer, size);
        }
        if (globalConfig().ascend_use_fabric_mem) {
            std::lock_guard<std::mutex> registration_lock(
                external_fabric_registration_mutex_);
            auto shm = std::make_shared<ShmHelper::ShmSegment>();
            shm->base_addr = buffer;
            shm->size = size;

            // Fabric host buffers are mapped by register_ascend_shm(), but
            // unlike Dummy SHM they are not discoverable through ShmHelper.
            // Track the range locally so put/get/ranged-read validation can
            // accept the same subranges after registration.
            if (register_external_buffer(buffer, size) != 0) {
                LOG(ERROR) << "Invalid or overlapping Fabric buffer"
                           << ", buffer=" << buffer << ", size=" << size;
                return -1;
            }
            if (register_ascend_shm(shm.get(), false) != 0) {
                (void)unregister_external_buffer(buffer);
                LOG(ERROR) << "Failed to register buffer, buffer=" << buffer
                           << ", size=" << size;
                return -1;
            }
            return 0;
        }
        // non-Fabric Host: same rules as shm
    }
#endif
    if (cuda_ipc_persistent_registration_ && is_device_buffer(buffer)) {
        return register_cuda_buffer(buffer, size);
    }
    if (shm_helper_ == nullptr) return -1;
    auto shm = shm_helper_->get_shm(buffer);
    if (shm && is_device_buffer(buffer)) shm = nullptr;
    if (!shm) {
        return register_external_buffer(buffer, size);
    }
    if (shm_helper_->is_hugepage()) {
        const size_t alignment = get_hugepage_size_from_env();
        if (alignment != 0 &&
            size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
            LOG(ERROR) << "Buffer size overflows hugepage alignment";
            return -1;
        }
        size = align_up(size, alignment);
    }
    // Check bounds. The buffer must be the segment base and the size must match
    // either the caller's original request (ShmSegment::requested_size) or the
    // padded mapping size (shm->size, aligned up to the hugepage/2MB boundary
    // for SPDK registration). Both are valid: IPC always sends shm->size, and
    // legacy callers may pass the padded size directly.
    if (reinterpret_cast<uint8_t*>(buffer) !=
            reinterpret_cast<uint8_t*>(shm->base_addr) ||
        (size != shm->requested_size && size != shm->size)) {
        LOG(ERROR) << "Invalid buffer address or size for registration: "
                      "Buffer addr: "
                   << buffer << ", need addr: " << shm->base_addr
                   << ", buffer size: " << size
                   << ", need size: " << shm->requested_size
                   << " (padded: " << shm->size << ")";
        return -1;
    }

    std::lock_guard lock(shm_mutex_);
    if (!registered_shms_.contains(buffer)) {
        if (map_shm(shm.get(), buffer == local_buffer_base_) != 0) return -1;
        registered_shms_.emplace(buffer, std::move(shm));
    }

    return 0;
}

int DummyClient::unregister_buffer(void* buffer) {
    if (buffer == nullptr) {
        LOG(ERROR) << "Invalid buffer pointer";
        return -1;
    }
    if (cuda_ipc_persistent_registration_) {
        if (auto result = unregister_cuda_buffer(buffer)) return *result;
    }

#if defined(USE_ASCEND_DIRECT)
    if (globalConfig().ascend_agent_mode) {
        if (globalConfig().ascend_use_fabric_mem) {
            std::lock_guard<std::mutex> registration_lock(
                external_fabric_registration_mutex_);
            const auto address = reinterpret_cast<uintptr_t>(buffer);
            std::lock_guard<std::mutex> lock(
                registered_external_buffers_mutex_);
            const auto action = release_buffer_registration(
                registered_external_buffers_, address);
            if (action != BufferReleaseAction::kReject) {
                if (action == BufferReleaseAction::kRetained) return 0;

                // Keep the local registration until the real-side mapping is
                // removed successfully.  This makes a failed unregister
                // retryable instead of silently losing the range.
                auto ret =
                    invoke_rpc<&RealClient::unregister_shm_buffer_internal,
                               void>(static_cast<uint64_t>(address),
                                     client_id_);
                if (ret.has_value())
                    registered_external_buffers_.erase(address);
                return to_py_ret(ret);
            }
        }
        aclrtPtrAttributes attributes{};
        auto acl_ret = aclrtPointerGetAttributes(buffer, &attributes);
        if (acl_ret == ACL_ERROR_NONE &&
            (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE ||
             globalConfig().ascend_use_fabric_mem)) {
            return unregister_device_buffer_for_reconnect(buffer);
        }
    }
#endif

    if (shm_helper_ == nullptr) return -1;
    auto shm = shm_helper_->get_shm(buffer);
    if (shm && is_device_buffer(buffer)) shm = nullptr;
    if (!shm) {
        return unregister_external_buffer(buffer);
    }
    std::lock_guard lock(shm_mutex_);
    if (!registered_shms_.contains(buffer)) {
        LOG(ERROR) << "Buffer is not registered with RealClient";
        return -1;
    }
    if (reinterpret_cast<uint8_t*>(buffer) !=
        reinterpret_cast<uint8_t*>(shm->base_addr)) {
        LOG(ERROR) << "Invalid buffer address for unregistration";
        return -1;
    }
    auto ret = invoke_rpc<&RealClient::unregister_shm_buffer_internal, void>(
        reinterpret_cast<uint64_t>(buffer), client_id_);
    // INTERNAL_ERROR means the receiver still executed the teardown and removed
    // the segment from its active mappings: it unmaps normally, or -- when the
    // SPDK or transfer engine unregister fails -- quarantines the mapping
    // (retained, never reused for new registrations). Either way the old
    // mapping is no longer usable on the receiver, so drop the local flag for
    // INTERNAL_ERROR and for INVALID_PARAMS (server has no such mapping). Keep
    // it on RPC_FAIL / RPC_TIMEOUT: the RPC may never have reached the server,
    // or the receiver may have refused before touching anything (Ascend context
    // setup failure in unregister_shm_buffer_internal returns RPC_FAIL for
    // exactly that reason), so the mapping can still be alive and a retry must
    // reach the receiver.
    if (ret.has_value() ||
        (!ret.has_value() && (ret.error() == ErrorCode::INTERNAL_ERROR ||
                              ret.error() == ErrorCode::INVALID_PARAMS))) {
        registered_shms_.erase(buffer);
    }
    return to_py_ret(ret);
}

uint64_t DummyClient::alloc_from_mem_pool(size_t size) {
    try {
        void* addr = shm_helper_->allocate(size);
        return reinterpret_cast<uint64_t>(addr);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to allocate from mem pool: " << e.what();
        return 0;
    }
}

int DummyClient::put(const std::string& key, std::span<const char> value,
                     const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::put_dummy_helper>(
        TransferOperationKind::kWrite, "put", value.size_bytes(), false, key,
        value, config, client_id_);
}

int DummyClient::put_batch(const std::vector<std::string>& keys,
                           const std::vector<std::span<const char>>& values,
                           const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::put_batch_dummy_helper>(
        TransferOperationKind::kWrite, "put_batch", sum_value_sizes(values),
        true, keys, values, config, client_id_);
}

int DummyClient::put_parts(const std::string& key,
                           std::vector<std::span<const char>> values,
                           const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::put_parts_dummy_helper>(
        TransferOperationKind::kWrite, "put_parts", sum_value_sizes(values),
        false, key, values, config, client_id_);
}

int DummyClient::upsert(const std::string& key, std::span<const char> value,
                        const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::upsert_dummy_helper>(
        TransferOperationKind::kWrite, "upsert", value.size_bytes(), false, key,
        value, config, client_id_);
}

int DummyClient::upsert_from(const std::string& key, void* buffer, size_t size,
                             const ReplicateConfig& config) {
    auto prepared = prepare_buffer(buffer, size, true);
    if (!prepared) return toInt(ErrorCode::INVALID_PARAMS);
    const uint64_t dummy_addr = reinterpret_cast<uint64_t>(prepared->dummy);
    const int rc =
        invoke_observed_void_rpc<&RealClient::upsert_from_dummy_helper>(
            TransferOperationKind::kWrite, "upsert_from", size, false, key,
            dummy_addr, size, config, client_id_);
    return toInt(buffer_rpc_error(static_cast<ErrorCode>(rc), {&*prepared, 1}));
}

std::vector<int> DummyClient::batch_upsert_from(
    const std::vector<std::string>& keys, const std::vector<void*>& buffer_ptrs,
    const std::vector<size_t>& sizes, const ReplicateConfig& config) {
    if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    std::vector<PreparedBuffer> prepared;
    std::vector<uint64_t> buffers;
    if (!prepare_buffers(buffer_ptrs, sizes, true, buffers, prepared))
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_upsert_from_dummy_helper, void>(
            prepared, keys.size(), keys, buffers, sizes, config, client_id_);
    auto results = expected_results_to_py<int>(internal_results);
    const size_t successful_bytes = sum_successful_sizes(results, sizes);
    if (successful_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kWrite,
                              "batch_upsert_from", successful_bytes,
                              elapsed_us_since(start_time), true);
    }
    return results;
}

int DummyClient::upsert_parts(const std::string& key,
                              std::vector<std::span<const char>> values,
                              const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::upsert_parts_dummy_helper>(
        TransferOperationKind::kWrite, "upsert_parts", sum_value_sizes(values),
        false, key, values, config, client_id_);
}

int DummyClient::upsert_batch(const std::vector<std::string>& keys,
                              const std::vector<std::span<const char>>& values,
                              const ReplicateConfig& config) {
    return invoke_observed_void_rpc<&RealClient::upsert_batch_dummy_helper>(
        TransferOperationKind::kWrite, "upsert_batch", sum_value_sizes(values),
        true, keys, values, config, client_id_);
}

int DummyClient::remove(const std::string& key, bool force) {
    return to_py_ret(
        invoke_rpc<&RealClient::remove_internal, void>(key, force));
}

long DummyClient::removeByRegex(const std::string& str, bool force) {
    return to_py_ret(
        invoke_rpc<&RealClient::removeByRegex_internal, long>(str, force));
}

long DummyClient::removeAll(bool force) {
    return to_py_ret(
        invoke_rpc<&RealClient::removeAll_internal, int64_t>(force));
}

std::vector<int> DummyClient::batchRemove(const std::vector<std::string>& keys,
                                          bool force) {
    auto internal_results =
        invoke_batch_rpc<&RealClient::batchRemove_internal, void>(keys.size(),
                                                                  keys, force);
    std::vector<int> results;
    results.reserve(internal_results.size());
    for (const auto& result : internal_results) {
        results.push_back(to_py_ret(result));
    }
    return results;
}

int DummyClient::isExist(const std::string& key) {
    auto result = invoke_rpc<&RealClient::isExist_internal, bool>(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

int DummyClient::isExist(const std::string& key, const ExistOptions& options) {
    if (options.prefetch_to_memory) {
        VLOG(1) << "SSD prefetch is not supported via DummyClient; "
                << "prefetch_to_memory ignored for key=" << key;
    }
    return isExist(key);
}

std::vector<int> DummyClient::batchIsExist(
    const std::vector<std::string>& keys) {
    auto internal_results =
        invoke_batch_rpc<&RealClient::batchIsExist_internal, bool>(keys.size(),
                                                                   keys);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        if (result.has_value()) {
            results.push_back(result.value() ? 1 : 0);
        } else {
            LOG(ERROR) << "Batch isExist failed: " << toString(result.error());
            results.push_back(-1);
        }
    }

    return results;
}

int DummyClient::probeKey(const std::string& key) {
    auto result = invoke_rpc<&RealClient::probeKey_internal, bool>(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

std::vector<int> DummyClient::batchProbeKey(
    const std::vector<std::string>& keys, const GrantLeasePolicy& policy) {
    auto internal_results =
        invoke_batch_rpc<&RealClient::batchProbeKey_internal, bool>(
            keys.size(), keys, policy);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        if (result.has_value()) {
            results.push_back(result.value() ? 1 : 0);
        } else {
            LOG(ERROR) << "Batch probeKey failed: " << toString(result.error());
            results.push_back(-1);
        }
    }

    return results;
}

std::vector<int> DummyClient::batchIsExist(const std::vector<std::string>& keys,
                                           const ExistOptions& options) {
    if (options.prefetch_to_memory) {
        VLOG(1) << "SSD prefetch is not supported via DummyClient; "
                << "prefetch_to_memory ignored";
    }
    return batchIsExist(keys);
}

int64_t DummyClient::getSize(const std::string& key) {
    return to_py_ret(invoke_rpc<&RealClient::getSize_internal, int64_t>(key));
}

std::shared_ptr<BufferHandle> DummyClient::get_buffer(const std::string& key) {
    const auto start_time = std::chrono::steady_clock::now();
    // Try hot cache path if shm is mapped
    if (hot_cache_base_) {
        auto result = invoke_rpc<&RealClient::acquire_hot_cache,
                                 std::tuple<uint64_t, size_t>>(key);
        if (result.has_value()) {
            auto [offset, size] = result.value();
            if (offset + size > hot_cache_size_) {
                LOG(ERROR) << "Hot cache offset out of bounds: offset="
                           << offset << " size=" << size
                           << " cache_size=" << hot_cache_size_;
                return nullptr;
            }
            void* local_ptr = static_cast<uint8_t*>(hot_cache_base_) + offset;
            std::string key_copy = key;
            auto release = [this, key_copy]() {
                (void)invoke_rpc<&RealClient::release_hot_cache, void>(
                    key_copy);
            };
            ObserveTransferMetric(TransferOperationKind::kRead, "get_buffer",
                                  size, elapsed_us_since(start_time), false);
            return std::make_shared<BufferHandle>(local_ptr, size,
                                                  std::move(release));
        }
    }

    // Fallback: allocator-backed buffer via shm
    auto result = invoke_rpc<&RealClient::acquire_buffer_dummy,
                             std::tuple<uint64_t, size_t>>(key, client_id_);
    if (!result.has_value()) {
        return nullptr;
    }

    auto [dummy_addr, size] = result.value();
    void* local_ptr = reinterpret_cast<void*>(dummy_addr);
    auto release = [this, dummy_addr]() {
        (void)invoke_rpc<&RealClient::release_buffer_dummy, void>(dummy_addr,
                                                                  client_id_);
    };
    ObserveTransferMetric(TransferOperationKind::kRead, "get_buffer", size,
                          elapsed_us_since(start_time), false);
    return std::make_shared<BufferHandle>(local_ptr, size, std::move(release));
}

std::vector<std::shared_ptr<BufferHandle>> DummyClient::batch_get_buffer(
    const std::vector<std::string>& keys) {
    const auto start_time = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<BufferHandle>> results(keys.size(), nullptr);
    if (keys.empty()) return results;

    // Phase 1: batch hot cache acquire
    std::vector<size_t> miss_indices;
    if (hot_cache_base_) {
        auto hot_results =
            invoke_batch_rpc<&RealClient::batch_acquire_hot_cache,
                             std::tuple<uint64_t, size_t>>(keys.size(), keys);
        for (size_t i = 0; i < keys.size(); ++i) {
            if (hot_results[i].has_value()) {
                auto [offset, size] = hot_results[i].value();
                if (offset + size > hot_cache_size_) {
                    LOG(ERROR)
                        << "Hot cache offset out of bounds: offset=" << offset
                        << " size=" << size
                        << " cache_size=" << hot_cache_size_;
                    miss_indices.push_back(i);
                    continue;
                }
                void* ptr = static_cast<uint8_t*>(hot_cache_base_) + offset;
                auto release = [this, key = keys[i]]() {
                    (void)invoke_rpc<&RealClient::release_hot_cache, void>(key);
                };
                results[i] = std::make_shared<BufferHandle>(ptr, size,
                                                            std::move(release));
            } else {
                miss_indices.push_back(i);
            }
        }
    } else {
        for (size_t i = 0; i < keys.size(); ++i) miss_indices.push_back(i);
    }

    if (miss_indices.empty()) return results;

    // Phase 2: batch allocator acquire for misses
    std::vector<std::string> miss_keys;
    miss_keys.reserve(miss_indices.size());
    for (size_t idx : miss_indices) {
        miss_keys.push_back(keys[idx]);
    }

    auto alloc_results =
        invoke_batch_rpc<&RealClient::batch_acquire_buffer_dummy,
                         std::tuple<uint64_t, size_t>>(miss_keys.size(),
                                                       miss_keys, client_id_);

    for (size_t i = 0; i < miss_indices.size(); ++i) {
        if (!alloc_results[i].has_value()) continue;
        auto [dummy_addr, size] = alloc_results[i].value();
        void* ptr = reinterpret_cast<void*>(dummy_addr);
        auto release = [this, dummy_addr]() {
            (void)invoke_rpc<&RealClient::release_buffer_dummy, void>(
                dummy_addr, client_id_);
        };
        results[miss_indices[i]] =
            std::make_shared<BufferHandle>(ptr, size, std::move(release));
    }

    size_t total_bytes = 0;
    for (const auto& result : results) {
        if (result != nullptr) {
            total_bytes += result->size();
        }
    }
    if (total_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kRead, "batch_get_buffer",
                              total_bytes, elapsed_us_since(start_time), true);
    }

    return results;
}

int64_t DummyClient::get_into(const std::string& key, void* buffer,
                              size_t size) {
    if (!cuda_ipc_persistent_registration_ &&
        is_registered_buffer(buffer, size)) {
        if (auto dst_buffer = try_export_cuda_ipc_buffers({buffer}, {size})) {
            std::vector<CudaIpcReadRequest> requests{
                CudaIpcReadRequest{
                    .key = key,
                    .destination = (*dst_buffer)[0],
                    .source_offset = 0,
                    .size = static_cast<uint64_t>(size),
                },
            };
            auto results = batch_get_into_cuda_ipc(requests);
            return results.empty() ? toInt(ErrorCode::INVALID_PARAMS)
                                   : results[0];
        }
    }

    auto prepared = prepare_buffer(buffer, size, false);
    if (!prepared) return toInt(ErrorCode::INVALID_PARAMS);
    const uint64_t dummy_addr = reinterpret_cast<uint64_t>(prepared->dummy);
    const auto start_time = std::chrono::steady_clock::now();
    auto result = invoke_rpc<&RealClient::get_into_range_shm_helper,
                             tl::expected<int64_t, ErrorCode>>(
        key, dummy_addr, 0, 0, size, true, true, client_id_);
    if (!result || !*result)
        return toInt(buffer_rpc_error(result ? result->error() : result.error(),
                                      {&*prepared, 1}));

    const int64_t bytes_read = **result;
    if (bytes_read >= 0) {
        if (!copy_from_staging(*prepared, static_cast<size_t>(bytes_read))) {
            return toInt(ErrorCode::INTERNAL_ERROR);
        }
        ObserveTransferMetric(TransferOperationKind::kRead, "get_into",
                              static_cast<size_t>(bytes_read),
                              elapsed_us_since(start_time), false);
    }
    return bytes_read;
}

std::vector<std::vector<std::vector<int64_t>>> DummyClient::get_into_ranges(
    const std::vector<void*>& buffers,
    const std::vector<std::vector<std::string>>& all_keys,
    const std::vector<std::vector<std::vector<size_t>>>& all_dst_offsets,
    const std::vector<std::vector<std::vector<size_t>>>& all_src_offsets,
    const std::vector<std::vector<std::vector<size_t>>>& all_sizes,
    const QueryResultCache* query_result_cache) {
    if (buffers.size() != all_keys.size() ||
        buffers.size() != all_dst_offsets.size() ||
        buffers.size() != all_src_offsets.size() ||
        buffers.size() != all_sizes.size()) {
        return build_ranged_read_error_results(buffers.size(), all_keys,
                                               all_dst_offsets,
                                               ErrorCode::INVALID_PARAMS);
    }

    std::vector<PreparedBuffer> prepared;
    std::vector<uint64_t> dummy_buffers = void_ptrs_to_u64(buffers);
    std::vector<size_t> dummy_buffer_sizes;
    auto dummy_dst_offsets = all_dst_offsets;
    bool requires_staging = false;
    prepared.reserve(buffers.size());
    dummy_buffer_sizes.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        if (all_keys[i].size() != all_dst_offsets[i].size() ||
            all_keys[i].size() != all_src_offsets[i].size() ||
            all_keys[i].size() != all_sizes[i].size()) {
            return build_ranged_read_error_results(buffers.size(), all_keys,
                                                   all_dst_offsets,
                                                   ErrorCode::INVALID_PARAMS);
        }
        for (size_t j = 0; j < all_dst_offsets[i].size(); ++j) {
            if (all_dst_offsets[i][j].size() != all_src_offsets[i][j].size() ||
                all_dst_offsets[i][j].size() != all_sizes[i][j].size()) {
                return build_ranged_read_error_results(
                    buffers.size(), all_keys, all_dst_offsets,
                    ErrorCode::INVALID_PARAMS);
            }
        }

        auto item = prepare_ranged_read_buffer(buffers[i], dummy_dst_offsets[i],
                                               all_sizes[i]);
        if (!item) {
            return build_ranged_read_error_results(buffers.size(), all_keys,
                                                   all_dst_offsets,
                                                   ErrorCode::INVALID_PARAMS);
        }
        dummy_buffers[i] = reinterpret_cast<uint64_t>(item->dummy);
        dummy_buffer_sizes.push_back(item->staging ? item->staging->size()
                                                   : item->size);
        requires_staging |= item->staging != nullptr;
        prepared.push_back(std::move(*item));
    }

    auto cached_query_results =
        build_cached_query_results_from_query_result_cache(query_result_cache);
    const auto start_time = std::chrono::steady_clock::now();
    using RangedReadResults =
        std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>;
    auto internal_results =
        requires_staging
            ? invoke_rpc<&RealClient::get_into_ranges_staged_shm_helper,
                         RangedReadResults>(
                  dummy_buffers, dummy_buffer_sizes, all_keys,
                  dummy_dst_offsets, all_src_offsets, all_sizes,
                  cached_query_results, device_id_, client_id_)
            : invoke_rpc<&RealClient::get_into_ranges_shm_helper,
                         RangedReadResults>(
                  dummy_buffers, all_keys, all_dst_offsets, all_src_offsets,
                  all_sizes, cached_query_results, device_id_, client_id_);

    if (!internal_results) {
        LOG(ERROR) << "get_into_ranges RPC failed";
        return build_ranged_read_error_results(
            buffers.size(), all_keys, all_dst_offsets,
            buffer_rpc_error(internal_results.error(), prepared));
    }
    if (cuda_ipc_persistent_registration_ &&
        cuda_buffer_in_use(*internal_results))
        return build_ranged_read_error_results(buffers.size(), all_keys,
                                               all_dst_offsets,
                                               drain_failed_cuda_transfer());
    auto results = convert_ranged_read_results(internal_results.value());
    if (results.size() != prepared.size()) {
        LOG(ERROR) << "get_into_ranges response size mismatch: expected "
                   << prepared.size() << ", got " << results.size();
        return build_ranged_read_error_results(buffers.size(), all_keys,
                                               all_dst_offsets,
                                               ErrorCode::INTERNAL_ERROR);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].size() != all_keys[i].size()) {
            LOG(ERROR) << "get_into_ranges response key count mismatch at " << i
                       << ": expected " << all_keys[i].size() << ", got "
                       << results[i].size();
            return build_ranged_read_error_results(buffers.size(), all_keys,
                                                   all_dst_offsets,
                                                   ErrorCode::INTERNAL_ERROR);
        }
        for (size_t j = 0; j < results[i].size(); ++j) {
            if (results[i][j].size() != all_sizes[i][j].size()) {
                LOG(ERROR)
                    << "get_into_ranges response fragment count mismatch at ["
                    << i << "][" << j << "]: expected "
                    << all_sizes[i][j].size() << ", got "
                    << results[i][j].size();
                return build_ranged_read_error_results(
                    buffers.size(), all_keys, all_dst_offsets,
                    ErrorCode::INTERNAL_ERROR);
            }
        }
    }

    for (size_t i = 0; i < results.size(); ++i) {
        for (size_t j = 0; j < results[i].size(); ++j) {
            for (size_t k = 0; k < results[i][j].size(); ++k) {
                auto& result = results[i][j][k];
                if (result < 0) continue;
                if (static_cast<uint64_t>(result) > all_sizes[i][j][k] ||
                    !copy_from_staging(prepared[i], static_cast<size_t>(result),
                                       all_dst_offsets[i][j][k],
                                       dummy_dst_offsets[i][j][k])) {
                    result = toInt(ErrorCode::INTERNAL_ERROR);
                }
            }
        }
    }
    const size_t total_bytes = sum_positive_ranges(results);
    if (total_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kRead, "get_into_ranges",
                              total_bytes, elapsed_us_since(start_time), true);
    }
    return results;
}

std::vector<std::vector<std::vector<int64_t>>>
DummyClient::get_into_ranges_from_snapshot(
    const std::vector<void*>& buffers,
    const std::vector<std::vector<std::string>>& all_keys,
    const std::vector<std::vector<std::vector<size_t>>>& all_dst_offsets,
    const std::vector<std::vector<std::vector<size_t>>>& all_src_offsets,
    const std::vector<std::vector<std::vector<size_t>>>& all_sizes,
    const QueryResultCache& query_result_cache) {
    return get_into_ranges(buffers, all_keys, all_dst_offsets, all_src_offsets,
                           all_sizes, &query_result_cache);
}

std::vector<tl::expected<QueryResult, ErrorCode>> DummyClient::batch_query(
    const std::vector<std::string>& keys) {
    auto cached_results =
        invoke_rpc<&RealClient::batch_get_query_results,
                   std::vector<CachedQueryResultResponse>>(keys);
    if (!cached_results) {
        return std::vector<tl::expected<QueryResult, ErrorCode>>(
            keys.size(), tl::unexpected(cached_results.error()));
    }
    if (cached_results->size() != keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch: expected "
                   << keys.size() << ", got " << cached_results->size();
        return std::vector<tl::expected<QueryResult, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::RPC_FAIL));
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(keys.size());
    const auto now = std::chrono::steady_clock::now();
    for (const auto& cached_result : *cached_results) {
        results.emplace_back(
            from_cached_query_result_response(cached_result, now));
    }
    return results;
}

std::string DummyClient::get_hostname() const {
    // Dummy client does not have a hostname
    return "";
}

std::vector<int> DummyClient::batch_put_from(
    const std::vector<std::string>& keys, const std::vector<void*>& buffer_ptrs,
    const std::vector<size_t>& sizes, const ReplicateConfig& config) {
    if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    if (cuda_ipc_persistent_registration_ &&
        std::any_of(buffer_ptrs.begin(), buffer_ptrs.end(), [this](void* ptr) {
            return registered_cuda_buffer_remaining(ptr).has_value();
        })) {
        // Use the same positional rows and placement policy as CUDA IPC
        // writes, with each persistent address translated by the daemon.
        std::vector<std::vector<void*>> rows;
        std::vector<std::vector<size_t>> row_sizes;
        rows.reserve(keys.size());
        row_sizes.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            rows.push_back({buffer_ptrs[i]});
            row_sizes.push_back({sizes[i]});
        }
        return batch_put_from_multi_buffers(keys, rows, row_sizes, config);
    }
    if (can_export_cuda_buffers(buffer_ptrs, sizes)) {
        if (auto payloads = try_export_cuda_ipc_buffers(buffer_ptrs, sizes);
            payloads && keys.size() == payloads->size()) {
            std::vector<CudaIpcWriteRequest> requests;
            requests.reserve(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                requests.push_back(CudaIpcWriteRequest{
                    .key = keys[i],
                    .metadata = CudaIpcShmBufferRef{},
                    .payload = (*payloads)[i],
                });
            }
            return batch_put_from_cuda_ipc(requests, config);
        }
    }

    std::vector<PreparedBuffer> prepared;
    std::vector<uint64_t> buffers;
    if (!prepare_buffers(buffer_ptrs, sizes, true, buffers, prepared))
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_put_from_dummy_helper, void>(
            prepared, keys.size(), keys, buffers, sizes, config, device_id_,
            client_id_);
    auto results = expected_results_to_py<int>(internal_results);

    const size_t successful_bytes = sum_successful_sizes(results, sizes);
    if (successful_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kWrite, "batch_put_from",
                              successful_bytes, elapsed_us_since(start_time),
                              true);
    }

    return results;
}

int DummyClient::put_from(const std::string& key, void* buffer, size_t size,
                          const ReplicateConfig& config) {
    auto results = batch_put_from({key}, {buffer}, {size}, config);
    return results.empty() ? -1 : results[0];
}

std::vector<int64_t> DummyClient::batch_get_into(
    const std::vector<std::string>& keys, const std::vector<void*>& buffer_ptrs,
    const std::vector<size_t>& sizes) {
    if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
        return std::vector<int64_t>(keys.size(),
                                    toInt(ErrorCode::INVALID_PARAMS));
    }
    if (can_export_cuda_buffers(buffer_ptrs, sizes)) {
        if (auto dst_buffers = try_export_cuda_ipc_buffers(buffer_ptrs, sizes);
            dst_buffers && keys.size() == dst_buffers->size()) {
            std::vector<CudaIpcReadRequest> requests;
            requests.reserve(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                requests.push_back(CudaIpcReadRequest{
                    .key = keys[i],
                    .destination = (*dst_buffers)[i],
                    .source_offset = 0,
                    .size = static_cast<uint64_t>(sizes[i]),
                });
            }
            return batch_get_into_cuda_ipc(requests);
        }
    }

    std::vector<PreparedBuffer> prepared;
    std::vector<uint64_t> buffers;
    if (!prepare_buffers(buffer_ptrs, sizes, false, buffers, prepared))
        return std::vector<int64_t>(keys.size(),
                                    toInt(ErrorCode::INVALID_PARAMS));
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_get_into_dummy_helper, int64_t>(
            prepared, keys.size(), keys, buffers, sizes, device_id_,
            client_id_);
    auto results = expected_results_to_py(internal_results);

    for (size_t i = 0; i < results.size() && i < prepared.size(); ++i) {
        if (results[i] >= 0 &&
            !copy_from_staging(prepared[i], static_cast<size_t>(results[i]))) {
            results[i] = toInt(ErrorCode::INTERNAL_ERROR);
        }
    }

    const size_t total_bytes = sum_positive_results(results);
    if (total_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kRead, "batch_get_into",
                              total_bytes, elapsed_us_since(start_time), true);
    }

    return results;
}

std::vector<int> DummyClient::batch_get_session_start(
    const std::vector<std::string>& keys) {
    return session_results(
        invoke_rpc<&RealClient::batch_get_session_start_dummy_helper,
                   std::vector<int>>(keys, client_id_),
        keys.size());
}

int DummyClient::batch_get_session_end(const std::vector<std::string>& keys) {
    auto result =
        invoke_rpc<&RealClient::batch_get_session_end_dummy_helper, int>(
            keys, client_id_);
    return result ? *result : toInt(result.error());
}

std::vector<int> DummyClient::batch_put_session_start(
    const std::vector<std::string>& keys, const std::vector<size_t>& sizes,
    const ReplicateConfig& config) {
    if (keys.size() != sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    return session_results(
        invoke_rpc<&RealClient::batch_put_session_start_dummy_helper,
                   std::vector<int>>(keys, sizes, config, client_id_),
        keys.size());
}

std::vector<int> DummyClient::batch_put_session_end(
    const std::vector<std::string>& keys) {
    return session_results(
        invoke_rpc<&RealClient::finish_dummy_put_sessions, std::vector<int>>(
            keys, client_id_, true),
        keys.size());
}

std::vector<int> DummyClient::batch_put_session_revoke(
    const std::vector<std::string>& keys) {
    return session_results(
        invoke_rpc<&RealClient::finish_dummy_put_sessions, std::vector<int>>(
            keys, client_id_, false),
        keys.size());
}

std::vector<int> DummyClient::batch_get_into_multi_buffer_ranges(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const std::vector<std::vector<size_t>>& all_src_offsets) {
    return transfer_session_ranges(keys, all_buffers, all_sizes,
                                   all_src_offsets, false);
}

std::vector<int> DummyClient::batch_put_from_multi_buffer_ranges(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const std::vector<std::vector<size_t>>& all_dst_offsets) {
    return transfer_session_ranges(keys, all_buffers, all_sizes,
                                   all_dst_offsets, true);
}

std::vector<int> DummyClient::transfer_session_ranges(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const std::vector<std::vector<size_t>>& all_offsets, bool is_write) {
    if (cuda_transfer_pending())
        return std::vector<int>(keys.size(),
                                toInt(ErrorCode::CUDA_BUFFER_IN_USE));
    std::vector<int> results(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size() ||
        keys.size() != all_offsets.size()) {
        return results;
    }

    // Record the expected byte count for each valid row. Negative results
    // are skipped both when constructing the RPC and when unpacking its reply.
    std::vector<std::vector<uint64_t>> addresses(keys.size());
    size_t total = 0;
    bool needs_staging = false, direct_cuda = false, caller_uses_pool = false;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& sizes = all_sizes[i];
        auto& row = addresses[i];
        if (sizes.size() != all_buffers[i].size() ||
            sizes.size() != all_offsets[i].size())
            continue;
        row.reserve(sizes.size());
        size_t bytes = 0, staged = 0, gpu_fragments = 0;
        for (size_t j = 0; j < sizes.size(); ++j) {
            void* buffer = all_buffers[i][j];
            const size_t size = sizes[j];
            if (size > std::numeric_limits<int>::max() - bytes ||
                size > std::numeric_limits<size_t>::max() - all_offsets[i][j])
                break;
            bool gpu = false;
            if (!is_dummy_shm_buffer(buffer, size, &caller_uses_pool) &&
                !is_registered_buffer(buffer, size, &gpu))
                break;
            row.push_back(gpu ? reinterpret_cast<uint64_t>(buffer) : 0);
            gpu_fragments += gpu;
            if (!gpu) staged += size;
            bytes += size;
        }
        if (row.size() == sizes.size() &&
            staged <= std::numeric_limits<size_t>::max() - total) {
            results[i] = static_cast<int>(bytes);
            total += staged;
            needs_staging |= gpu_fragments != row.size();
            direct_cuda |= gpu_fragments != 0;
        }
    }
    if (std::none_of(results.begin(), results.end(),
                     [](int result) { return result >= 0; }))
        return results;
    auto fail = [&](ErrorCode error) {
        for (int& result : results)
            if (result >= 0) result = toInt(error);
        return results;
    };
    if (needs_staging && !shm_helper_) return fail(ErrorCode::INVALID_PARAMS);

    // Keep a private allocation even for SHM callers: a timed-out RPC can
    // still read/write its buffers after the synchronous API has returned.
    const size_t capacity = std::max<size_t>(total, 1);
    uint64_t address = 0;
    if (needs_staging && local_buffer_base_ && !caller_uses_pool) {
        auto allocation =
            invoke_rpc<&RealClient::allocate_buffer_dummy,
                       std::tuple<uint64_t, size_t>>(capacity, client_id_);
        if (allocation) {
            address = std::get<0>(*allocation);
        } else if (allocation.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
            return fail(allocation.error());
        }
    }
    const bool pooled = address != 0;
    if (needs_staging && !pooled) {
        address = alloc_from_mem_pool(capacity);
        if (!address) return fail(ErrorCode::NO_AVAILABLE_HANDLE);
        auto shm = shm_helper_->get_shm(reinterpret_cast<void*>(address));
        if (map_shm(shm.get(), false) != 0) {
            // A late registration can still refer to this VA. Leave it
            // reserved.
            return fail(ErrorCode::RPC_FAIL);
        }
    }

    const auto& runtime =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    size_t offset = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto& row = addresses[i];
        if (results[i] < 0) {
            row = {0};  // An unmappable row must never submit a partial write.
            continue;
        }
        for (size_t j = 0; j < row.size(); ++j) {
            if (row[j])
                continue;  // The daemon translates persistent GPU addresses.
            const size_t size = all_sizes[i][j];
            const uint64_t slice = size ? address + offset : address;
            if (is_write && size &&
                !runtime.CopyToHost(reinterpret_cast<void*>(slice),
                                    all_buffers[i][j], size)) {
                results[i] = toInt(ErrorCode::INTERNAL_ERROR);
                row = {0};
                break;
            }
            row[j] = slice;
            offset += size;
        }
    }

    const auto start_time = std::chrono::steady_clock::now();
    auto response =
        invoke_rpc<&RealClient::execute_dummy_session_ranges, std::vector<int>>(
            keys, addresses, all_sizes, all_offsets, device_id_, client_id_,
            is_write);
    if ((direct_cuda && !response) ||
        (cuda_ipc_persistent_registration_ && response &&
         cuda_buffer_in_use(*response)))
        response = tl::unexpected(drain_failed_cuda_transfer());
    auto reply = session_results(std::move(response), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (results[i] < 0) continue;
        if (reply[i] >= 0 && reply[i] != results[i]) {
            results[i] = toInt(ErrorCode::INTERNAL_ERROR);
            continue;
        }
        results[i] = reply[i];
        if (is_write || results[i] <= 0) continue;
        for (size_t j = 0; j < addresses[i].size(); ++j) {
            const size_t size = all_sizes[i][j];
            if (addresses[i][j] ==
                reinterpret_cast<uint64_t>(all_buffers[i][j]))
                continue;
            if (size && !runtime.CopyFromHost(
                            all_buffers[i][j],
                            reinterpret_cast<void*>(addresses[i][j]), size)) {
                results[i] = toInt(ErrorCode::INTERNAL_ERROR);
                break;
            }
        }
    }
    // A transport failure can leave the handler or a lower-level transfer
    // using staging. Unmapping the client reclaims these allocations.
    const bool uncertain = std::any_of(reply.begin(), reply.end(), [](int rc) {
        return rc == toInt(ErrorCode::RPC_FAIL) ||
               rc == toInt(ErrorCode::CUDA_BUFFER_IN_USE) ||
               rc == toInt(ErrorCode::RPC_TIMEOUT) ||
               rc == toInt(ErrorCode::TRANSFER_FAIL) ||
               rc == toInt(ErrorCode::WRITE_FAIL) ||
               rc == toInt(ErrorCode::INTERNAL_ERROR);
    });
    if (pooled && !uncertain) {
        (void)invoke_rpc<&RealClient::release_buffer_dummy, void>(address,
                                                                  client_id_);
    } else if (needs_staging && !pooled) {
        void* buffer = reinterpret_cast<void*>(address);
        bool retry_free = uncertain;
        if (!uncertain) {
            auto unmapped =
                invoke_rpc<&RealClient::unregister_shm_buffer_internal, void>(
                    address, client_id_);
            // An unconfirmed unregister can still arrive after client teardown.
            // Keep its VA reserved; only a confirmed unregister permits reuse.
            retry_free = unmapped && shm_helper_->free(buffer) != 0;
        }
        if (retry_free) {
            std::lock_guard lock(shm_mutex_);
            if (cuda_transfer_state_ != CudaTransferState::kDrained ||
                shm_helper_->free(buffer) != 0)
                pending_session_staging_.push_back(buffer);
        }
    }
    ObserveTransferMetric(
        is_write ? TransferOperationKind::kWrite : TransferOperationKind::kRead,
        is_write ? "batch_put_from_multi_buffer_ranges"
                 : "batch_get_into_multi_buffer_ranges",
        sum_positive_results(results), elapsed_us_since(start_time), true);
    return results;
}

std::vector<int64_t> DummyClient::batch_get_into_cuda_ipc(
    const std::vector<mooncake::CudaIpcReadRequest>& requests) {
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_get_into_cuda_ipc_dummy_helper,
                         int64_t>(requests.size(), requests, client_id_);
    auto results = expected_results_to_py(internal_results);

    const size_t total_bytes = sum_positive_results(results);
    if (total_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kRead,
                              "batch_get_into_cuda_ipc", total_bytes,
                              elapsed_us_since(start_time), true);
    }

    return results;
}

int DummyClient::put_from_with_metadata(const std::string& key, void* buffer,
                                        void* metadata_buffer, size_t size,
                                        size_t metadata_size,
                                        const ReplicateConfig& config) {
    std::vector<std::vector<void*>> buffers = {{metadata_buffer}};
    std::vector<std::vector<size_t>> sizes = {{metadata_size}};
    if (size > 0) {
        buffers[0].push_back(buffer);
        sizes[0].push_back(size);
    }
    auto results = batch_put_from_multi_buffers({key}, buffers, sizes, config);
    return results.empty() ? -1 : results[0];
}

std::vector<int> DummyClient::batch_put_from_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffer_ptrs,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReplicateConfig& config) {
    if (keys.size() != all_buffer_ptrs.size() ||
        keys.size() != all_sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    auto prepared = prepare_multi_buffers(all_buffer_ptrs, all_sizes);
    if (!prepared) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_put_from_multi_buffers_dummy_helper,
                         void>(prepared->buffers, keys.size(), keys,
                               prepared->dummy_buffers, all_sizes, config,
                               device_id_, client_id_);
    auto results = expected_results_to_py<int>(internal_results);
    const size_t successful_bytes =
        sum_successful_nested_sizes(results, all_sizes);
    if (successful_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kWrite,
                              "batch_put_from_multi_buffers", successful_bytes,
                              elapsed_us_since(start_time), true);
    }
    return results;
}

std::vector<int> DummyClient::batch_put_from_cuda_ipc(
    const std::vector<mooncake::CudaIpcWriteRequest>& requests,
    const ReplicateConfig& config) {
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_put_from_cuda_ipc_dummy_helper,
                         void>(requests.size(), requests, config, client_id_);
    auto results = expected_results_to_py<int>(internal_results);
    const size_t successful_bytes =
        sum_successful_cuda_ipc_sizes(results, requests);
    if (successful_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kWrite,
                              "batch_put_from_cuda_ipc", successful_bytes,
                              elapsed_us_since(start_time), true);
    }
    return results;
}

std::vector<int> DummyClient::batch_upsert_from_cuda_ipc(
    const std::vector<mooncake::CudaIpcWriteRequest>& requests,
    const ReplicateConfig& config) {
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_upsert_from_cuda_ipc_dummy_helper,
                         void>(requests.size(), requests, config, client_id_);
    auto results = expected_results_to_py<int>(internal_results);
    const size_t successful_bytes =
        sum_successful_cuda_ipc_sizes(results, requests);
    if (successful_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kWrite,
                              "batch_upsert_from_cuda_ipc", successful_bytes,
                              elapsed_us_since(start_time), true);
    }
    return results;
}

std::vector<int> DummyClient::batch_upsert_from_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffer_ptrs,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReplicateConfig& config) {
    if (keys.size() != all_buffer_ptrs.size() ||
        keys.size() != all_sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    auto prepared = prepare_multi_buffers(all_buffer_ptrs, all_sizes);
    if (!prepared) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results = invoke_batch_rpc<
        &RealClient::batch_upsert_from_multi_buffers_dummy_helper, void>(
        prepared->buffers, keys.size(), keys, prepared->dummy_buffers,
        all_sizes, config, device_id_, client_id_);
    auto results = expected_results_to_py<int>(internal_results);
    const size_t successful_bytes =
        sum_successful_nested_sizes(results, all_sizes);
    if (successful_bytes > 0) {
        ObserveTransferMetric(
            TransferOperationKind::kWrite, "batch_upsert_from_multi_buffers",
            successful_bytes, elapsed_us_since(start_time), true);
    }
    return results;
}

std::vector<int> DummyClient::batch_get_into_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffer_ptrs,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node) {
    if (keys.size() != all_buffer_ptrs.size() ||
        keys.size() != all_sizes.size()) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    auto prepared = prepare_multi_buffers(all_buffer_ptrs, all_sizes, false);
    if (!prepared) {
        return std::vector<int>(keys.size(), toInt(ErrorCode::INVALID_PARAMS));
    }
    const auto start_time = std::chrono::steady_clock::now();
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_get_into_multi_buffers_dummy_helper,
                         int64_t>(
            prepared->buffers, keys.size(), keys, prepared->dummy_buffers,
            all_sizes, prefer_alloc_in_same_node, device_id_, client_id_);
    auto results = expected_results_to_py<int>(internal_results);

    if (internal_results.size() != all_sizes.size()) {
        LOG(ERROR) << "batch_get_into_multi_buffers response size mismatch: "
                   << "expected " << all_sizes.size() << ", got "
                   << internal_results.size();
        return std::vector<int>(keys.size(), toInt(ErrorCode::INTERNAL_ERROR));
    }

    size_t prepared_index = 0;
    for (size_t i = 0; i < all_sizes.size(); ++i) {
        size_t remaining = 0;
        const bool has_result = internal_results[i].has_value();
        bool copy_succeeded = false;
        if (has_result && internal_results[i].value() >= 0 &&
            static_cast<uint64_t>(internal_results[i].value()) <=
                std::numeric_limits<size_t>::max()) {
            remaining = static_cast<size_t>(internal_results[i].value());
            copy_succeeded = true;
        }
        for (size_t size : all_sizes[i]) {
            const size_t copy_size = std::min(size, remaining);
            if (!copy_from_staging(prepared->buffers[prepared_index],
                                   copy_size)) {
                copy_succeeded = false;
            }
            remaining -= copy_size;
            ++prepared_index;
        }
        if (has_result && (!copy_succeeded || remaining != 0)) {
            results[i] = toInt(ErrorCode::INTERNAL_ERROR);
        }
    }
    const size_t total_bytes = sum_positive_results(results);
    if (total_bytes > 0) {
        ObserveTransferMetric(TransferOperationKind::kRead,
                              "batch_get_into_multi_buffers", total_bytes,
                              elapsed_us_since(start_time), true);
    }
    return results;
}

std::map<std::string, std::vector<Replica::Descriptor>>
DummyClient::batch_get_replica_desc(const std::vector<std::string>& keys) {
    std::map<std::string, std::vector<Replica::Descriptor>> replica_list_map =
        {};
    auto batch_result =
        invoke_rpc<&RealClient::batch_get_replica_desc,
                   std::map<std::string, std::vector<Replica::Descriptor>>>(
            keys);
    if (!batch_result.has_value()) {
        LOG(ERROR) << "Batch get replica failed."
                   << "Error is: " << toString(batch_result.error());
        return replica_list_map;
    }
    replica_list_map = std::move(batch_result.value());
    return replica_list_map;
}

std::vector<Replica::Descriptor> DummyClient::get_replica_desc(
    const std::string& key) {
    std::vector<Replica::Descriptor> replica_list = {};
    auto result = invoke_rpc<&RealClient::get_replica_desc,
                             std::vector<Replica::Descriptor>>(key);
    if (!result.has_value()) {
        LOG(ERROR) << "Get replica failed for key: " << key
                   << " with error: " << toString(result.error());
        return replica_list;
    }
    replica_list = std::move(result.value());
    return replica_list;
}

tl::expected<UUID, ErrorCode> DummyClient::create_copy_task(
    const std::string& key, const std::vector<std::string>& targets) {
    return invoke_rpc<&RealClient::create_copy_task, UUID>(key, targets);
}

tl::expected<UUID, ErrorCode> DummyClient::create_move_task(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return invoke_rpc<&RealClient::create_move_task, UUID>(key, source, target);
}

tl::expected<UUID, ErrorCode> DummyClient::create_drain_job(
    const CreateDrainJobRequest& request) {
    return invoke_rpc<&RealClient::create_drain_job, UUID>(request);
}

tl::expected<UUID, ErrorCode> DummyClient::create_timed_drain_job(
    const CreateDrainJobRequest& request, uint64_t timeout_ms) {
    return invoke_rpc<&RealClient::create_timed_drain_job, UUID>(request,
                                                                 timeout_ms);
}

tl::expected<QueryJobResponse, ErrorCode> DummyClient::query_drain_job(
    const UUID& job_id) {
    return invoke_rpc<&RealClient::query_drain_job, QueryJobResponse>(job_id);
}

tl::expected<void, ErrorCode> DummyClient::cancel_drain_job(
    const UUID& job_id) {
    return invoke_rpc<&RealClient::cancel_drain_job, void>(job_id);
}

tl::expected<ListDrainJobsResponse, ErrorCode> DummyClient::list_drain_jobs(
    const DrainJobListRequest& request) {
    return invoke_rpc<&RealClient::list_drain_jobs, ListDrainJobsResponse>(
        request);
}

tl::expected<ListTasksResponse, ErrorCode> DummyClient::list_tasks(
    const TaskListRequest& request) {
    return invoke_rpc<&RealClient::list_tasks, ListTasksResponse>(request);
}

tl::expected<void, ErrorCode> DummyClient::cancel_task(const UUID& task_id) {
    return invoke_rpc<&RealClient::cancel_task, void>(task_id);
}

tl::expected<QueryTaskResponse, ErrorCode> DummyClient::query_task(
    const UUID& task_id) {
    return invoke_rpc<&RealClient::query_task, QueryTaskResponse>(task_id);
}

tl::expected<PingResponse, ErrorCode> DummyClient::ping_daemon(
    bool establish_context) {
    if (cuda_ipc_persistent_registration_) {
        return invoke_rpc<&RealClient::ping_persistent_dummy, PingResponse>(
            client_id_, establish_context);
    }
    return invoke_rpc<&RealClient::ping, PingResponse>(client_id_);
}

void DummyClient::ping_thread_main() {
    const int max_ping_fail_count = 1;
    const int success_ping_interval_ms = 1000;
    const int fail_ping_interval_ms = 1000;
    const int retry_connect_interval_ms = 2000;

    int ping_fail_count = 0;

    while (ping_running_) {
        auto ping_result = ping_daemon();

        if (ping_result.has_value() &&
            ping_result.value().client_status == ClientStatus::OK) {
            ping_fail_count = 0;
            last_ping_healthy_.store(true);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        // Ping failed
        ping_fail_count++;
        last_ping_healthy_.store(false);
        LOG(WARNING) << "Ping failed " << ping_fail_count << "/"
                     << max_ping_fail_count;

        if (ping_fail_count >= max_ping_fail_count) {
            connected_.store(false);
            LOG(ERROR) << "RealClient lost, entering reconnection loop...";

            // Reconnection Loop
            while (ping_running_) {
                if (cuda_ipc_persistent_registration_) {
                    auto context = ping_daemon(true);
                    if (!context ||
                        context->client_status != ClientStatus::OK) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(
                            retry_connect_interval_ms));
                        continue;
                    }
                }
                bool all_registered;
                {
                    std::lock_guard lock(shm_mutex_);
                    all_registered = std::all_of(
                        registered_shms_.begin(), registered_shms_.end(),
                        [this](const auto& item) {
                            return map_shm(item.second.get(),
                                           item.first == local_buffer_base_) ==
                                   0;
                        });
                }

#if defined(USE_ASCEND_DIRECT)
                if (all_registered && globalConfig().ascend_agent_mode) {
                    if (globalConfig().ascend_use_fabric_mem &&
                        reregister_fabric_buffers() != 0) {
                        LOG(WARNING) << "Failed to re-register Fabric buffers "
                                        "during reconnection";
                        all_registered = false;
                    }
                    if (all_registered && reregister_device_buffers() != 0) {
                        LOG(WARNING) << "Failed to re-register device buffers "
                                        "during reconnection";
                        all_registered = false;
                    }
                }
#endif

                if (all_registered && reregister_cuda_buffers() != 0) {
                    LOG(WARNING) << "Failed to re-register CUDA buffers during "
                                    "reconnection";
                    all_registered = false;
                }

                if (all_registered) {
                    LOG(INFO)
                        << "Re-registered all shared memorys successfully";

                    // Try to validate RPC connection
                    // Even if register_shm_via_ipc succeeded, we should check
                    // if RPC is responsive
                    auto check_rpc = ping_daemon();
                    if (check_rpc.has_value() &&
                        check_rpc.value().client_status == ClientStatus::OK) {
                        std::lock_guard lock(registered_cuda_buffers_mutex_);
                        if (cuda_transfer_state_ != CudaTransferState::kActive)
                            return;
                        LOG(INFO) << "RPC connection restored";
                        ping_fail_count = 0;
                        connected_.store(true);
                        break;  // Exit reconnection loop
                    }
                }

                LOG(WARNING) << "Reconnection attempt failed, retrying in "
                             << retry_connect_interval_ms << "ms";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(retry_connect_interval_ms));
            }
        } else {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
        }
    }
}

int DummyClient::health_check() {
    if (!ping_running_.load()) return HC_NOT_INITIALIZED;
    if (!connected_.load()) return HC_MASTER_UNREACHABLE;
    if (!last_ping_healthy_.load()) return HC_MASTER_UNREACHABLE;
    return HC_HEALTHY;
}

int DummyClient::request_hot_cache_fd() {
    UdsConnector connector(ipc_socket_path_);
    auto connection_result = connector.connect();
    if (!connection_result) {
        LOG(ERROR) << "Failed to connect IPC socket '" << ipc_socket_path_
                   << "': " << connection_result.error();
        return -1;
    }
    auto connection = std::move(connection_result.value());

    // Send request type
    IpcRequestType type = IPC_SHM_FD_REQUEST;
    if (connection->sendRaw(&type, sizeof(type)) < 0) {
        LOG(ERROR) << "Failed to send IPC request type";
        return -1;
    }

    // Send payload
    ShmFdRequest req;
    req.client_id_first = client_id_.first;
    req.client_id_second = client_id_.second;
    req.segment_type = SHM_SEG_HOT_CACHE;
    if (connection->sendRaw(&req, sizeof(req)) < 0) {
        LOG(ERROR) << "Failed to send ShmFdRequest";
        return -1;
    }

    // Receive fd + response
    ShmFdResponse resp;
    int fd = connection->recvFd(&resp, sizeof(resp));

    if (fd < 0 || resp.status != 0) {
        LOG(ERROR) << "Failed to receive hot cache fd, status=" << resp.status;
        if (fd >= 0) close(fd);
        return -1;
    }

    // mmap the received fd
    void* base = mmap(nullptr, resp.shm_size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        LOG(ERROR) << "Failed to mmap hot cache shm: " << strerror(errno);
        close(fd);
        return -1;
    }

    hot_cache_fd_ = fd;
    hot_cache_base_ = base;
    hot_cache_size_ = resp.shm_size;
    LOG(INFO) << "Hot cache shm mapped at " << base
              << ", size=" << resp.shm_size;
    return 0;
}

}  // namespace mooncake
