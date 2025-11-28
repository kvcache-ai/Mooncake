#include <async_simple/coro/SyncAwait.h>
#include <ylt/easylog/record.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink

#include "real_client.h"
#include "dummy_client.h"
#include "utils/scoped_vlog_timer.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> DummyClient::invoke_rpc(Args&&... args) {
    auto pool = client_accessor_.GetClientPool();

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
    auto pool = client_accessor_.GetClientPool();

    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                  std::vector<tl::expected<ResultType, ErrorCode>>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Dummy Client not available";
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "Batch RPC call failed: " << result.error().msg;
                std::vector<tl::expected<ResultType, ErrorCode>> error_results;
                error_results.reserve(input_size);
                for (size_t i = 0; i < input_size; ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
}

DummyClient::DummyClient() : client_id_(generate_uuid()) {
    // Initialize logging severity (leave as before)
    easylog::set_min_severity(easylog::Severity::WARN);
    // Initialize client pools
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
}

DummyClient::~DummyClient() { tearDownAll(); }

ErrorCode DummyClient::connect(const std::string& server_address) {
    ScopedVLogTimer timer(1, "DummyClient::Connect");
    timer.LogRequest("real_client_addr=", server_address);

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ != server_address) {
        // WARNING: The existing client pool cannot be erased. So if there are a
        // lot of different addresses, there will be resource leak problems.
        auto client_pool = client_pools_->at(server_address);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = server_address;
    }
    auto pool = client_accessor_.GetClientPool();
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result = invoke_rpc<&RealClient::service_ready_internal, void>();
    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        return result.error();
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return ErrorCode::OK;
}

int DummyClient::setup_dummy(size_t mem_pool_size, size_t local_buffer_size,
                             const std::string& server_address) {
    int64_t ret;
    ErrorCode err = connect(server_address);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to real client";
        return -1;
    }

    shm_name_ = "/dummy_client_shm_" + std::to_string(client_id_.first) + "_" +
                std::to_string(client_id_.second);
    shm_size_ = local_buffer_size + mem_pool_size;
    local_buffer_size_ = local_buffer_size;

    // Open or create shared memory object
    int shm_fd =
        shm_open(shm_name_.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (shm_fd == -1) {
        LOG(ERROR) << "Failed to open shared memory: " << shm_name_
                   << ", error: " << strerror(errno);
        return -1;
    }

    // Set the size of the shared memory object
    if (ftruncate(shm_fd, shm_size_) == -1) {
        LOG(ERROR) << "Failed to set shared memory size: " << shm_name_
                   << ", error: " << strerror(errno);
        close(shm_fd);
        shm_unlink(shm_name_.c_str());
        return -1;
    }

    // Map shared memory into the process's address space
    shm_base_addr_ =
        mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_base_addr_ == MAP_FAILED) {
        LOG(ERROR) << "Failed to map shared memory: " << shm_name_
                   << ", error: " << strerror(errno);
        close(shm_fd);
        shm_unlink(shm_name_.c_str());
        return -1;
    }

    // Close the shared memory file descriptor after mapping
    close(shm_fd);

    // Inform the RealClient about the shared memory
    ret = register_shm(shm_name_, reinterpret_cast<uint64_t>(shm_base_addr_),
                       shm_size_, local_buffer_size);

    if (ret) {
        LOG(ERROR) << "RPC call to register_shm failed\n";
        // Clean up shared memory if RPC fails
        munmap(shm_base_addr_, shm_size_);
        shm_unlink(shm_name_.c_str());
        shm_base_addr_ = nullptr;
        shm_size_ = 0;
        return -1;
    }

    // Unlink immediately after real client mmap to avoid leaks, real client
    // can still access it via their own mappings. Also avoid other processes
    // from opening it.
    shm_unlink(shm_name_.c_str());

    ping_running_ = true;
    ping_thread_ = std::thread([this]() mutable { this->ping_thread_main(); });

    return 0;
}

int DummyClient::initAll(const std::string& protocol,
                         const std::string& device_name,
                         size_t mount_segment_size) {
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    return setup_real("", "", 0, buffer_allocator_size, protocol, device_name,
                      "127.0.0.1:50052", nullptr);
}

int DummyClient::tearDownAll() {
    unregister_shm();
    if (shm_base_addr_ != nullptr) {
        if (munmap(shm_base_addr_, shm_size_) == -1) {
            LOG(ERROR) << "Failed to unmap shared memory: " << shm_name_
                       << ", error: " << strerror(errno);
            return -1;
        }
        shm_base_addr_ = nullptr;
        shm_size_ = 0;
        // Dummy client unlinks shm in setup after real client mmap
    }
    if (ping_running_) {
        ping_running_ = false;
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
    }
    return 0;
}

int64_t DummyClient::register_shm(const std::string& shm_name,
                                  uint64_t shm_base_addr, size_t shm_size,
                                  size_t local_buffer_size) {
    return to_py_ret(invoke_rpc<&RealClient::map_shm_internal, void>(
        shm_name, shm_base_addr, shm_size, local_buffer_size));
}

int64_t DummyClient::unregister_shm() {
    return to_py_ret(invoke_rpc<&RealClient::unmap_shm_internal, void>());
}

// Dummy only register buffer within the shared memory region
int DummyClient::register_buffer(void* buffer, size_t size) {
    if (buffer == nullptr) {
        LOG(ERROR) << "Invalid buffer pointer";
        return -1;
    }
    if (shm_base_addr_ == nullptr) {
        LOG(ERROR) << "Shared memory is not registered";
        return -1;
    }
    if (buffer < shm_base_addr_ ||
        reinterpret_cast<uint8_t*>(buffer) + static_cast<uint64_t>(size) >
            reinterpret_cast<uint8_t*>(shm_base_addr_) + shm_size_) {
        LOG(ERROR) << "Buffer to register is out of shared memory bounds";
        return -1;
    }
    auto ret = invoke_rpc<&RealClient::register_shm_buffer_internal, void>(
        reinterpret_cast<uint64_t>(buffer), static_cast<uint64_t>(size));
    if (ret.has_value()) {
        registered_size_ += size;
    }
    return to_py_ret(ret);
}

int DummyClient::unregister_buffer(void* buffer) {
    auto ret = invoke_rpc<&RealClient::unregister_shm_buffer_internal, size_t>(
        reinterpret_cast<uint64_t>(buffer));
    if (ret.has_value()) {
        registered_size_ -= ret.value();
        return 0;
    }
    return to_py_ret(ret);
}

int64_t DummyClient::alloc_from_mem_pool(size_t size) {
    // TODO: using offset allocation strategy later
    if (shm_base_addr_ == nullptr) {
        LOG(ERROR) << "Shared memory is not registered";
        return -1;
    }
    if (registered_size_ + local_buffer_size_ + size > shm_size_) {
        LOG(ERROR) << "Not enough space in shared memory";
        return -1;
    }
    return reinterpret_cast<int64_t>(shm_base_addr_) +
           static_cast<int64_t>(registered_size_);
}

int DummyClient::put(const std::string& key, std::span<const char> value,
                     const ReplicateConfig& config) {
    return to_py_ret(
        invoke_rpc<&RealClient::put_internal, void>(key, value, config));
}

int DummyClient::put_batch(const std::vector<std::string>& keys,
                           const std::vector<std::span<const char>>& values,
                           const ReplicateConfig& config) {
    return to_py_ret(invoke_rpc<&RealClient::put_batch_internal, void>(
        keys, values, config));
}

int DummyClient::put_parts(const std::string& key,
                           std::vector<std::span<const char>> values,
                           const ReplicateConfig& config) {
    return to_py_ret(
        invoke_rpc<&RealClient::put_parts_internal, void>(key, values, config));
}

int DummyClient::remove(const std::string& key) {
    return to_py_ret(invoke_rpc<&RealClient::remove_internal, void>(key));
}

long DummyClient::removeByRegex(const std::string& str) {
    return to_py_ret(
        invoke_rpc<&RealClient::removeByRegex_internal, long>(str));
}

long DummyClient::removeAll() {
    return to_py_ret(invoke_rpc<&RealClient::removeAll_internal, int64_t>());
}

int DummyClient::isExist(const std::string& key) {
    auto result = invoke_rpc<&RealClient::isExist_internal, bool>(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
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

int64_t DummyClient::getSize(const std::string& key) {
    return to_py_ret(invoke_rpc<&RealClient::getSize_internal, int64_t>(key));
}

std::shared_ptr<BufferHandle> DummyClient::get_buffer(const std::string& key) {
    // Dummy client does not use BufferHandle, so we return nullptr
    return nullptr;
}

std::tuple<uint64_t, size_t> DummyClient::get_buffer_info(
    const std::string& key) {
    auto result = invoke_rpc<&RealClient::get_dummy_buffer_internal,
                             std::tuple<uint64_t, size_t>>(key);
    if (!result.has_value()) {
        LOG(ERROR) << "Get buffer failed: " << toString(result.error());
        return std::make_tuple(0, 0);
    }
    return result.value();
}

std::vector<std::shared_ptr<BufferHandle>> DummyClient::batch_get_buffer(
    const std::vector<std::string>& keys) {
    // TODO: implement this function
    return std::vector<std::shared_ptr<BufferHandle>>();
}

int64_t DummyClient::get_into(const std::string& key, void* buffer,
                              size_t size) {
    // TODO: implement this function
    return -1;
}

std::string DummyClient::get_hostname() const {
    // Dummy client does not have a hostname
    return "";
}

std::vector<int> DummyClient::batch_put_from(
    const std::vector<std::string>& keys, const std::vector<void*>& buffer_ptrs,
    const std::vector<size_t>& sizes, const ReplicateConfig& config) {
    std::vector<uint64_t> buffers;
    for (auto ptr : buffer_ptrs) {
        buffers.push_back(reinterpret_cast<uint64_t>(ptr));
    }
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_put_from_dummy_internal, void>(
            keys.size(), keys, buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

int DummyClient::put_from(const std::string& key, void* buffer, size_t size,
                          const ReplicateConfig& config) {
    // TODO: implement this function
    return -1;
}

std::vector<int64_t> DummyClient::batch_get_into(
    const std::vector<std::string>& keys, const std::vector<void*>& buffer_ptrs,
    const std::vector<size_t>& sizes) {
    std::vector<uint64_t> buffers;
    for (auto ptr : buffer_ptrs) {
        buffers.push_back(reinterpret_cast<uint64_t>(ptr));
    }
    auto internal_results =
        invoke_batch_rpc<&RealClient::batch_get_into_dummy_internal, int64_t>(
            keys.size(), keys, buffers, sizes);
    std::vector<int64_t> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

int DummyClient::put_from_with_metadata(const std::string& key, void* buffer,
                                        void* metadata_buffer, size_t size,
                                        size_t metadata_size,
                                        const ReplicateConfig& config) {
    // TODO: implement this function
    return -1;
}

std::vector<int> DummyClient::batch_put_from_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffer_ptrs,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReplicateConfig& config) {
    // TODO: implement this function
    std::vector<int> vec(keys.size(), -1);
    return vec;
}

std::vector<int> DummyClient::batch_get_into_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffer_ptrs,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node) {
    // TODO: implement this function
    std::vector<int> vec(keys.size(), -1);
    return vec;
}

void DummyClient::ping_thread_main() {
    // How many failed pings before getting latest master view from etcd
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;
    while (ping_running_) {
        auto ping_result =
            invoke_rpc<&RealClient::ping, PingResponse>(client_id_);
        if (ping_result.has_value() &&
            ping_result.value().client_status == ClientStatus::OK) {
            ping_fail_count = 0;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        } else {
            ping_fail_count++;
            if (ping_fail_count >= max_ping_fail_count) {
                LOG(ERROR) << "Ping failed " << ping_fail_count
                           << " times, reconnecting...";
                // TODO: Need to realize reconnect logic here
                ping_fail_count = 0;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }
    }
}

}  // namespace mooncake