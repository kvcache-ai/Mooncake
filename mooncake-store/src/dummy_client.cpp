#include <async_simple/coro/SyncAwait.h>
#include <ylt/easylog/record.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink
#include <cstdlib>

#include "real_client.h"
#include "dummy_client.h"
#include "utils.h"
#include "utils/scoped_vlog_timer.h"
#include "rpc_types.h"
#include "types.h"
#include "default_config.h"

namespace mooncake {

std::mutex ShmHelper::shm_mutex_;

static int memfd_create_wrapper(const char* name, unsigned int flags) {
#ifdef __NR_memfd_create
    return syscall(__NR_memfd_create, name, flags);
#else
    return -1;  // Or appropriate fallback/error
#endif
}

ShmHelper* ShmHelper::getInstance() {
    static ShmHelper instance;
    return &instance;
}

ShmHelper::ShmHelper() {
    const char* hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
}

ShmHelper::~ShmHelper() { cleanup(); }

bool ShmHelper::cleanup() {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    bool ret = true;
    for (auto& shm : shms_) {
        if (shm->fd != -1) {
            close(shm->fd);
            shm->fd = -1;
        }
        if (shm->base_addr) {
            if (munmap(shm->base_addr, shm->size) == -1) {
                LOG(ERROR) << "Failed to unmap shared memory: "
                           << strerror(errno);
                ret = false;
            }
            shm->base_addr = nullptr;
        }
    }
    shms_.clear();
    return ret;
}

void* ShmHelper::allocate(size_t size) {
    std::lock_guard<std::mutex> lock(shm_mutex_);

    unsigned int flags = MFD_CLOEXEC;
    if (use_hugepage_) {
        bool use_memfd = true;
        size = align_up(size, get_hugepage_size_from_env(&flags, use_memfd));
        LOG(INFO) << "Using huge pages for shared memory, size: " << size;
    }

    // Create memfd
    int fd = memfd_create_wrapper(MOONCAKE_SHM_NAME, flags);
    if (fd == -1) {
        std::string extra_msg =
            use_hugepage_ ? " (Check /proc/sys/vm/nr_hugepages?)" : "";
        throw std::runtime_error("Failed to create anonymous shared memory" +
                                 extra_msg + ": " +
                                 std::string(strerror(errno)));
    }

    // Set size
    if (ftruncate(fd, size) == -1) {
        close(fd);
        throw std::runtime_error("Failed to set shared memory size: " +
                                 std::string(strerror(errno)));
    }

    // Map memory
    void* base_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_POPULATE, fd, 0);
    if (base_addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to map shared memory: " +
                                 std::string(strerror(errno)));
    }

    auto shm = std::make_shared<ShmSegment>();
    shm->fd = fd;
    shm->base_addr = base_addr;
    shm->size = size;
    shm->name = MOONCAKE_SHM_NAME;
    shm->registered = false;
    shms_.push_back(shm);

    return base_addr;
}

std::shared_ptr<ShmHelper::ShmSegment> ShmHelper::get_shm(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto& shm : shms_) {
        if (addr >= shm->base_addr &&
            reinterpret_cast<uint8_t*>(addr) <
                reinterpret_cast<uint8_t*>(shm->base_addr) + shm->size) {
            return shm;
        }
    }
    return nullptr;
}

int ShmHelper::free(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto it = shms_.begin(); it != shms_.end(); ++it) {
        if ((*it)->base_addr == addr) {
            if ((*it)->fd != -1) {
                close((*it)->fd);
            }
            if ((*it)->base_addr &&
                munmap((*it)->base_addr, (*it)->size) == -1) {
                LOG(ERROR) << "Failed to unmap shared memory during free: "
                           << strerror(errno);
                return -1;
            }
            LOG(INFO) << "Freed shared memory at " << addr
                      << ", size: " << (*it)->size;
            shms_.erase(it);
            return 0;
        }
    }
    LOG(ERROR) << "Attempted to free unknown shared memory address: " << addr;
    return -1;
}

static int send_fd(int socket, int fd, void* data, size_t data_len) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));

    memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));

    return sendmsg(socket, &msg, 0);
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> DummyClient::invoke_rpc(Args&&... args) {
    auto pool = client_accessor_.GetClientPool();

    if constexpr (!std::is_same_v<
                      std::remove_reference_t<decltype(ServiceMethod)>,
                      std::remove_reference_t<decltype(&RealClient::ping)>> &&
                  !std::is_same_v<
                      std::remove_reference_t<decltype(ServiceMethod)>,
                      std::remove_reference_t<
                          decltype(&RealClient::service_ready_internal)>>) {
        if (!connected_) {
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
    auto pool = client_accessor_.GetClientPool();
    if (!connected_) {
        LOG(ERROR) << "Dummy Client not connected";
        std::vector<tl::expected<ResultType, ErrorCode>> error_results;
        error_results.reserve(input_size);
        for (size_t i = 0; i < input_size; ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

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
    mooncake::init_ylt_log_level();
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
    connected_ = true;
    return ErrorCode::OK;
}

int DummyClient::register_shm_via_ipc(const ShmHelper::ShmSegment* shm,
                                      bool is_local) {
    if (shm->fd < 0) {
        LOG(ERROR) << "Invalid shm_fd during IPC registration";
        return -1;
    }

    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        LOG(ERROR) << "Failed to create IPC socket: " << strerror(errno);
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    // Use abstract namespace
    std::string abstract_name = ipc_socket_path_;
    if (abstract_name.size() > sizeof(addr.sun_path) - 2) {
        LOG(ERROR) << "IPC socket path too long";
        close(sock_fd);
        return -1;
    }
    addr.sun_path[0] = '\0';
    strncpy(addr.sun_path + 1, abstract_name.c_str(),
            sizeof(addr.sun_path) - 2);
    socklen_t addr_len = sizeof(sa_family_t) + 1 + abstract_name.length();
    LOG(INFO) << "Connecting to IPC socket: " << abstract_name;

    if (::connect(sock_fd, (struct sockaddr*)&addr, addr_len) < 0) {
        // This is expected if RealClient is down
        close(sock_fd);
        return -1;
    }

    ShmRegisterRequest req;
    req.client_id_first = client_id_.first;
    req.client_id_second = client_id_.second;
    req.dummy_base_addr = reinterpret_cast<uintptr_t>(shm->base_addr);
    req.shm_size = shm->size;
    req.is_local_buffer = is_local;

    if (send_fd(sock_fd, shm->fd, &req, sizeof(req)) < 0) {
        LOG(ERROR) << "Failed to send FD to RealClient: " << strerror(errno);
        close(sock_fd);
        return -1;
    }

    int status = -1;
    if (recv(sock_fd, &status, sizeof(status), 0) < 0) {
        LOG(ERROR) << "Failed to receive response from RealClient";
        close(sock_fd);
        return -1;
    }

    close(sock_fd);

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
    void* base_addr = nullptr;
    ErrorCode err = connect(server_address);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to real client";
        return -1;
    }

    shm_helper_ = ShmHelper::getInstance();
    try {
        base_addr = shm_helper_->allocate(local_buffer_size);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to allocate shared memory: " << e.what();
        return -1;
    }

    ipc_socket_path_ = ipc_socket_path;

    // Attempt registration for the primary segment
    auto local_buffer_shm = shm_helper_->get_shm(base_addr);
    if (!local_buffer_shm) {
        LOG(ERROR) << "Failed to get shm segment for base address";
        shm_helper_->free(base_addr);
        return -1;
    }

    if (register_shm_via_ipc(local_buffer_shm.get(), true) != 0) {
        LOG(ERROR) << "Failed to register SHM via IPC";
        // Register failed, cleanup
        shm_helper_->free(local_buffer_shm->base_addr);
        return -1;
    }
    local_buffer_shm->registered = true;
    local_buffer_shm->is_local = true;

    ping_running_ = true;
    ping_thread_ = std::thread([this]() mutable { this->ping_thread_main(); });

    return 0;
}

int DummyClient::tearDownAll() {
    unregister_shm();

    if (ping_running_) {
        ping_running_ = false;
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
    }
    return 0;
}

int64_t DummyClient::unregister_shm() {
    return to_py_ret(
        invoke_rpc<&RealClient::unmap_shm_internal, void>(client_id_));
}

// Dummy only register buffer within the shared memory region
int DummyClient::register_buffer(void* buffer, size_t size) {
    if (buffer == nullptr) {
        LOG(ERROR) << "Invalid buffer pointer";
        return -1;
    }
    // Find which shm this buffer belongs to
    auto shm = shm_helper_->get_shm(buffer);
    if (!shm) {
        LOG(ERROR) << "Buffer is not in any registered shared memory";
        return -1;
    }
    if (shm_helper_->is_hugepage()) {
        size = align_up(size, get_hugepage_size_from_env());
    }
    // Check bounds
    if (reinterpret_cast<uint8_t*>(buffer) !=
            reinterpret_cast<uint8_t*>(shm->base_addr) ||
        size != shm->size) {
        LOG(ERROR) << "Invalid buffer address or size for registration: "
                      "Buffer addr: "
                   << buffer << ", need addr: " << shm->base_addr
                   << ", buffer size: " << size << ", need size: " << shm->size;
        return -1;
    }

    // If this shm is not registered with RealClient yet, do it now
    if (!shm->registered) {
        if (register_shm_via_ipc(shm.get(), shm->is_local) != 0) {
            LOG(ERROR) << "Failed to implicitly register new SHM via IPC";
            return -1;
        }
        shm->registered = true;
    }

    return 0;
}

int DummyClient::unregister_buffer(void* buffer) {
    if (buffer == nullptr) {
        LOG(ERROR) << "Invalid buffer pointer";
        return -1;
    }

    auto shm = shm_helper_->get_shm(buffer);
    if (!shm) {
        LOG(ERROR) << "Buffer is not in any registered shared memory";
        return -1;
    }
    if (!shm->registered) {
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
    if (ret.has_value()) {
        shm->registered = false;
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
    return to_py_ret(invoke_rpc<&RealClient::put_dummy_helper, void>(
        key, value, config, client_id_));
}

int DummyClient::put_batch(const std::vector<std::string>& keys,
                           const std::vector<std::span<const char>>& values,
                           const ReplicateConfig& config) {
    return to_py_ret(invoke_rpc<&RealClient::put_batch_dummy_helper, void>(
        keys, values, config, client_id_));
}

int DummyClient::put_parts(const std::string& key,
                           std::vector<std::span<const char>> values,
                           const ReplicateConfig& config) {
    return to_py_ret(invoke_rpc<&RealClient::put_parts_dummy_helper, void>(
        key, values, config, client_id_));
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

int64_t DummyClient::get_buffer_range(const std::string& key, void* dest_buffer,
                                      size_t dest_offset, size_t source_offset,
                                      size_t size) {
    // TODO: implement this function
    // void* cannot be serialized over RPC, need to use helper function similar
    // to batch_get_into
    return -1;
}

std::tuple<uint64_t, size_t> DummyClient::get_buffer_info(
    const std::string& key) {
    auto result = invoke_rpc<&RealClient::get_buffer_info_dummy_helper,
                             std::tuple<uint64_t, size_t>>(key, client_id_);
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
        invoke_batch_rpc<&RealClient::batch_put_from_dummy_helper, void>(
            keys.size(), keys, buffers, sizes, config, client_id_);
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
        invoke_batch_rpc<&RealClient::batch_get_into_dummy_helper, int64_t>(
            keys.size(), keys, buffers, sizes, client_id_);
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

void DummyClient::ping_thread_main() {
    const int max_ping_fail_count = 1;
    const int success_ping_interval_ms = 1000;
    const int fail_ping_interval_ms = 1000;
    const int retry_connect_interval_ms = 2000;

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
        }

        // Ping failed
        ping_fail_count++;
        LOG(WARNING) << "Ping failed " << ping_fail_count << "/"
                     << max_ping_fail_count;

        if (ping_fail_count >= max_ping_fail_count) {
            connected_ = false;
            LOG(ERROR) << "RealClient lost, entering reconnection loop...";

            // Reconnection Loop
            while (ping_running_) {
                // Re-register ALL shms
                bool all_registered = true;
                const auto& shms = shm_helper_->get_shms();
                for (const auto& shm_ptr : shms) {
                    if (shm_ptr->registered) {
                        if (register_shm_via_ipc(shm_ptr.get(),
                                                 shm_ptr->is_local) != 0) {
                            LOG(WARNING)
                                << "Failed to re-register shared memory "
                                   "during reconnection";
                            all_registered = false;
                            break;
                        }
                    }
                }

                if (all_registered) {
                    LOG(INFO)
                        << "Re-registered all shared memorys successfully";

                    // Try to validate RPC connection
                    // Even if register_shm_via_ipc succeeded, we should check
                    // if RPC is responsive
                    auto check_rpc =
                        invoke_rpc<&RealClient::ping, PingResponse>(client_id_);
                    if (check_rpc.has_value()) {
                        LOG(INFO) << "RPC connection restored";
                        ping_fail_count = 0;
                        connected_ = true;
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

}  // namespace mooncake
