#pragma once

#include <string>
#include <memory>
#include <optional>
#include <vector>
#include <span>
#include <tuple>
#include <ylt/util/tl/expected.hpp>

#include "common_types.h"
#if defined(MOONCAKE_STORE_CLIENT_P2P)
#include "p2p/common/p2p_types.h"
#else
#include "rpc_types.h"
#endif
#include "client_buffer.hpp"

namespace mooncake {

#define MOONCAKE_SHM_NAME "mooncake_shm"
// Protocol structure for IPC registration
struct ShmRegisterRequest {
    uint64_t client_id_first;
    uint64_t client_id_second;
    uint64_t dummy_base_addr;
    uint64_t shm_size;
    bool is_local_buffer;
};

#if defined(MOONCAKE_STORE_CLIENT_P2P)
using WriteConfig = P2PWriteRouteConfig;
using ReadConfig = P2PReadRouteConfig;
using ObjectDescriptor = P2PRouteDescriptor;
#else
using WriteConfig = ReplicateConfig;
using ReadConfig = CentralizedReadConfig;
using ObjectDescriptor = Replica::Descriptor;
#endif

enum class WriteOperation { Put, Publish };

// Python-specific wrapper class for client interface
class PyClient {
   public:
    virtual ~PyClient() = 0;

    virtual int initAll(const std::string& protocol,
                        const std::string& device_name,
                        size_t mount_segment_size) = 0;

    virtual uint64_t alloc_from_mem_pool(size_t size) = 0;

    virtual DeploymentMode deployment_mode() const = 0;

    virtual int put(
        const std::string& key, std::span<const char> value,
        const std::optional<WriteConfig>& config = std::nullopt) = 0;

    virtual int register_buffer(void* buffer, size_t size) = 0;

    virtual int unregister_buffer(void* buffer) = 0;

    virtual int64_t get_into(
        const std::string& key, void* buffer, size_t size,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual std::vector<int64_t> batch_get_into(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual std::vector<int> batch_get_into_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        bool aggregate_same_segment_task,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual int put_from(
        const std::string& key, void* buffer, size_t size,
        const std::optional<WriteConfig>& config = std::nullopt) = 0;

    virtual int put_from_with_metadata(
        const std::string& key, void* buffer, void* metadata_buffer,
        size_t size, size_t metadata_size,
        const std::optional<WriteConfig>& config = std::nullopt) = 0;

    virtual std::vector<int> batch_put_from(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<WriteConfig>& config = std::nullopt,
        WriteOperation operation = WriteOperation::Put) = 0;

    virtual std::vector<int> batch_put_from_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const std::optional<WriteConfig>& config = std::nullopt) = 0;

    virtual std::shared_ptr<BufferHandle> get_buffer(
        const std::string& key,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual std::tuple<uint64_t, size_t> get_buffer_info(
        const std::string& key,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string>& keys,
        const std::optional<ReadConfig>& config = std::nullopt) = 0;

    virtual int put_parts(
        const std::string& key, std::vector<std::span<const char>> values,
        const std::optional<WriteConfig>& config = std::nullopt,
        WriteOperation operation = WriteOperation::Put) = 0;

    virtual int put_batch(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const std::optional<WriteConfig>& config = std::nullopt) = 0;

    [[nodiscard]] virtual std::string get_hostname() const = 0;

    virtual int remove(const std::string& key, bool force = false) = 0;

    virtual long removeByRegex(const std::string& str, bool force = false) = 0;

    virtual long removeAll(bool force = false) = 0;

    virtual long removeAllLocal() = 0;

    virtual int removeLocal(const std::string& key) = 0;

    virtual int isExist(const std::string& key) = 0;

    virtual std::vector<int> batchIsExist(
        const std::vector<std::string>& keys) = 0;

    virtual int64_t getSize(const std::string& key) = 0;

    virtual std::map<std::string, std::vector<ObjectDescriptor>>
    batch_get_replica_desc(const std::vector<std::string>& keys) = 0;
    virtual std::vector<ObjectDescriptor> get_replica_desc(
        const std::string& key) = 0;

    virtual int tearDownAll() = 0;

#if !defined(MOONCAKE_STORE_CLIENT_P2P)
    virtual tl::expected<UUID, ErrorCode> create_copy_task(
        const std::string& key, const std::vector<std::string>& targets) = 0;

    virtual tl::expected<UUID, ErrorCode> create_move_task(
        const std::string& key, const std::string& source,
        const std::string& target) = 0;

    virtual tl::expected<QueryTaskResponse, ErrorCode> query_task(
        const UUID& task_id) = 0;
#endif

    virtual bool is_initialized() const = 0;
};

}  // namespace mooncake
