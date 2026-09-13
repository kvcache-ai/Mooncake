#include "engram/engram_store.h"

#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <vector>

#include "client_buffer.h"
#include "pyclient.h"

namespace mooncake {
namespace engram {

// Local table mappings use the same RAII buffer handle as Store client buffers.
// A layer directory is published atomically, after all heads have been copied.
// Published files are immutable; removing them is an explicit deployment
// action.
struct EngramStore::LocalTables {
    std::vector<BufferHandle> heads;

    struct File {
        int fd;
        File(const std::filesystem::path& path, int flags)
            : fd(::open(path.c_str(), flags | O_CLOEXEC, 0600)) {
            if (fd < 0)
                throw std::system_error(errno, std::generic_category(),
                                        path.string());
        }
        ~File() { ::close(fd); }
        File(const File&) = delete;
        File& operator=(const File&) = delete;
    };

    static std::filesystem::path path(const std::string& root, int layer_id) {
        return std::filesystem::path(root) /
               ("layer-" + std::to_string(layer_id));
    }

    static std::string layout(const EngramStoreConfig& cfg) {
        std::ostringstream out;
        out << "mooncake-engram-v1\n" << cfg.row_bytes << '\n';
        for (auto rows : cfg.table_vocab_sizes) out << rows << '\n';
        return out.str();
    }

    static BufferHandle map(int fd, size_t size, bool writable) {
        void* ptr =
            ::mmap(nullptr, size, PROT_READ | (writable ? PROT_WRITE : 0),
                   MAP_SHARED | (writable ? 0 : MAP_POPULATE), fd, 0);
        if (ptr == MAP_FAILED)
            throw std::system_error(errno, std::generic_category(),
                                    "Engram mmap");
        return BufferHandle(ptr, size, [ptr, size] { ::munmap(ptr, size); });
    }

    static std::shared_ptr<LocalTables> open(const std::filesystem::path& dir,
                                             const EngramStoreConfig& cfg) {
        if (!std::filesystem::exists(dir)) return nullptr;
        std::ifstream manifest(dir / "layout");
        std::string actual((std::istreambuf_iterator<char>(manifest)), {});
        if (!manifest || actual != layout(cfg))
            throw std::runtime_error("Local Engram layout mismatch: " +
                                     dir.string());
        auto tables = std::make_shared<LocalTables>();
        tables->heads.reserve(cfg.table_vocab_sizes.size());
        for (size_t h = 0; h < cfg.table_vocab_sizes.size(); ++h) {
            const size_t size =
                static_cast<size_t>(cfg.table_vocab_sizes[h]) * cfg.row_bytes;
            File file(dir / ("head-" + std::to_string(h) + ".bin"), O_RDONLY);
            struct stat statbuf{};
            if (::fstat(file.fd, &statbuf) != 0 || statbuf.st_size < 0 ||
                static_cast<uint64_t>(statbuf.st_size) != size)
                throw std::runtime_error("Local Engram table size mismatch: " +
                                         dir.string());
            tables->heads.push_back(map(file.fd, size, false));
        }
        return tables;
    }

    static std::shared_ptr<LocalTables> populate(
        const std::filesystem::path& dir, const EngramStoreConfig& cfg,
        const std::vector<void*>& buffers, const std::vector<size_t>& sizes) {
        File lock(dir.string() + ".lock", O_CREAT | O_RDWR);
        if (::flock(lock.fd, LOCK_EX) != 0)
            throw std::system_error(errno, std::generic_category(),
                                    "Engram populate lock");
        if (std::filesystem::exists(dir))
            throw std::runtime_error(
                "Local Engram populate requires an absent layer: " +
                dir.string());
        const std::filesystem::path temporary = dir.string() + ".tmp";
        // Only a writer holding this layer's lock may recover an interrupted
        // load.
        std::filesystem::remove_all(temporary);
        std::filesystem::create_directory(temporary);
        try {
            for (size_t h = 0; h < buffers.size(); ++h) {
                File file(temporary / ("head-" + std::to_string(h) + ".bin"),
                          O_CREAT | O_EXCL | O_RDWR);
                if (sizes[h] >
                    static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
                    throw std::runtime_error(
                        "Local Engram table exceeds file offset range");
                // Reserve space before memcpy so an exhausted tmpfs reports an
                // error here instead of delivering SIGBUS during a mapped
                // write.
                int rc =
                    ::posix_fallocate(file.fd, 0, static_cast<off_t>(sizes[h]));
                if (rc != 0)
                    throw std::system_error(rc, std::generic_category(),
                                            "Engram allocation");
                auto mapping = map(file.fd, sizes[h], true);
                std::memcpy(mapping.ptr(), buffers[h], sizes[h]);
                if (::fchmod(file.fd, 0400) != 0)
                    throw std::system_error(errno, std::generic_category(),
                                            "Engram read-only table");
            }
            std::ofstream manifest(temporary / "layout");
            manifest << layout(cfg);
            manifest.close();
            if (!manifest)
                throw std::runtime_error("Failed to write local Engram layout");
            auto tables = open(temporary, cfg);
            std::filesystem::rename(temporary, dir);
            return tables;
        } catch (...) {
            std::filesystem::remove_all(temporary);
            throw;
        }
    }
};

EngramStore::EngramStore(const std::map<int, EngramStoreConfig>& layers,
                         std::shared_ptr<PyClient> store,
                         const std::string& local_dir)
    : store_(std::move(store)), local_dir_(local_dir) {
    if (store_ && !local_dir_.empty()) {
        throw std::invalid_argument(
            "store_client and local_dir are mutually exclusive");
    }
    if (layers.empty()) {
        throw std::invalid_argument("EngramStore requires at least one layer");
    }
    for (const auto& [layer_id, config] : layers) {
        if (layer_id < 0 || config.row_bytes <= 0 ||
            config.table_vocab_sizes.empty()) {
            throw std::invalid_argument(
                "EngramStore requires nonnegative layer IDs, positive "
                "row_bytes and nonempty tables");
        }
        Layer layer{config, {}, {}};
        for (size_t h = 0; h < config.table_vocab_sizes.size(); ++h) {
            const int64_t vocab_size = config.table_vocab_sizes[h];
            if (vocab_size <= 0 ||
                static_cast<uint64_t>(vocab_size) >
                    std::numeric_limits<size_t>::max() / config.row_bytes) {
                throw std::invalid_argument("Invalid Engram table size");
            }
            std::ostringstream key;
            key << "engram:l" << layer_id << ":h" << h;
            layer.keys.push_back(key.str());
        }
        layers_.emplace(layer_id, std::move(layer));
    }
    if (!local_dir_.empty()) {
        local_dir_ = std::filesystem::absolute(local_dir_).string();
        std::filesystem::create_directories(local_dir_);
        for (auto& [id, layer] : layers_) {
            layer.local_tables = LocalTables::open(
                LocalTables::path(local_dir_, id), layer.config);
        }
    }
}

const EngramStore::Layer& EngramStore::get_layer(int layer_id) const {
    auto it = layers_.find(layer_id);
    if (it == layers_.end()) {
        throw std::invalid_argument("Unknown Engram layer: " +
                                    std::to_string(layer_id));
    }
    return it->second;
}

int EngramStore::lookup_into(int layer_id, const int64_t* row_ids, int B, int L,
                             void* output_buffer, size_t output_size) const {
    const auto& layer = get_layer(layer_id);
    const auto& table_vocab_sizes = layer.config.table_vocab_sizes;
    const auto& embed_keys = layer.keys;
    if ((!store_ && local_dir_.empty()) || row_ids == nullptr ||
        output_buffer == nullptr || B <= 0 || L <= 0) {
        return -1;
    }

    const int num_heads = static_cast<int>(table_vocab_sizes.size());
    const size_t row_bytes = layer.config.row_bytes;
    const size_t max_size = std::numeric_limits<size_t>::max();
    if (static_cast<size_t>(B) > max_size / static_cast<size_t>(L)) {
        return -1;
    }
    const size_t token_count = static_cast<size_t>(B) * static_cast<size_t>(L);
    if (token_count > max_size / static_cast<size_t>(num_heads) ||
        token_count * static_cast<size_t>(num_heads) > max_size / row_bytes) {
        return -1;
    }
    const size_t expected_size =
        token_count * static_cast<size_t>(num_heads) * row_bytes;
    if (output_size < expected_size) {
        return -1;
    }

    auto fail_lookup = [&]() {
        std::memset(output_buffer, 0, expected_size);
        return -1;
    };

    if (!local_dir_.empty()) {
        auto tables = std::atomic_load(&layer.local_tables);
        if (!tables) {
            // Also support a reader constructed before the writer publishes.
            // Only the first successful lookup opens files; subsequent reads
            // use the retained mappings, including after the writer exits.
            tables = LocalTables::open(LocalTables::path(local_dir_, layer_id),
                                       layer.config);
            if (!tables) return fail_lookup();
            std::atomic_store(&layer.local_tables, tables);
        }
        // Validate all IDs before touching output. No metadata or transfer
        // work.
        for (size_t t = 0; t < token_count; ++t) {
            for (int h = 0; h < num_heads; ++h) {
                const auto id = row_ids[t * num_heads + h];
                if (id < 0 || id >= table_vocab_sizes[h]) return fail_lookup();
            }
        }
        auto* dst = static_cast<char*>(output_buffer);
        for (size_t t = 0; t < token_count; ++t) {
            for (int h = 0; h < num_heads; ++h) {
                const size_t index = t * num_heads + h;
                const auto* src =
                    static_cast<const char*>(tables->heads[h].ptr());
                std::memcpy(
                    dst + index * row_bytes,
                    src + static_cast<size_t>(row_ids[index]) * row_bytes,
                    row_bytes);
            }
        }
        return 0;
    }

    std::vector<void*> buffers{output_buffer};
    std::vector<std::vector<std::string>> all_keys(1);
    std::vector<std::vector<std::vector<size_t>>> all_dst_offsets(1);
    std::vector<std::vector<std::vector<size_t>>> all_src_offsets(1);
    std::vector<std::vector<std::vector<size_t>>> all_sizes(1);

    all_keys[0].reserve(static_cast<size_t>(num_heads));
    all_dst_offsets[0].reserve(static_cast<size_t>(num_heads));
    all_src_offsets[0].reserve(static_cast<size_t>(num_heads));
    all_sizes[0].reserve(static_cast<size_t>(num_heads));

    for (int h = 0; h < num_heads; ++h) {
        all_keys[0].push_back(embed_keys[h]);
        all_dst_offsets[0].emplace_back();
        all_src_offsets[0].emplace_back();
        all_sizes[0].emplace_back();
        all_dst_offsets[0].back().reserve(static_cast<size_t>(B) * L);
        all_src_offsets[0].back().reserve(static_cast<size_t>(B) * L);
        all_sizes[0].back().reserve(static_cast<size_t>(B) * L);
    }

    for (int b = 0; b < B; ++b) {
        for (int l = 0; l < L; ++l) {
            const size_t token_index = static_cast<size_t>(b) * L + l;
            const size_t row_offset =
                token_index * static_cast<size_t>(num_heads);
            for (int h = 0; h < num_heads; ++h) {
                const int64_t idx =
                    row_ids[row_offset + static_cast<size_t>(h)];
                if (idx < 0 || idx >= table_vocab_sizes[h]) {
                    return fail_lookup();
                }
                all_dst_offsets[0][h].push_back(
                    (row_offset + static_cast<size_t>(h)) * row_bytes);
                all_src_offsets[0][h].push_back(static_cast<size_t>(idx) *
                                                row_bytes);
                all_sizes[0][h].push_back(row_bytes);
            }
        }
    }

    mooncake::PyClient::QueryResultCache query_result_cache;
    auto query_results = store_->batch_query(embed_keys);
    if (query_results.size() != embed_keys.size()) {
        return fail_lookup();
    }
    query_result_cache.reserve(embed_keys.size());
    for (size_t i = 0; i < embed_keys.size(); ++i) {
        query_result_cache.emplace(embed_keys[i], query_results[i]);
    }

    auto results = store_->get_into_ranges(buffers, all_keys, all_dst_offsets,
                                           all_src_offsets, all_sizes,
                                           &query_result_cache);
    if (results.size() != 1 ||
        results[0].size() != static_cast<size_t>(num_heads)) {
        return fail_lookup();
    }
    for (int h = 0; h < num_heads; ++h) {
        if (results[0][h].size() != all_sizes[0][h].size()) {
            return fail_lookup();
        }
        for (int64_t bytes_read : results[0][h]) {
            if (bytes_read != static_cast<int64_t>(row_bytes)) {
                return fail_lookup();
            }
        }
    }

    return 0;
}

std::vector<int> EngramStore::get_layer_ids() const {
    std::vector<int> ids;
    for (const auto& [id, layer] : layers_) ids.push_back(id);
    return ids;
}

std::vector<int64_t> EngramStore::get_table_vocab_sizes(int layer_id) const {
    return get_layer(layer_id).config.table_vocab_sizes;
}

std::vector<std::string> EngramStore::get_store_keys(int layer_id) const {
    return get_layer(layer_id).keys;
}

int EngramStore::get_num_heads(int layer_id) const {
    return static_cast<int>(get_layer(layer_id).keys.size());
}

int EngramStore::get_row_bytes(int layer_id) const {
    return get_layer(layer_id).config.row_bytes;
}

int EngramStore::remove_from_store(int layer_id, bool force) {
    const auto& embed_keys = get_layer(layer_id).keys;
    if (store_ == nullptr) {
        return static_cast<int>(ErrorCode::INVALID_PARAMS);
    }

    constexpr int kObjectNotFound =
        static_cast<int>(ErrorCode::OBJECT_NOT_FOUND);
    int removed = 0;
    int first_error = 0;

    for (const auto& key : embed_keys) {
        int rc = store_->remove(key, force);
        if (rc == 0) {
            ++removed;
            continue;
        }
        if (rc == kObjectNotFound) {
            continue;
        }
        if (first_error == 0) {
            first_error = rc;
        }
    }

    return first_error != 0 ? first_error : removed;
}

int EngramStore::populate(int layer_id,
                          const std::vector<void*>& embedding_buffers,
                          const std::vector<size_t>& buffer_sizes,
                          const ReplicateConfig& config) {
    const auto& layer = get_layer(layer_id);
    const auto& table_vocab_sizes = layer.config.table_vocab_sizes;
    const auto& embed_keys = layer.keys;
    if (store_ == nullptr && local_dir_.empty()) {
        return -1;
    }
    if (embedding_buffers.size() != embed_keys.size() ||
        buffer_sizes.size() != embed_keys.size()) {
        return -1;
    }

    for (size_t i = 0; i < buffer_sizes.size(); ++i) {
        const size_t expected =
            static_cast<size_t>(table_vocab_sizes[i]) * layer.config.row_bytes;
        if (embedding_buffers[i] == nullptr || buffer_sizes[i] != expected) {
            return -1;
        }
    }

    if (!local_dir_.empty()) {
        auto tables = LocalTables::populate(
            LocalTables::path(local_dir_, layer_id), layer.config,
            embedding_buffers, buffer_sizes);
        std::atomic_store(&layer.local_tables, tables);
        return 0;
    }

    std::vector<int> exists_results = store_->batchIsExist(embed_keys);
    if (exists_results.size() != embed_keys.size()) {
        LOG(ERROR) << "Failed to preflight EngramStore populate key existence";
        return -1;
    }
    for (size_t i = 0; i < exists_results.size(); ++i) {
        const int exists = exists_results[i];
        if (exists < 0) {
            LOG(ERROR) << "Failed to query EngramStore key '" << embed_keys[i]
                       << "' before populate, rc=" << exists;
            return -1;
        }
        if (exists != 0) {
            LOG(ERROR)
                << "EngramStore populate requires empty destination key '"
                << embed_keys[i] << "'. Remove the existing layer first.";
            return -1;
        }
    }

    auto cleanup_registered_buffers = [&](size_t count) {
        bool cleanup_failed = false;
        for (size_t i = 0; i < count; ++i) {
            int rc = store_->unregister_buffer(embedding_buffers[i]);
            if (rc != 0) {
                cleanup_failed = true;
                LOG(ERROR) << "Failed to unregister embedding buffer at index "
                           << i << ", rc=" << rc;
            }
        }
        return cleanup_failed;
    };

    for (size_t i = 0; i < embedding_buffers.size(); ++i) {
        int ret =
            store_->register_buffer(embedding_buffers[i], buffer_sizes[i]);
        if (ret != 0) {
            if (cleanup_registered_buffers(i)) {
                LOG(ERROR) << "Failed to clean up registered embedding buffers "
                              "after register_buffer error";
            }
            return -1;
        }
    }

    std::vector<int> put_results = store_->batch_put_from(
        embed_keys, embedding_buffers, buffer_sizes, config);
    const bool put_succeeded =
        put_results.size() == embed_keys.size() &&
        std::all_of(put_results.begin(), put_results.end(),
                    [](int result) { return result == 0; });

    const bool unregister_failed =
        cleanup_registered_buffers(embedding_buffers.size());

    if (!put_succeeded || unregister_failed) {
        const bool put_results_complete =
            put_results.size() == embed_keys.size();
        for (size_t i = 0; i < embed_keys.size(); ++i) {
            if (put_results_complete && put_results[i] != 0) {
                continue;
            }
            int rc = store_->remove(embed_keys[i], true);
            if (rc != 0 &&
                rc != static_cast<int>(ErrorCode::OBJECT_NOT_FOUND)) {
                LOG(ERROR)
                    << "Failed to roll back partially populated EngramStore "
                    << "key '" << embed_keys[i] << "', rc=" << rc;
            }
        }
        if (unregister_failed) {
            LOG(ERROR)
                << "Rolling back EngramStore populate because buffer cleanup "
                   "failed after publish";
        }
        return -1;
    }

    return 0;
}

}  // namespace engram
}  // namespace mooncake
