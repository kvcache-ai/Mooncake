#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <future>
#include <iterator>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "engram/engram_store.h"
#include "real_client.h"

namespace py = pybind11;

namespace mooncake::engram {
void bind_engram_store(py::module&);
}

namespace {

class FaultClient : public mooncake::RealClient,
                    public mooncake::StrictCreateClient {
   public:
    explicit FaultClient(std::string mode) : mode_(std::move(mode)) {
        if (mode_ == "remove_fail" || mode_ == "remove_throw" ||
            mode_ == "blocked_read") {
            published.insert("engram:l1:h0");
            published.insert("engram:l1:h1");
        }
    }

    void set_mode(std::string mode) {
        mode_ = std::move(mode);
        register_calls = 0;
    }

    std::vector<int> batchIsExist(
        const std::vector<std::string>& keys) override {
        return std::vector<int>(keys.size(), mode_ == "preflight" ? 1 : 0);
    }

    int register_buffer(void* buffer, size_t) override {
        ++register_calls;
        if (mode_ == "config_mutation" && register_calls == 1) {
            operation_started.set_value();
            resume_operation.get_future().wait();
        }
        if (register_calls == 2) {
            if (mode_ == "register_throw") throw std::runtime_error("register");
            if (mode_ == "register_fail") return -1;
            if (mode_ == "register_partial") {
                registered.insert(buffer);
                return -1;
            }
        }
        registered.insert(buffer);
        return 0;
    }

    int unregister_buffer(void* buffer) override {
        ++unregister_calls;
        if (mode_ == "unregister_throw") throw std::runtime_error("unregister");
        if (mode_ == "cleanup_fail" || mode_ == "register_fail" ||
            mode_ == "upload_cleanup_fail" || mode_ == "conflict_cleanup_fail")
            return -1;
        registered.erase(buffer);
        return 0;
    }

    std::vector<int> batch_put_from(const std::vector<std::string>& keys,
                                    const std::vector<void*>&,
                                    const std::vector<size_t>&,
                                    const mooncake::ReplicateConfig&) override {
        return write(keys, false);
    }

    std::vector<int> batch_put_from_create_only(
        const std::vector<std::string>& keys, const std::vector<void*>&,
        const std::vector<size_t>&,
        const mooncake::ReplicateConfig& config) override {
        last_nof_replica_num = config.nof_replica_num;
        return write(keys, true);
    }

    std::vector<tl::expected<mooncake::QueryResult, mooncake::ErrorCode>>
    batch_query(const std::vector<std::string>& keys) override {
        ++query_calls;
        std::vector<tl::expected<mooncake::QueryResult, mooncake::ErrorCode>>
            out;
        for (const auto& key : keys) {
            if (published.count(key))
                out.emplace_back(
                    mooncake::QueryResult({}, std::chrono::steady_clock::now() +
                                                  std::chrono::minutes(10)));
            else
                out.emplace_back(
                    tl::unexpected(mooncake::ErrorCode::OBJECT_NOT_FOUND));
        }
        return out;
    }

    std::vector<std::vector<std::vector<int64_t>>>
    get_into_ranges_from_snapshot(
        const std::vector<void*>& buffers,
        const std::vector<std::vector<std::string>>&,
        const std::vector<std::vector<std::vector<size_t>>>& offsets,
        const std::vector<std::vector<std::vector<size_t>>>&,
        const std::vector<std::vector<std::vector<size_t>>>& sizes,
        const QueryResultCache&) override {
        ++read_calls;
        if (mode_ == "blocked_read") {
            operation_started.set_value();
            resume_operation.get_future().wait();
        }
        std::vector<std::vector<std::vector<int64_t>>> result(sizes.size());
        for (size_t b = 0; b < sizes.size(); ++b) {
            result[b].resize(sizes[b].size());
            for (size_t h = 0; h < sizes[b].size(); ++h) {
                for (size_t i = 0; i < sizes[b][h].size(); ++i) {
                    std::memset(
                        static_cast<char*>(buffers[b]) + offsets[b][h][i], 7,
                        sizes[b][h][i]);
                    result[b][h].push_back(sizes[b][h][i]);
                }
            }
        }
        return result;
    }

    int remove(const std::string& key, bool) override {
        ++remove_calls;
        if (mode_ == "blocked_read" && remove_calls == 1)
            remove_started.set_value();
        if (remove_calls == 2 && mode_ == "remove_throw")
            throw std::runtime_error("remove");
        if (remove_calls == 2 && mode_ == "remove_fail") return -1;
        published.erase(key);
        return 0;
    }

    int register_calls = 0;
    int unregister_calls = 0;
    int remove_calls = 0;
    int query_calls = 0;
    int read_calls = 0;
    int last_nof_replica_num = -1;
    std::set<void*> registered;
    std::set<std::string> published;
    std::promise<void> operation_started;
    std::promise<void> resume_operation;
    std::promise<void> remove_started;

   private:
    std::vector<int> write(const std::vector<std::string>& keys,
                           bool create_only) {
        if (mode_ == "competing_throw" || mode_ == "competing_success") {
            published.insert(keys.begin(), keys.end());
            if (mode_ == "competing_throw")
                throw std::runtime_error("before own publication");
            return std::vector<int>(
                keys.size(),
                create_only ? static_cast<int>(
                                  mooncake::ErrorCode::OBJECT_ALREADY_EXISTS)
                            : 0);
        }
        if (mode_ == "upload_throw") throw std::runtime_error("upload");
        std::vector<int> results(keys.size(), 0);
        if (mode_ == "mixed_conflict" || mode_ == "conflict_cleanup_fail") {
            published.insert(keys.back());
            results.back() =
                static_cast<int>(mooncake::ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        if (mode_ == "upload_fail" || mode_ == "upload_cleanup_fail") {
            results.back() = -1;
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            if (results[i] == 0) published.insert(keys[i]);
        }
        if (mode_ == "upload_throw_after_publish")
            throw std::runtime_error("upload after publish");
        if (mode_ == "malformed") results.pop_back();
        return results;
    }

    std::string mode_;
};

bool read_excludes_remove() {
    auto client = std::make_shared<FaultClient>("blocked_read");
    mooncake::engram::EngramStoreConfig config;
    config.table_vocab_sizes = {4, 4};
    config.row_bytes = 8;
    mooncake::engram::EngramStore store({{1, config}}, client);
    int64_t ids[2] = {0, 0};
    uint8_t output[16] = {};
    auto started = client->operation_started.get_future();
    auto removed = client->remove_started.get_future();
    auto read = std::async(std::launch::async, [&] {
        return store.lookup_into(1, ids, 1, 1, output, sizeof(output));
    });
    if (started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        client->resume_operation.set_value();
        throw std::runtime_error("read did not start");
    }
    std::promise<void> remove_requested;
    auto requested = remove_requested.get_future();
    auto remove = std::async(std::launch::async, [&] {
        remove_requested.set_value();
        return store.remove_from_store(1, true);
    });
    requested.wait();
    const bool excluded = removed.wait_for(std::chrono::milliseconds(100)) ==
                          std::future_status::timeout;
    client->resume_operation.set_value();
    const int read_rc = read.get();
    const int remove_rc = remove.get();
    const int after_rc =
        store.lookup_into(1, ids, 1, 1, output, sizeof(output));
    return excluded && read_rc == 0 && remove_rc == 2 && after_rc != 0 &&
           client->query_calls == 2;
}

std::tuple<int, int, bool> unsupported_client_result() {
    // A legacy subclass has no strict-create capability. Neither registration
    // nor its ordinary write path may be used as an implicit fallback.
    struct LegacyClient : mooncake::RealClient {
        int registrations = 0;
        std::vector<int> batchIsExist(
            const std::vector<std::string>& keys) override {
            return std::vector<int>(keys.size(), 0);
        }
        int register_buffer(void*, size_t) override {
            ++registrations;
            return 0;
        }
        int unregister_buffer(void*) override { return 0; }
        std::vector<int> batch_put_from(
            const std::vector<std::string>&, const std::vector<void*>&,
            const std::vector<size_t>&,
            const mooncake::ReplicateConfig&) override {
            throw std::runtime_error("must not fall back to ordinary Put");
        }
    };
    auto client = std::make_shared<LegacyClient>();
    mooncake::engram::EngramStoreConfig config;
    config.table_vocab_sizes = {4};
    config.row_bytes = 8;
    mooncake::engram::EngramStore store({{1, config}}, client);
    auto data = std::make_shared<std::vector<uint8_t>>(32);
    std::weak_ptr<std::vector<uint8_t>> weak = data;
    int rc = store.populate(1, {data->data()}, {data->size()}, {}, data);
    data.reset();
    return {rc, client->registrations, weak.expired()};
}

std::tuple<int, int> populate_with_reentrant_owner(const std::string& mode) {
    auto client = std::make_shared<FaultClient>(mode);
    mooncake::engram::EngramStoreConfig config;
    config.table_vocab_sizes = {4, 4};
    config.row_bytes = 8;
    mooncake::engram::EngramStore store({{1, config}}, client);
    int removed = -1;
    auto owner = std::shared_ptr<std::vector<uint8_t>>(
        new std::vector<uint8_t>(64), [&](std::vector<uint8_t>* data) {
            std::unique_ptr<std::vector<uint8_t>> cleanup(data);
            try {
                removed = store.remove_from_store(1, true);
            } catch (const std::system_error&) {
                // Report recursive locking without throwing from the deleter.
            }
        });
    const std::vector<void*> buffers{owner->data(), owner->data() + 32};
    const std::vector<size_t> sizes{32, 32};
    // Transfer the last reference so populate itself runs the deleter.
    const int rc = store.populate(1, buffers, sizes, {}, std::move(owner));
    return {rc, removed};
}

std::tuple<int, int, int, bool> verify_real_create_only(
    const std::string& master, const std::string& metadata) {
    auto client = mooncake::RealClient::create();
    if (client->setup_real("127.0.0.1", metadata, 128UL << 20, 64UL << 20,
                           "tcp", "", master, nullptr, "") != 0)
        throw std::runtime_error("real client setup failed");
    auto data = std::make_shared<std::vector<uint8_t>>(32, 0x11);
    mooncake::engram::EngramStoreConfig config;
    config.table_vocab_sizes = {4};
    config.row_bytes = 8;
    mooncake::engram::EngramStore store({{1, config}}, client);
    const std::vector<void*> buffers{data->data()};
    const std::vector<size_t> sizes{data->size()};
    const auto keys = store.get_store_keys(1);
    const int initial = store.populate(1, buffers, sizes, {}, data);
    if (initial != 0) throw std::runtime_error("initial strict write failed");
    std::fill(data->begin(), data->end(), 0x22);
    if (client->register_buffer(data->data(), data->size()) != 0)
        throw std::runtime_error("source registration failed");
    const int legacy = client->batch_put_from(keys, buffers, sizes, {}).at(0);
    const int strict =
        client->batch_put_from_create_only(keys, buffers, sizes, {}).at(0);
    const int source_cleanup = client->unregister_buffer(data->data());
    uint8_t output[32] = {};
    if (client->register_buffer(output, sizeof(output)) != 0)
        throw std::runtime_error("output registration failed");
    const auto bytes = client->get_into(keys[0], output, sizeof(output));
    const int output_cleanup = client->unregister_buffer(output);
    const bool unchanged = bytes == static_cast<int64_t>(sizeof(output)) &&
                           std::all_of(std::begin(output), std::end(output),
                                       [](uint8_t x) { return x == 0x11; });
    if (source_cleanup != 0 || output_cleanup != 0 ||
        store.remove_from_store(1, true) != 1)
        throw std::runtime_error("real client cleanup failed");
    return {initial, legacy, strict, unchanged};
}

}  // namespace

PYBIND11_MODULE(engram_store_test_client, m) {
    m.def("populate_with_reentrant_owner", &populate_with_reentrant_owner);
    m.def("unsupported_client_result", &unsupported_client_result);
    m.attr("OBJECT_ALREADY_EXISTS") =
        static_cast<int>(mooncake::ErrorCode::OBJECT_ALREADY_EXISTS);
    m.def("read_excludes_remove", &read_excludes_remove,
          py::call_guard<py::gil_scoped_release>());
    m.def("verify_real_create_only", &verify_real_create_only,
          py::call_guard<py::gil_scoped_release>());
    py::class_<mooncake::ReplicateConfig>(m, "ReplicateConfig")
        .def(py::init<>())
        .def_readwrite("replica_num", &mooncake::ReplicateConfig::replica_num)
        .def_readwrite("nof_replica_num",
                       &mooncake::ReplicateConfig::nof_replica_num);
    py::class_<FaultClient, std::shared_ptr<FaultClient>>(m, "FaultClient")
        .def(py::init<std::string>())
        .def("set_mode", &FaultClient::set_mode)
        .def(
            "wait_for_registration",
            [](FaultClient& self) {
                return self.operation_started.get_future().wait_for(
                           std::chrono::seconds(5)) ==
                       std::future_status::ready;
            },
            py::call_guard<py::gil_scoped_release>())
        .def("resume_registration",
             [](FaultClient& self) { self.resume_operation.set_value(); })
        .def("_get_pyclient_capsule",
             [](const std::shared_ptr<FaultClient>& self) {
                 auto* client = new std::shared_ptr<mooncake::PyClient>(self);
                 return py::capsule(
                     client, "mooncake.PyClient.shared_ptr", [](void* p) {
                         delete static_cast<
                             std::shared_ptr<mooncake::PyClient>*>(p);
                     });
             })
        .def_property_readonly(
            "registered_count",
            [](const FaultClient& self) { return self.registered.size(); })
        .def_property_readonly(
            "published_count",
            [](const FaultClient& self) { return self.published.size(); })
        .def_readonly("register_calls", &FaultClient::register_calls)
        .def_readonly("unregister_calls", &FaultClient::unregister_calls)
        .def_readonly("remove_calls", &FaultClient::remove_calls)
        .def_readonly("query_calls", &FaultClient::query_calls)
        .def_readonly("read_calls", &FaultClient::read_calls)
        .def_readonly("last_nof_replica_num",
                      &FaultClient::last_nof_replica_num);
    mooncake::engram::bind_engram_store(m);
    m.def("populate_raw",
          [](mooncake::engram::EngramStore& store, py::list arrays) {
              std::vector<void*> buffers;
              std::vector<size_t> sizes;
              for (const auto& value : arrays) {
                  auto array = py::cast<py::array>(value);
                  buffers.push_back(array.mutable_data());
                  sizes.push_back(array.nbytes());
              }
              return store.populate(1, buffers, sizes);
          });
}
