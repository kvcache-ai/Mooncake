#include "distributed_object_store.h"

using namespace mooncake;

DistributedObjectStore::DistributedObjectStore() {}

DistributedObjectStore::~DistributedObjectStore() {
    if (client_ && segment_ptr_) {
        // Try to unmount the segment using saved local_hostname
        ErrorCode rc =
            client_->UnmountSegment(local_hostname, (void *)segment_ptr_);
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(rc);
        }
    }
}

int DistributedObjectStore::setup(const std::string &local_hostname,
                                  const std::string &metadata_server,
                                  size_t global_segment_size,
                                  size_t local_buffer_size,
                                  const std::string &protocol,
                                  const std::string &rdma_devices,
                                  const std::string &master_server_addr) {
    this->protocol = protocol;
    this->local_hostname = local_hostname;  // Save the local hostname
    client_ = std::make_unique<mooncake::Client>();

    void **args = (protocol == "rdma") ? rdma_args(rdma_devices) : nullptr;
    client_->Init(local_hostname, metadata_server, protocol, args,
                  master_server_addr);

    client_buffer_allocator_ =
        std::make_unique<SimpleAllocator>(local_buffer_size);
    ErrorCode rc = client_->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), local_buffer_size,
        kWildcardLocation, false, false);
    segment_ptr_ =
        (uint64_t)allocate_buffer_allocator_memory(global_segment_size);
    if (segment_ptr_ == 0) {
        LOG(ERROR) << "Failed to allocate segment memory";
        return 1;
    }
    rc = client_->MountSegment(local_hostname, (void *)segment_ptr_,
                               global_segment_size);
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to mount segment: " << toString(rc);
        return 1;
    }
    return 0;
}

int DistributedObjectStore::initAll(const std::string &protocol_,
                                    const std::string &device_name,
                                    size_t mount_segment_size) {
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    return setup("localhost:12345", "127.0.0.1:2379", mount_segment_size,
                 buffer_allocator_size, protocol_, device_name);
}

int DistributedObjectStore::allocateSlices(std::vector<Slice> &slices,
                                           const std::string &value) {
    uint64_t offset = 0;
    while (offset < value.size()) {
        auto chunk_size = std::min(value.size() - offset, kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) {
            // Deallocate any previously allocated slices
            for (auto &slice : slices) {
                client_buffer_allocator_->deallocate(slice.ptr, slice.size);
            }
            slices.clear();
            return 1;
        }
        memcpy(ptr, value.data() + offset, chunk_size);
        slices.emplace_back(Slice{ptr, chunk_size});
        offset += chunk_size;
    }
    return 0;
}

int DistributedObjectStore::allocateSlices(
    std::vector<mooncake::Slice> &slices,
    const mooncake::Client::ObjectInfo &object_info, uint64_t &length) {
    length = 0;
    if (!object_info.replica_list_size()) return -1;
    auto &replica = object_info.replica_list(0);
    for (auto &handle : replica.handles()) {
        auto chunk_size = handle.size();
        assert(chunk_size <= kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) return 1;
        slices.emplace_back(Slice{ptr, chunk_size});
        length += chunk_size;
    }
    return 0;
}

char *DistributedObjectStore::exportSlices(
    const std::vector<mooncake::Slice> &slices, uint64_t length) {
    char *buf = new char[length + 1];
    buf[length] = '\0';
    uint64_t offset = 0;
    for (auto slice : slices) {
        memcpy(buf + offset, slice.ptr, slice.size);
        offset += slice.size;
    }
    return buf;
}

int DistributedObjectStore::freeSlices(
    const std::vector<mooncake::Slice> &slices) {
    for (auto slice : slices) {
        client_buffer_allocator_->deallocate(slice.ptr, slice.size);
    }
    return 0;
}

int DistributedObjectStore::put(const std::string &key,
                                const std::string &value) {
    ReplicateConfig config;
    config.replica_num = 1;  // TODO

    std::vector<Slice> slices;
    int ret = allocateSlices(slices, value);
    if (ret) return ret;
    ErrorCode error_code = client_->Put(std::string(key), slices, config);
    freeSlices(slices);
    if (error_code != ErrorCode::OK) return 1;
    return 0;
}

pybind11::bytes DistributedObjectStore::get(const std::string &key) {
    mooncake::Client::ObjectInfo object_info;
    std::vector<Slice> slices;

    const auto kNullString = pybind11::bytes("\0", 0);
    ErrorCode error_code = client_->Query(key, object_info);
    if (error_code != ErrorCode::OK) return kNullString;

    uint64_t str_length = 0;
    int ret = allocateSlices(slices, object_info, str_length);
    if (ret) return kNullString;

    error_code = client_->Get(key, object_info, slices);
    if (error_code != ErrorCode::OK) {
        freeSlices(slices);
        return kNullString;
    }

    const char *str = exportSlices(slices, str_length);
    freeSlices(slices);
    if (!str) return kNullString;

    pybind11::bytes result(str, str_length);
    delete[] str;
    return result;
}

int DistributedObjectStore::remove(const std::string &key) {
    ErrorCode error_code = client_->Remove(key);
    if (error_code != ErrorCode::OK) return 1;
    return 0;
}

namespace py = pybind11;

PYBIND11_MODULE(distributed_object_store, m) {
    py::class_<DistributedObjectStore>(m, "DistributedObjectStore")
        .def(py::init<>())
        .def("setup", &DistributedObjectStore::setup)
        .def("initAll", &DistributedObjectStore::initAll)
        .def("get", &DistributedObjectStore::get)
        .def("put", &DistributedObjectStore::put)
        .def("remove", &DistributedObjectStore::remove);
}
