#pragma once

#include <pybind11/pybind11.h>

#include <string>

#include "allocator.h"
#include "client.h"
#include "utils.h"

class DistributedObjectStore {
   public:
    DistributedObjectStore();
    ~DistributedObjectStore();

    int setup(const std::string &local_hostname,
              const std::string &metadata_server,
              size_t global_segment_size = 1024 * 1024 * 16,
              size_t local_buffer_size = 1024 * 1024 * 16,
              const std::string &protocol = "tcp",
              const std::string &rdma_devices = "",
              const std::string &master_server_addr = "127.0.0.1:50051");

    int initAll(const std::string &protocol, const std::string &device_name,
                size_t mount_segment_size = 1024 * 1024 * 16);  // Default 16MB

    int put(const std::string &key, const std::string &value);

    pybind11::bytes get(const std::string &key);

    int remove(const std::string &key);

   private:
    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const std::string &value);

    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const mooncake::Client::ObjectInfo &object_info,
                       uint64_t &length);

    char *exportSlices(const std::vector<mooncake::Slice> &slices,
                       uint64_t length);

    int freeSlices(const std::vector<mooncake::Slice> &slices);

   public:
    std::unique_ptr<mooncake::Client> client_ = nullptr;
    std::unique_ptr<mooncake::SimpleAllocator> client_buffer_allocator_ =
        nullptr;
    uint64_t segment_ptr_;
    std::string protocol;
    std::string device_name;
    std::string local_hostname;
};