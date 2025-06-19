#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "types.h"
#include "utils.h"

// Command line flags
DEFINE_string(metadata_connstring, "http://127.0.0.1:8080/metadata",
              "Metadata connection string for transfer engine");
DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(master_server_entry, "localhost:50051",
              "Master server address");

namespace mooncake {
namespace testing {

struct ClientInfo {
    std::shared_ptr<Client> client;
    void* segment_buffers{nullptr};
    std::string hostname;
    ~ClientInfo() {
        if (segment_buffers) {
            free(segment_buffers);
        }
    }
};

class ClientCtl {
public:
    void Run() {
        std::string line;
        while (std::getline(std::cin, line)) {
            std::istringstream iss(line);
            std::string cmd;
            iss >> cmd;

            if (cmd == "create") {
                HandleCreate(iss);
            } else if (cmd == "put") {
                HandlePut(iss);
            } else if (cmd == "get") {
                HandleGet(iss);
            } else if (cmd == "mount") {
                HandleMount(iss);
            } else {
                std::cout << "Unknown command: " << cmd << std::endl;
            }
        }
    }

private:
    void HandleCreate(std::istringstream& iss) {
        std::string name;
        std::string port;
        iss >> name >> port;

        if (name.empty() || port.empty()) {
            std::cout << "Invalid create command format. Expected: create [name] [port]" << std::endl;
            return;
        }

        void** args = (FLAGS_protocol == "rdma") ? rdma_args(FLAGS_device_name) : nullptr;

        std::string hostname = "localhost:" + port;
        
        auto client_opt = Client::Create(
            hostname,  // Local hostname
            FLAGS_metadata_connstring,
            FLAGS_protocol,
            args,
            FLAGS_master_server_entry
        );

        if (!client_opt.has_value()) {
            std::cout << "Failed to create client: " << name << std::endl;
            return;
        }

        clients_[name] = ClientInfo{client_opt.value(), nullptr, hostname};
        std::cout << "Successfully created client: " << name << std::endl;
    }

    void HandlePut(std::istringstream& iss) {
        std::string name, key, value;
        iss >> name >> key >> value;

        auto it = clients_.find(name);
        if (it == clients_.end()) {
            std::cout << "Client not found: " << name << std::endl;
            return;
        }

        // Allocate buffer for the value
        void* buffer = malloc(value.size());
        if (!buffer) {
            std::cout << "Failed to allocate memory for value" << std::endl;
            return;
        }

        // Copy value to buffer
        memcpy(buffer, value.data(), value.size());

        // Create slices
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buffer, value.size()});

        // Configure replication
        ReplicateConfig config;
        config.replica_num = 1;

        // Perform put operation
        ErrorCode error_code = it->second.client->Put(key, slices, config);
        
        // Free the buffer
        free(buffer);

        if (error_code != ErrorCode::OK) {
            std::cout << "Failed to put value: " << toString(error_code) << std::endl;
            return;
        }

        std::cout << "Successfully put value for key: " << key << std::endl;
    }

    void HandleGet(std::istringstream& iss) {
        std::string name, key;
        iss >> name >> key;

        auto it = clients_.find(name);
        if (it == clients_.end()) {
            std::cout << "Client not found: " << name << std::endl;
            return;
        }

        Client::ObjectInfo object_info;
        if (it->second.client->Query(key, object_info) != ErrorCode::OK) {
            std::cout << "Key not found: " << key << std::endl;
            return;
        }

        // Create slices
        std::vector<AllocatedBuffer::Descriptor>& descriptors = object_info.replica_list[0].buffer_descriptors;
        std::vector<Slice> slices(descriptors.size());
        for (size_t i = 0; i < descriptors.size(); i++) {
            void *buffer = malloc(descriptors[i].size_);
            slices[i] = Slice{buffer, descriptors[i].size_};
        }
        auto free_slices = [&]() {
            for (auto& slice : slices) {
                free(slice.ptr);
            }
        };

        // Perform get operation
        ErrorCode error_code = it->second.client->Get(key, object_info, slices);

        if (error_code != ErrorCode::OK) {
            free_slices();
            std::cout << "Failed to get value: " << toString(error_code) << std::endl;
            return;
        }

        // Print the value
        std::string value;
        for (const auto& slice : slices) {
            value.append(static_cast<const char*>(slice.ptr), slice.size);
        }
        std::cout << "Value: " << value << std::endl;

        // Free the buffer
        free_slices();
    }

    void HandleMount(std::istringstream& iss) {
        std::string client_name;
        size_t size;
        iss >> client_name >> size;

        auto it = clients_.find(client_name);
        if (it == clients_.end()) {
            std::cout << "Client not found: " << client_name << std::endl;
            return;
        }

        if (it->second.segment_buffers) {
            std::cout << "Client " << client_name << " has mounted segments, cannot mount" << std::endl;
            return;
        }

        void* buffer;
        buffer = allocate_buffer_allocator_memory(size);
        if (!buffer) {
            std::cout << "Failed to allocate memory for segment" << std::endl;
            return;
        }

        ErrorCode error_code = it->second.client->MountSegment(buffer, size);
        if (error_code != ErrorCode::OK) {
            std::cout << "Failed to mount segment: " << toString(error_code) << std::endl;
            free(buffer);
            return;
        }
        it->second.segment_buffers = buffer;

        std::cout << "Successfully mounted segment on client " << client_name << std::endl;
    }

    std::unordered_map<std::string, ClientInfo> clients_;
};

} // namespace testing
} // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    mooncake::testing::ClientCtl ctl;
    ctl.Run();

    return 0;
}
