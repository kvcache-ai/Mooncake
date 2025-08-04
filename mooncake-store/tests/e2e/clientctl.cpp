#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "client_wrapper.h"
#include "types.h"
#include "utils.h"
#include "e2e_utils.h"

// Command line flags
USE_engine_flags DEFINE_string(master_server_entry, "localhost:50051",
                               "Master server address");

namespace mooncake {
namespace testing {

struct ClientInfo {
    std::shared_ptr<ClientTestWrapper> client;
    std::unordered_map<std::string, void*> segments;
    std::string hostname;
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
            } else if (cmd == "remove") {
                HandleRemove(iss);
            } else if (cmd == "sleep") {
                HandleSleep(iss);
            } else if (cmd[0] == '#') {
                // Ignore comment lines
                continue;
            } else if (cmd == "terminate") {
                std::exit(0);
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
            std::cout << "Invalid create command format. Expected: create "
                         "[name] [port]"
                      << std::endl;
            return;
        }

        std::string hostname = "localhost:" + port;

        auto client_opt = ClientTestWrapper::CreateClientWrapper(
            hostname, FLAGS_engine_meta_url, FLAGS_protocol, FLAGS_device_name,
            FLAGS_master_server_entry);

        if (!client_opt.has_value()) {
            std::cout << "Failed to create client: " << name << std::endl;
            return;
        }

        clients_[name] = ClientInfo{client_opt.value(), {}, hostname};
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

        if (key.empty() || value.empty()) {
            std::cout << "Empty key or value" << std::endl;
            return;
        }

        ErrorCode error_code = it->second.client->Put(key, value);

        if (error_code != ErrorCode::OK) {
            std::cout << "Failed to put value: " << toString(error_code)
                      << std::endl;
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

        if (key.empty()) {
            std::cout << "Empty key" << std::endl;
            return;
        }

        std::string value;
        ErrorCode error_code = it->second.client->Get(key, value);
        if (error_code != ErrorCode::OK) {
            std::cout << "Failed to get value: " << toString(error_code)
                      << std::endl;
            return;
        }
        std::cout << "Get value: " << value << std::endl;
    }

    void HandleMount(std::istringstream& iss) {
        std::string client_name;
        std::string segment_name;
        size_t size;
        iss >> client_name >> segment_name >> size;

        if (segment_name.empty() || client_name.empty() || size == 0) {
            std::cout << "Invalid mount command format. Expected: mount "
                         "[client_name] [segment_name] [size]"
                      << std::endl;
            return;
        }

        auto it = clients_.find(client_name);
        if (it == clients_.end()) {
            std::cout << "Client not found: " << client_name << std::endl;
            return;
        }

        if (it->second.segments.find(segment_name) !=
            it->second.segments.end()) {
            std::cout << "Segment " << segment_name << " already mounted"
                      << std::endl;
            return;
        }

        void* base;
        ErrorCode error_code = it->second.client->Mount(size, base);
        if (error_code != ErrorCode::OK) {
            std::cout << "Failed to mount segment: " << toString(error_code)
                      << std::endl;
            return;
        }

        it->second.segments[segment_name] = base;

        std::cout << "Successfully mounted segment on client " << client_name
                  << std::endl;
    }

    void HandleRemove(std::istringstream& iss) {
        std::string name;
        iss >> name;

        auto it = clients_.find(name);
        if (it == clients_.end()) {
            std::cout << "Client not found: " << name << std::endl;
            return;
        }

        clients_.erase(it);
        std::cout << "Successfully removed client: " << name << std::endl;
    }

    void HandleSleep(std::istringstream& iss) {
        int seconds;
        iss >> seconds;

        if (seconds <= 0) {
            std::cout
                << "Invalid sleep command format. Expected: sleep [seconds]"
                << std::endl;
            return;
        }

        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        std::cout << "Slept for " << seconds << " seconds" << std::endl;
    }

    std::unordered_map<std::string, ClientInfo> clients_;
};

}  // namespace testing
}  // namespace mooncake

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
