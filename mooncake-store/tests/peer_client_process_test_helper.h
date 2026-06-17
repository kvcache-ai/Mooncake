#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <json/json.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_rpc_service.h"
#include "data_manager.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"
#include "utils/common.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace mooncake::testing {

inline std::string& PeerClientTestBinaryPath() {
    static std::string path;
    return path;
}

inline void SetPeerClientTestBinaryPath(const char* argv0) {
    if (!PeerClientTestBinaryPath().empty()) {
        return;
    }
    std::error_code ec;
    auto absolute = std::filesystem::absolute(argv0, ec);
    PeerClientTestBinaryPath() = ec ? std::string(argv0) : absolute.string();
}

inline std::optional<std::string> FindArgValue(int argc, char** argv,
                                               std::string_view prefix) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg.rfind(prefix, 0) == 0) {
            return std::string(arg.substr(prefix.size()));
        }
    }
    return std::nullopt;
}

inline bool ParseJsonString(const std::string& json_str, Json::Value& value) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    return reader->parse(json_str.data(), json_str.data() + json_str.size(),
                         &value, &errs);
}

class PeerClientRpcServerStack {
   public:
    bool Start(uint16_t port) {
        port_ = port;
        transfer_engine_ = std::make_shared<TransferEngine>(false);

        const std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        if (!ParseJsonString(json_config_str, config)) {
            return false;
        }

        auto tiered_backend = std::make_unique<TieredBackend>();
        auto init_result = InitTieredBackendForTest(*tiered_backend, config);
        if (!init_result.has_value()) {
            return false;
        }

        LocalTransferConfig local_transfer_config;
        local_transfer_config.mode = LocalTransferMode::MEMCPY;
        local_transfer_config.local_memcpy_async_worker_num = 32;
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend), transfer_engine_,
            /*lock_shard_count=*/1024, local_transfer_config);
        rpc_service_ = std::make_unique<ClientRpcService>(*data_manager_);

        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/1, port_);
        RegisterClientRpcService(*server_, *rpc_service_);

        server_thread_ = std::thread([this]() {
            auto ec = server_->start();
            if (ec) {
                LOG(ERROR) << "RPC server start failed: " << ec.message();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return true;
    }

    bool Put(std::string_view key, std::string_view data) {
        if (!data_manager_) {
            return false;
        }
        std::vector<Slice> slices{
            {const_cast<char*>(data.data()), data.size()}};
        auto put_result = data_manager_->Put(key, slices);
        if (!put_result.has_value()) {
            return false;
        }
        return put_result.value()->Wait().has_value();
    }

    std::optional<UUID> GetFirstTierId() const {
        if (!data_manager_) {
            return std::nullopt;
        }
        auto tier_views = data_manager_->GetTierViews();
        if (tier_views.empty()) {
            return std::nullopt;
        }
        return tier_views[0].id;
    }

    bool WriteStateFile(const std::string& path) const {
        auto tier_id = GetFirstTierId();
        if (!tier_id.has_value()) {
            return false;
        }
        std::ofstream out(path, std::ios::trunc);
        if (!out) {
            return false;
        }
        out << tier_id->first << "," << tier_id->second;
        return true;
    }

    void Stop() {
        if (server_) {
            server_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        server_.reset();
        rpc_service_.reset();
        data_manager_.reset();
        transfer_engine_.reset();
    }

   private:
    uint16_t port_ = 0;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<ClientRpcService> rpc_service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::thread server_thread_;
};

inline std::optional<UUID> ReadTierIdFromStateFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        return std::nullopt;
    }
    std::string line;
    std::getline(in, line);
    const auto comma = line.find(',');
    if (comma == std::string::npos) {
        return std::nullopt;
    }
    UUID tier_id;
    tier_id.first = std::stoull(line.substr(0, comma));
    tier_id.second = std::stoull(line.substr(comma + 1));
    return tier_id;
}

inline std::string MakeTempStateFilePath() {
#ifdef _WIN32
    const auto pid = static_cast<unsigned long>(GetCurrentProcessId());
#else
    const auto pid = static_cast<unsigned long>(getpid());
#endif
    return (std::filesystem::temp_directory_path() /
            ("peer_client_test_state_" + std::to_string(pid) + "_" +
             std::to_string(getFreeTcpPort()) + ".txt"))
        .string();
}

inline int RunRpcServerChildProcess(int argc, char** argv) {
    google::InitGoogleLogging("PeerClientRpcServerChild");
    FLAGS_logtostderr = 1;

    const int rpc_port = std::stoi(
        FindArgValue(argc, argv, "--mooncake-rpc-port=").value_or("0"));
    if (rpc_port <= 0) {
        google::ShutdownGoogleLogging();
        return 2;
    }

    PeerClientRpcServerStack stack;
    if (!stack.Start(static_cast<uint16_t>(rpc_port))) {
        google::ShutdownGoogleLogging();
        return 3;
    }

    if (auto pre_put_key =
            FindArgValue(argc, argv, "--mooncake-pre-put-key=")) {
        const std::string pre_put_data =
            FindArgValue(argc, argv, "--mooncake-pre-put-data=").value_or("");
        if (!stack.Put(*pre_put_key, pre_put_data)) {
            stack.Stop();
            google::ShutdownGoogleLogging();
            return 4;
        }
    }

    if (auto state_file = FindArgValue(argc, argv, "--mooncake-state-file=")) {
        if (!stack.WriteStateFile(*state_file)) {
            stack.Stop();
            google::ShutdownGoogleLogging();
            return 5;
        }
    }

    LOG(INFO) << "PeerClient RPC server child ready on port " << rpc_port;
    for (;;) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

inline std::optional<int> MaybeRunPeerClientTestChildProcess(int argc,
                                                             char** argv) {
    auto child_mode = FindArgValue(argc, argv, "--mooncake-child-mode=");
    if (!child_mode.has_value()) {
        return std::nullopt;
    }
    if (*child_mode == "rpc-server") {
        return RunRpcServerChildProcess(argc, argv);
    }
    LOG(ERROR) << "Unknown child mode: " << *child_mode;
    return 5;
}

class PeerClientTestChildProcess {
   public:
    PeerClientTestChildProcess() = default;
    PeerClientTestChildProcess(const PeerClientTestChildProcess&) = delete;
    PeerClientTestChildProcess& operator=(const PeerClientTestChildProcess&) =
        delete;
    ~PeerClientTestChildProcess() { Stop(); }

    bool Start(const std::vector<std::string>& args) {
        Stop();

        if (PeerClientTestBinaryPath().empty()) {
            LOG(ERROR) << "Peer client test binary path is empty";
            return false;
        }

#ifdef _WIN32
        std::string command_line = QuoteArg(PeerClientTestBinaryPath());
        for (const auto& arg : args) {
            command_line += " ";
            command_line += QuoteArg(arg);
        }

        STARTUPINFOA si;
        ZeroMemory(&si, sizeof(si));
        si.cb = sizeof(si);
        ZeroMemory(&process_info_, sizeof(process_info_));

        std::vector<char> command_buffer(command_line.begin(),
                                         command_line.end());
        command_buffer.push_back('\0');
        if (!CreateProcessA(PeerClientTestBinaryPath().c_str(),
                            command_buffer.data(), nullptr, nullptr, FALSE, 0,
                            nullptr, nullptr, &si, &process_info_)) {
            LOG(ERROR) << "CreateProcess failed, err=" << GetLastError();
            ZeroMemory(&process_info_, sizeof(process_info_));
            return false;
        }
        owns_process_ = true;
        return true;
#else
        pid_ = fork();
        if (pid_ < 0) {
            pid_ = -1;
            return false;
        }
        if (pid_ == 0) {
            std::vector<char*> argv_ptrs;
            argv_ptrs.reserve(args.size() + 2);
            argv_ptrs.push_back(
                const_cast<char*>(PeerClientTestBinaryPath().c_str()));
            for (const auto& arg : args) {
                argv_ptrs.push_back(const_cast<char*>(arg.c_str()));
            }
            argv_ptrs.push_back(nullptr);
            execv(PeerClientTestBinaryPath().c_str(), argv_ptrs.data());
            std::_Exit(127);
        }
        owns_process_ = true;
        return true;
#endif
    }

    void Stop() {
        if (!owns_process_) {
            return;
        }
#ifdef _WIN32
        if (process_info_.hProcess != nullptr) {
            TerminateProcess(process_info_.hProcess, 0);
            WaitForSingleObject(process_info_.hProcess, 5000);
            CloseHandle(process_info_.hThread);
            CloseHandle(process_info_.hProcess);
            ZeroMemory(&process_info_, sizeof(process_info_));
        }
#else
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            int status = 0;
            waitpid(pid_, &status, 0);
            pid_ = -1;
        }
#endif
        owns_process_ = false;
    }

   private:
#ifdef _WIN32
    static std::string QuoteArg(const std::string& arg) {
        if (arg.find(' ') == std::string::npos &&
            arg.find('"') == std::string::npos) {
            return arg;
        }
        std::string out = "\"";
        for (char ch : arg) {
            if (ch == '"') {
                out += '\\';
            }
            out += ch;
        }
        out += "\"";
        return out;
    }

    PROCESS_INFORMATION process_info_{};
#else
    pid_t pid_ = -1;
#endif
    bool owns_process_ = false;
};

class ScopedPeerClientRpcServerProcess {
   public:
    bool Start(const std::optional<std::string>& pre_put_key = std::nullopt,
               const std::optional<std::string>& pre_put_data = std::nullopt,
               const std::optional<std::string>& state_file = std::nullopt) {
        port_ = static_cast<uint16_t>(getFreeTcpPort());
        std::vector<std::string> args = {
            "--mooncake-child-mode=rpc-server",
            "--mooncake-rpc-port=" + std::to_string(port_)};
        if (pre_put_key.has_value()) {
            args.push_back("--mooncake-pre-put-key=" + *pre_put_key);
            args.push_back("--mooncake-pre-put-data=" +
                           pre_put_data.value_or(""));
        }
        if (state_file.has_value()) {
            args.push_back("--mooncake-state-file=" + *state_file);
        }
        if (!process_.Start(args)) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return true;
    }

    void Stop() { process_.Stop(); }

    uint16_t port() const { return port_; }
    std::string endpoint() const {
        return "127.0.0.1:" + std::to_string(port_);
    }

   private:
    PeerClientTestChildProcess process_;
    uint16_t port_ = 0;
};

}  // namespace mooncake::testing
