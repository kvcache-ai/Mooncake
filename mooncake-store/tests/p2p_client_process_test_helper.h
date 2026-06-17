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

#include "client_config_builder.h"
#include "p2p_client_service.h"
#include "test_p2p_server_helpers.h"
#include "types.h"
#include "utils.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace mooncake::testing {

inline std::string& P2PClientIntegrationTestBinaryPath() {
    static std::string path;
    return path;
}

inline void SetP2PClientIntegrationTestBinaryPath(const char* argv0) {
    if (!P2PClientIntegrationTestBinaryPath().empty()) {
        return;
    }
    std::error_code ec;
    auto absolute = std::filesystem::absolute(argv0, ec);
    P2PClientIntegrationTestBinaryPath() =
        ec ? std::string(argv0) : absolute.string();
}

inline std::optional<std::string> FindP2PArgValue(int argc, char** argv,
                                                  std::string_view prefix) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg.rfind(prefix, 0) == 0) {
            return std::string(arg.substr(prefix.size()));
        }
    }
    return std::nullopt;
}

inline std::string MakeP2PTempStateFilePath(std::string_view prefix) {
#ifdef _WIN32
    const auto pid = static_cast<unsigned long>(GetCurrentProcessId());
#else
    const auto pid = static_cast<unsigned long>(getpid());
#endif
    return (std::filesystem::temp_directory_path() /
            (std::string(prefix) + "_" + std::to_string(pid) + "_" +
             std::to_string(getFreeTcpPort()) + ".txt"))
        .string();
}

inline bool WriteReadyStateFile(const std::string& path) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        return false;
    }
    out << "ready";
    return true;
}

inline bool WaitForReadyStateFile(
    const std::string& path,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream in(path);
        if (in) {
            std::string line;
            std::getline(in, line);
            if (line == "ready") {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

class P2PClientChildProcess {
   public:
    P2PClientChildProcess() = default;
    P2PClientChildProcess(const P2PClientChildProcess&) = delete;
    P2PClientChildProcess& operator=(const P2PClientChildProcess&) = delete;
    ~P2PClientChildProcess() { Stop(); }

    bool Start(const std::vector<std::string>& args) {
        Stop();

        if (P2PClientIntegrationTestBinaryPath().empty()) {
            LOG(ERROR) << "P2P integration test binary path is empty";
            return false;
        }

#ifdef _WIN32
        std::string command_line =
            QuoteArg(P2PClientIntegrationTestBinaryPath());
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
        if (!CreateProcessA(P2PClientIntegrationTestBinaryPath().c_str(),
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
            argv_ptrs.push_back(const_cast<char*>(
                P2PClientIntegrationTestBinaryPath().c_str()));
            for (const auto& arg : args) {
                argv_ptrs.push_back(const_cast<char*>(arg.c_str()));
            }
            argv_ptrs.push_back(nullptr);
            execv(P2PClientIntegrationTestBinaryPath().c_str(),
                  argv_ptrs.data());
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

inline int RunP2PMasterChildProcess(int argc, char** argv) {
    google::InitGoogleLogging("P2PMasterChild");
    FLAGS_logtostderr = 1;

    const int rpc_port =
        std::stoi(FindP2PArgValue(argc, argv, "--mooncake-master-rpc-port=")
                      .value_or("0"));
    if (rpc_port <= 0) {
        google::ShutdownGoogleLogging();
        return 2;
    }

    InProcP2PMaster master;
    InProcMasterConfig config;
    config.rpc_port = rpc_port;
    if (!master.Start(config)) {
        google::ShutdownGoogleLogging();
        return 3;
    }

    if (auto state_file =
            FindP2PArgValue(argc, argv, "--mooncake-state-file=")) {
        if (!WriteReadyStateFile(*state_file)) {
            master.Stop();
            google::ShutdownGoogleLogging();
            return 4;
        }
    }

    for (;;) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

class P2PPeerProcessStack {
   public:
    bool Start(const std::string& master_address, const std::string& host_name,
               uint16_t rpc_port, const std::string& local_transfer_mode,
               TransferDirectionMode transfer_direction_mode) {
        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);

        config.enable_http_server = false;
        config.http_port = 0;
        config.local_transfer_mode = local_transfer_mode == "te"
                                         ? LocalTransferMode::TE
                                         : LocalTransferMode::MEMCPY;
        config.transfer_direction_mode = transfer_direction_mode;

        client_ = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);
        return client_->Init(config) == ErrorCode::OK;
    }

    void Stop() { client_.reset(); }

   private:
    std::shared_ptr<P2PClientService> client_;
};

inline int RunP2PPeerChildProcess(int argc, char** argv) {
    google::InitGoogleLogging("P2PPeerChild");
    FLAGS_logtostderr = 1;

    auto master_address =
        FindP2PArgValue(argc, argv, "--mooncake-master-address=");
    auto host_name = FindP2PArgValue(argc, argv, "--mooncake-host-name=");
    const int rpc_port =
        std::stoi(FindP2PArgValue(argc, argv, "--mooncake-client-rpc-port=")
                      .value_or("0"));
    const std::string local_transfer_mode =
        FindP2PArgValue(argc, argv, "--mooncake-local-transfer-mode=")
            .value_or("te");
    const std::string transfer_direction =
        FindP2PArgValue(argc, argv, "--mooncake-transfer-direction-mode=")
            .value_or("reverse");

    if (!master_address.has_value() || !host_name.has_value() ||
        rpc_port <= 0) {
        google::ShutdownGoogleLogging();
        return 2;
    }

    P2PPeerProcessStack peer;
    const auto direction = transfer_direction == "forward"
                               ? TransferDirectionMode::FORWARD
                               : TransferDirectionMode::REVERSE;
    if (!peer.Start(*master_address, *host_name,
                    static_cast<uint16_t>(rpc_port), local_transfer_mode,
                    direction)) {
        google::ShutdownGoogleLogging();
        return 3;
    }

    if (auto state_file =
            FindP2PArgValue(argc, argv, "--mooncake-state-file=")) {
        if (!WriteReadyStateFile(*state_file)) {
            peer.Stop();
            google::ShutdownGoogleLogging();
            return 4;
        }
    }

    for (;;) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

inline std::optional<int> MaybeRunP2PClientIntegrationTestChildProcess(
    int argc, char** argv) {
    auto child_mode = FindP2PArgValue(argc, argv, "--mooncake-child-mode=");
    if (!child_mode.has_value()) {
        return std::nullopt;
    }
    if (*child_mode == "p2p-master") {
        return RunP2PMasterChildProcess(argc, argv);
    }
    if (*child_mode == "p2p-peer") {
        return RunP2PPeerChildProcess(argc, argv);
    }
    LOG(ERROR) << "Unknown child mode: " << *child_mode;
    return 5;
}

class ScopedP2PMasterProcess {
   public:
    bool Start() {
        Stop();
        CleanupStateFile();

        rpc_port_ = static_cast<uint16_t>(getFreeTcpPort());
        state_file_path_ = MakeP2PTempStateFilePath("p2p_master_ready");
        std::vector<std::string> args = {
            "--mooncake-child-mode=p2p-master",
            "--mooncake-master-rpc-port=" + std::to_string(rpc_port_),
            "--mooncake-state-file=" + *state_file_path_};
        if (!process_.Start(args)) {
            CleanupStateFile();
            return false;
        }
        return WaitForReadyStateFile(*state_file_path_);
    }

    void Stop() {
        process_.Stop();
        CleanupStateFile();
    }

    std::string master_address() const {
        return "127.0.0.1:" + std::to_string(rpc_port_);
    }

   private:
    void CleanupStateFile() {
        if (!state_file_path_.has_value()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove(*state_file_path_, ec);
        state_file_path_.reset();
    }

    P2PClientChildProcess process_;
    uint16_t rpc_port_ = 0;
    std::optional<std::string> state_file_path_;
};

class ScopedP2PRemotePeerProcess {
   public:
    bool Start(const std::string& master_address,
               const std::string& local_transfer_mode = "te",
               TransferDirectionMode transfer_direction_mode =
                   TransferDirectionMode::REVERSE) {
        Stop();
        CleanupStateFile();

        host_name_ = "localhost:" + std::to_string(getFreeTcpPort());
        rpc_port_ = static_cast<uint16_t>(getFreeTcpPort());
        state_file_path_ = MakeP2PTempStateFilePath("p2p_peer_ready");
        std::vector<std::string> args = {
            "--mooncake-child-mode=p2p-peer",
            "--mooncake-master-address=" + master_address,
            "--mooncake-host-name=" + host_name_,
            "--mooncake-client-rpc-port=" + std::to_string(rpc_port_),
            "--mooncake-local-transfer-mode=" + local_transfer_mode,
            "--mooncake-transfer-direction-mode=" +
                std::string(transfer_direction_mode ==
                                    TransferDirectionMode::FORWARD
                                ? "forward"
                                : "reverse"),
            "--mooncake-state-file=" + *state_file_path_};
        if (!process_.Start(args)) {
            CleanupStateFile();
            return false;
        }
        return WaitForReadyStateFile(*state_file_path_);
    }

    void Stop() {
        process_.Stop();
        CleanupStateFile();
    }

   private:
    void CleanupStateFile() {
        if (!state_file_path_.has_value()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove(*state_file_path_, ec);
        state_file_path_.reset();
    }

    P2PClientChildProcess process_;
    std::string host_name_;
    uint16_t rpc_port_ = 0;
    std::optional<std::string> state_file_path_;
};

}  // namespace mooncake::testing
