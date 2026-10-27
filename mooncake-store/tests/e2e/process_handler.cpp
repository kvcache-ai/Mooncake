#include "process_handler.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

namespace mooncake {
namespace testing {

namespace {

std::string ResolveMasterBackendConnstring(const MasterRunnerConfig& config) {
    if (!config.ha_backend_connstring.empty()) {
        return config.ha_backend_connstring;
    }
    return config.etcd_endpoints;
}

bool SignalProcess(pid_t pid, int signo) {
    if (pid == 0) {
        return false;
    }
    if (::kill(-pid, signo) == 0) {
        return true;
    }
    return errno == ESRCH && ::kill(pid, signo) == 0;
}

bool WaitForProcess(pid_t& pid, std::chrono::milliseconds timeout,
                    int* status) {
    if (pid == 0) {
        return true;
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int child_status = 0;
    do {
        const pid_t result = waitpid(pid, &child_status, WNOHANG);
        if (result == pid) {
            pid = 0;
            if (status != nullptr) {
                *status = child_status;
            }
            return true;
        }
        if (result == -1 && errno != EINTR) {
            if (errno == ECHILD) {
                pid = 0;
                return true;
            }
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

bool StopProcess(pid_t& pid, std::chrono::milliseconds timeout) {
    if (pid == 0) {
        return true;
    }
    SignalProcess(pid, SIGTERM);
    if (WaitForProcess(pid, timeout, nullptr)) {
        return true;
    }
    SignalProcess(pid, SIGKILL);
    return WaitForProcess(pid, std::chrono::seconds(2), nullptr);
}

}  // namespace

MasterProcessHandler::MasterProcessHandler(const std::string& path,
                                           const std::string& etcd_endpoints,
                                           const int port, const int index,
                                           const std::string& out_dir)
    : master_path_(path),
      config_(MasterRunnerConfig{
          .enable_ha = true,
          .ha_backend_type = "etcd",
          .ha_backend_connstring = "",
          .etcd_endpoints = etcd_endpoints,
          .cluster_id = "",
          .oplog_store_type = "",
          .metrics_port = 0,
          .oplog_batch_max_entries = 1024,
          .batch_oplog_retry_timeout_sec = 180,
          .extra_args = {},
      }),
      port_(port),
      index_(index),
      out_dir_(out_dir) {}

MasterProcessHandler::MasterProcessHandler(const std::string& path,
                                           const MasterRunnerConfig& config,
                                           const int port, const int index,
                                           const std::string& out_dir)
    : master_path_(path),
      config_(config),
      port_(port),
      index_(index),
      out_dir_(out_dir) {}

MasterProcessHandler::~MasterProcessHandler() {
    if (master_pid_ != 0) {
        stop(std::chrono::seconds(2));
    }
}

bool MasterProcessHandler::start() {
    if (master_pid_ != 0) {
        LOG(ERROR) << "[m" << index_
                   << "] Master process already running with PID: "
                   << master_pid_;
        return false;
    }

    // Create output directory if it doesn't exist
    if (mkdir(out_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG(ERROR) << "[m" << index_
                   << "] Failed to create output directory: " << out_dir_
                   << ", error: " << strerror(errno);
        return false;
    }

    stdout_path_ = out_dir_ + "/master_" + std::to_string(index_) + ".out";
    stderr_path_ = out_dir_ + "/master_" + std::to_string(index_) + ".err";

    pid_t pid = fork();

    if (pid == -1) {
        LOG(ERROR) << "[m" << index_ << "] Failed to fork process for master: "
                   << strerror(errno);
        return false;
    }

    if (pid == 0) {
        // Child process
        setpgid(0, 0);

        // Determine file opening mode based on whether it's the first start
        int open_flags = O_WRONLY | O_CREAT;
        if (first_start_) {
            open_flags |= O_TRUNC;  // Override file content
        } else {
            open_flags |= O_APPEND;  // Append to existing file
        }

        // Open stdout file
        int stdout_fd = open(stdout_path_.c_str(), open_flags, 0644);
        if (stdout_fd == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to open stdout file: " << stdout_path_
                       << ", error: " << strerror(errno);
            exit(1);
        }

        // Open stderr file
        int stderr_fd = open(stderr_path_.c_str(), open_flags, 0644);
        if (stderr_fd == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to open stderr file: " << stderr_path_
                       << ", error: " << strerror(errno);
            close(stdout_fd);
            exit(1);
        }

        // Redirect stdout and stderr
        if (dup2(stdout_fd, STDOUT_FILENO) == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to redirect stdout: " << strerror(errno);
            close(stdout_fd);
            close(stderr_fd);
            exit(1);
        }

        if (dup2(stderr_fd, STDERR_FILENO) == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to redirect stderr: " << strerror(errno);
            close(stdout_fd);
            close(stderr_fd);
            exit(1);
        }

        // Close the original file descriptors
        close(stdout_fd);
        close(stderr_fd);

        // Build command line arguments.
        std::string rpc_address_arg = "--rpc-address=0.0.0.0";
        std::string rpc_port_arg = "--rpc-port=" + std::to_string(port_);
        std::vector<std::string> args;
        args.push_back(master_path_);
        args.push_back(std::string("--enable-ha=") +
                       (config_.enable_ha ? "true" : "false"));
        args.push_back("--ha-backend-type=" + config_.ha_backend_type);

        const auto backend_connstring = ResolveMasterBackendConnstring(config_);
        if (!backend_connstring.empty()) {
            args.push_back("--ha-backend-connstring=" + backend_connstring);
        }
        if (!config_.etcd_endpoints.empty()) {
            args.push_back("--etcd-endpoints=" + config_.etcd_endpoints);
        }
        if (!config_.cluster_id.empty()) {
            args.push_back("--cluster-id=" + config_.cluster_id);
        }
        if (!config_.oplog_store_type.empty()) {
            args.push_back("--oplog-store-type=" + config_.oplog_store_type);
        }
        const uint32_t metrics_port =
            config_.metrics_port == 0 ? 9003 + index_ : config_.metrics_port;
        args.push_back("--metrics-port=" + std::to_string(metrics_port));
        args.push_back("--oplog-batch-max-entries=" +
                       std::to_string(config_.oplog_batch_max_entries));
        args.push_back("--batch-oplog-retry-timeout-sec=" +
                       std::to_string(config_.batch_oplog_retry_timeout_sec));
        args.push_back(rpc_address_arg);
        args.push_back(rpc_port_arg);
        args.insert(args.end(), config_.extra_args.begin(),
                    config_.extra_args.end());

        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        std::string cmdline;
        for (const auto& arg : args) {
            if (!cmdline.empty()) {
                cmdline += ' ';
            }
            cmdline += arg;
        }
        LOG(INFO) << "[m" << index_ << "] Executing: " << cmdline;
        execv(master_path_.c_str(), argv.data());

        // If execv returns, it means there was an error.
        LOG(ERROR) << "[m" << index_
                   << "] Master process terminated with error: "
                   << strerror(errno);
        exit(1);
    } else {
        // Parent process - store the PID
        setpgid(pid, pid);
        master_pid_ = pid;
        LOG(INFO) << "[m" << index_
                  << "] Started master process with PID: " << pid;

        // Mark that this is no longer the first start
        first_start_ = false;

        return true;
    }
}

bool MasterProcessHandler::kill() {
    if (master_pid_ == 0) {
        LOG(ERROR) << "[m" << index_ << "] Master process not running";
        return false;
    }

    bool success = false;
    // Kill the process forcefully to simulate a crash
    if (SignalProcess(master_pid_, SIGKILL)) {
        LOG(INFO) << "[m" << index_
                  << "] Force killed master process with PID: " << master_pid_
                  << " (simulating crash)";

        // Wait for the process to be reaped
        success = WaitForProcess(master_pid_, std::chrono::seconds(2), nullptr);
    } else {
        LOG(ERROR) << TEST_ERROR_STR << " [m" << index_
                   << "] Failed to kill master process with PID: "
                   << master_pid_ << ": " << strerror(errno);
        success = false;
    }

    if (!success) {
        master_pid_ = 0;
    }
    return success;
}

bool MasterProcessHandler::stop(std::chrono::milliseconds timeout) {
    return StopProcess(master_pid_, timeout);
}

bool MasterProcessHandler::signal(int signo) {
    return SignalProcess(master_pid_, signo);
}

bool MasterProcessHandler::wait_for_exit(std::chrono::milliseconds timeout,
                                         int* status) {
    return WaitForProcess(master_pid_, timeout, status);
}

bool MasterProcessHandler::is_running() const {
    if (master_pid_ == 0) {
        return false;
    }
    int status = 0;
    const pid_t result = waitpid(master_pid_, &status, WNOHANG);
    if (result == master_pid_ || (result == -1 && errno == ECHILD)) {
        master_pid_ = 0;
        return false;
    }
    return result == 0 || (result == -1 && errno == EINTR);
}

ClientProcessHandler::ClientProcessHandler(const std::string& path,
                                           const int index,
                                           const std::string& out_dir,
                                           const ClientRunnerConfig& config)
    : client_path_(path), index_(index), out_dir_(out_dir), config_(config) {}

ClientProcessHandler::~ClientProcessHandler() {
    if (client_pid_ != 0) {
        stop(std::chrono::seconds(2));
    }
}

bool ClientProcessHandler::start() {
    if (client_pid_ != 0) {
        LOG(ERROR) << "[c" << index_
                   << "] Client process already running with PID: "
                   << client_pid_;
        return false;
    }

    // Create output directory if it doesn't exist
    if (mkdir(out_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG(ERROR) << "[c" << index_
                   << "] Failed to create output directory: " << out_dir_
                   << ", error: " << strerror(errno);
        return false;
    }

    stdout_path_ = out_dir_ + "/client_" + std::to_string(index_) + ".out";
    stderr_path_ = out_dir_ + "/client_" + std::to_string(index_) + ".err";

    pid_t pid = fork();

    if (pid == -1) {
        LOG(ERROR) << "[c" << index_ << "] Failed to fork process for client: "
                   << strerror(errno);
        return false;
    }

    if (pid == 0) {
        // Child process
        setpgid(0, 0);

        // Determine file opening mode based on whether it's the first start
        int open_flags = O_WRONLY | O_CREAT;
        if (first_start_) {
            open_flags |= O_TRUNC;  // Override file content
        } else {
            open_flags |= O_APPEND;  // Append to existing file
        }

        // Open stdout file
        int stdout_fd = open(stdout_path_.c_str(), open_flags, 0644);
        if (stdout_fd == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to open stdout file: " << stdout_path_
                       << ", error: " << strerror(errno);
            exit(1);
        }

        // Open stderr file
        int stderr_fd = open(stderr_path_.c_str(), open_flags, 0644);
        if (stderr_fd == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to open stderr file: " << stderr_path_
                       << ", error: " << strerror(errno);
            close(stdout_fd);
            exit(1);
        }

        // Redirect stdout and stderr
        if (dup2(stdout_fd, STDOUT_FILENO) == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to redirect stdout: " << strerror(errno);
            close(stdout_fd);
            close(stderr_fd);
            exit(1);
        }

        if (dup2(stderr_fd, STDERR_FILENO) == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to redirect stderr: " << strerror(errno);
            close(stdout_fd);
            close(stderr_fd);
            exit(1);
        }

        // Close the original file descriptors
        close(stdout_fd);
        close(stderr_fd);

        // Build command line arguments
        std::vector<std::string> args;
        args.push_back(client_path_);  // argv[0] - program name

        // Add configurable probability parameters if they have values
        if (config_.put_prob.has_value()) {
            args.push_back("--put-prob=" +
                           std::to_string(config_.put_prob.value()));
        }
        if (config_.get_prob.has_value()) {
            args.push_back("--get-prob=" +
                           std::to_string(config_.get_prob.value()));
        }
        if (config_.mount_prob.has_value()) {
            args.push_back("--mount-prob=" +
                           std::to_string(config_.mount_prob.value()));
        }
        if (config_.unmount_prob.has_value()) {
            args.push_back("--unmount-prob=" +
                           std::to_string(config_.unmount_prob.value()));
        }
        if (config_.port.has_value()) {
            args.push_back("--port=" + std::to_string(config_.port.value()));
        }
        if (config_.master_server_entry.has_value()) {
            args.push_back("--master-server-entry=" +
                           config_.master_server_entry.value());
        }
        if (config_.engine_meta_url.has_value()) {
            args.push_back("--engine-meta-url=" +
                           config_.engine_meta_url.value());
        }
        if (config_.protocol.has_value()) {
            args.push_back("--protocol=" + config_.protocol.value());
        }
        if (config_.device_name.has_value()) {
            args.push_back("--device-name=" + config_.device_name.value());
        }

        // Convert vector of strings to char* array for execv
        std::vector<char*> argv;
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);  // null-terminate the array

        // Log the command being executed
        std::string cmd_str;
        for (const auto& arg : args) {
            cmd_str += arg + " ";
        }
        LOG(INFO) << "[c" << index_ << "] Executing: " << cmd_str;

        // Execute the client using execv
        execv(client_path_.c_str(), argv.data());

        // If execv returns, it means there was an error
        LOG(ERROR) << "[c" << index_
                   << "] Client process terminated with error: "
                   << strerror(errno);
        exit(1);
    } else {
        // Parent process - store the PID
        setpgid(pid, pid);
        client_pid_ = pid;
        LOG(INFO) << "[c" << index_
                  << "] Started client process with PID: " << pid;
        // Mark that this is no longer the first start
        first_start_ = false;

        return true;
    }
}

bool ClientProcessHandler::kill() {
    if (client_pid_ == 0) {
        LOG(ERROR) << "[c" << index_ << "] Client process not running";
        return false;
    }

    bool success = false;
    // Kill the process forcefully to simulate a crash
    if (SignalProcess(client_pid_, SIGKILL)) {
        LOG(INFO) << "[c" << index_
                  << "] Force killed client process with PID: " << client_pid_
                  << " (simulating crash)";

        // Wait for the process to be reaped
        success = WaitForProcess(client_pid_, std::chrono::seconds(2), nullptr);
    } else {
        LOG(ERROR) << TEST_ERROR_STR << " [c" << index_
                   << "] Failed to kill client process with PID: "
                   << client_pid_ << ": " << strerror(errno);
        success = false;
    }

    if (!success) {
        client_pid_ = 0;
    }
    return success;
}

bool ClientProcessHandler::stop(std::chrono::milliseconds timeout) {
    return StopProcess(client_pid_, timeout);
}

bool ClientProcessHandler::signal(int signo) {
    return SignalProcess(client_pid_, signo);
}

bool ClientProcessHandler::wait_for_exit(std::chrono::milliseconds timeout,
                                         int* status) {
    return WaitForProcess(client_pid_, timeout, status);
}

bool ClientProcessHandler::is_running() const {
    if (client_pid_ == 0) {
        return false;
    }
    int status = 0;
    const pid_t result = waitpid(client_pid_, &status, WNOHANG);
    if (result == client_pid_ || (result == -1 && errno == ECHILD)) {
        client_pid_ = 0;
        return false;
    }
    return result == 0 || (result == -1 && errno == EINTR);
}

}  // namespace testing
}  // namespace mooncake
