#include "process_handler.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

namespace mooncake {
namespace testing {

MasterProcessHandler::MasterProcessHandler(const std::string& path,
                                           const std::string& etcd_endpoints,
                                           const int port, const int index,
                                           const std::string& out_dir)
    : master_path_(path),
      etcd_endpoints_(etcd_endpoints),
      port_(port),
      index_(index),
      out_dir_(out_dir) {}

MasterProcessHandler::~MasterProcessHandler() {
    if (master_pid_ != 0) {
        kill();
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

    std::stringstream stdout_file, stderr_file;
    stdout_file << out_dir_ << "/master_" + std::to_string(index_) + ".out";
    stderr_file << out_dir_ << "/master_" + std::to_string(index_) + ".err";

    pid_t pid = fork();

    if (pid == -1) {
        LOG(ERROR) << "[m" << index_ << "] Failed to fork process for master: "
                   << strerror(errno);
        return false;
    }

    if (pid == 0) {
        // Child process

        // Determine file opening mode based on whether it's the first start
        int open_flags = O_WRONLY | O_CREAT;
        if (first_start_) {
            open_flags |= O_TRUNC;  // Override file content
        } else {
            open_flags |= O_APPEND;  // Append to existing file
        }

        // Open stdout file
        int stdout_fd = open(stdout_file.str().c_str(), open_flags, 0644);
        if (stdout_fd == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to open stdout file: " << stdout_file.str()
                       << ", error: " << strerror(errno);
            exit(1);
        }

        // Open stderr file
        int stderr_fd = open(stderr_file.str().c_str(), open_flags, 0644);
        if (stderr_fd == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to open stderr file: " << stderr_file.str()
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

        // Execute the master
        std::string rpc_address_arg = "--rpc-address=0.0.0.0";
        std::string rpc_port_arg = "--rpc-port=" + std::to_string(port_);
        LOG(INFO) << "[m" << index_ << "] Execl master" << " "
                  << rpc_address_arg << " " << rpc_port_arg;
        execl(master_path_.c_str(), master_path_.c_str(), "--enable-ha=true",
              ("--etcd-endpoints=" + etcd_endpoints_).c_str(),
              rpc_address_arg.c_str(), rpc_port_arg.c_str(), nullptr);

        // If execl returns, it means there was an error
        LOG(ERROR) << "[m" << index_
                   << "] Master process terminated with error: "
                   << strerror(errno);
        exit(1);
    } else {
        // Parent process - store the PID
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
    if (::kill(master_pid_, SIGKILL) == 0) {
        LOG(INFO) << "[m" << index_
                  << "] Force killed master process with PID: " << master_pid_
                  << " (simulating crash)";

        // Wait for the process to be reaped
        int status;
        waitpid(master_pid_, &status, 0);
        success = true;
    } else {
        LOG(ERROR) << TEST_ERROR_STR << " [m" << index_
                   << "] Failed to kill master process with PID: "
                   << master_pid_ << ": " << strerror(errno);
        success = false;
    }

    master_pid_ = 0;
    return success;
}

bool MasterProcessHandler::is_running() const { return master_pid_ != 0; }

ClientProcessHandler::ClientProcessHandler(const std::string& path,
                                           const int index,
                                           const std::string& out_dir,
                                           const ClientRunnerConfig& config)
    : client_path_(path), index_(index), out_dir_(out_dir), config_(config) {}

ClientProcessHandler::~ClientProcessHandler() {
    if (client_pid_ != 0) {
        kill();
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

    std::stringstream stdout_file, stderr_file;
    stdout_file << out_dir_ << "/client_" + std::to_string(index_) + ".out";
    stderr_file << out_dir_ << "/client_" + std::to_string(index_) + ".err";

    pid_t pid = fork();

    if (pid == -1) {
        LOG(ERROR) << "[c" << index_ << "] Failed to fork process for client: "
                   << strerror(errno);
        return false;
    }

    if (pid == 0) {
        // Child process

        // Determine file opening mode based on whether it's the first start
        int open_flags = O_WRONLY | O_CREAT;
        if (first_start_) {
            open_flags |= O_TRUNC;  // Override file content
        } else {
            open_flags |= O_APPEND;  // Append to existing file
        }

        // Open stdout file
        int stdout_fd = open(stdout_file.str().c_str(), open_flags, 0644);
        if (stdout_fd == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to open stdout file: " << stdout_file.str()
                       << ", error: " << strerror(errno);
            exit(1);
        }

        // Open stderr file
        int stderr_fd = open(stderr_file.str().c_str(), open_flags, 0644);
        if (stderr_fd == -1) {
            LOG(ERROR) << "[c" << index_
                       << "] Failed to open stderr file: " << stderr_file.str()
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
    if (::kill(client_pid_, SIGKILL) == 0) {
        LOG(INFO) << "[c" << index_
                  << "] Force killed client process with PID: " << client_pid_
                  << " (simulating crash)";

        // Wait for the process to be reaped
        int status;
        waitpid(client_pid_, &status, 0);
        success = true;
    } else {
        LOG(ERROR) << TEST_ERROR_STR << " [c" << index_
                   << "] Failed to kill client process with PID: "
                   << client_pid_ << ": " << strerror(errno);
        success = false;
    }

    client_pid_ = 0;
    return success;
}

bool ClientProcessHandler::is_running() const { return client_pid_ != 0; }

}  // namespace testing
}  // namespace mooncake