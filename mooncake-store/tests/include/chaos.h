#pragma once

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
#include <string>

namespace mooncake {
namespace testing {

inline const char* const CHAOS_ERROR_STR = "[CHAOS_ERROR]";
inline const char* const CHAOS_PUT_SUCCESS_STR = "[CHAOS_PUT_SUCCESS]";
inline const char* const CHAOS_PUT_FAILURE_STR = "[CHAOS_PUT_FAILURE]";
inline const char* const CHAOS_GET_SUCCESS_STR = "[CHAOS_GET_SUCCESS]";
inline const char* const CHAOS_GET_FAILURE_STR = "[CHAOS_GET_FAILURE]";
inline const char* const CHAOS_MOUNT_SUCCESS_STR = "[CHAOS_MOUNT_SUCCESS]";
inline const char* const CHAOS_MOUNT_FAILURE_STR = "[CHAOS_MOUNT_FAILURE]";
inline const char* const CHAOS_UNMOUNT_SUCCESS_STR = "[CHAOS_UNMOUNT_SUCCESS]";
inline const char* const CHAOS_UNMOUNT_FAILURE_STR = "[CHAOS_UNMOUNT_FAILURE]";

class MasterHandler {
   private:
    pid_t master_pid_{0};
    std::string master_path_;
    int port_;
    int index_;
    bool first_start_{true};
    std::string out_dir_;

   public:
    MasterHandler(const std::string& path, const int port, const int index,
                  const std::string& out_dir)
        : master_path_(path), port_(port), index_(index), out_dir_(out_dir) {}

    ~MasterHandler() {
        if (master_pid_ != 0) {
            kill();
        }
    }

    // Delete copy constructor and assignment operator
    MasterHandler(const MasterHandler&) = delete;
    MasterHandler& operator=(const MasterHandler&) = delete;

    // Delete move constructor and assignment operator
    MasterHandler(MasterHandler&&) = delete;
    MasterHandler& operator=(MasterHandler&&) = delete;

    bool start() {
        if (master_pid_ != 0) {
            LOG(ERROR) << "[m" << index_
                       << "] Master process already running with PID: "
                       << master_pid_;
            return false;
        }

        // Create output directory if it doesn't exist
        if (mkdir(out_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to create output directory: "
                       << out_dir_ << ", error: " << strerror(errno);
            return false;
        }

        std::stringstream stdout_file, stderr_file;
        stdout_file << out_dir_
                    << "/master_" + std::to_string(index_) + ".out";
        stderr_file << out_dir_
                    << "/master_" + std::to_string(index_) + ".err";

        pid_t pid = fork();

        if (pid == -1) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to fork process for master: "
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
                LOG(ERROR) << "[m" << index_ << "] Failed to open stdout file: "
                           << stdout_file.str()
                           << ", error: " << strerror(errno);
                exit(1);
            }

            // Open stderr file
            int stderr_fd = open(stderr_file.str().c_str(), open_flags, 0644);
            if (stderr_fd == -1) {
                LOG(ERROR) << "[m" << index_ << "] Failed to open stderr file: "
                           << stderr_file.str()
                           << ", error: " << strerror(errno);
                close(stdout_fd);
                exit(1);
            }

            // Redirect stdout and stderr
            if (dup2(stdout_fd, STDOUT_FILENO) == -1) {
                LOG(ERROR) << "[m" << index_ << "] Failed to redirect stdout: "
                           << strerror(errno);
                close(stdout_fd);
                close(stderr_fd);
                exit(1);
            }

            if (dup2(stderr_fd, STDERR_FILENO) == -1) {
                LOG(ERROR) << "[m" << index_ << "] Failed to redirect stderr: "
                           << strerror(errno);
                close(stdout_fd);
                close(stderr_fd);
                exit(1);
            }

            // Close the original file descriptors
            close(stdout_fd);
            close(stderr_fd);

            // Execute the master
            std::string host_ip_arg = "--host-ip=0.0.0.0";
            std::string port_arg = "--port=" + std::to_string(port_);
            LOG(INFO) << "[m" << index_ << "] Execl master" << " "
                      << host_ip_arg << " " << port_arg;
            execl(master_path_.c_str(), master_path_.c_str(),
                  "--enable-ha=true", "--etcd-endpoints=0.0.0.0:2379",
                  host_ip_arg.c_str(), port_arg.c_str(), nullptr);

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

    void kill() {
        if (master_pid_ == 0) {
            LOG(ERROR) << "[m" << index_ << "] Master process not running";
            return;
        }

        // For chaos testing, kill the process forcefully to simulate a crash
        if (::kill(master_pid_, SIGKILL) == 0) {
            LOG(INFO) << "[m" << index_
                      << "] Force killed master process with PID: "
                      << master_pid_ << " (simulating crash)";

            // Wait for the process to be reaped
            int status;
            waitpid(master_pid_, &status, 0);
        } else {
            LOG(ERROR) << CHAOS_ERROR_STR << " [m" << index_
                       << "] Failed to kill master process with PID: "
                       << master_pid_ << ": " << strerror(errno);
        }

        master_pid_ = 0;
    }

    bool is_running() const { return master_pid_ != 0; }
};

class ClientHandler {
   private:
    pid_t client_pid_{0};
    std::string client_path_;
    std::string out_dir_;
    int index_;
    bool first_start_{true};

   public:
    ClientHandler(const std::string& path, const std::string& out_dir,
                  const int index)
        : client_path_(path), out_dir_(out_dir), index_(index) {}

    ~ClientHandler() {
        if (client_pid_ != 0) {
            kill();
        }
    }

    // Delete copy constructor and assignment operator
    ClientHandler(const ClientHandler&) = delete;
    ClientHandler& operator=(const ClientHandler&) = delete;

    // Delete move constructor and assignment operator
    ClientHandler(ClientHandler&&) = delete;
    ClientHandler& operator=(ClientHandler&&) = delete;

    bool start() {
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
            LOG(ERROR) << "[c" << index_
                       << "] Failed to fork process for client: "
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
                LOG(ERROR) << "[c" << index_ << "] Failed to open stdout file: "
                           << stdout_file.str()
                           << ", error: " << strerror(errno);
                exit(1);
            }

            // Open stderr file
            int stderr_fd = open(stderr_file.str().c_str(), open_flags, 0644);
            if (stderr_fd == -1) {
                LOG(ERROR) << "[c" << index_ << "] Failed to open stderr file: "
                           << stderr_file.str()
                           << ", error: " << strerror(errno);
                close(stdout_fd);
                exit(1);
            }

            // Redirect stdout and stderr
            if (dup2(stdout_fd, STDOUT_FILENO) == -1) {
                LOG(ERROR) << "[c" << index_ << "] Failed to redirect stdout: "
                           << strerror(errno);
                close(stdout_fd);
                close(stderr_fd);
                exit(1);
            }

            if (dup2(stderr_fd, STDERR_FILENO) == -1) {
                LOG(ERROR) << "[c" << index_ << "] Failed to redirect stderr: "
                           << strerror(errno);
                close(stdout_fd);
                close(stderr_fd);
                exit(1);
            }

            // Close the original file descriptors
            close(stdout_fd);
            close(stderr_fd);

            // Execute the client
            std::string port_arg = "--port=" + std::to_string(14888 + index_);
            LOG(INFO) << "[c" << index_ << "] Execl client" << " " << port_arg;
            execl(client_path_.c_str(), client_path_.c_str(), port_arg.c_str(),
                  nullptr);

            // If execl returns, it means there was an error
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

    void kill() {
        if (client_pid_ == 0) {
            LOG(ERROR) << "[c" << index_ << "] Client process not running";
            return;
        }

        // For chaos testing, kill the process forcefully to simulate a crash
        if (::kill(client_pid_, SIGKILL) == 0) {
            LOG(INFO) << "[c" << index_
                      << "] Force killed client process with PID: "
                      << client_pid_ << " (simulating crash)";

            // Wait for the process to be reaped
            int status;
            waitpid(client_pid_, &status, 0);
        } else {
            LOG(ERROR) << CHAOS_ERROR_STR << " [c" << index_
                       << "] Failed to kill client process with PID: "
                       << client_pid_ << ": " << strerror(errno);
        }

        client_pid_ = 0;
    }

    bool is_running() const { return client_pid_ != 0; }
};

}  // namespace testing
}  // namespace mooncake