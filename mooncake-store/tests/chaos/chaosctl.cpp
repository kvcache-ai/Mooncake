#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "chaos.h"

// Define command line flags
DEFINE_int32(master_num, 1, "Number of master instances (must be > 0)");
DEFINE_int32(client_num, 1, "Number of client instances (must be > 0)");
DEFINE_string(master_path, "./mooncake-store/src/mooncake_master",
              "Path to the master executable");
DEFINE_string(client_path, "./mooncake-store/tests/chaos/chaosclient",
              "Path to the client executable");
DEFINE_string(out_dir, "./output", "Directory for output files");
DEFINE_bool(skip_run, false, "Only generate report, do not run the test");
DEFINE_int32(run_sec, 100, "Test duration in seconds");
DEFINE_int32(master_op_prob, 1000, "Probability weight for master operations");
DEFINE_int32(client_op_prob, 1000, "Probability weight for client operations");
DEFINE_int32(rand_seed, 0, "Seed for random number generator");

// Custom validators for the flags
DEFINE_validator(master_num, [](const char* flagname, int32_t value) {
    if (value <= 0) {
        LOG(FATAL) << "master_num must be greater than 0, got: " << value;
        return false;
    }
    return true;
});

DEFINE_validator(client_num, [](const char* flagname, int32_t value) {
    if (value <= 0) {
        LOG(FATAL) << "client_num must be greater than 0, got: " << value;
        return false;
    }
    return true;
});

namespace mooncake {
namespace testing {

class MasterHandler {
   private:
    pid_t master_pid_{0};
    std::string master_path_;
    int port_;
    int index_;
    bool first_start_{true};

   public:
    MasterHandler(const std::string& path, const int port, const int index)
        : master_path_(path), port_(port), index_(index) {}

    bool start() {
        if (master_pid_ != 0) {
            LOG(ERROR) << "[m" << index_
                       << "] Master process already running with PID: "
                       << master_pid_;
            return false;
        }

        // Create output directory if it doesn't exist
        if (mkdir(FLAGS_out_dir.c_str(), 0755) != 0 && errno != EEXIST) {
            LOG(ERROR) << "[m" << index_
                       << "] Failed to create output directory: "
                       << FLAGS_out_dir << ", error: " << strerror(errno);
            return false;
        }

        std::stringstream stdout_file, stderr_file;
        stdout_file << FLAGS_out_dir
                    << "/master_" + std::to_string(index_) + ".out";
        stderr_file << FLAGS_out_dir
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

// Function to count occurrences of CHAOS_xxx_STR in a file
std::map<std::string, int> count_chaos_occurrences(
    const std::string& filename) {
    std::map<std::string, int> counts;

    // Initialize all CHAOS_xxx_STR to 0
    counts[mooncake::testing::CHAOS_ERROR_STR] = 0;
    counts[mooncake::testing::CHAOS_PUT_SUCCESS_STR] = 0;
    counts[mooncake::testing::CHAOS_PUT_FAILURE_STR] = 0;
    counts[mooncake::testing::CHAOS_GET_SUCCESS_STR] = 0;
    counts[mooncake::testing::CHAOS_GET_FAILURE_STR] = 0;
    counts[mooncake::testing::CHAOS_MOUNT_SUCCESS_STR] = 0;
    counts[mooncake::testing::CHAOS_MOUNT_FAILURE_STR] = 0;
    counts[mooncake::testing::CHAOS_UNMOUNT_SUCCESS_STR] = 0;
    counts[mooncake::testing::CHAOS_UNMOUNT_FAILURE_STR] = 0;

    std::ifstream file(filename);
    if (!file.is_open()) {
        LOG(WARNING) << "Could not open file for analysis: " << filename;
        return counts;
    }

    std::string line;
    while (std::getline(file, line)) {
        for (auto& [str, count] : counts) {
            size_t pos = 0;
            while ((pos = line.find(str, pos)) != std::string::npos) {
                count++;
                pos += str.length();
            }
        }
    }

    return counts;
}

// Function to generate and output the testing report
void generate_testing_report(int master_num, int client_num,
                             const std::string& out_dir) {
    std::stringstream report;
    std::map<std::string, int> total_counts;

    // Define the order of CHAOS_xxx_STR as they appear in chaos.h
    std::vector<std::string> chaos_strings = {
        mooncake::testing::CHAOS_ERROR_STR,
        mooncake::testing::CHAOS_PUT_SUCCESS_STR,
        mooncake::testing::CHAOS_PUT_FAILURE_STR,
        mooncake::testing::CHAOS_GET_SUCCESS_STR,
        mooncake::testing::CHAOS_GET_FAILURE_STR,
        mooncake::testing::CHAOS_MOUNT_SUCCESS_STR,
        mooncake::testing::CHAOS_MOUNT_FAILURE_STR,
        mooncake::testing::CHAOS_UNMOUNT_SUCCESS_STR,
        mooncake::testing::CHAOS_UNMOUNT_FAILURE_STR};

    // master's log only has the following chaos strings
    std::vector<std::string> master_chaos_strings = {
        mooncake::testing::CHAOS_ERROR_STR};

    // Initialize total counts
    for (const auto& str : chaos_strings) {
        total_counts[str] = 0;
    }

    // Generate report for each master
    for (int i = 0; i < master_num; ++i) {
        std::string filename =
            out_dir + "/master_" + std::to_string(i) + ".err";
        auto counts = count_chaos_occurrences(filename);

        report << "=======================\n";
        report << "report: master_" << i << "\n";
        for (const auto& str : master_chaos_strings) {
            report << str << " " << counts[str] << "\n";
            total_counts[str] += counts[str];
        }
        report << "\n";
    }

    // Generate report for each client
    for (int i = 0; i < client_num; ++i) {
        std::string filename =
            out_dir + "/client_" + std::to_string(i) + ".err";
        auto counts = count_chaos_occurrences(filename);

        report << "=======================\n";
        report << "report: client_" << i << "\n";
        for (const auto& str : chaos_strings) {
            report << str << " " << counts[str] << "\n";
            total_counts[str] += counts[str];
        }
        report << "\n";
    }

    // Generate summary report
    report << "=======================\n";
    report << "report: summary\n";
    for (const auto& str : chaos_strings) {
        report << str << " " << total_counts[str] << "\n";
    }

    // Output the report
    LOG(INFO) << "Testing Report:\n" << report.str();
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);

    // Set log level to INFO
    google::SetStderrLogging(google::INFO);

    // Parse command line flags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Log the parsed parameters
    LOG(INFO) << "Chaos testing configuration:";
    LOG(INFO) << "  master_num: " << FLAGS_master_num;
    LOG(INFO) << "  client_num: " << FLAGS_client_num;
    LOG(INFO) << "  master_path: " << FLAGS_master_path;
    LOG(INFO) << "  out_dir: " << FLAGS_out_dir;
    LOG(INFO) << "  skip_run: " << FLAGS_skip_run;
    LOG(INFO) << "  run_sec: " << FLAGS_run_sec;
    LOG(INFO) << "  rand_seed: " << FLAGS_rand_seed;

    int master_num = FLAGS_master_num;
    int client_num = FLAGS_client_num;
    srand(FLAGS_rand_seed);

    if (!FLAGS_skip_run) {
        std::vector<mooncake::testing::MasterHandler> masters;
        for (int i = 0; i < master_num; ++i) {
            masters.emplace_back(FLAGS_master_path, 50051 + i, i);
            masters.back().start();
        }

        std::vector<mooncake::testing::ClientHandler> clients;
        for (int i = 0; i < client_num; ++i) {
            clients.emplace_back(FLAGS_client_path, FLAGS_out_dir, i);
            clients.back().start();
        }

        const int kOpProbTotal = FLAGS_master_op_prob + FLAGS_client_op_prob;

        // Log start time
        auto start_time = std::chrono::steady_clock::now();
        LOG(INFO) << "Chaos testing started at: "
                  << std::chrono::duration_cast<std::chrono::seconds>(
                         start_time.time_since_epoch())
                         .count();

        while (true) {
            // Check if we've exceeded the run time
            auto current_time = std::chrono::steady_clock::now();
            auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::seconds>(current_time -
                                                                 start_time)
                    .count();
            if (elapsed_seconds >= FLAGS_run_sec) {
                LOG(INFO) << "Chaos testing completed after " << elapsed_seconds
                          << " seconds";
                break;
            }

            if (rand() % kOpProbTotal < FLAGS_master_op_prob) {
                int index = rand() % master_num;
                if (masters[index].is_running()) {
                    masters[index].kill();
                } else {
                    masters[index].start();
                }
            } else {
                int index = rand() % client_num;
                if (clients[index].is_running()) {
                    clients[index].kill();
                } else {
                    clients[index].start();
                }
            }
            sleep(5);
        }

        for (int i = 0; i < master_num; ++i) {
            if (masters[i].is_running()) {
                masters[i].kill();
            }
        }
        for (int i = 0; i < client_num; ++i) {
            if (clients[i].is_running()) {
                clients[i].kill();
            }
        }
    }

    generate_testing_report(master_num, client_num, FLAGS_out_dir);

    return 0;
}
