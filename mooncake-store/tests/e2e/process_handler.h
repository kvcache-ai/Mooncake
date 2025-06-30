#pragma once

#include <sys/types.h>

#include <optional>
#include <string>

namespace mooncake {
namespace testing {

// Strings for test output. When the master or client runs in a different
// process, the output is redirected to files. Marking lines with these strings
// makes parsing the output easier.
inline const char* const TEST_ERROR_STR = "[TEST_ERROR]";
inline const char* const TEST_PUT_SUCCESS_STR = "[TEST_PUT_SUCCESS]";
inline const char* const TEST_PUT_FAILURE_STR = "[TEST_PUT_FAILURE]";
inline const char* const TEST_GET_SUCCESS_STR = "[TEST_GET_SUCCESS]";
inline const char* const TEST_GET_FAILURE_STR = "[TEST_GET_FAILURE]";
inline const char* const TEST_MOUNT_SUCCESS_STR = "[TEST_MOUNT_SUCCESS]";
inline const char* const TEST_MOUNT_FAILURE_STR = "[TEST_MOUNT_FAILURE]";
inline const char* const TEST_UNMOUNT_SUCCESS_STR = "[TEST_UNMOUNT_SUCCESS]";
inline const char* const TEST_UNMOUNT_FAILURE_STR = "[TEST_UNMOUNT_FAILURE]";

/**
 * @brief A wrapper for the master process.
 *
 * This class is used to start and stop the master process.
 */
class MasterProcessHandler {
   public:
    /**
     * @brief Constructor.
     *
     * @param path The path to the master executable.
     * @param etcd_endpoints The etcd endpoints.
     * @param port The port of the master service.
     * @param index The index of the master, used for log.
     * @param out_dir The directory to store the log files.
     */
    MasterProcessHandler(const std::string& path,
                         const std::string& etcd_endpoints, const int port,
                         const int index, const std::string& out_dir);
    ~MasterProcessHandler();

    // Delete copy constructor and assignment operator
    MasterProcessHandler(const MasterProcessHandler&) = delete;
    MasterProcessHandler& operator=(const MasterProcessHandler&) = delete;

    // Delete move constructor and assignment operator
    MasterProcessHandler(MasterProcessHandler&&) = delete;
    MasterProcessHandler& operator=(MasterProcessHandler&&) = delete;

    // Start the master process.
    bool start();

    // Kill the master process.
    bool kill();

    // Check if the process is started and is not killed yet.
    bool is_running() const;

   private:
    // The PID of the master process.
    pid_t master_pid_{0};
    // The path to the master executable.
    std::string master_path_;
    // The etcd endpoints.
    std::string etcd_endpoints_;
    // The port of the master process.
    int port_;
    // The index of the master, used to name the log file.
    int index_;
    // Whether the master is started for the first time.
    bool first_start_{true};
    // The directory to store the log files.
    std::string out_dir_;
};

/**
 * @brief Configuration for the client process.
 *
 * This specifies the client process parameters.
 */
struct ClientRunnerConfig {
    std::optional<int> put_prob;
    std::optional<int> get_prob;
    std::optional<int> mount_prob;
    std::optional<int> unmount_prob;
    std::optional<int> port;
    std::optional<std::string> master_server_entry;
    std::optional<std::string> engine_meta_url;
    std::optional<std::string> protocol;
    std::optional<std::string> device_name;
};

/**
 * @brief A wrapper for the client process.
 *
 * This class is used to start and stop the client process.
 */
class ClientProcessHandler {
   public:
    /**
     * @brief Constructor.
     *
     * @param path The path to the client executable.
     * @param index The index of the client, used for log.
     * @param out_dir The directory to store the log files.
     */
    ClientProcessHandler(const std::string& path, const int index,
                         const std::string& out_dir,
                         const ClientRunnerConfig& config);
    ~ClientProcessHandler();

    // Delete copy constructor and assignment operator
    ClientProcessHandler(const ClientProcessHandler&) = delete;
    ClientProcessHandler& operator=(const ClientProcessHandler&) = delete;

    // Delete move constructor and assignment operator
    ClientProcessHandler(ClientProcessHandler&&) = delete;
    ClientProcessHandler& operator=(ClientProcessHandler&&) = delete;

    // Start the client process with the given parameters.
    bool start();

    // Kill the client process.
    bool kill();

    // Check if the process is started and is not killed yet.
    bool is_running() const;

   private:
    // The PID of the client process.
    pid_t client_pid_{0};
    // The path to the client executable.
    std::string client_path_;
    // The index of the client, used for log.
    int index_;
    // The directory to store the log files.
    std::string out_dir_;
    // The start parameters for the client runner.
    ClientRunnerConfig config_;
    // Whether the client is started for the first time.
    bool first_start_{true};
};

}  // namespace testing
}  // namespace mooncake