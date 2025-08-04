#include <glog/logging.h>

#include <chrono>
#include <fstream>
#include <map>
#include <memory>

#include "e2e_utils.h"
#include "process_handler.h"

// Define command line flags
USE_engine_flags;
FLAG_etcd_endpoints;
FLAG_master_path;
FLAG_client_path;
FLAG_out_dir;
FLAG_rand_seed;
DEFINE_int32(master_num, 1, "Number of master instances (must be > 0)");
DEFINE_int32(client_num, 1, "Number of client instances (must be > 0)");
DEFINE_bool(skip_run, false, "Only generate report, do not run the test");
DEFINE_int32(run_sec, 100, "Test duration in seconds");
DEFINE_int32(master_op_prob, 1000, "Probability weight for master operations");
DEFINE_int32(client_op_prob, 1000, "Probability weight for client operations");

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

// Function to count occurrences of TEST_xxx_STR in a file
std::map<std::string, int> count_test_string(const std::string& filename) {
    std::map<std::string, int> counts;

    // Initialize all TEST_xxx_STR to 0
    counts[mooncake::testing::TEST_ERROR_STR] = 0;
    counts[mooncake::testing::TEST_PUT_SUCCESS_STR] = 0;
    counts[mooncake::testing::TEST_PUT_FAILURE_STR] = 0;
    counts[mooncake::testing::TEST_GET_SUCCESS_STR] = 0;
    counts[mooncake::testing::TEST_GET_FAILURE_STR] = 0;
    counts[mooncake::testing::TEST_MOUNT_SUCCESS_STR] = 0;
    counts[mooncake::testing::TEST_MOUNT_FAILURE_STR] = 0;
    counts[mooncake::testing::TEST_UNMOUNT_SUCCESS_STR] = 0;
    counts[mooncake::testing::TEST_UNMOUNT_FAILURE_STR] = 0;

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

    // Define the output order of TEST_xxx_STR
    std::vector<std::string> test_strings = {
        mooncake::testing::TEST_ERROR_STR,
        mooncake::testing::TEST_PUT_SUCCESS_STR,
        mooncake::testing::TEST_PUT_FAILURE_STR,
        mooncake::testing::TEST_GET_SUCCESS_STR,
        mooncake::testing::TEST_GET_FAILURE_STR,
        mooncake::testing::TEST_MOUNT_SUCCESS_STR,
        mooncake::testing::TEST_MOUNT_FAILURE_STR,
        mooncake::testing::TEST_UNMOUNT_SUCCESS_STR,
        mooncake::testing::TEST_UNMOUNT_FAILURE_STR};

    // master's log only has the following test strings
    std::vector<std::string> master_test_strings = {
        mooncake::testing::TEST_ERROR_STR};

    // Initialize total counts
    for (const auto& str : test_strings) {
        total_counts[str] = 0;
    }

    // Generate report for each master
    for (int i = 0; i < master_num; ++i) {
        std::string filename =
            out_dir + "/master_" + std::to_string(i) + ".err";
        auto counts = count_test_string(filename);

        report << "=======================\n";
        report << "report: master_" << i << "\n";
        for (const auto& str : master_test_strings) {
            report << str << " " << counts[str] << "\n";
            total_counts[str] += counts[str];
        }
        report << "\n";
    }

    // Generate report for each client
    for (int i = 0; i < client_num; ++i) {
        std::string filename =
            out_dir + "/client_" + std::to_string(i) + ".err";
        auto counts = count_test_string(filename);

        report << "=======================\n";
        report << "report: client_" << i << "\n";
        for (const auto& str : test_strings) {
            report << str << " " << counts[str] << "\n";
            total_counts[str] += counts[str];
        }
        report << "\n";
    }

    // Generate summary report
    report << "=======================\n";
    report << "report: summary\n";
    for (const auto& str : test_strings) {
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

    // If Flags_skip_run is true, only generate the report based on the logs
    // generated on previous runs. Otherwise, run the chaos testing and generate
    // the report.
    if (!FLAGS_skip_run) {
        // Create and start the master processes.
        std::vector<std::unique_ptr<mooncake::testing::MasterProcessHandler>>
            masters;
        for (int i = 0; i < master_num; ++i) {
            masters.emplace_back(
                std::make_unique<mooncake::testing::MasterProcessHandler>(
                    FLAGS_master_path, FLAGS_etcd_endpoints, 50051 + i, i,
                    FLAGS_out_dir));
            masters.back()->start();
        }

        // Create and start the client processes.
        std::vector<std::unique_ptr<mooncake::testing::ClientProcessHandler>>
            clients;
        for (int i = 0; i < client_num; ++i) {
            mooncake::testing::ClientRunnerConfig client_config{
                .put_prob = std::nullopt,
                .get_prob = std::nullopt,
                .mount_prob = std::nullopt,
                .unmount_prob = std::nullopt,
                .port = 17812 + i,
                .master_server_entry = "etcd://" + FLAGS_etcd_endpoints,
                .engine_meta_url = FLAGS_engine_meta_url,
                .protocol = FLAGS_protocol,
                .device_name = FLAGS_device_name,
            };
            clients.emplace_back(
                std::make_unique<mooncake::testing::ClientProcessHandler>(
                    FLAGS_client_path, i, FLAGS_out_dir, client_config));
            clients.back()->start();
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

            // Randomly kill or start a master or a client.
            if (rand() % kOpProbTotal < FLAGS_master_op_prob) {
                int index = rand() % master_num;
                if (masters[index]->is_running()) {
                    masters[index]->kill();
                } else {
                    masters[index]->start();
                }
            } else {
                int index = rand() % client_num;
                if (clients[index]->is_running()) {
                    clients[index]->kill();
                } else {
                    clients[index]->start();
                }
            }
            sleep(5);
        }

        for (int i = 0; i < master_num; ++i) {
            masters[i].reset();
        }
        masters.clear();
        for (int i = 0; i < client_num; ++i) {
            clients[i].reset();
        }
        clients.clear();
    }

    generate_testing_report(master_num, client_num, FLAGS_out_dir);

    return 0;
}