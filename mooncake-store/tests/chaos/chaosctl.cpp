

#include "chaos.h"

#include <map>
#include <fstream>

// Define command line flags
DEFINE_int32(master_num, 1, "Number of master instances (must be > 0)");
DEFINE_int32(client_num, 1, "Number of client instances (must be > 0)");
DEFINE_string(master_path, "./mooncake-store/src/mooncake_master",
              "Path to the master executable");
DEFINE_string(client_path, "./mooncake-store/tests/chaos/chaosclient",
              "Path to the client executable");
DEFINE_string(out_dir, "./output", "Directory for log files");
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
        std::vector<std::unique_ptr<mooncake::testing::MasterHandler>> masters;
        for (int i = 0; i < master_num; ++i) {
            masters.emplace_back(
                std::make_unique<mooncake::testing::MasterHandler>(
                    FLAGS_master_path, 50051 + i, i, FLAGS_out_dir));
            masters.back()->start();
        }

        std::vector<std::unique_ptr<mooncake::testing::ClientHandler>> clients;
        for (int i = 0; i < client_num; ++i) {
            clients.emplace_back(
                std::make_unique<mooncake::testing::ClientHandler>(
                    FLAGS_client_path, FLAGS_out_dir, i));
            clients.back()->start(mooncake::testing::ChaosClientConfig());
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
                    clients[index]->start(mooncake::testing::ChaosClientConfig());
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
