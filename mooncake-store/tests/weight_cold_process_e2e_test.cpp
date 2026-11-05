#include <gtest/gtest.h>

#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "real_client.h"
#include "test_server_helpers.h"

extern char** environ;

namespace mooncake::testing {
namespace {

using namespace std::chrono_literals;
constexpr size_t kMiB = 1024 * 1024;
constexpr size_t kPayloadBytes = 32 * kMiB;

void Require(bool success, const std::string& message) {
    if (!success) throw std::runtime_error(message);
}

void SaveText(const std::filesystem::path& path, const std::string& value) {
    auto temporary = path.string() + ".tmp";
    {
        std::ofstream output(temporary);
        output << value;
        output.flush();
        Require(output.good(), "cannot write " + temporary);
    }
    std::filesystem::rename(temporary, path);
}

std::string ReadText(const std::filesystem::path& path) {
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

char Pattern(size_t offset, size_t payload) {
    return static_cast<char>((offset * 131 + (offset >> 13) + payload * 17) %
                             251);
}

WeightRevisionIdentity Identity(const std::string& mode) {
    return {.tenant_id = "default",
            .name_space = "cold-process-e2e",
            .resource_id = "weights",
            .revision = mode,
            .weight_generation = 1};
}

std::vector<std::string> PayloadKeys(const std::string& mode) {
    return mode == "batch" ? std::vector<std::string>{"process-payload-0",
                                                      "process-payload-1"}
                           : std::vector<std::string>{"process-payload-0"};
}

int OwnerMain(char** argv) {
    RealClient owner;
    Require(owner.setup_real(argv[5], argv[3], 16 * kMiB, kMiB, "tcp", "",
                             argv[2], nullptr, "", true, argv[4]) == 0,
            "owner setup failed");
    SaveText(argv[6], std::to_string(getpid()) + "\nowner\n" + argv[5]);
    const auto deadline = std::chrono::steady_clock::now() + 120s;
    while (!std::filesystem::exists(argv[7])) {
        Require(std::chrono::steady_clock::now() < deadline,
                "owner did not receive stop marker");
        std::this_thread::sleep_for(20ms);
    }
    Require(owner.tearDownAll() == 0, "owner teardown failed");
    return 0;
}

int ProducerMain(char** argv) {
    const std::string mode = argv[5];
    const auto keys = PayloadKeys(mode);
    RealClient source;
    Require(
        source.setup_real(argv[4], argv[3], 0, kMiB, "tcp", "", argv[2]) == 0,
        "producer setup failed");
    std::vector<std::string> payloads(keys.size());
    std::vector<WeightAffinityMappingEntry> layout;
    std::vector<std::string> affinities;
    for (size_t index = 0; index < keys.size(); ++index) {
        payloads[index].resize(kPayloadBytes);
        for (size_t offset = 0; offset < kPayloadBytes; ++offset)
            payloads[index][offset] = Pattern(offset, index);
        affinities.push_back("tensor-" + std::to_string(index));
        layout.push_back({keys[index], kPayloadBytes, affinities.back()});
    }
    auto begin = source.begin_weight_import(
        {.identity = Identity(mode),
         .payload_group_id = {},
         .expected_payload_count = keys.size(),
         .expected_logical_bytes = keys.size() * kPayloadBytes,
         .policy = WeightStoragePolicy{.preferred_residency =
                                           WeightResidencyState::COLD},
         .affinity_summary = {.affinity_count = keys.size(),
                              .affinity_digest =
                                  ComputeWeightAffinityMappingSha256(layout)},
         .payload_layout = layout});
    Require(begin && begin->has_value(), "begin import failed");
    const auto group = (*begin)->manifest.payload_group_id;
    ReplicateConfig config;
    config.data_type = ObjectDataType::WEIGHT;
    if (mode == "batch") {
        std::vector<void*> addresses;
        for (auto& payload : payloads) {
            Require(source.register_buffer(payload.data(), payload.size()) == 0,
                    "register producer payload failed");
            addresses.push_back(payload.data());
        }
        config.group_ids = std::vector<std::string>(keys.size(), group);
        config.residency_affinity_ids = affinities;
        const auto statuses = source.batch_put_from(
            keys, addresses, std::vector<size_t>(keys.size(), kPayloadBytes),
            config);
        Require(statuses.size() == keys.size() &&
                    std::all_of(statuses.begin(), statuses.end(),
                                [](int status) { return status == 0; }),
                "batch cold put failed");
        for (auto& payload : payloads)
            Require(source.unregister_buffer(payload.data()) == 0,
                    "unregister producer payload failed");
    } else {
        config.group_ids = std::vector<std::string>{group};
        config.residency_affinity_ids = affinities;
        Require(source.put(keys.front(), payloads.front(), config) == 0,
                "scalar cold put failed");
    }
    config.data_type = ObjectDataType::METADATA;
    config.group_ids = std::vector<std::string>{group};
    config.residency_affinity_ids.reset();
    Require(
        source.put(MakeWeightManifestKey(Identity(mode)), "{}", config) == 0,
        "manifest put failed");
    auto committed = source.commit_weight_import(
        {.identity = Identity(mode),
         .expected_metadata_generation = (*begin)->metadata_generation,
         .manifest = {
             .manifest_key = MakeWeightManifestKey(Identity(mode)),
             .manifest_sha256 =
                 "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c"
                 "060f61caaff8a",
             .payload_group_id = group,
             .payload_keys_sha256 = ComputeWeightPayloadKeysSha256(keys),
             .payload_count = keys.size(),
             .logical_bytes = keys.size() * kPayloadBytes}});
    Require(committed && committed->has_value(), "commit import failed");
    Require((*committed)->availability == WeightAvailabilityState::READY &&
                (*committed)->residency == WeightResidencyState::COLD &&
                !(*committed)->operation_id,
            "commit did not publish READY/COLD");
    SaveText(argv[6], std::to_string(getpid()) + "\nproducer\n" + argv[4]);
    return 0;
}

int ReaderMain(char** argv) {
    const std::string mode = argv[5];
    const auto keys = PayloadKeys(mode);
    RealClient reader;
    Require(
        reader.setup_real(argv[4], argv[3], 0, kMiB, "tcp", "", argv[2]) == 0,
        "reader setup failed");
    auto revision = reader.get_weight_revision({.identity = Identity(mode)});
    Require(revision && revision->has_value() &&
                (*revision)->metadata.availability ==
                    WeightAvailabilityState::READY &&
                (*revision)->metadata.residency == WeightResidencyState::COLD,
            "producer exit lost READY/COLD revision");
    auto placements = reader.batch_query(keys);
    Require(placements.size() == keys.size(), "payload query size mismatch");
    for (size_t index = 0; index < keys.size(); ++index) {
        Require(placements[index].has_value(), "payload not readable");
        const auto& replicas = placements[index]->replicas;
        Require(!replicas.empty() &&
                    std::all_of(replicas.begin(), replicas.end(),
                                [](const auto& replica) {
                                    return replica.is_local_disk_replica();
                                }),
                "cold payload borrowed a HOT replica");
        std::string actual(kPayloadBytes, '\0');
        Require(reader.register_buffer(actual.data(), actual.size()) == 0,
                "register reader destination failed");
        Require(reader.get_into(keys[index], actual.data(), actual.size()) ==
                    static_cast<int64_t>(actual.size()),
                "cross-process cold read failed");
        Require(reader.unregister_buffer(actual.data()) == 0,
                "unregister reader destination failed");
        for (size_t offset = 0; offset < actual.size(); ++offset) {
            if (actual[offset] != Pattern(offset, index))
                throw std::runtime_error("payload mismatch at " +
                                         std::to_string(offset));
        }
    }
    auto manifest = reader.batch_query({MakeWeightManifestKey(Identity(mode))});
    Require(manifest.size() == 1 && manifest.front() &&
                manifest.front()->replicas.size() == 1 &&
                manifest.front()->replicas.front().is_memory_replica(),
            "manifest is not HOT");
    SaveText(argv[6], std::to_string(getpid()) + "\nreader\n" + argv[4]);
    return 0;
}

class ExecProcess {
   public:
    ~ExecProcess() { Stop(); }

    bool Start(std::vector<std::string> arguments,
               const std::filesystem::path& log) {
        arguments.insert(arguments.begin(), "/proc/self/exe");
        std::vector<char*> argv;
        for (auto& argument : arguments) argv.push_back(argument.data());
        argv.push_back(nullptr);
        std::vector<std::string> environment;
        for (char** entry = environ; *entry; ++entry) {
            const std::string value = *entry;
            if (value.starts_with("MC_STORE_MEMCPY=") ||
                value.starts_with(
                    "MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=") ||
                value.starts_with(
                    "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=") ||
                value.starts_with(
                    "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS="))
                continue;
            environment.push_back(value);
        }
        environment.insert(
            environment.end(),
            {"MC_STORE_MEMCPY=0",
             "MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=67108864",
             "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1",
             "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS=1"});
        std::vector<char*> envp;
        for (auto& entry : environment) envp.push_back(entry.data());
        envp.push_back(nullptr);
        posix_spawn_file_actions_t actions;
        if (posix_spawn_file_actions_init(&actions) != 0) return false;
        const int opened = posix_spawn_file_actions_addopen(
            &actions, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
            0600);
        const int duplicated = posix_spawn_file_actions_adddup2(
            &actions, STDOUT_FILENO, STDERR_FILENO);
        const int error = opened || duplicated
                              ? EINVAL
                              : posix_spawn(&pid_, "/proc/self/exe", &actions,
                                            nullptr, argv.data(), envp.data());
        posix_spawn_file_actions_destroy(&actions);
        if (error != 0) pid_ = -1;
        return error == 0;
    }

    std::optional<int> Wait(std::chrono::seconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (pid_ > 0 && std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            const auto waited = waitpid(pid_, &status, WNOHANG);
            if (waited == pid_) {
                pid_ = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : 128;
            }
            if (waited < 0 && errno != EINTR) {
                if (errno == ECHILD) pid_ = -1;
                return std::nullopt;
            }
            std::this_thread::sleep_for(20ms);
        }
        return std::nullopt;
    }

    void Stop() {
        if (pid_ <= 0) return;
        kill(pid_, SIGKILL);
        while (waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
        }
        pid_ = -1;
    }

    pid_t pid() const { return pid_; }

   private:
    pid_t pid_{-1};
};

class WeightColdProcessE2ETest : public ::testing::Test {
   protected:
    void Run(const std::string& mode) {
        auto pattern =
            (std::filesystem::current_path() / "weight-cold-process-XXXXXX")
                .string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        directory_ = pattern;
        ASSERT_TRUE(server_.Start(InProcMasterConfigBuilder()
                                      .set_enable_offload(true)
                                      .set_default_kv_lease_ttl(100)
                                      .build()));
        const auto ports = getFreeTcpPorts(3);
        ASSERT_EQ(ports.size(), 3u);
        const auto hostname = [&](size_t index) {
            return "127.0.0.1:" + std::to_string(ports[index]);
        };
        const auto ready = directory_ / "owner.ready";
        stop_ = directory_ / "owner.stop";
        ASSERT_TRUE(
            owner_.Start({"--cold-process-owner", server_.master_address(),
                          server_.metadata_url(), directory_.string(),
                          hostname(0), ready.string(), stop_.string()},
                         directory_ / "owner.log"));
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        while (!std::filesystem::exists(ready) &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(20ms);
        ASSERT_TRUE(std::filesystem::exists(ready));
        const auto owner_pid = std::stol(ReadText(ready));
        ASSERT_EQ(owner_pid, owner_.pid());
        ASSERT_NE(owner_pid, getpid());

        ExecProcess producer;
        const auto producer_report = directory_ / "producer.report";
        ASSERT_TRUE(
            producer.Start({"--cold-process-producer", server_.master_address(),
                            server_.metadata_url(), hostname(1), mode,
                            producer_report.string()},
                           directory_ / "producer.log"));
        const auto producer_pid = producer.pid();
        ASSERT_NE(producer_pid, owner_pid);
        const auto produced = producer.Wait(60s);
        ASSERT_TRUE(produced);
        ASSERT_EQ(*produced, 0);
        ASSERT_EQ(std::stol(ReadText(producer_report)), producer_pid);

        // The producer is reaped before the reader starts: no producer heap or
        // surviving producer endpoint can supply this readback.
        ExecProcess reader;
        const auto reader_report = directory_ / "reader.report";
        ASSERT_TRUE(reader.Start(
            {"--cold-process-reader", server_.master_address(),
             server_.metadata_url(), hostname(2), mode, reader_report.string()},
            directory_ / "reader.log"));
        const auto reader_pid = reader.pid();
        ASSERT_NE(reader_pid, owner_pid);
        ASSERT_NE(reader_pid, producer_pid);
        const auto read = reader.Wait(60s);
        ASSERT_TRUE(read);
        ASSERT_EQ(*read, 0);
        ASSERT_EQ(std::stol(ReadText(reader_report)), reader_pid);
        RecordProperty("owner_pid", owner_pid);
        RecordProperty("producer_pid", producer_pid);
        RecordProperty("reader_pid", reader_pid);
        RecordProperty("payload_bytes", kPayloadBytes);
        RecordProperty("hot_segment_bytes", 16 * kMiB);
        RecordProperty("producer_buffer_bytes", kMiB);
        RecordProperty("memcpy_enabled", false);
    }

    void TearDown() override {
        if (!stop_.empty()) SaveText(stop_, "stop");
        if (owner_.pid() > 0) {
            const auto stopped = owner_.Wait(15s);
            EXPECT_TRUE(stopped);
            if (stopped) {
                EXPECT_EQ(*stopped, 0);
            }
        }
        owner_.Stop();
        server_.Stop();
        if (HasFailure()) {
            for (const auto* role : {"owner", "producer", "reader"})
                std::cerr << role << " log:\n"
                          << ReadText(directory_ /
                                      (std::string(role) + ".log"));
        }
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }

    InProcMaster server_;
    ExecProcess owner_;
    std::filesystem::path directory_;
    std::filesystem::path stop_;
};

TEST_F(WeightColdProcessE2ETest, ScalarColdWriteSurvivesProducerProcessExit) {
    Run("scalar");
}

TEST_F(WeightColdProcessE2ETest, BatchColdWriteSurvivesProducerProcessExit) {
    Run("batch");
}

}  // namespace
}  // namespace mooncake::testing

int main(int argc, char** argv) {
    try {
        if (argc == 8 && std::string(argv[1]) == "--cold-process-owner")
            return mooncake::testing::OwnerMain(argv);
        if (argc == 7 && std::string(argv[1]) == "--cold-process-producer")
            return mooncake::testing::ProducerMain(argv);
        if (argc == 7 && std::string(argv[1]) == "--cold-process-reader")
            return mooncake::testing::ReaderMain(argv);
        ::testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
    } catch (const std::exception& error) {
        std::cerr << "cold process child failed: " << error.what() << '\n';
        return 2;
    }
}
