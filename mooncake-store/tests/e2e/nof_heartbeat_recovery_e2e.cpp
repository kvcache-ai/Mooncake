#include <gflags/gflags.h>
#include <glog/logging.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "master_client.h"

DEFINE_string(master_server, "", "Address of the runner's mooncake_master");
DEFINE_string(endpoint, "", "TCP controller endpoint without a namespace ID");
DEFINE_string(control_dir, "",
              "Target lifecycle markers written by the runner");
DEFINE_uint32(timeout_sec, 45, "Timeout for each test phase");

namespace {
using namespace mooncake;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
constexpr size_t kNamespaceSize = 32 * 1024 * 1024;

void Require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

template <typename Predicate>
void WaitFor(Predicate ready, const std::string& message) {
    const auto deadline =
        Clock::now() + std::chrono::seconds(FLAGS_timeout_sec);
    while (!ready()) {
        Require(Clock::now() < deadline, message);
        std::this_thread::sleep_for(100ms);
    }
}

void WaitForMarker(const char* name) {
    const auto path = std::filesystem::path(FLAGS_control_dir) / name;
    WaitFor([&] { return std::filesystem::exists(path); },
            "Timed out waiting for " + path.string());
}

struct Observations {
    size_t successes{0};
    size_t failures{0};
    size_t timeouts{0};
    size_t unmounts{0};
};

using Segments = std::array<NoFSegment, 2>;

std::array<Observations, 2> Observe(const Segments& segments) {
    std::ifstream log(std::filesystem::path(FLAGS_control_dir) / "master.log");
    Require(log.is_open(), "Cannot read master.log");
    std::array<Observations, 2> observations{};
    std::string line;
    while (std::getline(log, line)) {
        for (size_t i = 0; i < segments.size(); ++i) {
            if (line.find(", segment_name=" + segments[i].name + ",") ==
                std::string::npos)
                continue;
            auto& observation = observations[i];
            if (line.find(", action=nof_heartbeat_success,") !=
                std::string::npos) {
                ++observation.successes;
            } else if (line.find(", action=nof_heartbeat_failure,") !=
                       std::string::npos) {
                ++observation.failures;
                if (line.find("reason=completion_timeout") != std::string::npos)
                    ++observation.timeouts;
            } else if (line.find(
                           ", action=unmount_nof_segment_by_heartbeat,") !=
                       std::string::npos) {
                ++observation.unmounts;
            }
        }
    }
    return observations;
}

void CheckRegistrations(MasterClient& client, const Segments& expected) {
    const auto mounted = client.GetAllNoFSegments();
    Require(mounted.has_value(), "GetAllNoFSegments RPC failed");
    Require(mounted->size() == expected.size(),
            "Mounted namespace count changed during recovery");
    for (const auto& segment : expected)
        Require(std::any_of(mounted->begin(), mounted->end(),
                            [&](const NoFSegment& current) {
                                return current.id == segment.id &&
                                       current.name == segment.name &&
                                       current.te_endpoint ==
                                           segment.te_endpoint &&
                                       current.size == segment.size &&
                                       current.base == segment.base;
                            }),
                "Recovery lost the original registration: " + segment.name);
}

void Run() {
    Require(!FLAGS_master_server.empty() && !FLAGS_endpoint.empty() &&
                !FLAGS_control_dir.empty() && FLAGS_timeout_sec > 30,
            "Specify --master_server, --endpoint, --control_dir and "
            "--timeout_sec > 30");
    MasterClient client(generate_uuid());
    WaitFor(
        [&] { return client.Connect(FLAGS_master_server) == ErrorCode::OK; },
        "Master RPC did not become ready");
    Segments segments;
    for (size_t i = 0; i < segments.size(); ++i) {
        auto& segment = segments[i];
        segment.id = generate_uuid();
        segment.name = "heartbeat_recovery_ns" + std::to_string(i + 1);
        segment.base = 0;
        segment.size = kNamespaceSize;
        segment.te_endpoint = FLAGS_endpoint + " ns:" + std::to_string(i + 1);
        Require(client.MountNoFSegment(segment).has_value(),
                "MountNoFSegment RPC failed: " + segment.name);
    }
    CheckRegistrations(client, segments);
    WaitFor(
        [&] {
            const auto observations = Observe(segments);
            return observations[0].successes > 0 &&
                   observations[1].successes > 0;
        },
        "Initial heartbeats did not succeed on both namespaces");
    const auto initial = Observe(segments);
    std::cout << "PASS initial_heartbeats" << std::endl;

    // SIGSTOP preserves TCP connections while preventing reads from completing.
    WaitForMarker("target-paused");
    WaitFor(
        [&] {
            const auto observations = Observe(segments);
            return observations[0].failures > initial[0].failures &&
                   observations[1].failures > initial[1].failures &&
                   observations[0].timeouts + observations[1].timeouts >
                       initial[0].timeouts + initial[1].timeouts;
        },
        "Paused target did not produce a read timeout and failed probes");
    CheckRegistrations(client, segments);
    const auto before_restart = Observe(segments);
    std::cout << "PASS probe_timeout" << std::endl;

    WaitForMarker("target-restarted");
    WaitFor(
        [&] {
            const auto observations = Observe(segments);
            return observations[0].successes > before_restart[0].successes &&
                   observations[1].successes > before_restart[1].successes;
        },
        "Original registrations did not recover successful heartbeats");
    CheckRegistrations(client, segments);
    std::cout << "PASS heartbeats_recovered" << std::endl;

    // Persistent failure must still remove both original registrations.
    WaitForMarker("target-down");
    WaitFor(
        [&] {
            const auto observations = Observe(segments);
            return observations[0].unmounts > 0 && observations[1].unmounts > 0;
        },
        "Namespaces were not removed by heartbeat");
    const auto mounted = client.GetAllNoFSegments();
    Require(mounted.has_value(), "GetAllNoFSegments RPC failed");
    Require(mounted->empty(), "Removed namespaces are still registered");
    std::cout << "PASS failed_namespaces_removed" << std::endl;

    // The runner checks SIGTERM and exit of the independent Master process.
}
}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Run();
        std::cout << "PASS heartbeat_checks" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL heartbeat_recovery: " << error.what() << std::endl;
        return 1;
    }
}
