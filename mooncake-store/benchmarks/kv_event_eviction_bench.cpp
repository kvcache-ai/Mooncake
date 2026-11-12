// Fill a real MasterService allocator, then let allocation failure trigger its
// background evictor. No synthetic Removed calls or inter-event pacing.
// Like batch_evict_bench, this exercises metadata/allocation, not payload I/O.
#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <msgpack.hpp>
#include <zmq.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <endian.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <vector>

DEFINE_uint64(num_objects, 300000, "Pool capacity in fixed-size objects");
DEFINE_uint64(object_bytes, 524288, "Bytes allocated per object");
DEFINE_string(windows_us, "0,50,100,250,500,1000",
              "Removal batching windows to compare");
DEFINE_uint32(rounds, 3, "Repetitions; rotate window order each repetition");

namespace mooncake {
namespace {
using Clock = std::chrono::steady_clock;

// Timestamp the existing round markers without instrumenting individual
// objects or imposing a producer rate. send() only records monotonic time.
class EvictionMarkers : public google::LogSink {
   public:
    std::atomic<int64_t> start{0}, end{0};

    void send(google::LogSeverity, const char*, const char*, int,
              const std::tm*, const char* message, size_t length) override {
        const std::string_view text(message, length);
        const auto now = Clock::now().time_since_epoch().count();
        if (text.starts_with("[EVICT-TRIGGER]")) start.store(now);
        if (text.starts_with("[EVICT-DONE]")) end.store(now);
    }
};

std::string Key(size_t i) {
    auto suffix = std::to_string(i);
    return "sglang-rkv-v4:dsa:pp0:" + std::string(64 - suffix.size(), '0') +
           suffix;
}

std::string Endpoint() {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK_GE(socket, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK_EQ(
        ::bind(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    socklen_t size = sizeof(address);
    CHECK_EQ(getsockname(socket, reinterpret_cast<sockaddr*>(&address), &size),
             0);
    ::close(socket);
    return "tcp://127.0.0.1:" + std::to_string(ntohs(address.sin_port));
}

struct Capture {
    std::atomic<bool> ready{false}, stop{false};
    std::atomic<size_t> count{0};
    std::unordered_set<std::string> removed;
    uint64_t batches = 0, bytes = 0, events = 0, errors = 0;
    uint64_t previous_sequence = 0, previous_event = 0;
    Clock::time_point first{}, last{};

    void Run(const std::string& endpoint) {
        void* context = zmq_ctx_new();
        CHECK(context);
        void* socket = zmq_socket(context, ZMQ_SUB);
        CHECK(socket);
        const int hwm = 1000000, timeout = 20, linger = 0;
        CHECK_EQ(zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm)), 0);
        CHECK_EQ(
            zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout)), 0);
        CHECK_EQ(zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger)),
                 0);
        CHECK_EQ(zmq_setsockopt(socket, ZMQ_SUBSCRIBE, "", 0), 0);
        CHECK_EQ(zmq_connect(socket, endpoint.c_str()), 0);
        ready.store(true);
        while (!stop.load()) {
            std::vector<std::string> frames;
            while (true) {
                zmq_msg_t message;
                zmq_msg_init(&message);
                const int result = zmq_msg_recv(&message, socket, 0);
                if (result < 0) {
                    zmq_msg_close(&message);
                    break;
                }
                frames.emplace_back(
                    static_cast<const char*>(zmq_msg_data(&message)),
                    zmq_msg_size(&message));
                const bool more = zmq_msg_more(&message);
                zmq_msg_close(&message);
                if (!more) break;
            }
            if (frames.empty()) continue;
            if (frames.size() != 3 || frames[1].size() != 8) {
                ++errors;
                continue;
            }
            uint64_t sequence;
            std::memcpy(&sequence, frames[1].data(), 8);
            sequence = be64toh(sequence);
            if (sequence != previous_sequence + 1) ++errors;
            previous_sequence = sequence;
            ++batches;
            for (const auto& frame : frames) bytes += frame.size();
            auto unpacked = msgpack::unpack(frames[2].data(), frames[2].size());
            using Fields = std::map<std::string, msgpack::object>;
            auto batch =
                unpacked.get()
                    .as<std::tuple<int64_t, std::vector<Fields>, uint32_t>>();
            const auto& items = std::get<1>(batch);
            if (items.empty() || items.size() > 64) ++errors;
            const auto received = Clock::now();
            if (!events) first = received;
            for (const auto& item : items) {
                const auto id = item.at("event_id").as<uint64_t>();
                if (id != previous_event + 1) ++errors;
                previous_event = id;
                if (item.at("event_type").as<std::string>() != "removed" ||
                    item.at("medium").as<std::string>() != "cpu" ||
                    item.at("tenant_id").as<std::string>() != "default" ||
                    !removed.insert(item.at("object_key").as<std::string>())
                         .second) {
                    ++errors;
                }
                ++events;
            }
            last = Clock::now();
            count.store(events);
        }
        zmq_close(socket);
        zmq_ctx_term(context);
    }
};

bool Run(uint32_t window_us, unsigned round) {
    auto config = MasterServiceConfig::builder()
                      .set_memory_allocator(BufferAllocatorType::OFFSET)
                      .set_default_kv_lease_ttl(0)
                      .set_default_kv_soft_pin_ttl(0)
                      .set_client_live_ttl_sec(3600)
                      .set_eviction_high_watermark_ratio(1.0)
                      .build();
    // High watermark 1.0 isolates one allocation-pressure round: filling the
    // pool cannot trigger an earlier watermark round. The eviction ratio is
    // unchanged at its default 5%; leases/pins do not hide eligible objects.
    MasterService service(config);
    test::MasterServiceTestPeer peer(service);
    const UUID client = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "kv-event-eviction-bench";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000;
    segment.size = FLAGS_num_objects * FLAGS_object_bytes;
    CHECK(service.MountSegment(segment, client).has_value());
    ReplicateConfig replication;
    replication.replica_num = 1;
    for (size_t i = 0; i < FLAGS_num_objects; ++i) {
        const auto key = Key(i);
        CHECK(service
                  .PutStart(client, key, TenantId::Default(),
                            FLAGS_object_bytes, replication)
                  .has_value())
            << "warmup object " << i;
        CHECK(service
                  .PutEnd(client, key, TenantId::Default(), ReplicaType::MEMORY)
                  .has_value());
    }
    const auto usage_before = service.QuerySegments(segment.name);
    CHECK(usage_before.has_value());
    CHECK_EQ(usage_before->first, usage_before->second)
        << "allocator must be full before the pressure trigger";

    // Warmup stores are irrelevant to this measurement. Start the publisher
    // and TCP subscriber only after the allocator is populated.
    KvEventConfig publishing;
    publishing.enabled = true;
    publishing.backend_id = "real-eviction-bench";
    publishing.bind_endpoint = Endpoint();
    publishing.removal_batch_timeout_us = window_us;
    peer.SetKvEventPublisherForTesting(
        std::make_unique<KvEventPublisher>(publishing));
    Capture capture;
    std::thread receiver([&] { capture.Run(publishing.bind_endpoint); });
    while (!capture.ready.load()) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto& metrics = MasterMetricManager::instance();
    const auto successes = metrics.get_mem_eviction_success();
    const auto evicted_before = metrics.get_evicted_key_count();
    EvictionMarkers markers;
    google::AddLogSink(&markers);
    const auto triggered = Clock::now();
    const auto failed =
        service.PutStart(client, "capacity-pressure-trigger",
                         TenantId::Default(), FLAGS_object_bytes, replication);
    CHECK(!failed.has_value()) << "full allocator must reject another object";
    const auto timeout = triggered + std::chrono::seconds(30);
    auto last_not_done = triggered;
    while (metrics.get_mem_eviction_success() == successes &&
           Clock::now() < timeout) {
        last_not_done = Clock::now();
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    const auto done_observed = Clock::now();
    const auto evicted = metrics.get_evicted_key_count() - evicted_before;
    while (capture.count.load() < static_cast<size_t>(evicted) &&
           Clock::now() < timeout) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    capture.stop.store(true);
    receiver.join();
    google::RemoveLogSink(&markers);
    const auto started =
        Clock::time_point(Clock::duration(markers.start.load()));
    const auto ended = Clock::time_point(Clock::duration(markers.end.load()));
    bool valid =
        metrics.get_mem_eviction_success() == successes + 1 &&
        markers.start.load() != 0 && markers.end.load() != 0 &&
        evicted == static_cast<int64_t>(
                       std::ceil(FLAGS_num_objects * config.eviction_ratio)) &&
        capture.events == static_cast<size_t>(evicted) && capture.errors == 0 &&
        service.GetKvEventStats().published_events == capture.events &&
        service.GetKvEventStats().dropped_events == 0;
    // Compare every object's actual metadata existence with the Removed view.
    std::unordered_set<std::string> remaining;
    const auto* ns = test::MasterServiceTestPeer::Namespaces(service).Lookup(
        TenantId::Default());
    if (ns) {
        for (auto object : ns->objects.ReadCursor()) {
            remaining.insert(std::string(object.key()));
        }
    }
    valid =
        valid && remaining.size() + capture.removed.size() == FLAGS_num_objects;
    for (size_t i = 0; i < FLAGS_num_objects; ++i) {
        const auto key = Key(i);
        if (remaining.contains(key) == capture.removed.contains(key))
            valid = false;
    }
    const auto us = [](auto duration) {
        return std::chrono::duration<double, std::micro>(duration).count();
    };
    std::cout << "{\"round\":" << round << ",\"window_us\":" << window_us
              << ",\"objects\":" << FLAGS_num_objects
              << ",\"pool_bytes\":" << usage_before->second
              << ",\"allocated_bytes\":" << usage_before->first
              << ",\"evicted\":" << evicted
              << ",\"received\":" << capture.events
              << ",\"batches\":" << capture.batches
              << ",\"payload_bytes\":" << capture.bytes
              << ",\"observer_burst_us\":" << us(capture.last - capture.first)
              << ",\"eviction_round_us\":" << us(ended - started)
              << ",\"view_convergence_tail_us\":" << us(capture.last - ended)
              << ",\"trigger_to_done_us\":" << us(done_observed - triggered)
              << ",\"tail_lag_lower_us\":" << us(capture.last - done_observed)
              << ",\"tail_lag_upper_us\":" << us(capture.last - last_not_done)
              << ",\"valid\":" << (valid ? "true" : "false") << "}\n"
              << std::flush;
    return valid;
}
}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_minloglevel = 0;  // The round-marker LogSink needs the INFO markers.
    std::vector<uint32_t> windows;
    std::stringstream input(FLAGS_windows_us);
    for (std::string token; std::getline(input, token, ',');) {
        windows.push_back(std::stoul(token));
    }
    CHECK(!windows.empty());
    CHECK_GE(FLAGS_num_objects, 4096u);
    CHECK_GE(FLAGS_object_bytes, 4096u);
    for (unsigned round = 0; round < FLAGS_rounds; ++round) {
        for (size_t i = 0; i < windows.size(); ++i) {
            if (!mooncake::Run(windows[(i + round) % windows.size()], round)) {
                return 1;
            }
        }
    }
}
