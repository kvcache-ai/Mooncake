#pragma once

#include <thread>

#include "store_segment_cleanup_fixture.h"

namespace mooncake::cleanup_test {
class MigrationGate {
   public:
    explicit MigrationGate(void* source) : source_(source) {}

    bool Matches(const std::vector<Slice>& slices) const {
        if (slices.empty()) return false;
        const auto address = reinterpret_cast<uintptr_t>(slices.front().ptr);
        const auto base = reinterpret_cast<uintptr_t>(source_);
        return address >= base && address - base < Scenario::size;
    }
    void Block(const std::vector<Slice>& slices) {
        std::unique_lock lock(mutex_);
        EXPECT_EQ(slices.size(), 1u);
        EXPECT_EQ(slices.front().size, 4096u);
        const auto* data = static_cast<const char*>(slices.front().ptr);
        EXPECT_TRUE(std::all_of(data, data + slices.front().size,
                                [](char value) { return value == 'w'; }));
        worker_thread_ = std::this_thread::get_id();
        captured_ = true;
        event_.notify_all();
        event_.wait(lock, [this] { return proceed_; });
    }
    bool WaitCaptured() {
        std::unique_lock lock(mutex_);
        return event_.wait_for(lock, 30s, [this] { return captured_; });
    }
    void BeginClose() {
        std::lock_guard lock(mutex_);
        close_thread_ = std::this_thread::get_id();
    }
    bool IsCloseThread() {
        std::lock_guard lock(mutex_);
        return close_thread_ == std::this_thread::get_id();
    }
    bool IsSourceWorker(const std::thread& worker) {
        std::lock_guard lock(mutex_);
        return captured_ && worker.get_id() == worker_thread_;
    }
    void BeginDrain() {
        std::lock_guard lock(mutex_);
        drain_started_ = true;
        event_.notify_all();
    }
    void EndDrain() {
        std::lock_guard lock(mutex_);
        drained_ = true;
    }
    bool ObserveRelease() {
        std::lock_guard lock(mutex_);
        if (!drained_) ++premature_releases_;
        event_.notify_all();
        EXPECT_TRUE(drained_)
            << "Migration source release before task-pool drain: captured="
            << captured_ << " blocked=" << !proceed_;
        return drained_;
    }
    bool WaitCloseBoundary() {
        std::unique_lock lock(mutex_);
        return event_.wait_for(lock, 30s, [this] {
            return drain_started_ || premature_releases_ != 0;
        });
    }
    int PrematureReleases() {
        std::lock_guard lock(mutex_);
        return premature_releases_;
    }
    void Unblock() {
        std::lock_guard lock(mutex_);
        proceed_ = true;
        event_.notify_all();
    }

   private:
    void* source_;
    std::mutex mutex_;
    std::condition_variable event_;
    std::thread::id close_thread_;
    std::thread::id worker_thread_;
    bool captured_ = false, proceed_ = false, drain_started_ = false,
         drained_ = false;
    int premature_releases_ = 0;
};

inline std::atomic<std::shared_ptr<MigrationGate>> migration_gate;

class MigrationScope {
   public:
    explicit MigrationScope(std::shared_ptr<MigrationGate> gate)
        : gate_(std::move(gate)) {
        migration_gate.store(gate_);
    }
    ~MigrationScope() {
        gate_->Unblock();
        migration_gate.store(nullptr);
    }

   private:
    std::shared_ptr<MigrationGate> gate_;
};

class MigrationDestination {
   public:
    void Start(Scenario& source) {
        int socket = -1;
        const auto port = findAvailableTcpPort(socket);
        ASSERT_NE(port, 0);
        name = "127.0.0.1:" + std::to_string(port);
        close(socket);
        engine_ = std::make_shared<TransferEngine>(false);
        ASSERT_EQ(engine_->init(source.uri, name, "127.0.0.1", 0), 0);
        ASSERT_NE(engine_->installTransport("tcp", nullptr), nullptr);
        auto created = Client::Create(name, source.uri, "tcp", std::nullopt,
                                      source.master.master_address(), engine_);
        ASSERT_TRUE(created);
        client = *created;
        base_ = allocate_buffer_allocator_memory(Scenario::size, "tcp");
        ASSERT_NE(base_, nullptr);
        ASSERT_TRUE(client->MountSegment(base_, Scenario::size, "tcp", ""));
    }
    ~MigrationDestination() {
        if (client && base_) {
            EXPECT_TRUE(client->UnmountSegment(base_, Scenario::size));
        }
        client.reset();
        if (base_) RealFree("tcp", base_, nullptr);
        engine_.reset();
    }
    std::shared_ptr<Client> client;
    std::string name;

   private:
    void* base_ = nullptr;
    std::shared_ptr<TransferEngine> engine_;
};
}  // namespace mooncake::cleanup_test
