#include "master_service/dsl/scenario.h"

#include <atomic>
#include <thread>

namespace mooncake::test {

MountMemoryNodeAction MountMemoryNode(std::string node) {
    MountMemoryNodeAction action{};
    action.node = std::move(node);
    return action;
}

RacePutStartAction RacePutStart(std::string key, uint64_t size) {
    RacePutStartAction action{};
    action.key = std::move(key);
    action.size = size;
    return action;
}

ConcurrentMountUnmountAction ConcurrentMountUnmount(
    std::string segment_prefix) {
    ConcurrentMountUnmountAction action{};
    action.segment_prefix = std::move(segment_prefix);
    return action;
}

ConcurrentWriteAndRemoveAllAction ConcurrentWriteAndRemoveAll(
    std::string key_prefix) {
    ConcurrentWriteAndRemoveAllAction action{};
    action.key_prefix = std::move(key_prefix);
    return action;
}

ConcurrentReadAndRemoveAllAction ConcurrentReadAndRemoveAll(
    std::string key_prefix, size_t object_count) {
    ConcurrentReadAndRemoveAllAction action{};
    action.key_prefix = std::move(key_prefix);
    action.object_count = object_count;
    return action;
}

ConcurrentRemoveAllAction ConcurrentRemoveAll(size_t expected_total_removed) {
    ConcurrentRemoveAllAction action{};
    action.expected_total_removed = expected_total_removed;
    return action;
}

MasterScenario& MasterScenario::When(MountMemoryNodeAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto segment = segments_.find(action.node);
    if (segment == segments_.end()) {
        Fail("MountMemoryNode references undeclared node " + action.node);
        return *this;
    }
    const auto actor = action.actor.empty() ? action.node : action.actor;
    const auto result = service_->MountSegment(segment->second, ActorId(actor));
    ValidateActionResult("MountMemoryNode(" + action.node + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(RacePutStartAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.thread_count == 0 || action.group_ids.empty()) {
        Fail("RacePutStart requires threads and at least one group choice");
        return *this;
    }

    const UUID actor = ActorId(action.actor);
    const TenantId tenant(action.tenant);
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<size_t> successes{0};
    std::atomic<size_t> completions{0};
    std::atomic<size_t> unexpected_errors{0};
    std::vector<std::thread> threads;
    threads.reserve(action.thread_count);
    for (size_t index = 0; index < action.thread_count; ++index) {
        threads.emplace_back([&, index] {
            ReplicateConfig config;
            config.replica_num = 1;
            const auto& group =
                action.group_ids[index % action.group_ids.size()];
            if (!group.empty()) {
                config.group_ids = std::vector<std::string>{group};
            }
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto put = service_->PutStart(actor, action.key, tenant,
                                                action.size, config);
            if (!put) {
                if (put.error() != ErrorCode::OBJECT_ALREADY_EXISTS) {
                    unexpected_errors.fetch_add(1, std::memory_order_relaxed);
                }
                return;
            }
            successes.fetch_add(1, std::memory_order_relaxed);
            if (service_->PutEnd(actor, action.key, tenant,
                                 ReplicaType::MEMORY)) {
                completions.fetch_add(1, std::memory_order_relaxed);
            } else {
                unexpected_errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) < action.thread_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    if (unexpected_errors.load() != 0) {
        Fail("RacePutStart observed unexpected RPC errors");
    }
    if (successes.load() != action.expected_successes) {
        Fail("RacePutStart succeeded " + std::to_string(successes.load()) +
             " times; expected " + std::to_string(action.expected_successes));
    }
    if (completions.load() != action.expected_completions) {
        Fail("RacePutStart completed " + std::to_string(completions.load()) +
             " times; expected " + std::to_string(action.expected_completions));
    }
    return *this;
}

MasterScenario& MasterScenario::When(ConcurrentMountUnmountAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.thread_count == 0 || action.iterations == 0 ||
        action.capacity == 0) {
        Fail("ConcurrentMountUnmount requires non-zero parameters");
        return *this;
    }

    std::atomic<size_t> successes{0};
    std::atomic<size_t> unexpected_errors{0};
    std::vector<std::thread> threads;
    threads.reserve(action.thread_count);
    for (size_t index = 0; index < action.thread_count; ++index) {
        threads.emplace_back([&, index] {
            Segment segment;
            segment.id = generate_uuid();
            segment.name = action.segment_prefix + std::to_string(index);
            segment.base = 0x10000000000ULL + index * (action.capacity + 4096);
            segment.size = action.capacity;
            segment.te_endpoint = segment.name;
            const UUID actor = generate_uuid();
            for (size_t iteration = 0; iteration < action.iterations;
                 ++iteration) {
                const auto mounted = service_->MountSegment(segment, actor);
                if (!mounted) {
                    continue;
                }
                const auto unmounted =
                    service_->UnmountSegment(segment.id, actor);
                if (!unmounted) {
                    unexpected_errors.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                successes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    if (unexpected_errors.load() != 0) {
        Fail("ConcurrentMountUnmount failed to unmount a mounted segment");
    }
    if (successes.load() == 0) {
        Fail("ConcurrentMountUnmount completed no mount/unmount cycle");
    }
    return *this;
}

MasterScenario& MasterScenario::When(ConcurrentWriteAndRemoveAllAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.writer_count == 0 || action.objects_per_writer == 0 ||
        action.object_size == 0) {
        Fail("ConcurrentWriteAndRemoveAll requires non-zero parameters");
        return *this;
    }

    const UUID actor = ActorId("concurrent-writer");
    std::atomic<size_t> successful_writes{0};
    std::vector<std::thread> writers;
    writers.reserve(action.writer_count);
    for (size_t writer = 0; writer < action.writer_count; ++writer) {
        writers.emplace_back([&, writer] {
            ReplicateConfig config;
            config.replica_num = 1;
            for (size_t index = 0; index < action.objects_per_writer; ++index) {
                const std::string key = action.key_prefix +
                                        std::to_string(writer) + "-" +
                                        std::to_string(index);
                const auto put =
                    service_->PutStart(actor, key, TenantId::Default(),
                                       action.object_size, config);
                if (put && service_->PutEnd(actor, key, TenantId::Default(),
                                            ReplicaType::MEMORY)) {
                    successful_writes.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(index % 10));
            }
        });
    }
    long first_removed = 0;
    std::thread remover([&] {
        std::this_thread::sleep_for(action.remove_delay);
        first_removed = service_->RemoveAll();
    });
    for (auto& writer : writers) {
        writer.join();
    }
    remover.join();
    const long final_removed = service_->RemoveAll();
    const size_t expected = action.writer_count * action.objects_per_writer;
    if (first_removed <= 0 || final_removed <= 0) {
        Fail("ConcurrentWriteAndRemoveAll did not overlap both phases");
    }
    if (successful_writes.load() != expected ||
        first_removed + final_removed != static_cast<long>(expected)) {
        Fail("ConcurrentWriteAndRemoveAll did not account for every write");
    }
    return *this;
}

MasterScenario& MasterScenario::When(ConcurrentReadAndRemoveAllAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.reader_count == 0 || action.object_count == 0) {
        Fail("ConcurrentReadAndRemoveAll requires non-zero parameters");
        return *this;
    }

    std::atomic<size_t> successful_reads{0};
    std::vector<std::thread> readers;
    readers.reserve(action.reader_count);
    for (size_t reader = 0; reader < action.reader_count; ++reader) {
        readers.emplace_back([&] {
            for (size_t index = 0; index < action.object_count; ++index) {
                const auto result = service_->GetReplicaList(
                    action.key_prefix + std::to_string(index),
                    TenantId::Default());
                if (result) {
                    successful_reads.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(index % 5));
            }
        });
    }
    std::thread remover([&] {
        std::this_thread::sleep_for(action.remove_delay);
        service_->RemoveAll();
    });
    for (auto& reader : readers) {
        reader.join();
    }
    remover.join();
    const size_t attempts = action.reader_count * action.object_count;
    if (successful_reads.load() == 0 || successful_reads.load() >= attempts) {
        Fail("ConcurrentReadAndRemoveAll did not overlap reads and removal");
    }
    return *this;
}

MasterScenario& MasterScenario::When(ConcurrentRemoveAllAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.thread_count == 0) {
        Fail("ConcurrentRemoveAll requires at least one thread");
        return *this;
    }
    std::atomic<long> total_removed{0};
    std::vector<std::thread> threads;
    threads.reserve(action.thread_count);
    for (size_t index = 0; index < action.thread_count; ++index) {
        threads.emplace_back(
            [&] { total_removed.fetch_add(service_->RemoveAll()); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    if (total_removed.load() !=
        static_cast<long>(action.expected_total_removed)) {
        Fail("ConcurrentRemoveAll removed " +
             std::to_string(total_removed.load()) + "; expected " +
             std::to_string(action.expected_total_removed));
    }
    return *this;
}

}  // namespace mooncake::test
