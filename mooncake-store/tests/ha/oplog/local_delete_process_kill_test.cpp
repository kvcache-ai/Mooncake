#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <thread>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/mock_metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "local_delete.h"

namespace mooncake::test {
namespace {

class FileHaKvBackend final : public HaKvBackend {
   public:
    explicit FileHaKvBackend(std::filesystem::path root)
        : root_(std::move(root)) {
        std::filesystem::create_directories(root_);
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        std::ifstream input(Path(key), std::ios::binary);
        if (!input) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value.assign(std::istreambuf_iterator<char>(input), {});
        return input.bad() ? ErrorCode::ETCD_OPERATION_ERROR : ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        return Write(Path(key), value);
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        kvs.clear();
        for (const auto& entry : std::filesystem::directory_iterator(root_)) {
            auto key = Decode(entry.path().filename().string());
            if (!key || *key < begin_key || *key >= end_key) {
                continue;
            }
            std::string value;
            if (Get(*key, value) != ErrorCode::OK) {
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
            kvs.push_back({std::move(*key), std::move(value)});
        }
        std::sort(kvs.begin(), kvs.end(), [](const KvPair& a, const KvPair& b) {
            return a.key < b.key;
        });
        if (limit != 0 && kvs.size() > limit) {
            kvs.resize(limit);
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        for (const auto& compare : txn.compares) {
            std::string current;
            const auto result = Get(compare.key, current);
            if ((compare.kind == KvCompareKind::kKeyNotExists &&
                 result != ErrorCode::ETCD_KEY_NOT_EXIST) ||
                (compare.kind == KvCompareKind::kValueEquals &&
                 (result != ErrorCode::OK ||
                  current != compare.expected_value))) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            const auto result = Write(Path(put.key), put.value);
            if (result != ErrorCode::OK) {
                return result;
            }
        }
        return ErrorCode::OK;
    }

   private:
    static std::string Encode(std::string_view value) {
        constexpr char kHex[] = "0123456789abcdef";
        std::string encoded;
        encoded.reserve(value.size() * 2);
        for (unsigned char c : value) {
            encoded.push_back(kHex[c >> 4]);
            encoded.push_back(kHex[c & 0xf]);
        }
        return encoded;
    }

    static std::optional<std::string> Decode(std::string_view encoded) {
        if (encoded.size() % 2 != 0) {
            return std::nullopt;
        }
        auto nibble = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            return -1;
        };
        std::string value;
        value.reserve(encoded.size() / 2);
        for (size_t i = 0; i < encoded.size(); i += 2) {
            const int high = nibble(encoded[i]);
            const int low = nibble(encoded[i + 1]);
            if (high < 0 || low < 0) return std::nullopt;
            value.push_back(static_cast<char>((high << 4) | low));
        }
        return value;
    }

    std::filesystem::path Path(std::string_view key) const {
        return root_ / Encode(key);
    }

    ErrorCode Write(const std::filesystem::path& path, std::string_view value) {
        const auto temporary =
            path.string() + ".tmp." + std::to_string(::getpid());
        const int fd =
            ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd < 0) return ErrorCode::ETCD_OPERATION_ERROR;
        size_t written = 0;
        while (written < value.size()) {
            const auto result =
                ::write(fd, value.data() + written, value.size() - written);
            if (result <= 0) {
                ::close(fd);
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
            written += static_cast<size_t>(result);
        }
        if (::fsync(fd) != 0 || ::close(fd) != 0 ||
            ::rename(temporary.c_str(), path.c_str()) != 0) {
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        const int directory_fd = ::open(root_.c_str(), O_RDONLY | O_DIRECTORY);
        const bool synced = directory_fd >= 0 && ::fsync(directory_fd) == 0;
        if (directory_fd >= 0) ::close(directory_fd);
        return synced ? ErrorCode::OK : ErrorCode::ETCD_OPERATION_ERROR;
    }

    std::filesystem::path root_;
};

bool WaitForPath(const std::filesystem::path& path) {
    for (int i = 0; i < 500; ++i) {
        if (std::filesystem::exists(path)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

int RunWriterUntilKilled(const std::filesystem::path& kv_root,
                         const std::filesystem::path& failpoint_root,
                         std::string_view failpoint, const std::string& cluster,
                         OpLogEntry entry) {
    ::setenv("MOONCAKE_TEST_FAILPOINT_DIR", failpoint_root.c_str(), 1);
    ::setenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC", "30", 1);
    FileHaKvBackend backend(kv_root);
    OpLogBatchStorage storage(cluster, backend);
    DurablePrefix prefix;
    if (storage.InitDurablePrefix(prefix) != ErrorCode::OK) return 2;
    OrderedOpLogWriter writer(
        {.max_entries_per_batch = 1, .initial_durable_prefix = prefix},
        [&storage](const OpLogBatchRecord& batch,
                   const DurablePrefix& expected) {
            return storage.WriteBatchAndAdvancePrefix(batch, expected);
        });
    writer.Start();
    auto reservation = writer.Reserve();
    if (!reservation) return 3;
    auto committed =
        writer.Commit(std::move(*reservation), std::move(entry),
                      [failpoint_root](const OpLogEntry&) {
                          std::ofstream(failpoint_root / "callback-ran") << "1";
                      });
    if (!committed) return 4;
    for (;;) pause();
}

class LocalDeleteProcessKillTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto path = std::filesystem::temp_directory_path() /
                    ("local-delete-kill-" + std::to_string(::getpid()));
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / "kv");
        std::filesystem::create_directories(path / "failpoints");
        root_ = std::move(path);
    }

    void TearDown() override { std::filesystem::remove_all(root_); }

    void KillAt(std::string_view failpoint, const std::string& cluster,
                OpLogEntry entry) {
        const auto arm =
            root_ / "failpoints" / (std::string(failpoint) + ".arm");
        std::ofstream(arm) << "1";
        const pid_t child = ::fork();
        ASSERT_NE(child, -1);
        if (child == 0) {
            _exit(RunWriterUntilKilled(root_ / "kv", root_ / "failpoints",
                                       failpoint, cluster, std::move(entry)));
        }
        const auto hit =
            root_ / "failpoints" / (std::string(failpoint) + ".hit");
        ASSERT_TRUE(WaitForPath(hit));
        ASSERT_EQ(::kill(child, SIGKILL), 0);
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFSIGNALED(status));
        ASSERT_EQ(WTERMSIG(status), SIGKILL);
        EXPECT_FALSE(
            std::filesystem::exists(root_ / "failpoints" / "callback-ran"));
    }

    std::filesystem::path root_;
};

TEST_F(LocalDeleteProcessKillTest,
       DurableRemoveSurvivesPrimaryKillBeforeCallback) {
    const std::string cluster = "local-delete-remove-kill";
    LocalDeleteTask task{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = "key",
        .object_incarnation = GenerateObjectIncarnation(),
    };
    const auto payload = struct_pack::serialize(LocalDeleteRemovePayloadV1{
        .schema_version = 1,
        .object_incarnation = task.object_incarnation,
        .delete_intents = {task},
    });
    KillAt("batch_txn_succeeded_before_callback", cluster,
           {.op_type = OpType::REMOVE,
            .tenant_id = "default",
            .object_key = "key",
            .payload = std::string(payload.begin(), payload.end())});

    FileHaKvBackend backend(root_ / "kv");
    OpLogBatchStorage storage(cluster, backend);
    DurablePrefix prefix;
    ASSERT_EQ(storage.ReadDurablePrefix(prefix), ErrorCode::OK);
    EXPECT_EQ(prefix.last_seq, 1);
    OpLogBatchRecord batch;
    ASSERT_EQ(storage.ReadBatch(1, batch), ErrorCode::OK);
    ASSERT_EQ(batch.entries.size(), 1);
    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, cluster);
    ASSERT_TRUE(applier.ApplyOpLogEntry(batch.entries.front()));
    ASSERT_EQ(metadata.SnapshotLocalDeleteTasks().size(), 1);
    EXPECT_EQ(metadata.SnapshotLocalDeleteTasks().front(), task);
}

TEST_F(LocalDeleteProcessKillTest, AckKilledBeforeDurabilityLeavesTaskPending) {
    const std::string cluster = "local-delete-ack-kill";
    LocalDeleteTask task{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = "key",
        .object_incarnation = GenerateObjectIncarnation(),
    };
    const auto payload = struct_pack::serialize(LocalDeleteAckPayloadV1{
        .schema_version = 1,
        .local_disk_segment_id = task.local_disk_segment_id,
        .task_ids = {task.task_id},
    });
    KillAt("batch_before_txn", cluster,
           {.op_type = OpType::LOCAL_DELETE_ACK,
            .tenant_id = "default",
            .object_key = task.local_disk_segment_id,
            .payload = std::string(payload.begin(), payload.end())});

    FileHaKvBackend backend(root_ / "kv");
    OpLogBatchStorage storage(cluster, backend);
    DurablePrefix prefix;
    ASSERT_EQ(storage.ReadDurablePrefix(prefix), ErrorCode::OK);
    EXPECT_EQ(prefix.last_seq, 0);
    MockMetadataStore metadata;
    ASSERT_TRUE(metadata.ApplyLocalDeleteTasks({task}));
    EXPECT_EQ(metadata.SnapshotLocalDeleteTasks().size(), 1);
}

}  // namespace
}  // namespace mooncake::test
