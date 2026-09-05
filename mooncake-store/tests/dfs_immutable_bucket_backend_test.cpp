#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "replica.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage_backend.h"

namespace mooncake::test {

namespace {

constexpr uint64_t kAlignment = 4096;
constexpr uint64_t kBucketCapacity = 1024 * 1024;

class BucketTempDir {
   public:
    explicit BucketTempDir(const std::string& prefix) {
        static std::atomic<int64_t> counter{0};
        path_ = std::filesystem::temp_directory_path() /
                (prefix + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(++counter));
        path_str_ = path_.string();
        std::filesystem::create_directories(path_);
    }

    ~BucketTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    BucketTempDir(const BucketTempDir&) = delete;
    BucketTempDir& operator=(const BucketTempDir&) = delete;

    const std::string& path() const { return path_str_; }
    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

   private:
    std::filesystem::path path_;
    std::string path_str_;
};

// Injects failures on the Nth WriteAt/ReadAt, and can force short results, to
// exercise the backend's partial-I/O and error paths.
class FaultyPosixFsAdapter : public PosixFsAdapter {
   public:
    void FailWriteCall(int call) { fail_write_call_.store(call); }
    void FailReadCall(int call) { fail_read_call_.store(call); }
    void FailWriteOffset(int64_t offset) { fail_write_offset_.store(offset); }
    // Truncates the Nth write to `bytes`, simulating a short pwritev.
    void ShortWriteCall(int call, size_t bytes) {
        short_write_call_.store(call);
        short_write_bytes_.store(bytes);
    }
    void ShortReadCall(int call, size_t bytes) {
        short_read_call_.store(call);
        short_read_bytes_.store(bytes);
    }
    void ZeroReadCall(int call) { zero_read_call_.store(call); }
    int WriteCalls() const { return write_calls_.load(); }
    int ReadCalls() const { return read_calls_.load(); }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        if (call == fail_write_call_.load() ||
            offset == fail_write_offset_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (call == short_write_call_.load()) {
            // Write only the first `bytes` bytes of the first iov.
            const size_t limit = short_write_bytes_.load();
            std::vector<iovec> trimmed;
            size_t remaining = limit;
            for (int i = 0; i < iovcnt && remaining > 0; ++i) {
                const size_t take = std::min(iov[i].iov_len, remaining);
                trimmed.push_back({iov[i].iov_base, take});
                remaining -= take;
            }
            if (trimmed.empty()) return size_t{0};
            return PosixFsAdapter::WriteAt(fd, trimmed.data(),
                                           static_cast<int>(trimmed.size()),
                                           offset);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override {
        const int call = ++read_calls_;
        if (call == fail_read_call_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return PosixFsAdapter::ReadAt(fd, iov, iovcnt, offset);
    }

    tl::expected<size_t, ErrorCode> DirectReadAt(
        int fd, iovec* iov, int iovcnt, int64_t offset) override {
        const int call = ++read_calls_;
        if (call == fail_read_call_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (call == zero_read_call_.load()) return size_t{0};
        if (call == short_read_call_.load()) {
            std::vector<iovec> trimmed;
            size_t remaining = short_read_bytes_.load();
            for (int i = 0; i < iovcnt && remaining > 0; ++i) {
                const size_t take = std::min(iov[i].iov_len, remaining);
                trimmed.push_back({iov[i].iov_base, take});
                remaining -= take;
            }
            if (trimmed.empty()) return size_t{0};
            return PosixFsAdapter::DirectReadAt(
                fd, trimmed.data(), static_cast<int>(trimmed.size()), offset);
        }
        return PosixFsAdapter::DirectReadAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> read_calls_{0};
    std::atomic<int> fail_write_call_{-1};
    std::atomic<int64_t> fail_write_offset_{-1};
    std::atomic<int> fail_read_call_{-1};
    std::atomic<int> short_read_call_{-1};
    std::atomic<size_t> short_read_bytes_{0};
    std::atomic<int> zero_read_call_{-1};
    std::atomic<int> short_write_call_{-1};
    std::atomic<size_t> short_write_bytes_{0};
};

DistributedStorageConfig MakeBucketConfig(const std::string& fsdir) {
    DistributedStorageConfig config;
    config.fsdir = fsdir;
    config.fs_adapter_type = "posix";
    config.allocator_type = DfsAllocatorType::BUCKET;
    config.bucket_capacity = kBucketCapacity;
    config.max_bucket_count = 16;
    config.alignment = kAlignment;
    config.single_tenant = true;
    config.eviction_enabled = false;
    config.deferred_free_duration = std::chrono::seconds(0);
    config.eviction_check_interval = std::chrono::seconds(1);
    config.shard_count = 1;
    config.shard_capacity = kBucketCapacity;
    return config;
}

}  // namespace

class DfsBucketBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tmp_ = std::make_unique<BucketTempDir>("dfs_bucket_backend");
        config_ = MakeBucketConfig(tmp_->path());

        allocator_ = std::make_unique<ImmutableBucketAllocator>();
        ASSERT_TRUE(allocator_->Init(config_).has_value());

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = tmp_->path();

        auto adapter = std::make_unique<FaultyPosixFsAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, config_, std::move(adapter));
        ASSERT_TRUE(backend_->Init().has_value());
    }

    void TearDown() override {
        backend_.reset();
        allocator_.reset();
        tmp_.reset();
    }

    // Writes `value` for `key` through the allocator + backend, as the client
    // would, and returns the descriptor.
    DistributedFSDescriptor WriteObject(const std::string& key,
                                        const std::string& value) {
        auto desc = allocator_->Allocate(key, value.size());
        EXPECT_TRUE(desc.has_value());
        if (!desc) return {};
        std::vector<Slice> slices{
            {const_cast<char*>(value.data()), value.size()}};
        auto results = backend_->BatchWrite({{key, *desc, slices}});
        EXPECT_EQ(results.size(), 1u);
        EXPECT_TRUE(results[0].has_value());
        EXPECT_TRUE(allocator_->MarkCommitted(key, *desc));
        return *desc;
    }

    std::string ReadObject(const std::string& key,
                           const DistributedFSDescriptor& desc) {
        std::string out(desc.object_size, '\0');
        std::vector<Slice> slices{{out.data(), out.size()}};
        auto results = backend_->BatchRead({{key, desc, slices}});
        EXPECT_EQ(results.size(), 1u);
        EXPECT_TRUE(results[0].has_value());
        return out;
    }

    std::unique_ptr<BucketTempDir> tmp_;
    DistributedStorageConfig config_;
    std::unique_ptr<ImmutableBucketAllocator> allocator_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    FaultyPosixFsAdapter* adapter_ = nullptr;
};

TEST_F(DfsBucketBackendTest, WriteThenReadRoundTrip) {
    const std::string key = "roundtrip_key";
    const std::string value(1000, 'A');
    auto desc = WriteObject(key, value);
    ASSERT_EQ(desc.object_size, value.size());
    EXPECT_EQ(ReadObject(key, desc), value);
}

TEST_F(DfsBucketBackendTest, WrittenEntryContainsHeaderKeyValueAndPadding) {
    const std::string key = "header_key";
    const std::string value(64, 'Z');
    auto desc = WriteObject(key, value);

    const uint64_t entry_start =
        desc.offset - BucketEntryLayout::kHeaderSize - key.size();
    const int fd = ::open(desc.file_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<char> buffer(desc.aligned_size, '\0');
    ASSERT_EQ(::pread(fd, buffer.data(), buffer.size(),
                      static_cast<off_t>(entry_start)),
              static_cast<ssize_t>(buffer.size()));
    ::close(fd);

    uint64_t stored_key_size = 0;
    for (size_t i = 0; i < BucketEntryLayout::kHeaderSize; ++i) {
        stored_key_size |= static_cast<uint64_t>(
                               static_cast<unsigned char>(buffer[i]))
                           << (8 * i);
    }
    EXPECT_EQ(stored_key_size, key.size());
    EXPECT_EQ(std::string(buffer.data() + BucketEntryLayout::kHeaderSize,
                          key.size()),
              key);
    EXPECT_EQ(std::string(buffer.data() + BucketEntryLayout::kHeaderSize +
                              key.size(),
                          value.size()),
              value);
    const size_t entry_size = BucketEntryLayout::kHeaderSize + key.size() +
                              value.size();
    EXPECT_TRUE(std::all_of(buffer.begin() + entry_size, buffer.end(),
                            [](char byte) { return byte == 0; }));
}

TEST_F(DfsBucketBackendTest, MultiSliceValueWriteAndRead) {
    const std::string key = "multi_slice";
    std::string part_a(500, 'X');
    std::string part_b(700, 'Y');
    auto desc = allocator_->Allocate(key, part_a.size() + part_b.size());
    ASSERT_TRUE(desc.has_value());

    std::vector<Slice> write_slices{{part_a.data(), part_a.size()},
                                    {part_b.data(), part_b.size()}};
    auto write_results = backend_->BatchWrite({{key, *desc, write_slices}});
    ASSERT_EQ(write_results.size(), 1u);
    ASSERT_TRUE(write_results[0].has_value());

    // Read back into differently-sized slices.
    std::vector<char> out_a(300), out_b(900);
    std::vector<Slice> read_slices{{out_a.data(), out_a.size()},
                                  {out_b.data(), out_b.size()}};
    auto read_results = backend_->BatchRead({{key, *desc, read_slices}});
    ASSERT_EQ(read_results.size(), 1u);
    ASSERT_TRUE(read_results[0].has_value());

    const std::string expected = part_a + part_b;
    std::string actual(out_a.begin(), out_a.end());
    actual.append(out_b.begin(), out_b.begin() + (expected.size() - 300));
    EXPECT_EQ(actual, expected);
}

TEST_F(DfsBucketBackendTest, BatchWriteAndBatchReadPreserveRequestOrder) {
    std::vector<std::string> keys{"batch_a", "batch_b", "batch_c"};
    std::vector<std::string> values{std::string(100, 'a'),
                                    std::string(200, 'b'),
                                    std::string(300, 'c')};

    std::vector<BatchAllocateRequest> requests;
    for (size_t i = 0; i < keys.size(); ++i) {
        requests.push_back({keys[i], values[i].size()});
    }
    auto allocations = allocator_->BatchAllocate(requests);
    ASSERT_EQ(allocations.size(), keys.size());

    std::vector<DfsWriteRequest> writes;
    std::vector<std::vector<Slice>> write_slices(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(allocations[i].success);
        write_slices[i] = {{values[i].data(), values[i].size()}};
        writes.push_back({keys[i], allocations[i].descriptor, write_slices[i]});
    }
    auto write_results = backend_->BatchWrite(writes);
    ASSERT_EQ(write_results.size(), writes.size());
    for (size_t i = 0; i < write_results.size(); ++i) {
        EXPECT_TRUE(write_results[i].has_value()) << "entry " << i;
    }

    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices(keys.size());
    std::vector<DfsReadRequest> reads;
    outputs.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        outputs.emplace_back(values[i].size(), '\0');
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[i] = {{outputs[i].data(), outputs[i].size()}};
        reads.push_back({keys[i], allocations[i].descriptor, read_slices[i]});
    }
    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value()) << "entry " << i;
        EXPECT_EQ(outputs[i], values[i]) << "entry " << i;
    }
}

TEST_F(DfsBucketBackendTest, LargeBucketIdIsAddressable) {
    // Bucket ids travel in `shard_idx`, which SHARD mode used as an index into
    // a small fixed table. Bucket mode must not inherit that limit: with a
    // one-entry bucket capacity the ids quickly exceed any shard_count.
    BucketTempDir dir("dfs_bucket_large_id");
    auto config = MakeBucketConfig(dir.path());
    config.bucket_capacity = kAlignment;
    config.max_bucket_count = 16;
    // shard_count stays 1, so a bucket id > 0 would be rejected by the old
    // shard-only validation.
    config.shard_count = 1;
    config.shard_capacity = kAlignment;

    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config).has_value());

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = dir.path();
    auto backend = std::make_shared<DistributedStorageBackend>(
        file_config, config, std::make_unique<PosixFsAdapter>());
    ASSERT_TRUE(backend->Init().has_value());

    // Push the id well past shard_count.
    DistributedFSDescriptor desc;
    std::string key;
    const std::string value(64, 'L');
    for (int i = 0; i < 6; ++i) {
        key = "large_id_" + std::to_string(i);
        auto allocated = allocator.Allocate(key, value.size());
        ASSERT_TRUE(allocated.has_value()) << "allocation " << i;
        desc = *allocated;
    }
    ASSERT_GT(desc.shard_idx, config.shard_count);

    std::vector<Slice> write_slices{
        {const_cast<char*>(value.data()), value.size()}};
    auto write_results = backend->BatchWrite({{key, desc, write_slices}});
    ASSERT_EQ(write_results.size(), 1u);
    ASSERT_TRUE(write_results[0].has_value())
        << "bucket id " << desc.shard_idx << " must be writable";

    std::string out(value.size(), '\0');
    std::vector<Slice> read_slices{{out.data(), out.size()}};
    auto read_results = backend->BatchRead({{key, desc, read_slices}});
    ASSERT_EQ(read_results.size(), 1u);
    ASSERT_TRUE(read_results[0].has_value());
    EXPECT_EQ(out, value);
}

TEST_F(DfsBucketBackendTest, RejectsDescriptorOutsideDfsRoot) {
    const std::string key = "escape_key";
    const std::string value(100, 'E');
    auto desc = WriteObject(key, value);

    // A descriptor pointing outside the configured root must be refused before
    // any file is opened, even if it names a plausible bucket file.
    auto escaped = desc;
    escaped.file_path = tmp_->path() + "/../bucket_" +
                        ImmutableBucketAllocator::FormatBucketId(0) + ".data";
    std::string out(value.size(), '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};
    auto results = backend_->BatchRead({{key, escaped, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(DfsBucketBackendTest, RejectsMismatchedBucketFileName) {
    const std::string key = "mismatch_key";
    const std::string value(100, 'M');
    auto desc = WriteObject(key, value);

    // The path must name the bucket the descriptor claims.
    auto wrong_name = desc;
    wrong_name.file_path =
        tmp_->file("bucket_" + ImmutableBucketAllocator::FormatBucketId(99) +
                   ".data");
    std::string out(value.size(), '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};
    auto results = backend_->BatchRead({{key, wrong_name, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(DfsBucketBackendTest, RejectsInconsistentDescriptors) {
    const std::string key = "bad_desc";
    const std::string value(100, 'B');
    auto desc = WriteObject(key, value);

    struct Case {
        const char* name;
        DistributedFSDescriptor descriptor;
    };
    std::vector<Case> cases;
    {
        auto bad = desc;
        bad.object_size = 0;
        cases.push_back({"zero object_size", bad});
    }
    {
        auto bad = desc;
        // aligned_size no longer matches the reserved size for this entry.
        bad.aligned_size = kAlignment * 2;
        cases.push_back({"wrong aligned_size", bad});
    }
    {
        auto bad = desc;
        // An offset smaller than the header + key cannot be a valid value
        // offset.
        bad.offset = 1;
        cases.push_back({"offset before header", bad});
    }
    {
        auto bad = desc;
        bad.shard_idx = -1;
        cases.push_back({"negative bucket id", bad});
    }
    {
        auto bad = desc;
        bad.object_size = kBucketCapacity;
        cases.push_back({"entry past bucket capacity", bad});
    }

    for (const auto& test_case : cases) {
        std::string out(std::max<uint64_t>(1, test_case.descriptor.object_size),
                        '\0');
        std::vector<Slice> slices{{out.data(), out.size()}};
        auto read_results =
            backend_->BatchRead({{key, test_case.descriptor, slices}});
        ASSERT_EQ(read_results.size(), 1u) << test_case.name;
        EXPECT_FALSE(read_results[0].has_value()) << test_case.name;

        auto write_results =
            backend_->BatchWrite({{key, test_case.descriptor, slices}});
        ASSERT_EQ(write_results.size(), 1u) << test_case.name;
        EXPECT_FALSE(write_results[0].has_value()) << test_case.name;
    }
}

TEST_F(DfsBucketBackendTest, PerKeyErrorsDoNotAffectOtherKeys) {
    std::vector<std::string> keys{"ok_key", "fail_key"};
    std::vector<std::string> values{std::string(100, 'o'),
                                    std::string(100, 'f')};
    std::vector<BatchAllocateRequest> requests{{keys[0], values[0].size()},
                                               {keys[1], values[1].size()}};
    auto allocations = allocator_->BatchAllocate(requests);
    ASSERT_EQ(allocations.size(), 2u);
    ASSERT_TRUE(allocations[0].success);
    ASSERT_TRUE(allocations[1].success);

    // Make only the second request invalid before I/O; the first request must
    // still be written successfully.
    auto failed_descriptor = allocations[1].descriptor;
    failed_descriptor.shard_idx = -1;

    std::vector<std::vector<Slice>> slices(2);
    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < keys.size(); ++i) {
        slices[i] = {{values[i].data(), values[i].size()}};
        writes.push_back({keys[i], i == 1 ? failed_descriptor
                                           : allocations[i].descriptor,
                          slices[i]});
    }
    auto results = backend_->BatchWrite(writes);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);

    // The successful key is intact and readable.
    EXPECT_EQ(ReadObject(keys[0], allocations[0].descriptor), values[0]);
}

TEST_F(DfsBucketBackendTest, ShortWriteIsResumedNotReportedAsFailure) {
    const std::string key = "short_write";
    const std::string value(2000, 'S');
    auto desc = allocator_->Allocate(key, value.size());
    ASSERT_TRUE(desc.has_value());

    // Force the first pwritev to stop after 16 bytes; the backend must resume
    // from where it left off rather than declaring a short write.
    adapter_->ShortWriteCall(adapter_->WriteCalls() + 1, 16);

    std::vector<Slice> slices{
        {const_cast<char*>(value.data()), value.size()}};
    auto results = backend_->BatchWrite({{key, *desc, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    EXPECT_EQ(ReadObject(key, *desc), value);
}

TEST_F(DfsBucketBackendTest, ReadFailurePropagates) {
    const std::string key = "read_fail";
    const std::string value(100, 'R');
    auto desc = WriteObject(key, value);

    adapter_->FailReadCall(adapter_->ReadCalls() + 1);
    std::string out(value.size(), '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};
    auto results = backend_->BatchRead({{key, desc, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(DfsBucketBackendTest, ShortDirectReadIsResumed) {
    const std::string key = "short_direct_read";
    const std::string value(2000, 'D');
    auto desc = WriteObject(key, value);

    adapter_->ShortReadCall(adapter_->ReadCalls() + 1, 17);
    EXPECT_EQ(ReadObject(key, desc), value);
    EXPECT_GE(adapter_->ReadCalls(), 2);
}

TEST_F(DfsBucketBackendTest, PartialDirectReadFailurePropagates) {
    const std::string key = "partial_direct_failure";
    const std::string value(2000, 'P');
    auto desc = WriteObject(key, value);

    const int next_call = adapter_->ReadCalls() + 1;
    adapter_->ShortReadCall(next_call, 17);
    adapter_->FailReadCall(next_call + 1);

    std::string out(value.size(), '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};
    auto results = backend_->BatchRead({{key, desc, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(DfsBucketBackendTest, ZeroLengthDirectReadIsAnError) {
    const std::string key = "zero_direct_read";
    const std::string value(100, 'Z');
    auto desc = WriteObject(key, value);

    adapter_->ZeroReadCall(adapter_->ReadCalls() + 1);
    std::string out(value.size(), '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};
    auto results = backend_->BatchRead({{key, desc, slices}});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(DfsBucketBackendTest, UninitializedBackendFailsEveryRequest) {
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp_->path();
    auto backend = std::make_shared<DistributedStorageBackend>(
        file_config, config_, std::make_unique<PosixFsAdapter>());
    // Deliberately not Init()ed.

    DistributedFSDescriptor desc;
    desc.file_path = tmp_->file("bucket_000000.data");
    desc.offset = 12;
    desc.object_size = 4;
    desc.aligned_size = kAlignment;
    std::string out(4, '\0');
    std::vector<Slice> slices{{out.data(), out.size()}};

    auto read_results = backend->BatchRead({{"k", desc, slices}});
    ASSERT_EQ(read_results.size(), 1u);
    ASSERT_FALSE(read_results[0].has_value());
    EXPECT_EQ(read_results[0].error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);

    auto write_results = backend->BatchWrite({{"k", desc, slices}});
    ASSERT_EQ(write_results.size(), 1u);
    ASSERT_FALSE(write_results[0].has_value());
    EXPECT_EQ(write_results[0].error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
}

TEST_F(DfsBucketBackendTest, ObjectsSurviveAcrossBucketRollover) {
    // Force many rollovers by using a tiny bucket capacity.
    BucketTempDir dir("dfs_bucket_rollover_io");
    auto config = MakeBucketConfig(dir.path());
    config.bucket_capacity = 2 * kAlignment;
    config.max_bucket_count = 16;

    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config).has_value());

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = dir.path();
    auto backend = std::make_shared<DistributedStorageBackend>(
        file_config, config, std::make_unique<PosixFsAdapter>());
    ASSERT_TRUE(backend->Init().has_value());

    std::vector<std::pair<std::string, DistributedFSDescriptor>> written;
    std::vector<std::string> values;
    for (int i = 0; i < 8; ++i) {
        const std::string key = "rollover_" + std::to_string(i);
        values.push_back(std::string(500, static_cast<char>('a' + i)));
        auto desc = allocator.Allocate(key, values.back().size());
        ASSERT_TRUE(desc.has_value()) << "allocation " << i;
        std::vector<Slice> slices{
            {values.back().data(), values.back().size()}};
        auto results = backend->BatchWrite({{key, *desc, slices}});
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value()) << "write " << i;
        written.emplace_back(key, *desc);
    }

    // Several buckets must have been created, and every object still reads back
    // correctly through its own bucket file.
    EXPECT_GT(allocator.GetBucketCount(), 1u);
    for (size_t i = 0; i < written.size(); ++i) {
        std::string out(values[i].size(), '\0');
        std::vector<Slice> slices{{out.data(), out.size()}};
        auto results =
            backend->BatchRead({{written[i].first, written[i].second, slices}});
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value()) << "read " << i;
        EXPECT_EQ(out, values[i]) << "entry " << i;
    }
}

TEST_F(DfsBucketBackendTest, ConcurrentReadsAndWritesOnSharedBucket) {
    constexpr int kKeys = 16;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;
    for (int i = 0; i < kKeys; ++i) {
        keys.push_back("conc_" + std::to_string(i));
        values.push_back(std::string(512, static_cast<char>('A' + i % 26)));
        auto desc = allocator_->Allocate(keys.back(), values.back().size());
        ASSERT_TRUE(desc.has_value());
        descriptors.push_back(*desc);
    }

    // Concurrent writers on the same bucket file (distinct offsets).
    std::vector<std::thread> writers;
    for (int i = 0; i < kKeys; ++i) {
        writers.emplace_back([this, i, &keys, &values, &descriptors]() {
            std::vector<Slice> slices{
                {values[i].data(), values[i].size()}};
            auto results =
                backend_->BatchWrite({{keys[i], descriptors[i], slices}});
            EXPECT_EQ(results.size(), 1u);
            EXPECT_TRUE(results[0].has_value());
        });
    }
    for (auto& writer : writers) writer.join();

    // Concurrent readers must each see their own value, not a neighbour's.
    std::vector<std::string> outputs(kKeys);
    std::vector<std::thread> readers;
    for (int i = 0; i < kKeys; ++i) {
        readers.emplace_back([this, i, &keys, &values, &descriptors,
                              &outputs]() {
            outputs[i].assign(values[i].size(), '\0');
            std::vector<Slice> slices{{outputs[i].data(), outputs[i].size()}};
            auto results =
                backend_->BatchRead({{keys[i], descriptors[i], slices}});
            EXPECT_EQ(results.size(), 1u);
            EXPECT_TRUE(results[0].has_value());
        });
    }
    for (auto& reader : readers) reader.join();

    for (int i = 0; i < kKeys; ++i) {
        EXPECT_EQ(outputs[i], values[i]) << "entry " << i;
    }
}

TEST_F(DfsBucketBackendTest, ReadAfterAllocatorRecoveryReturnsSameBytes) {
    // Write data, drop the allocator, recover it, and read via the recovered
    // descriptors: recovery must restore usable descriptors, not just bookkeeping.
    BucketTempDir dir("dfs_bucket_recover_io");
    auto config = MakeBucketConfig(dir.path());

    std::vector<std::string> keys{"rec_a", "rec_b"};
    std::vector<std::string> values{std::string(300, 'p'),
                                    std::string(400, 'q')};

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = dir.path();
    auto backend = std::make_shared<DistributedStorageBackend>(
        file_config, config, std::make_unique<PosixFsAdapter>());
    ASSERT_TRUE(backend->Init().has_value());

    {
        ImmutableBucketAllocator allocator;
        ASSERT_TRUE(allocator.Init(config).has_value());
        for (size_t i = 0; i < keys.size(); ++i) {
            auto desc = allocator.Allocate(keys[i], values[i].size());
            ASSERT_TRUE(desc.has_value());
            std::vector<Slice> slices{{values[i].data(), values[i].size()}};
            auto results = backend->BatchWrite({{keys[i], *desc, slices}});
            ASSERT_EQ(results.size(), 1u);
            ASSERT_TRUE(results[0].has_value());
            ASSERT_TRUE(allocator.MarkCommitted(keys[i], *desc));
        }
    }

    ImmutableBucketAllocator recovered;
    ASSERT_TRUE(recovered.Init(config).has_value());
    auto replicas = recovered.TakeRecoveredReplicas();
    ASSERT_EQ(replicas.size(), keys.size());

    for (const auto& replica : replicas) {
        const auto it = std::find(keys.begin(), keys.end(), replica.key);
        ASSERT_NE(it, keys.end());
        const size_t index = std::distance(keys.begin(), it);
        std::string out(replica.descriptor.object_size, '\0');
        std::vector<Slice> slices{{out.data(), out.size()}};
        auto results =
            backend->BatchRead({{replica.key, replica.descriptor, slices}});
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value()) << "key=" << replica.key;
        EXPECT_EQ(out, values[index]) << "key=" << replica.key;
    }
}

TEST_F(DfsBucketBackendTest, ParallelBatchReadAcrossBuckets) {
    // Test parallel reads across different buckets
    constexpr size_t kNumBuckets = 4;
    constexpr size_t kEntriesPerBucket = 5;

    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;

    // Create multiple objects in each bucket
    for (size_t bucket = 0; bucket < kNumBuckets; ++bucket) {
        for (size_t i = 0; i < kEntriesPerBucket; ++i) {
            std::string key =
                "parallel_bucket_" + std::to_string(bucket) + "_key_" +
                std::to_string(i);
            std::string value(100 + i * 10, static_cast<char>('A' + bucket));

            auto desc = allocator_->Allocate(key, value.size());
            ASSERT_TRUE(desc.has_value()) << "Allocate failed for " << key;

            std::vector<Slice> slices{{const_cast<char*>(value.data()),
                                       value.size()}};
            auto results = backend_->BatchWrite({{key, *desc, slices}});
            ASSERT_EQ(results.size(), 1u);
            ASSERT_TRUE(results[0].has_value()) << "Write failed for " << key;
            ASSERT_TRUE(allocator_->MarkCommitted(key, *desc));

            keys.push_back(key);
            values.push_back(value);
            descriptors.push_back(*desc);
        }
    }

    // Batch read all objects
    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices;
    std::vector<DfsReadRequest> reads;
    outputs.reserve(keys.size());
    read_slices.resize(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        outputs.emplace_back(values[i].size(), '\0');
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[i] = {{outputs[i].data(), outputs[i].size()}};
        reads.push_back({keys[i], descriptors[i], read_slices[i]});
    }

    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value())
            << "Read failed for " << keys[i];
        EXPECT_EQ(outputs[i], values[i]) << "Mismatch for " << keys[i];
    }
}

TEST_F(DfsBucketBackendTest, NonContiguousEntriesInSameBucket) {
    // Test non-contiguous entries within the same bucket
    // Create multiple objects with gaps between them
    std::vector<std::string> keys{"key1", "key2", "key3"};
    std::vector<std::string> values{
        std::string(100, '1'),
        std::string(200, '2'),
        std::string(300, '3')};

    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto desc = allocator_->Allocate(keys[i], values[i].size());
        ASSERT_TRUE(desc.has_value());

        std::vector<Slice> slices{{const_cast<char*>(values[i].data()),
                                   values[i].size()}};
        auto results = backend_->BatchWrite({{keys[i], *desc, slices}});
        ASSERT_TRUE(results[0].has_value());
        ASSERT_TRUE(allocator_->MarkCommitted(keys[i], *desc));
        descriptors.push_back(*desc);
    }

    // Shuffle read order
    std::vector<size_t> indices{2, 0, 1};
    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices;
    std::vector<DfsReadRequest> reads;
    read_slices.resize(keys.size());

    for (size_t i : indices) {
        outputs.emplace_back(values[i].size(), '\0');
    }
    size_t out_idx = 0;
    for (size_t i : indices) {
        read_slices[out_idx] = {{outputs[out_idx].data(),
                                 outputs[out_idx].size()}};
        reads.push_back({keys[i], descriptors[i], read_slices[out_idx]});
        ++out_idx;
    }

    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value());
        EXPECT_EQ(outputs[i], values[indices[i]]);
    }
}

TEST_F(DfsBucketBackendTest, SingleBucketPathFallsBackToSequential) {
    // Test single bucket path falls back to sequential execution
    constexpr size_t kNumEntries = 10;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;

    // Create multiple objects in the same bucket
    for (size_t i = 0; i < kNumEntries; ++i) {
        std::string key = "single_bucket_key_" + std::to_string(i);
        std::string value(100 + i * 50, static_cast<char>('A' + i));

        auto desc = allocator_->Allocate(key, value.size());
        ASSERT_TRUE(desc.has_value());

        std::vector<Slice> slices{{const_cast<char*>(value.data()),
                                   value.size()}};
        auto results = backend_->BatchWrite({{key, *desc, slices}});
        ASSERT_TRUE(results[0].has_value());
        ASSERT_TRUE(allocator_->MarkCommitted(key, *desc));

        keys.push_back(key);
        values.push_back(value);
        descriptors.push_back(*desc);
    }

    // Batch read
    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices;
    std::vector<DfsReadRequest> reads;
    outputs.reserve(keys.size());
    read_slices.resize(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        outputs.emplace_back(values[i].size(), '\0');
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[i] = {{outputs[i].data(), outputs[i].size()}};
        reads.push_back({keys[i], descriptors[i], read_slices[i]});
    }

    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value());
        EXPECT_EQ(outputs[i], values[i]);
    }
}

TEST_F(DfsBucketBackendTest, ContiguousEntriesAreMerged) {
    // Test contiguous entries are merged for optimal IO
    constexpr size_t kNumEntries = 5;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;

    // Create contiguous objects (same size to ensure alignment)
    for (size_t i = 0; i < kNumEntries; ++i) {
        std::string key = "contiguous_key_" + std::to_string(i);
        std::string value(kAlignment / 2, static_cast<char>('X' + i));

        auto desc = allocator_->Allocate(key, value.size());
        ASSERT_TRUE(desc.has_value());

        std::vector<Slice> slices{{const_cast<char*>(value.data()),
                                   value.size()}};
        auto results = backend_->BatchWrite({{key, *desc, slices}});
        ASSERT_TRUE(results[0].has_value());
        ASSERT_TRUE(allocator_->MarkCommitted(key, *desc));

        keys.push_back(key);
        values.push_back(value);
        descriptors.push_back(*desc);
    }

    // Batch read
    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices;
    std::vector<DfsReadRequest> reads;
    outputs.reserve(keys.size());
    read_slices.resize(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        outputs.emplace_back(values[i].size(), '\0');
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[i] = {{outputs[i].data(), outputs[i].size()}};
        reads.push_back({keys[i], descriptors[i], read_slices[i]});
    }

    // Use default parallel read threads
    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value());
        EXPECT_EQ(outputs[i], values[i]);
    }
}

TEST_F(DfsBucketBackendTest, BatchReadWithDifferentThreadCounts) {
    // Test BatchRead behavior with different thread count configurations
    constexpr size_t kNumEntries = 20;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;

    // Create objects across multiple buckets
    for (size_t i = 0; i < kNumEntries; ++i) {
        std::string key = "thread_count_key_" + std::to_string(i);
        std::string value(100 + i * 25, static_cast<char>('A' + i % 26));

        auto desc = allocator_->Allocate(key, value.size());
        ASSERT_TRUE(desc.has_value());

        std::vector<Slice> slices{{const_cast<char*>(value.data()),
                                   value.size()}};
        auto results = backend_->BatchWrite({{key, *desc, slices}});
        ASSERT_TRUE(results[0].has_value());
        ASSERT_TRUE(allocator_->MarkCommitted(key, *desc));

        keys.push_back(key);
        values.push_back(value);
        descriptors.push_back(*desc);
    }

    // Test different batch_read_threads configurations
    std::vector<int> thread_counts{1, 2, 4, 8};
    for (int threads : thread_counts) {
        config_.batch_read_threads = threads;

        // Recreate backend to apply new configuration
        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = tmp_->path();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, config_, std::make_unique<FaultyPosixFsAdapter>());
        ASSERT_TRUE(backend_->Init().has_value());

        // Batch read
        std::vector<std::string> outputs;
        std::vector<std::vector<Slice>> read_slices;
        std::vector<DfsReadRequest> reads;
        outputs.reserve(keys.size());
        read_slices.resize(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            outputs.emplace_back(values[i].size(), '\0');
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            read_slices[i] = {{outputs[i].data(), outputs[i].size()}};
            reads.push_back({keys[i], descriptors[i], read_slices[i]});
        }

        auto read_results = backend_->BatchRead(reads);
        ASSERT_EQ(read_results.size(), reads.size());
        for (size_t i = 0; i < read_results.size(); ++i) {
            ASSERT_TRUE(read_results[i].has_value())
                << "Failed with " << threads << " threads, entry " << i;
            EXPECT_EQ(outputs[i], values[i])
                << "Mismatch with " << threads << " threads, entry " << i;
        }
    }
}

TEST_F(DfsBucketBackendTest, BatchReadMixedContiguousAndNonContiguous) {
    // Test mixed contiguous and non-contiguous entries
    // Create some contiguous objects and some scattered objects
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;

    // Create 3 contiguous objects
    for (size_t i = 0; i < 3; ++i) {
        std::string key = "mixed_contiguous_" + std::to_string(i);
        std::string value(200, static_cast<char>('A' + i));
        keys.push_back(key);
        values.push_back(value);
    }

    // Create 2 scattered objects (different sizes to break alignment)
    std::string key4 = "mixed_scattered_0";
    std::string value4(150, 'D');
    keys.push_back(key4);
    values.push_back(value4);

    std::string key5 = "mixed_scattered_1";
    std::string value5(250, 'E');
    keys.push_back(key5);
    values.push_back(value5);

    // Create 2 more contiguous objects
    std::string key6 = "mixed_contiguous_3";
    std::string value6(200, 'F');
    keys.push_back(key6);
    values.push_back(value6);

    std::string key7 = "mixed_contiguous_4";
    std::string value7(200, 'G');
    keys.push_back(key7);
    values.push_back(value7);

    // Write all objects
    for (size_t i = 0; i < keys.size(); ++i) {
        auto desc = allocator_->Allocate(keys[i], values[i].size());
        ASSERT_TRUE(desc.has_value()) << "Allocate failed for " << keys[i];

        std::vector<Slice> slices{{const_cast<char*>(values[i].data()),
                                   values[i].size()}};
        auto results = backend_->BatchWrite({{keys[i], *desc, slices}});
        ASSERT_TRUE(results[0].has_value()) << "Write failed for " << keys[i];
        ASSERT_TRUE(allocator_->MarkCommitted(keys[i], *desc));
        descriptors.push_back(*desc);
    }

    // Random order read
    std::vector<size_t> indices{6, 0, 4, 2, 5, 1, 3};
    std::vector<std::string> outputs;
    std::vector<std::vector<Slice>> read_slices;
    std::vector<DfsReadRequest> reads;
    read_slices.resize(keys.size());

    for (size_t i : indices) {
        outputs.emplace_back(values[i].size(), '\0');
    }

    size_t out_idx = 0;
    for (size_t i : indices) {
        read_slices[out_idx] = {{outputs[out_idx].data(),
                                 outputs[out_idx].size()}};
        reads.push_back({keys[i], descriptors[i], read_slices[out_idx]});
        ++out_idx;
    }

    // Use parallel reads
    config_.batch_read_threads = 4;
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp_->path();
    backend_ = std::make_shared<DistributedStorageBackend>(
        file_config, config_, std::make_unique<FaultyPosixFsAdapter>());
    ASSERT_TRUE(backend_->Init().has_value());

    auto read_results = backend_->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    for (size_t i = 0; i < read_results.size(); ++i) {
        ASSERT_TRUE(read_results[i].has_value())
            << "Read failed for index " << indices[i];
        EXPECT_EQ(outputs[i], values[indices[i]])
            << "Mismatch for index " << indices[i];
    }
}

TEST(PosixFsAdapterDirectReadTest, HandlesUnalignedScatterBuffers) {
    BucketTempDir dir("dfs_direct_unaligned");
    PosixFsAdapter adapter;
    ASSERT_TRUE(adapter.Init(dir.path()).has_value());

    std::vector<char> data(2 * kAlignment);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<char>(i % 251);
    }
    const std::string path = dir.file("direct.data");
    ASSERT_TRUE(adapter
                    .WriteFile(path, std::span<const char>(data.data(),
                                                           data.size()))
                    .has_value());

    auto fd = adapter.OpenFileDirect(path);
    ASSERT_TRUE(fd.has_value());
    std::vector<char> first(124, '\0');
    std::vector<char> second(388, '\0');
    iovec iovs[] = {{first.data() + 1, 123}, {second.data() + 1, 387}};
    auto read = adapter.DirectReadAt(*fd, iovs, 2, 37);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, 510u);
    EXPECT_TRUE(std::equal(first.begin() + 1, first.end(), data.begin() + 37));
    EXPECT_TRUE(
        std::equal(second.begin() + 1, second.end(), data.begin() + 160));
    EXPECT_TRUE(adapter.CloseFile(*fd).has_value());
}

TEST(PosixFsAdapterDirectReadTest, ReportsShortReadPastEndOfFile) {
    BucketTempDir dir("dfs_direct_short");
    PosixFsAdapter adapter;
    ASSERT_TRUE(adapter.Init(dir.path()).has_value());

    std::vector<char> data(5000, 's');
    const std::string path = dir.file("short.data");
    ASSERT_TRUE(adapter
                    .WriteFile(path, std::span<const char>(data.data(),
                                                           data.size()))
                    .has_value());
    auto fd = adapter.OpenFileDirect(path);
    ASSERT_TRUE(fd.has_value());

    std::vector<char> output(200, '\0');
    iovec iov{output.data(), output.size()};
    auto read = adapter.DirectReadAt(*fd, &iov, 1, 4900);
    ASSERT_FALSE(read.has_value());
    EXPECT_EQ(read.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_TRUE(adapter.CloseFile(*fd).has_value());
}

TEST(PosixFsAdapterDirectReadTest, ReusesStagingUnderConcurrentReads) {
    BucketTempDir dir("dfs_direct_pool");
    PosixFsAdapter adapter;
    ASSERT_TRUE(adapter.Init(dir.path()).has_value());

    std::vector<char> data(4 * kAlignment);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<char>(i % 127);
    }
    const std::string path = dir.file("pool.data");
    ASSERT_TRUE(adapter
                    .WriteFile(path, std::span<const char>(data.data(),
                                                           data.size()))
                    .has_value());
    auto fd = adapter.OpenFileDirect(path);
    ASSERT_TRUE(fd.has_value());

    std::atomic<int> failures{0};
    std::vector<std::thread> readers;
    for (size_t thread = 0; thread < 32; ++thread) {
        readers.emplace_back([&, thread]() {
            for (size_t iteration = 0; iteration < 20; ++iteration) {
                const size_t offset = 13 + ((thread + iteration) % 100);
                std::vector<char> output(1001, '\0');
                iovec iov{output.data() + 1, 1000};
                auto read = adapter.DirectReadAt(*fd, &iov, 1, offset);
                if (!read || *read != 1000 ||
                    !std::equal(output.begin() + 1, output.end(),
                                data.begin() + offset)) {
                    ++failures;
                }
            }
        });
    }
    for (auto& reader : readers) reader.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_TRUE(adapter.CloseFile(*fd).has_value());
}

}  // namespace mooncake::test
