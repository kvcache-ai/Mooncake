// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_peer_directory.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <json/json.h>

namespace mooncake::ssd_offload {
namespace {
class SsdPeerDirectoryTest : public ::testing::Test {
   protected:
    std::filesystem::path path;
    void SetUp() override {
        char name[] = "/tmp/mooncake-ssd-peers-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        path = name;
        ASSERT_EQ(chmod(path.c_str(), 0700), 0);
    }
    void TearDown() override { std::filesystem::remove_all(path); }
    SsdPeerDirectoryEntry Entry(uint64_t id = 1, uint64_t boot = 3) {
        SsdFixedL2Peer fixed{{id, 2, boot}, "default", "requester-te", {7, 8}};
        return {fixed,
                {fixed.identity,
                 fixed.tenant_id,
                 "owner:100",
                 "127.0.0.1",
                 1200,
                 fixed.transport_endpoint,
                 {1, fixed.identity, 15, 64, 1024, 64, 1024, 1024, {"tcp"}}},
                "tcp",
                true};
    }
    std::filesystem::path Record() {
        for (const auto& item : std::filesystem::directory_iterator(path))
            if (item.path().extension() == ".json") return item.path();
        return {};
    }
};
TEST_F(SsdPeerDirectoryTest, AutomaticallyPublishedPeerHasPrivateTrustedRoute) {
    SsdPeerDirectory writer(path), reader(path);
    auto entry = Entry();
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_EQ(found.error, DestinationError::kOk);
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->fixed.identity, entry.fixed.identity);
    EXPECT_EQ(found.peer->fixed.credential, entry.fixed.credential);
    EXPECT_EQ(found.peer->route.control_port, 1200);
    struct stat info{};
    ASSERT_EQ(stat(Record().c_str(), &info), 0);
    EXPECT_EQ(info.st_mode & 0777, 0600);
}
TEST_F(SsdPeerDirectoryTest, LookupRequiresMatchingIdentityTenantAndProtocol) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    EXPECT_EQ(reader.LookupRequester({1, 2, 99}, "default", "tcp").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "foreign", "tcp").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "rdma").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "default", "tcp").error,
              DestinationError::kOk);
}
TEST_F(SsdPeerDirectoryTest, PublishesActualBatchCapability) {
    SsdPeerDirectory writer(path), reader(path);
    auto entry = Entry();
    entry.route.capability.max_batch_objects = 512;
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);
    Json::Value value;
    std::ifstream(Record()) >> value;
    EXPECT_EQ(value["max_batch_objects"].asUInt64(), 512u);
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->route.capability.max_batch_objects, 512u);
}
TEST_F(SsdPeerDirectoryTest, MissingAdvertisementKeepsOldOwnerBatchLimit) {
    SsdPeerDirectory writer(path), reader(path);
    auto entry = Entry();
    entry.route.capability.max_batch_objects = 512;
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);
    Json::Value value;
    std::ifstream(Record()) >> value;
    value.removeMember("max_batch_objects");
    std::ofstream(Record()) << value;
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->route.capability.max_batch_objects, 64u);
}
TEST_F(SsdPeerDirectoryTest, UnsafeDirectoryAndSymlinkAreRejected) {
    ASSERT_EQ(chmod(path.c_str(), 0750), 0);
    EXPECT_THROW(SsdPeerDirectory{path}, std::invalid_argument);
    ASSERT_EQ(chmod(path.c_str(), 0700), 0);
    auto link = path.string() + "-link";
    ASSERT_EQ(symlink(path.c_str(), link.c_str()), 0);
    EXPECT_THROW(SsdPeerDirectory{link}, std::invalid_argument);
    std::filesystem::remove(link);
}
TEST_F(SsdPeerDirectoryTest, UnsafeRecordPermissionsFailClosed) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    ASSERT_EQ(chmod(Record().c_str(), 0640), 0);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kUnauthorized);
}
TEST_F(SsdPeerDirectoryTest, RecordSymlinkAndOversizeFailClosed) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    auto file = Record();
    auto outside = path / "outside";
    std::filesystem::rename(file, outside);
    ASSERT_EQ(symlink(outside.c_str(), file.c_str()), 0);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kUnauthorized);
    std::filesystem::remove(file);
    std::filesystem::rename(outside, file);
    std::ofstream(file) << std::string(16385, 'x');
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kInvalidArgument);
}
TEST_F(SsdPeerDirectoryTest, ConflictingLegacyRoutesFailClosed) {
    SsdPeerDirectory first(path), second(path), reader(path);
    ASSERT_EQ(first.Publish(Entry()), DestinationError::kOk);
    ASSERT_EQ(second.Publish(Entry(2)), DestinationError::kOk);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kDuplicate);
}
TEST_F(SsdPeerDirectoryTest, OldEpochCleanupCannotRemoveReplacement) {
    SsdPeerDirectory old(path), replacement(path), reader(path);
    ASSERT_EQ(old.Publish(Entry()), DestinationError::kOk);
    auto next = Entry(1, 4);
    ASSERT_EQ(replacement.Publish(next), DestinationError::kOk);
    ASSERT_EQ(old.Unpublish(), DestinationError::kOk);
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->fixed.identity.boot_epoch, 4u);
    EXPECT_EQ(replacement.Unpublish(), DestinationError::kOk);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kNotFound);
}
TEST_F(SsdPeerDirectoryTest,
       InvalidPublicationNeverCreatesAuthoritativeRecord) {
    SsdPeerDirectory writer(path);
    auto entry = Entry();
    entry.fixed.credential = {};
    EXPECT_EQ(writer.Publish(entry), DestinationError::kInvalidArgument);
    EXPECT_TRUE(std::filesystem::is_empty(path));
}
TEST_F(SsdPeerDirectoryTest, MoreThanBoundedPeerCountFailsClosed) {
    std::vector<std::unique_ptr<SsdPeerDirectory>> writers;
    for (uint64_t id = 1; id <= 65; ++id) {
        auto writer = std::make_unique<SsdPeerDirectory>(path);
        auto entry = Entry(id);
        entry.route.legacy_rpc_endpoint += std::to_string(id);
        ASSERT_EQ(writer->Publish(entry), DestinationError::kOk);
        writers.push_back(std::move(writer));
    }
    SsdPeerDirectory reader(path);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "default", "tcp").error,
              DestinationError::kLimit);
}
TEST_F(SsdPeerDirectoryTest, NonRegularRecordFailsWithoutBlocking) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    auto file = Record();
    std::filesystem::remove(file);
    ASSERT_EQ(mkfifo(file.c_str(), 0600), 0);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "default", "tcp").error,
              DestinationError::kUnauthorized);
}
TEST_F(SsdPeerDirectoryTest, DirectoryPermissionChangesInvalidateTrust) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    ASSERT_EQ(chmod(path.c_str(), 0755), 0);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "default", "tcp").error,
              DestinationError::kUnauthorized);
}
TEST_F(SsdPeerDirectoryTest, TrailingSlashCannotBypassSymlinkRejection) {
    auto link = path.string() + "-link";
    ASSERT_EQ(symlink(path.c_str(), link.c_str()), 0);
    EXPECT_THROW(SsdPeerDirectory{link + "/"}, std::invalid_argument);
    std::filesystem::remove(link);
}
}  // namespace
}  // namespace mooncake::ssd_offload
