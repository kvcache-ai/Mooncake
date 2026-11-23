// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_source.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <future>
#include <sys/stat.h>
#include <memory>
#include <vector>

namespace mooncake::ssd_offload {
namespace {

class SourceFile {
   public:
    SourceFile() {
        char name[] = "/tmp/mooncake-ssd-source-XXXXXX";
        fd = mkstemp(name);
        path = name;
        bytes.resize(12017);
        for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = std::byte(i % 251);
        if (fd >= 0) written = write(fd, bytes.data(), bytes.size());
    }
    ~SourceFile() {
        if (fd >= 0) close(fd);
        unlink(path.c_str());
    }
    int fd{-1};
    ssize_t written{-1};
    std::string path;
    std::vector<std::byte> bytes;
};

TEST(SsdReadSource, ReadsRealFileUsingImmutableValueMetadata) {
    SourceFile file;
    ASSERT_GE(file.fd, 0);
    ASSERT_EQ(file.written, static_cast<ssize_t>(file.bytes.size()));
    auto result = OpenPosixReadSource(file.path, {12000, 17, 1});
    ASSERT_EQ(result.error, Error::kOk);
    ASSERT_TRUE(result.source);
    EXPECT_EQ(result.source->info().object_bytes, 12000u);
    EXPECT_EQ(result.source->info().value_file_offset, 17u);
    std::array<std::byte, 257> buffer{};
    auto read = result.source->ReadAt(buffer, 617);
    ASSERT_EQ(read.error, Error::kOk);
    ASSERT_EQ(read.bytes, buffer.size());
    EXPECT_EQ(
        std::memcmp(buffer.data(), file.bytes.data() + 617, buffer.size()), 0);
}

TEST(SsdReadSource, AcceptsEofPaddingButReportsActualBytes) {
    SourceFile file;
    auto result = OpenPosixReadSource(file.path, {12000, 17, 4096});
    ASSERT_EQ(result.error, Error::kOk);
    std::array<std::byte, 32> buffer{};
    auto read = result.source->ReadAt(buffer, file.bytes.size() - 5);
    EXPECT_EQ(read.error, Error::kOk);
    EXPECT_EQ(read.bytes, 5u);
    EXPECT_EQ(std::memcmp(buffer.data(),
                          file.bytes.data() + file.bytes.size() - 5, 5),
              0);
}

TEST(SsdReadSource, RejectsInvalidMetadataAndUnsupportedFiles) {
    SourceFile file;
    EXPECT_EQ(OpenPosixReadSource(file.path, {12001, 17, 1}).error,
              Error::kInvalidArgument);
    EXPECT_EQ(OpenPosixReadSource(file.path, {1, UINT64_MAX, 1}).error,
              Error::kOverflow);
    EXPECT_EQ(OpenPosixReadSource(file.path, {1, 0, 3}).error,
              Error::kInvalidArgument);
    const auto directory = file.path + ".directory";
    std::filesystem::create_directory(directory);
    EXPECT_EQ(OpenPosixReadSource(directory, {1, 0, 1}).error,
              Error::kInvalidArgument);
    std::filesystem::remove(directory);
    EXPECT_EQ(OpenPosixReadSource(file.path + ".missing", {1, 0, 1}).error,
              Error::kIoError);
}

TEST(SsdReadSource, PinsFileAndLifetimeAfterUnlink) {
    SourceFile file;
    auto destroyed = std::make_shared<std::atomic<bool>>(false);
    auto lifetime = std::shared_ptr<void>(new int(0), [destroyed](void* value) {
        delete static_cast<int*>(value);
        destroyed->store(true);
    });
    auto result = OpenPosixReadSource(file.path, {12000, 17, 1}, lifetime);
    ASSERT_EQ(result.error, Error::kOk);
    lifetime.reset();
    ASSERT_EQ(unlink(file.path.c_str()), 0);
    EXPECT_FALSE(destroyed->load());
    std::array<std::byte, 32> buffer{};
    EXPECT_EQ(result.source->ReadAt(buffer, 17).bytes, buffer.size());
    EXPECT_EQ(std::memcmp(buffer.data(), file.bytes.data() + 17, buffer.size()),
              0);
    result.source.reset();
    EXPECT_TRUE(destroyed->load());
}

TEST(SsdReadSource, ChecksSignedOffsetsBeforeCallingPosix) {
    SourceFile file;
    auto result = OpenPosixReadSource(file.path, {12000, 17, 1});
    ASSERT_EQ(result.error, Error::kOk);
    std::array<std::byte, 32> buffer{};
    EXPECT_EQ(result.source->ReadAt(buffer, UINT64_MAX).error,
              Error::kOverflow);
    EXPECT_EQ(result.source->ReadAt(buffer, INT64_MAX).error, Error::kOverflow);
    EXPECT_EQ(result.source->ReadAt({}, 0).bytes, 0u);
}

TEST(SsdReadSource, RejectsFifoWithoutWaitingForAWriter) {
    SourceFile file;
    const auto fifo = file.path + ".fifo";
    ASSERT_EQ(mkfifo(fifo.c_str(), 0600), 0);
    auto opening = std::async(std::launch::async, [&] {
        return OpenPosixReadSource(fifo, {0, 0, 1});
    });
    const auto status = opening.wait_for(std::chrono::milliseconds(200));
    EXPECT_EQ(status, std::future_status::ready);
    // Unblock the old blocking-open implementation so RED cannot hang.
    if (status != std::future_status::ready) {
        const int writer = open(fifo.c_str(), O_WRONLY);
        if (writer >= 0) close(writer);
    }
    EXPECT_EQ(opening.get().error, Error::kInvalidArgument);
    unlink(fifo.c_str());
}
}  // namespace
}  // namespace mooncake::ssd_offload
