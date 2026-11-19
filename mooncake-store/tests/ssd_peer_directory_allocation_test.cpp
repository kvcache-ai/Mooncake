// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated allocation faults; production code has no test hooks.
#include "ssd_peer_directory.h"
#include <gtest/gtest.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <new>

namespace {
thread_local bool fail_next_allocation = false;
thread_local bool fail_after_publish = false;
thread_local bool published = false;
size_t OpenFiles() {
    size_t count = 0;
    for (int fd = 0; fd < 1024; ++fd)
        if (fcntl(fd, F_GETFD) != -1) ++count;
    return count;
}
}  // namespace
void* operator new(size_t bytes) {
    if (fail_next_allocation) {
        fail_next_allocation = false;
        throw std::bad_alloc();
    }
    if (void* result = std::malloc(bytes ? bytes : 1)) return result;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { std::free(pointer); }
// GCC's ASan supplies its own nothrow allocator. Route the complete ordinary
// allocation family through the same fault allocator and matching free path.
void* operator new(size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t bytes, const std::nothrow_t&) noexcept {
    return ::operator new(bytes, std::nothrow);
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
extern "C" int linkat(int old_directory, const char* old_name,
                      int new_directory, const char* new_name, int flags)
#ifdef __linux__
    noexcept
#endif
{
    using Link = int (*)(int, const char*, int, const char*, int);
    static auto real = reinterpret_cast<Link>(dlsym(RTLD_NEXT, "linkat"));
    if (!real) std::abort();
    const auto result =
        real(old_directory, old_name, new_directory, new_name, flags);
    if (!result && fail_after_publish) {
        published = true;
        fail_next_allocation = true;
    }
    return result;
}

namespace mooncake::ssd_offload {
namespace {
class SsdPeerDirectoryAllocationTest : public ::testing::Test {
   protected:
    std::filesystem::path path;
    void SetUp() override {
        char name[] = "/tmp/mooncake-ssd-peer-alloc-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        path = name;
        ASSERT_EQ(chmod(path.c_str(), 0700), 0);
    }
    void TearDown() override {
        fail_next_allocation = fail_after_publish = false;
        std::filesystem::remove_all(path);
    }
};
TEST_F(SsdPeerDirectoryAllocationTest, PublishedRecordOwnershipCannotAllocate) {
    SsdPeerDirectory directory(path);
    SsdFixedL2Peer fixed{{1, 2, 3}, "default", "requester-te", {7, 8}};
    SsdPeerDirectoryEntry entry{
        fixed,
        {fixed.identity,
         "default",
         "owner:100",
         "127.0.0.1",
         1200,
         "requester-te",
         {1, fixed.identity, 15, 1, 1024, 1, 1024, 1024, {"tcp"}}},
        "tcp",
        true};
    bool threw = false;
    DestinationError result = DestinationError::kInvalidArgument;
    published = false;
    fail_after_publish = true;
    try {
        result = directory.Publish(entry);
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    fail_next_allocation = fail_after_publish = false;
    ASSERT_TRUE(published);
    EXPECT_FALSE(threw);
    EXPECT_EQ(result, DestinationError::kOk);
    EXPECT_EQ(directory.Unpublish(), DestinationError::kOk);
    EXPECT_TRUE(std::filesystem::is_empty(path));
}
TEST_F(SsdPeerDirectoryAllocationTest,
       ConstructorAllocationFailureClosesAllFds) {
    std::string value = path.string();
    const auto before = OpenFiles();
    bool threw = false;
    fail_next_allocation = true;
    try {
        SsdPeerDirectory directory(std::move(value));
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    fail_next_allocation = false;
    ASSERT_TRUE(threw);
    EXPECT_EQ(OpenFiles(), before);
}
}  // namespace
}  // namespace mooncake::ssd_offload
