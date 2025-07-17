// buffer_allocator_test.cpp
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "allocator.h"

namespace mooncake {

// Test fixture for BufferAllocator tests
class BufferAllocatorTest : public ::testing::Test {
protected:
	void SetUp() override {
		// Initialize glog for logging
		google::InitGoogleLogging("BufferAllocatorTest");
		FLAGS_logtostderr = 1; // Output logs to stderr
	}

	void TearDown() override {
		// Cleanup glog
		google::ShutdownGoogleLogging();
	}
};

// Test basic allocation and deallocation functionality
TEST_F(BufferAllocatorTest, AllocateAndDeallocate) {
	std::string segment_name = "1";
	const size_t base = 0x100000000;
	const size_t size = 1024 * 1024 * 16; // 16MB (must be multiple of 4MB)

	auto allocator = std::make_shared<BufferAllocator>(segment_name, base, size);

	// Allocate memory block
	size_t alloc_size = 1024;
	auto bufHandle = allocator->allocate(alloc_size);
	auto descriptor = bufHandle->get_descriptor();
	// Verify allocation success and properties
	ASSERT_NE(bufHandle, nullptr);
	EXPECT_EQ(descriptor.segment_name_, segment_name);
	EXPECT_EQ(descriptor.size_, alloc_size);
	EXPECT_EQ(descriptor.status_, BufStatus::INIT);

	// Release memory
	bufHandle.reset();
}

// Test multiple allocations within the buffer
TEST_F(BufferAllocatorTest, AllocateMultiple) {
	std::string segment_name = "1";
	const size_t base = 0x200000000;
	const size_t size = 1024 * 1024 * 16; // 16MB (must be multiple of 4MB)

	auto allocator = std::make_shared<BufferAllocator>(segment_name, base, size);

	// Allocate multiple memory blocks
	size_t alloc_size = 1024 * 1024; // 1MB per block
	std::vector<std::unique_ptr<AllocatedBuffer>> handles;

	// Attempt to allocate 8 blocks (should succeed as total size is less than
	// buffer size)
	for (int i = 0; i < 8; ++i) {
		auto bufHandle = allocator->allocate(alloc_size);
		ASSERT_NE(bufHandle, nullptr);
		handles.push_back(std::move(bufHandle));
	}

	// Clean up allocated memory
	handles.clear();
	LOG(INFO) << "Cleaned up handles in AllocateMultiple test";
}

// Test allocation request larger than available space
TEST_F(BufferAllocatorTest, AllocateTooLarge) {
	std::string segment_name = "3";
	const size_t base = 0x300000000;
	const size_t size = 1024 * 1024 * 16; // 16MB (must be multiple of 4MB)

	auto allocator = std::make_shared<BufferAllocator>(segment_name, base, size);

	// Attempt to allocate more than total buffer size
	size_t alloc_size = size + 1;
	auto bufHandle = allocator->allocate(alloc_size);
	EXPECT_EQ(bufHandle, nullptr);
}

// Test fixture for SimpleAllocator tests
class SimpleAllocatorTest : public ::testing::Test {
protected:
	void SetUp() override {
		google::InitGoogleLogging("SimpleAllocatorTest");
		FLAGS_logtostderr = 1;
	}

	void TearDown() override {
		google::ShutdownGoogleLogging();
	}
};

// Test basic memory allocation and deallocation
TEST_F(SimpleAllocatorTest, BasicAllocationAndDeallocation) {
	const size_t total_size = 1024 * 1024 * 16; // 16MB (multiple of 4MB)
	SimpleAllocator allocator(total_size);

	// Test basic allocation
	size_t alloc_size = 1024; // 1KB
	void *ptr = allocator.allocate(alloc_size);
	ASSERT_NE(ptr, nullptr);

	// Verify memory alignment
	EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0) << "Memory not 8-byte aligned";

	// Verify memory is usable
	std::memset(ptr, 0xFF, alloc_size);

	// Clean up
	allocator.deallocate(ptr, alloc_size);
}

// Test multiple allocations and deallocations
TEST_F(SimpleAllocatorTest, MultipleAllocations) {
	const size_t total_size = 1024 * 1024 * 16; // 16MB
	SimpleAllocator allocator(total_size);

	std::vector<std::pair<void *, size_t>> allocations;
	const size_t alloc_size = 1024 * 1024; // 1MB per block

	// Allocate multiple blocks
	for (int i = 0; i < 8; ++i) {
		void *ptr = allocator.allocate(alloc_size);
		ASSERT_NE(ptr, nullptr) << "Failed to allocate block " << i;
		allocations.emplace_back(ptr, alloc_size);
	}

	// Verify and deallocate all blocks
	for (const auto &[ptr, size] : allocations) {
		EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0) << "Memory not 8-byte aligned";
		allocator.deallocate(ptr, size);
	}
}

// Test allocation request larger than available space
TEST_F(SimpleAllocatorTest, AllocationTooLarge) {
	const size_t total_size = 1024 * 1024 * 16; // 16MB
	SimpleAllocator allocator(total_size);

	void *ptr = allocator.allocate(total_size + 1);
	EXPECT_EQ(ptr, nullptr);
}

// Stress test with many small allocations
TEST_F(SimpleAllocatorTest, StressTest) {
	const size_t total_size = 1024 * 1024 * 256; // 256MB for stress testing
	SimpleAllocator allocator(total_size);

	std::vector<std::pair<void *, size_t>> allocations;
	const size_t num_allocations = 100;

	// Perform multiple allocations of varying sizes
	for (size_t i = 0; i < num_allocations; ++i) {
		size_t size = 1024 * (1 + (i % 10)); // Vary between 1KB and 10KB
		void *ptr = allocator.allocate(size);
		if (ptr) {
			EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0) << "Memory not 8-byte aligned";
			allocations.emplace_back(ptr, size);
		}
	}

	// Clean up all allocations in reverse order
	while (!allocations.empty()) {
		auto [ptr, size] = allocations.back();
		allocator.deallocate(ptr, size);
		allocations.pop_back();
	}
}

} // namespace mooncake

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
