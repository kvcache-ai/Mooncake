#include <gtest/gtest.h>

#include <linux/filter.h>
#include <linux/seccomp.h>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <vector>

#include "common/client_buffer_allocation.h"

namespace mooncake {
namespace {

// False under a seccomp profile that blocks the memory policy syscalls
// (Docker's default without CAP_SYS_NICE).
bool MemoryPolicyPermitted() {
    int mode = 0;
    return get_mempolicy(&mode, nullptr, 0, nullptr, 0) == 0;
}

// Makes get_mempolicy fail with EPERM in this process, as such a profile does.
void BlockGetMempolicy() {
    struct sock_filter filter[] = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_get_mempolicy, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog prog = {sizeof(filter) / sizeof(filter[0]), filter};
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 ||
        prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        _exit(2);
    }
}

// Exit code 0 if, with the allowed nodes unreadable, the nodes are kept.
int KeepsNodesWithGetMempolicyBlocked() {
    BlockGetMempolicy();
    const std::vector<int> nodes = {0, 1};
    return FilterNumaNodesToAllowed(nodes) == nodes ? 0 : 1;
}

class NumaAllowedNodesTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (numa_available() < 0) GTEST_SKIP() << "libnuma not available";
        if (!MemoryPolicyPermitted()) {
            GTEST_SKIP() << "memory policy syscalls are not permitted here";
        }
        struct bitmask* mask = numa_get_mems_allowed();
        for (int node = 0; node <= numa_max_node(); ++node) {
            if (numa_bitmask_isbitset(mask, node)) {
                allowed_.push_back(node);
            } else if (unusable_ < 0 && numa_node_size64(node, nullptr) > 0) {
                unusable_ = node;
            }
        }
        numa_bitmask_free(mask);
        ASSERT_FALSE(allowed_.empty());
        // Every node allowed (unconfined, or a 1-node host): mbind rejects a
        // node with no memory with the same EINVAL as one outside the cpuset.
        if (unusable_ < 0) unusable_ = numa_max_node() + 1;
    }

    static constexpr size_t kRegion = 2 * 1024 * 1024;
    std::vector<int> allowed_;
    int unusable_ = -1;
};

TEST_F(NumaAllowedNodesTest, KeepsExactlyTheAllowedNodesInOrder) {
    std::vector<int> nodes = {-1};
    for (int node = numa_max_node(); node >= 0; --node) nodes.push_back(node);
    nodes.push_back(numa_max_node() + 1);
    nodes.push_back(numa_max_node() + 64);

    std::vector<int> expected(allowed_.rbegin(), allowed_.rend());
    EXPECT_EQ(FilterNumaNodesToAllowed(nodes), expected);
    EXPECT_TRUE(FilterNumaNodesToAllowed({}).empty());
}

// What the filter is for: a NUMA-segmented buffer over a node this process
// cannot use fails, and over the filtered nodes it is allocated.
TEST_F(NumaAllowedNodesTest, SegmentedBufferNeedsTheFilteredNodes) {
    const std::vector<int> nodes = {allowed_.front(), unusable_};
    EXPECT_EQ(allocate_buffer_numa_segments(2 * kRegion, nodes), nullptr);

    const std::vector<int> filtered = FilterNumaNodesToAllowed(nodes);
    ASSERT_EQ(filtered, std::vector<int>{allowed_.front()});
    void* ptr = allocate_buffer_numa_segments(2 * kRegion, filtered);
    ASSERT_NE(ptr, nullptr);
    munmap(ptr, 2 * kRegion);
}

// numactl --membind: binding the segment would override the task policy, so
// its nodes narrow the allowed set like the cpuset does.
TEST_F(NumaAllowedNodesTest, BindTaskPolicyNarrowsTheAllowedNodes) {
    if (allowed_.size() < 2) GTEST_SKIP() << "needs two allowed NUMA nodes";
    const int keep = allowed_[0];
    const int drop = allowed_[1];
    struct bitmask* mask = numa_allocate_nodemask();
    numa_bitmask_setbit(mask, keep);
    ASSERT_EQ(set_mempolicy(MPOL_BIND, mask->maskp, mask->size + 1), 0);
    numa_bitmask_free(mask);
    const std::vector<int> filtered = FilterNumaNodesToAllowed({keep, drop});
    ASSERT_EQ(set_mempolicy(MPOL_DEFAULT, nullptr, 0), 0);

    EXPECT_EQ(filtered, std::vector<int>{keep});
}

TEST(NumaAllowedNodesFallbackTest, KeepsNodesWhenAllowedNodesAreUnreadable) {
    EXPECT_EXIT(_exit(KeepsNodesWithGetMempolicyBlocked()),
                ::testing::ExitedWithCode(0), "");
}

}  // namespace
}  // namespace mooncake
