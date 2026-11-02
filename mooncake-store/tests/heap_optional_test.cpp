#include "common/heap_optional.h"

#include <string>
#include <utility>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

struct Task {
    int id{0};
    std::string owner;
};

static_assert(sizeof(HeapOptional<Task>) == sizeof(void*));

TEST(HeapOptionalTest, StartsEmpty) {
    HeapOptional<Task> task;
    EXPECT_FALSE(task.has_value());
    EXPECT_FALSE(task);
}

TEST(HeapOptionalTest, AssignsEmplacesAndResets) {
    HeapOptional<Task> task;
    task = Task{.id = 1, .owner = "a"};
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->id, 1);

    // Assigning over a value updates it in place.
    const Task* before = &*task;
    task = Task{.id = 2, .owner = "b"};
    EXPECT_EQ(&*task, before);
    EXPECT_EQ((*task).owner, "b");

    task.emplace(3, "c");
    EXPECT_EQ(task->id, 3);

    task.reset();
    EXPECT_FALSE(task.has_value());
}

TEST(HeapOptionalTest, CopiesTheValue) {
    HeapOptional<Task> original;
    original = Task{.id = 1, .owner = "a"};

    HeapOptional<Task> copy(original);
    ASSERT_TRUE(copy.has_value());
    EXPECT_NE(&*copy, &*original);
    copy->owner = "changed";
    EXPECT_EQ(original->owner, "a");

    HeapOptional<Task> empty;
    copy = empty;
    EXPECT_FALSE(copy.has_value());
    EXPECT_TRUE(original.has_value());
}

TEST(HeapOptionalTest, MovesTheAllocation) {
    HeapOptional<Task> original;
    original = Task{.id = 1, .owner = "a"};
    const Task* value = &*original;

    HeapOptional<Task> moved(std::move(original));
    ASSERT_TRUE(moved.has_value());
    EXPECT_EQ(&*moved, value);
}

}  // namespace
}  // namespace mooncake
