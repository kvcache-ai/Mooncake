#include "common/intrusive_list.h"

#include <deque>
#include <iterator>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

struct FirstTag;
struct SecondTag;

// Derives once per tag, so one element can be on a list of each at once.
struct Node : IntrusiveListHook<FirstTag>, IntrusiveListHook<SecondTag> {
    explicit Node(int v) : value(v) {}
    int value;
};

using FirstList = IntrusiveList<Node, FirstTag>;
using SecondList = IntrusiveList<Node, SecondTag>;

// Joins only through a private hook, which the list reaches as a friend.
class PrivateNode : private IntrusiveListHook<> {
   public:
    explicit PrivateNode(int v) : value(v) {}
    int value;

   private:
    friend class IntrusiveList<PrivateNode>;
};

template <typename List>
std::vector<int> Values(const List& list) {
    std::vector<int> values;
    for (const auto& node : list) {
        values.push_back(node.value);
    }
    return values;
}

TEST(IntrusiveListTest, KeepsInsertionOrderAtBothEnds) {
    Node a(1), b(2), c(3);
    FirstList list;
    EXPECT_TRUE(list.Empty());

    list.PushBack(b);
    list.PushBack(c);
    list.PushFront(a);

    EXPECT_EQ(list.Size(), 3u);
    EXPECT_EQ(Values(list), (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(list.Front().value, 1);
    EXPECT_EQ(list.Back().value, 3);
    list.Clear();
}

TEST(IntrusiveListTest, EraseUnlinksOnlyThatElement) {
    Node a(1), b(2), c(3);
    FirstList list;
    list.PushBack(a);
    list.PushBack(b);
    list.PushBack(c);

    list.Erase(b);

    EXPECT_FALSE(FirstList::IsLinked(b));
    EXPECT_TRUE(FirstList::IsLinked(a));
    EXPECT_EQ(Values(list), (std::vector<int>{1, 3}));

    // An unlinked element can join again.
    list.PushFront(b);
    EXPECT_EQ(Values(list), (std::vector<int>{2, 1, 3}));
    list.Clear();
}

TEST(IntrusiveListTest, PopsFromBothEnds) {
    Node a(1), b(2), c(3);
    FirstList list;
    list.PushBack(a);
    list.PushBack(b);
    list.PushBack(c);

    list.PopFront();
    EXPECT_FALSE(FirstList::IsLinked(a));
    list.PopBack();
    EXPECT_FALSE(FirstList::IsLinked(c));
    EXPECT_EQ(Values(list), (std::vector<int>{2}));
    list.PopFront();
    EXPECT_TRUE(list.Empty());
}

TEST(IntrusiveListTest, MoveToBackReordersInPlace) {
    Node a(1), b(2), c(3);
    FirstList list;
    list.PushBack(a);
    list.PushBack(b);
    list.PushBack(c);

    list.MoveToBack(a);
    EXPECT_EQ(Values(list), (std::vector<int>{2, 3, 1}));
    // Already at the back: the order stays.
    list.MoveToBack(a);
    EXPECT_EQ(Values(list), (std::vector<int>{2, 3, 1}));
    EXPECT_EQ(list.Size(), 3u);
    list.Clear();
}

TEST(IntrusiveListTest, AWalkCanEraseTheElementItJustLeft) {
    // A deque never moves what it holds, and a hooked element cannot move.
    std::deque<Node> nodes;
    for (int i = 0; i < 6; ++i) {
        nodes.emplace_back(i);
    }
    FirstList list;
    for (auto& node : nodes) {
        list.PushBack(node);
    }

    for (auto it = list.begin(); it != list.end();) {
        Node& node = *it++;
        if (node.value % 2 == 1) {
            list.Erase(node);
        }
    }

    EXPECT_EQ(Values(list), (std::vector<int>{0, 2, 4}));
    list.Clear();
}

TEST(IntrusiveListTest, IteratesBackwards) {
    Node a(1), b(2), c(3);
    FirstList list;
    list.PushBack(a);
    list.PushBack(b);
    list.PushBack(c);

    std::vector<int> values;
    for (auto it = std::make_reverse_iterator(list.end());
         it != std::make_reverse_iterator(list.begin()); ++it) {
        values.push_back(it->value);
    }
    EXPECT_EQ(values, (std::vector<int>{3, 2, 1}));

    // A mutable iterator converts to a const one.
    FirstList::const_iterator first = list.begin();
    EXPECT_EQ(first->value, 1);
    list.Clear();
}

TEST(IntrusiveListTest, ClearAndDestructionReleaseEveryElement) {
    Node a(1), b(2);
    {
        FirstList list;
        list.PushBack(a);
        list.PushBack(b);
        list.Clear();
        EXPECT_TRUE(list.Empty());
        EXPECT_FALSE(FirstList::IsLinked(a));

        list.PushBack(a);
    }
    // The destroyed list let go of its element, so it can join another.
    EXPECT_FALSE(FirstList::IsLinked(a));
    FirstList other;
    other.PushBack(a);
    EXPECT_EQ(Values(other), (std::vector<int>{1}));
    other.Clear();
}

TEST(IntrusiveListTest, HooksOfDifferentTagsAreIndependent) {
    Node a(1), b(2), c(3);
    FirstList first;
    SecondList second;
    first.PushBack(a);
    first.PushBack(b);
    second.PushBack(b);
    second.PushBack(c);

    first.Erase(b);

    // Leaving one list does not touch the element's place on the other.
    EXPECT_FALSE(FirstList::IsLinked(b));
    EXPECT_TRUE(SecondList::IsLinked(b));
    EXPECT_EQ(Values(first), (std::vector<int>{1}));
    EXPECT_EQ(Values(second), (std::vector<int>{2, 3}));
    first.Clear();
    second.Clear();
}

TEST(IntrusiveListTest, APrivateHookWorksThroughTheFriendList) {
    PrivateNode a(1), b(2);
    IntrusiveList<PrivateNode> list;
    list.PushBack(a);
    list.PushBack(b);

    EXPECT_EQ(Values(list), (std::vector<int>{1, 2}));
    EXPECT_TRUE(IntrusiveList<PrivateNode>::IsLinked(b));
    list.Erase(a);
    EXPECT_EQ(list.Front().value, 2);
    list.Clear();
}

}  // namespace
}  // namespace mooncake
