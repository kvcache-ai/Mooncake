#pragma once

// IntrusiveList: a doubly linked list threaded through its elements, so
// linking allocates nothing and unlinking a known element is O(1). A type
// takes part by deriving from IntrusiveListHook<Tag>, once for every list it
// can be on at the same time; the tag tells those hooks apart. A type that
// derives privately befriends IntrusiveList<T, Tag>, which is the only code
// that converts between an element and its hook.
//
// The list never owns its elements, and neither the list nor the hook
// synchronizes: whatever guards a list also guards every hook linked into it.
// An element is unlinked before it is destroyed, which the hook asserts; a
// list unlinks whatever it still holds when it is cleared or destroyed.

#include <cassert>
#include <cstddef>
#include <iterator>
#include <type_traits>

namespace mooncake {

template <typename T, typename Tag = void>
class IntrusiveList;

template <typename Tag = void>
class IntrusiveListHook {
   public:
    IntrusiveListHook() noexcept = default;
    // A hook's address is what its neighbours point at, so it never moves.
    IntrusiveListHook(const IntrusiveListHook&) = delete;
    IntrusiveListHook& operator=(const IntrusiveListHook&) = delete;
    ~IntrusiveListHook() {
        assert(next_ == nullptr && "element destroyed while still linked");
    }

   private:
    template <typename, typename>
    friend class IntrusiveList;

    // Both null while unlinked.
    IntrusiveListHook* prev_{nullptr};
    IntrusiveListHook* next_{nullptr};
};

template <typename T, typename Tag>
class IntrusiveList {
    using Hook = IntrusiveListHook<Tag>;

    template <bool kConst>
    class Iterator {
       public:
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<kConst, const T*, T*>;
        using reference = std::conditional_t<kConst, const T&, T&>;

        Iterator() noexcept = default;
        // A mutable iterator converts to a const one, never the reverse.
        template <bool kOther, typename = std::enable_if_t<kConst && !kOther>>
        Iterator(const Iterator<kOther>& other) noexcept : hook_(other.hook_) {}

        reference operator*() const { return IntrusiveList::ElementOf(*hook_); }
        pointer operator->() const { return &**this; }
        Iterator& operator++() noexcept {
            hook_ = hook_->next_;
            return *this;
        }
        Iterator operator++(int) noexcept {
            Iterator before = *this;
            ++*this;
            return before;
        }
        Iterator& operator--() noexcept {
            hook_ = hook_->prev_;
            return *this;
        }
        Iterator operator--(int) noexcept {
            Iterator before = *this;
            --*this;
            return before;
        }
        friend bool operator==(const Iterator& a, const Iterator& b) noexcept {
            return a.hook_ == b.hook_;
        }
        friend bool operator!=(const Iterator& a, const Iterator& b) noexcept {
            return a.hook_ != b.hook_;
        }

       private:
        friend class IntrusiveList;
        template <bool>
        friend class Iterator;

        using HookPtr = std::conditional_t<kConst, const Hook*, Hook*>;
        explicit Iterator(HookPtr hook) noexcept : hook_(hook) {}

        HookPtr hook_{nullptr};
    };

   public:
    using iterator = Iterator<false>;
    using const_iterator = Iterator<true>;

    // The list is circular through `head_`, so linking and unlinking never
    // special-case the ends.
    IntrusiveList() noexcept { head_.prev_ = head_.next_ = &head_; }
    // Elements point at `head_`, so the list neither copies nor moves.
    IntrusiveList(const IntrusiveList&) = delete;
    IntrusiveList& operator=(const IntrusiveList&) = delete;
    ~IntrusiveList() {
        Clear();
        head_.prev_ = head_.next_ = nullptr;
    }

    [[nodiscard]] bool Empty() const noexcept { return size_ == 0; }
    [[nodiscard]] size_t Size() const noexcept { return size_; }

    // True while `element` is linked through this tag's hook. It does not say
    // into which list, so a type on several lists of one tag tracks that
    // itself.
    [[nodiscard]] static bool IsLinked(const T& element) noexcept {
        return HookOf(element).next_ != nullptr;
    }

    // Links an element that is on no list of this tag.
    void PushBack(T& element) noexcept { LinkBefore(head_, HookOf(element)); }
    void PushFront(T& element) noexcept {
        LinkBefore(*head_.next_, HookOf(element));
    }

    // Unlinks an element of this list. Only iterators to it are invalidated,
    // so a walk that erases as it goes advances before erasing.
    void Erase(T& element) noexcept { Unlink(HookOf(element)); }

    // Moves an element of this list to the back, as an LRU touch does.
    void MoveToBack(T& element) noexcept {
        Hook& hook = HookOf(element);
        Unlink(hook);
        LinkBefore(head_, hook);
    }

    [[nodiscard]] T& Front() noexcept {
        assert(!Empty());
        return ElementOf(*head_.next_);
    }
    [[nodiscard]] const T& Front() const noexcept {
        assert(!Empty());
        return ElementOf(*head_.next_);
    }
    [[nodiscard]] T& Back() noexcept {
        assert(!Empty());
        return ElementOf(*head_.prev_);
    }
    [[nodiscard]] const T& Back() const noexcept {
        assert(!Empty());
        return ElementOf(*head_.prev_);
    }

    void PopFront() noexcept { Erase(Front()); }
    void PopBack() noexcept { Erase(Back()); }

    // Unlinks every element, leaving each one free to join another list.
    void Clear() noexcept {
        while (!Empty()) {
            PopFront();
        }
    }

    [[nodiscard]] iterator begin() noexcept { return iterator(head_.next_); }
    [[nodiscard]] iterator end() noexcept { return iterator(&head_); }
    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator(head_.next_);
    }
    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator(&head_);
    }

   private:
    // The element and its hook are the same object seen through its base, so
    // converting between them is a static_cast. The check sits here rather
    // than on the class, so a list can be declared before T is complete.
    static Hook& HookOf(T& element) noexcept {
        static_assert(std::is_base_of_v<Hook, T>,
                      "T must derive from IntrusiveListHook<Tag>");
        return static_cast<Hook&>(element);
    }
    static const Hook& HookOf(const T& element) noexcept {
        static_assert(std::is_base_of_v<Hook, T>,
                      "T must derive from IntrusiveListHook<Tag>");
        return static_cast<const Hook&>(element);
    }
    static T& ElementOf(Hook& hook) noexcept { return static_cast<T&>(hook); }
    static const T& ElementOf(const Hook& hook) noexcept {
        return static_cast<const T&>(hook);
    }

    void LinkBefore(Hook& next, Hook& hook) noexcept {
        assert(hook.next_ == nullptr && "element is already linked");
        hook.prev_ = next.prev_;
        hook.next_ = &next;
        next.prev_->next_ = &hook;
        next.prev_ = &hook;
        ++size_;
    }

    void Unlink(Hook& hook) noexcept {
        assert(hook.next_ != nullptr && "element is not linked");
        hook.prev_->next_ = hook.next_;
        hook.next_->prev_ = hook.prev_;
        hook.prev_ = hook.next_ = nullptr;
        --size_;
    }

    Hook head_;
    size_t size_{0};
};

}  // namespace mooncake
