#pragma once
#include <cstdint>
#include <type_traits>

namespace iguana {

struct sint32_t {
  using value_type = int32_t;
  int32_t val;
  bool operator==(const sint32_t& other) const { return val == other.val; }
};

inline bool operator==(sint32_t value1, int32_t value2) {
  return value1.val == value2;
}

// for key in std::map
inline bool operator<(const sint32_t& lhs, const sint32_t& rhs) {
  return lhs.val < rhs.val;
}

struct sint64_t {
  using value_type = int64_t;
  int64_t val;
  bool operator==(const sint64_t& other) const { return val == other.val; }
};

inline bool operator==(sint64_t value1, int64_t value2) {
  return value1.val == value2;
}

inline bool operator<(const sint64_t& lhs, const sint64_t& rhs) {
  return lhs.val < rhs.val;
}

struct fixed32_t {
  using value_type = uint32_t;
  uint32_t val;
  bool operator==(const fixed32_t& other) const { return val == other.val; }
};

inline bool operator==(fixed32_t value1, uint32_t value2) {
  return value1.val == value2;
}

inline bool operator<(const fixed32_t& lhs, const fixed32_t& rhs) {
  return lhs.val < rhs.val;
}

struct fixed64_t {
  using value_type = uint64_t;
  uint64_t val;
  bool operator==(const fixed64_t& other) const { return val == other.val; }
};

inline bool operator==(fixed64_t value1, uint64_t value2) {
  return value1.val == value2;
}

inline bool operator<(const fixed64_t& lhs, const fixed64_t& rhs) {
  return lhs.val < rhs.val;
}

struct sfixed32_t {
  using value_type = int32_t;
  int32_t val;
  bool operator==(const sfixed32_t& other) const { return val == other.val; }
};

inline bool operator==(sfixed32_t value1, int32_t value2) {
  return value1.val == value2;
}

inline bool operator<(const sfixed32_t& lhs, const sfixed32_t& rhs) {
  return lhs.val < rhs.val;
}

struct sfixed64_t {
  using value_type = int64_t;
  int64_t val;
  bool operator==(const sfixed64_t& other) const { return val == other.val; }
};

inline bool operator==(sfixed64_t value1, int64_t value2) {
  return value1.val == value2;
}

inline bool operator<(const sfixed64_t& lhs, const sfixed64_t& rhs) {
  return lhs.val < rhs.val;
}

template <typename Type, typename T = std::decay_t<Type>>
constexpr bool is_pb_type_v =
    std::is_same_v<sint32_t, T> || std::is_same_v<sint64_t, T> ||
    std::is_same_v<fixed32_t, T> || std::is_same_v<fixed64_t, T> ||
    std::is_same_v<sfixed32_t, T> || std::is_same_v<sfixed64_t, T>;

}  // namespace iguana

// for key in std::unordered_map
namespace std {
template <>
struct hash<iguana::sint32_t> {
  size_t operator()(const iguana::sint32_t& x) const noexcept {
    return std::hash<int32_t>()(x.val);
  }
};

template <>
struct hash<iguana::sint64_t> {
  size_t operator()(const iguana::sint64_t& x) const noexcept {
    return std::hash<int64_t>()(x.val);
  }
};

template <>
struct hash<iguana::fixed32_t> {
  size_t operator()(const iguana::fixed32_t& x) const noexcept {
    return std::hash<uint32_t>()(x.val);
  }
};

template <>
struct hash<iguana::fixed64_t> {
  size_t operator()(const iguana::fixed64_t& x) const noexcept {
    return std::hash<uint64_t>()(x.val);
  }
};

template <>
struct hash<iguana::sfixed32_t> {
  size_t operator()(const iguana::sfixed32_t& x) const noexcept {
    return std::hash<int32_t>()(x.val);
  }
};

template <>
struct hash<iguana::sfixed64_t> {
  size_t operator()(const iguana::sfixed64_t& x) const noexcept {
    return std::hash<int64_t>()(x.val);
  }
};
}  // namespace std
