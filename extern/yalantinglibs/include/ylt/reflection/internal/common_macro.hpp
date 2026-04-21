#pragma once
#define YLT_CONCAT_(l, r) l##r

#define YLT_CONCAT(l, r) YLT_CONCAT_(l, r)

#define CONCAT_MEMBER(t, x) t.x

#define CONCAT_ADDR(T, x) &T::x

#define CONCAT_NAME(t, x) #x

namespace ylt::reflection {
template <typename T>
struct identity {};
}  // namespace ylt::reflection