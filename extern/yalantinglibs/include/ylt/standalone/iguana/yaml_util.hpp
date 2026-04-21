#pragma once

#include "common.hpp"
#include "detail/pb_type.hpp"
#include "util.hpp"

namespace iguana {

template <typename T>
constexpr inline bool yaml_not_support_v = variant_v<T>;

// skip non-linebreak whitespaces return true when it==end
template <typename It>
IGUANA_INLINE bool skip_space_till_end(It &&it, It &&end) {
  while (it != end && (*it == ' ' || *it == '\t')) ++it;
  return it == end;
}

// will not skip  '\n'
template <typename It>
IGUANA_INLINE auto skip_till_newline(It &&it, It &&end) {
  if (it == end)
    IGUANA_UNLIKELY { return it; }
  std::decay_t<decltype(it)> res = it;
  while ((it != end) && (*it != '\n')) {
    if (*it == ' ')
      IGUANA_UNLIKELY {
        res = it;
        while (it != end && *it == ' ') ++it;
      }
    else if (*it == '#')
      IGUANA_UNLIKELY {
        if (*(it - 1) == ' ') {
          // it - 1 should be legal because this func is for parse value
          while ((it != end) && *it != '\n') {
            ++it;
          }
          return res;
        }
        else {
          ++it;
        }
      }
    else
      IGUANA_LIKELY { ++it; }
  }
  return (*(it - 1) == ' ') ? res : it;
}

template <char... C, typename It>
IGUANA_INLINE auto yaml_skip_till(It &&it, It &&end) {
  if (it == end)
    IGUANA_UNLIKELY { return it; }
  std::decay_t<decltype(it)> res = it;
  while ((it != end) && (!((... || (*it == C))))) {
    if (*it == '\n')
      IGUANA_UNLIKELY { throw std::runtime_error("\\n is not expected"); }
    else if (*it == ' ')
      IGUANA_UNLIKELY {
        res = it;
        while (it != end && *it == ' ') ++it;
      }
    else if (*it == '#')
      IGUANA_UNLIKELY {
        if (*(it - 1) == ' ')
          IGUANA_UNLIKELY {
            // it - 1 may be illegal
            while ((it != end) && *it != '\n') {
              ++it;
            }
            return res;
          }
      }
    else
      IGUANA_LIKELY { ++it; }
  }

  if (it == end)
    IGUANA_UNLIKELY {
      static constexpr char b[] = {C..., '\0'};
      std::string error = std::string("Expected one of these: ").append(b);
      throw std::runtime_error(error);
    }
  ++it;  // skip
  return (*(it - 2) == ' ') ? res : it - 1;
}

// If there are '\n' , return indentation
// If not, return minspaces + space
// If Throw == true, check  res < minspaces
template <bool Throw = true, typename It>
IGUANA_INLINE size_t skip_space_and_lines(It &&it, It &&end, size_t minspaces) {
  size_t res = minspaces;
  while (it != end) {
    if (*it == '\n') {
      ++it;
      res = 0;
      auto start = it;
      // skip the --- line
      if ((it != end) && (*it == '-'))
        IGUANA_UNLIKELY {
          auto line_end = yaml_skip_till<false, '\n'>(it, end);
          auto line = std::string_view(
              &*start, static_cast<size_t>(std::distance(start, line_end)));
          if (line != "---") {
            it = start;
          }
        }
    }
    else if (*it == ' ' || *it == '\t') {
      ++it;
      ++res;
    }
    else if (*it == '#') {
      while (it != end && *it != '\n') {
        ++it;
      }
      res = 0;
    }
    else {
      if constexpr (Throw) {
        if (res < minspaces)
          IGUANA_UNLIKELY { throw std::runtime_error("Indentation problem"); }
      }
      return res;
    }
  }
  return res;  // throw in certain situations ?
}

}  // namespace iguana
