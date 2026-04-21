#pragma once
#include <cstddef>
#include <string>
#include <utility>

namespace cinatra::detail {

#if __cpp_lib_string_resize_and_overwrite >= 202110L
template <typename ch>
inline void resize(std::basic_string<ch> &str, std::size_t sz) {
  str.resize_and_overwrite(sz, [sz](ch *, std::size_t) {
    return sz;
  });
}
#elif (defined(_MSC_VER) && _MSC_VER <= 1920)
// old msvc don't support visit private, discard it.

#else

template <typename Function, Function func_ptr>
class string_thief {
 public:
  friend void string_set_length_hacker(std::string &self, std::size_t sz) {
#if defined(_MSVC_STL_VERSION)
    (self.*func_ptr)._Myval2._Mysize = sz;
#else
#if defined(_LIBCPP_VERSION)
    (self.*func_ptr)(sz);
#else
#if (_GLIBCXX_USE_CXX11_ABI == 0) && defined(__GLIBCXX__)
    (self.*func_ptr)()->_M_set_length_and_sharable(sz);
#else
#if defined(__GLIBCXX__)
    (self.*func_ptr)(sz);
#endif
#endif
#endif
#endif
  }
};

#if defined(__GLIBCXX__)  // libstdc++
#if (_GLIBCXX_USE_CXX11_ABI == 0)
template class string_thief<decltype(&std::string::_M_rep),
                            &std::string::_M_rep>;
#else
template class string_thief<decltype(&std::string::_M_set_length),
                            &std::string::_M_set_length>;
#endif
#elif defined(_LIBCPP_VERSION)
template class string_thief<decltype(&std::string::__set_size),
                            &std::string::__set_size>;
#elif defined(_MSVC_STL_VERSION)
template class string_thief<decltype(&std::string::_Mypair),
                            &std::string::_Mypair>;
#endif

void string_set_length_hacker(std::string &, std::size_t);

template <typename ch>
inline void resize(std::basic_string<ch> &raw_str, std::size_t sz) {
  std::string &str = *reinterpret_cast<std::string *>(&raw_str);
#if defined(__GLIBCXX__) || defined(_LIBCPP_VERSION) || \
    defined(_MSVC_STL_VERSION)
  if (sz > str.capacity()) {
    str.reserve(sz);
  }
  string_set_length_hacker(str, sz);
  str[sz] = '\0';
#else
  raw_str.resize(sz);
#endif
}

#endif

#if (defined(_MSC_VER) && _MSC_VER <= 1920)
#else
void vector_set_length_hacker(std::vector<char> &self, std::size_t sz);

template <typename Function, Function func_ptr>
class vector_thief {
 public:
  friend void vector_set_length_hacker(std::vector<char> &self,
                                       std::size_t sz) {
#if defined(_MSVC_STL_VERSION)
    (self.*func_ptr)._Myval2._Mylast = self.data() + sz;
#else
#if defined(_LIBCPP_VERSION)
#if _LIBCPP_VERSION < 14000
    ((*(std::__vector_base<char, std::allocator<char> > *)(&self)).*func_ptr) =
        self.data() + sz;
#else
    (self.*func_ptr) = self.data() + sz;
#endif
#else
#if defined(__GLIBCXX__)
    ((*(std::_Vector_base<char, std::allocator<char> > *)(&self)).*func_ptr)
        ._M_finish = self.data() + sz;
#endif
#endif
#endif
  }
};

#if defined(__GLIBCXX__)  // libstdc++
template class vector_thief<decltype(&std::vector<char>::_M_impl),
                            &std::vector<char>::_M_impl>;
#elif defined(_LIBCPP_VERSION)
template class vector_thief<decltype(&std::vector<char>::__end_),
                            &std::vector<char>::__end_>;
#elif defined(_MSVC_STL_VERSION)
template class vector_thief<decltype(&std::vector<char>::_Mypair),
                            &std::vector<char>::_Mypair>;
#endif

template <typename ch>
inline void resize(std::vector<ch> &raw_vec, std::size_t sz) {
#if defined(__GLIBCXX__) ||                                       \
    (defined(_LIBCPP_VERSION) && defined(_LIBCPP_HAS_NO_ASAN)) || \
    defined(_MSVC_STL_VERSION)
  std::vector<char> &vec = *reinterpret_cast<std::vector<char> *>(&raw_vec);
  vec.reserve(sz);
  vector_set_length_hacker(vec, sz);
#else
  raw_vec.resize(sz);
#endif
}
#endif
};  // namespace cinatra::detail