#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <iostream>
#include <string>

namespace my_space {
struct my_struct {
  int x, y, z;
  bool operator==(const my_struct& o) const {
    return x == o.x && y == o.y && z == o.z;
  }
};

void ylt_custom_reflect(my_struct*) {}

template <bool Is_writing_escape, typename Stream>
inline void to_json_impl(Stream& s, const my_struct& t) {
  struct_json::to_json<Is_writing_escape>(*(int(*)[3]) & t, s);
}

template <typename It>
IGUANA_INLINE void from_json_impl(my_struct& value, It&& it, It&& end) {
  struct_json::from_json(*(int(*)[3]) & value, it, end);
}

}  // namespace my_space

struct nest {
  std::string name;
  my_space::my_struct value;
  bool operator==(const nest& o) const {
    return name == o.name && value == o.value;
  }
};

YLT_REFL(nest, name, value);

void example1() {
  my_space::my_struct v{1, 2, 3}, v2;
  std::string s;
  struct_json::to_json(v, s);
  std::cout << s << std::endl;
  struct_json::from_json(v2, s);
  assert(v == v2);
};

void example2() {
  nest v{"Hi", {1, 2, 3}}, v2;
  std::string s;
  struct_json::to_json(v, s);
  std::cout << s << std::endl;
  struct_json::from_json(v2, s);
  assert(v == v2);
};

void test_user_defined_struct() {
  example1();
  example2();
}