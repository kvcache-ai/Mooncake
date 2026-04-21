#define STRUCT_PACK_DISABLE_OUTER_BRACE
#include <ylt/struct_pack.hpp>

#include "doctest.h"

using namespace struct_pack;

struct hi {
  struct hi2 {
    std::string a;
  } a;
};
TEST_CASE("test disable outer brace in type info") {
  static_assert(struct_pack::get_type_code<int>() ==
                struct_pack::get_type_code<std::tuple<int>>());
  static_assert(struct_pack::get_type_code<int>() ==
                struct_pack::get_type_code<std::tuple<int, compatible<int>>>());
  constexpr auto str = struct_pack::get_type_literal<hi>();
  constexpr auto str2 = struct_pack::get_type_literal<hi::hi2>();
  static_assert(str.size() == 4);
  static_assert(str2.size() == 2);
  constexpr auto str3 =
      struct_pack::get_type_literal<std::pair<int, std::string>>();
  static_assert(str3.size() == 5);
}