#include <sstream>
#include <utility>

#include "ylt/reflection/member_value.hpp"
#include "ylt/reflection/private_visitor.hpp"
#include "ylt/reflection/template_string.hpp"
#include "ylt/reflection/template_switch.hpp"
#include "ylt/reflection/user_reflect_macro.hpp"

using namespace ylt::reflection;

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest.h"

struct simple {
  int color;
  int id;
  std::string str;
  int age;
};

#if (defined(__GNUC__) && __GNUC__ > 10) || defined(__clang__) || \
    defined(_MSC_VER)
#include "ylt/reflection/member_names.hpp"
#include "ylt/reflection/member_value.hpp"

struct sub {
  int id;
};

enum class Color { red, black };

struct person {
  Color color;
  int id;
  sub s;
  std::string str;
  int arr[2];
};

TEST_CASE("test member names") {
  constexpr size_t size = members_count_v<person>;
  CHECK(size == 5);
  constexpr auto tp = struct_to_tuple<person>();
  constexpr size_t tp_size = std::tuple_size_v<decltype(tp)>;
  CHECK(tp_size == 5);

  constexpr auto arr = get_member_names<person>();
  for (auto name : arr) {
    std::cout << name << ", ";
  }
  std::cout << "\n";
  CHECK(arr ==
        std::array<std::string_view, size>{"color", "id", "s", "str", "arr"});
}

struct point_t {
  int x;
  int y;
};

void test_pointer() {
  simple t{};
  auto&& [o, b, c, d] = t;
  size_t offset1 = (const char*)(&o) - (const char*)(&t);
  size_t offset2 = (const char*)(&b) - (const char*)(&t);
  size_t offset3 = (const char*)(&c) - (const char*)(&t);
  size_t offset4 = (const char*)(&d) - (const char*)(&t);

  const auto& offset_arr = member_offsets<simple>;
  CHECK(offset_arr[0] == offset1);
  CHECK(offset_arr[1] == offset2);
  CHECK(offset_arr[2] == offset3);
  CHECK(offset_arr[3] == offset4);
}

TEST_CASE("test member pointer and offset") { test_pointer(); }

constexpr point_t pt{2, 4};

void test_pt() {
  constexpr size_t index1 = 1;
  constexpr auto y = get<1>(pt);
  static_assert(y == 4);
  CHECK(y == 4);

#if __cplusplus >= 202002L
  constexpr auto x = get<"x">(pt);
  static_assert(x == 2);
#endif
}

TEST_CASE("test member value") {
  simple p{.color = 2, .id = 10, .str = "hello reflection", .age = 6};
  auto ref_tp = object_to_tuple(p);
  constexpr auto arr = get_member_names<simple>();
  std::stringstream out;
  [&]<size_t... Is>(std::index_sequence<Is...>) {
    ((out << "name: " << arr[Is] << ", value: " << std::get<Is>(ref_tp)
          << "\n"),
     ...);
  }
  (std::make_index_sequence<arr.size()>{});

  std::string result = out.str();
  std::cout << out.str();

  std::string expected_str =
      "name: color, value: 2\nname: id, value: 10\nname: str, value: hello "
      "reflection\nname: age, value: 6\n";
  CHECK(result == expected_str);

  constexpr auto map = member_names_map<simple>;
  constexpr size_t index = map.at("age");
  CHECK(index == 3);
  auto age = std::get<index>(ref_tp);
  CHECK(age == 6);

  auto& age1 = get<int>(p, "age");
  CHECK(age1 == 6);

#if __cplusplus >= 202002L
  auto& age2 = get<"age">(p);
  CHECK(age2 == 6);

  auto& var1 = get<"str">(p);
  CHECK(var1 == "hello reflection");
#endif

  test_pt();

  CHECK_THROWS_AS(get<std::string>(p, 3), std::invalid_argument);
  CHECK_THROWS_AS(get<int>(p, 5), std::out_of_range);
  CHECK_THROWS_AS(get<int>(p, "no_such"), std::out_of_range);
  CHECK_THROWS_AS(get<std::string>(p, "age"), std::invalid_argument);

  auto str2 = get<2>(p);
  CHECK(str2 == "hello reflection");

  auto var = get(p, 3);
  CHECK(*std::get<3>(var) == 6);

  auto var2 = get(p, "str");
  CHECK(*std::get<2>(var2) == "hello reflection");
  std::visit(
      [](auto ptr) {
        std::cout << *ptr << "\n";
      },
      var2);

  for_each(p, [](auto& field) {
    std::cout << field << "\n";
  });

  for_each(p, [](auto& field, auto name, auto index) {
    std::cout << field << ", " << name << ", " << index << "\n";
  });

  for_each(p, [](auto& field, auto name) {
    std::cout << field << ", " << name << "\n";
  });

  for_each<simple>([](std::string_view field_name, size_t index) {
    std::cout << index << ", " << field_name << "\n";
  });

  for_each<simple>([](std::string_view field_name) {
    std::cout << field_name << "\n";
  });

  visit_members(p, [](auto&&... args) {
    ((std::cout << args << " "), ...);
    std::cout << "\n";
  });

  constexpr std::string_view name1 = name_of<simple, 2>();
  CHECK(name1 == "str");

  constexpr std::string_view name2 = name_of<simple>(2);
  CHECK(name2 == "str");

#if __cplusplus >= 202002L
  constexpr size_t idx = index_of<simple, "str">();
  CHECK(idx == 2);

  constexpr size_t idx2 = index_of<simple, "no_such">();
  CHECK(idx2 == 4);
#endif

  constexpr size_t idx1 = index_of<simple>("str");
  CHECK(idx1 == 2);

  size_t idx3 = index_of<simple>("no_such");
  CHECK(idx3 == 4);

  size_t idx4 = index_of(p, age1);
  CHECK(idx4 == 3);

  auto name3 = name_of(p, age1);
  CHECK(name3 == "age");

  int no_such = 100;
  size_t idx5 = index_of(p, no_such);
  CHECK(idx5 == 4);
}

struct switch_helper {
  template <size_t Index, typename U, typename T>
  static bool run(U& tp, T&& value) {
    if constexpr (Index > 3) {
      return false;
    }
    else {
      if constexpr (std::is_same_v<
                        std::tuple_element_t<Index, std::remove_cvref_t<U>>,
                        std::remove_cvref_t<T>>) {
        CHECK(std::get<Index>(tp) == value);
      }
      return true;
    }
  }
};

TEST_CASE("test template switch") {
  std::tuple<int, std::string, double, int> tuple(1, "test", 2, 3);
  template_switch<switch_helper>(0, tuple, 1);
  template_switch<switch_helper>(1, tuple, "test");
  template_switch<switch_helper>(2, tuple, 2);
  template_switch<switch_helper>(3, tuple, 3);
  CHECK_FALSE(template_switch<switch_helper>(4, tuple, 100));
}

TEST_CASE("test visitor") {
  simple p{.color = 2, .id = 10, .str = "hello reflection", .age = 6};
  size_t size = visit_members(p, [](auto&&... args) {
    ((std::cout << args << ", "), ...);
    std::cout << "\n";
    return sizeof...(args);
  });
  CHECK(size == 4);
}

#endif

struct dummy_t {
  int id;
  std::string name;
  int age;
  YLT_REFL(dummy_t, id, name, age);
};

struct dummy_t2 {
  int id;
  std::string name;
  int age;
};
YLT_REFL(dummy_t2, id, name, age);

struct dummy_t3 {
  int id;
  std::string name;
  int age;
};

struct dummy_t4 {
  int id;
  std::string name;
  int age;

 public:
  dummy_t4() {}
};
constexpr std::size_t refl_member_count(
    const ylt::reflection::identity<dummy_t4>& t) {
  return 3;
}

struct dummy_t5 {
 private:
  int id = 42;
  std::string name = "tom";
  int age = 20;

 public:
  int& get_id() { return id; }
  std::string& get_name() { return name; }
  int& get_age() { return age; }

  const int& get_id() const { return id; }
  const std::string& get_name() const { return name; }
  const int& get_age() const { return age; }
};
YLT_REFL(dummy_t5, get_id(), get_name(), get_age());

struct simple2 {
  int color;
  int id;
  std::string str;
  int age;
  simple2() = default;
  simple2(int a, int b, std::string c, int d)
      : color(a), id(b), str(c), age(d) {}
};
YLT_REFL(simple2, color, id, str, age);

TEST_CASE("test macros") {
  static_assert(!std::is_aggregate_v<simple2>);
  simple2 t{2, 10, "hello reflection", 6};
  constexpr auto arr = get_member_names<simple2>();
  static_assert(arr.size() == 4);
  constexpr auto map = member_names_map<simple2>;
  constexpr size_t index = map.at("age");
  CHECK(index == 3);
  constexpr size_t size = members_count_v<dummy_t>;
  static_assert(size == 3);

  constexpr auto idx = index_of<&simple2::age>();
  static_assert(idx == 3);
  constexpr auto idx2 = index_of<&simple2::id>();
  static_assert(idx2 == 1);

  auto i = index_of(&simple::id);
  CHECK(i == 1);
  i = index_of(&simple::age);
  CHECK(i == 3);

  auto ref_tp = object_to_tuple(t);
  auto& c = std::get<0>(ref_tp);
  c = 10;
  CHECK(t.color == 10);

  using Tuple = decltype(struct_to_tuple<simple2>());
  std::cout << type_string<Tuple>() << "\n";

  constexpr size_t size2 = members_count_v<dummy_t2>;
  static_assert(size2 == 3);

  constexpr size_t size3 = members_count_v<dummy_t3>;
  static_assert(size3 == 3);

  constexpr size_t size4 = members_count<dummy_t4>();
  static_assert(size4 == 3);

  constexpr size_t size5 = members_count_v<dummy_t5>;
  static_assert(size5 == 3);

  const dummy_t5 d5{};
  refl_visit_members(d5, [](auto&... args) {
    CHECK(sizeof...(args) == 3);
    ((std::cout << args << ", "), ...);
    std::cout << "\n";
  });

  visit_members(d5, [](auto&... args) {
    CHECK(sizeof...(args) == 3);
    ((std::cout << args << ", "), ...);
    std::cout << "\n";
  });

  for_each(d5, [](auto& arg) {
    std::cout << arg << "\n";
  });

  auto& age1 = get<int>(t, "age");
  CHECK(age1 == 6);

  auto str2 = get<2>(t);
  CHECK(str2 == "hello reflection");

  auto var = get(t, 3);
  CHECK(*std::get<3>(var) == 6);

#if __cplusplus >= 202002L
  auto& age2 = get<"age">(t);
  CHECK(age2 == 6);

  auto& var1 = get<"str">(t);
  CHECK(var1 == "hello reflection");

  constexpr size_t i3 = index_of<simple2, "str">();
  CHECK(i3 == 2);

  constexpr size_t i4 = index_of<simple2, "no_such">();
  CHECK(i4 == 4);
#endif

  constexpr std::string_view name1 = name_of<simple2, 2>();
  CHECK(name1 == "str");

  constexpr std::string_view name2 = name_of<simple2>(2);
  CHECK(name2 == "str");

  constexpr size_t idx1 = index_of<simple2>("str");
  CHECK(idx1 == 2);

  size_t idx3 = index_of<simple2>("no_such");
  CHECK(idx3 == 4);

  size_t idx4 = index_of(t, age1);
  CHECK(idx4 == 3);

  auto name3 = name_of(t, age1);
  CHECK(name3 == "age");

  int no_such = 100;
  size_t idx5 = index_of(t, no_such);
  CHECK(idx5 == 4);

  for_each(t, [](auto& arg) {
    if constexpr (std::is_same_v<std::string, remove_cvref_t<decltype(arg)>>) {
      arg = "test";
    }
    std::cout << arg << "\n";
  });
  CHECK(t.str == "test");

  for_each<simple2>([](std::string_view field_name, size_t index) {
    std::cout << index << ", " << field_name << "\n";
  });

  for_each<simple2>([](std::string_view field_name) {
    std::cout << field_name << "\n";
  });
}

class Bank_t {
  int id;
  std::string name;

 public:
  Bank_t(int i, std::string str) : id(i), name(str) {}
};
YLT_REFL_PRIVATE(Bank_t, id, name);

class private_struct {
  int a;
  int b;

 public:
  private_struct(int x, int y) : a(x), b(y) {}
};
YLT_REFL_PRIVATE(private_struct, a, b);

TEST_CASE("test visit private") {
  const Bank_t bank(1, "ok");
  constexpr auto tp = get_private_ptrs(identity<Bank_t>{});
  ylt::reflection::visit_members(bank, [](auto&... args) {
    ((std::cout << args << " "), ...);
    std::cout << "\n";
  });

  private_struct st(2, 4);
  visit_members(st, [](auto&... args) {
    ((std::cout << args << " "), ...);
    std::cout << "\n";
  });

  auto ref_tp = refl_object_to_tuple(st);
  std::get<1>(ref_tp) = 8;

  auto names = refl_member_names(identity<private_struct>{});
  auto names1 = refl_member_names(identity<Bank_t>{});
  auto count = refl_member_count(identity<private_struct>{});

  auto id = bank.*(std::get<0>(tp));    // 1
  auto name = bank.*(std::get<1>(tp));  // ok
}

namespace test_type_string {
struct struct_test {};
class class_test {};
union union_test {};
}  // namespace test_type_string

TEST_CASE("test type_string") {
  CHECK(type_string<int>() == "int");
  CHECK(type_string<const int>() == "const int");
  CHECK(type_string<volatile int>() == "volatile int");

#if defined(__clang__)
  CHECK(type_string<int&>() == "int &");
  CHECK(type_string<int&&>() == "int &&");
  CHECK(type_string<const int&>() == "const int &");
  CHECK(type_string<const int&&>() == "const int &&");
  CHECK(type_string<volatile int&>() == "volatile int &");
  CHECK(type_string<volatile int&&>() == "volatile int &&");
#else
  CHECK(type_string<int&>() == "int&");
  CHECK(type_string<int&&>() == "int&&");
  CHECK(type_string<const int&>() == "const int&");
  CHECK(type_string<const int&&>() == "const int&&");
  CHECK(type_string<volatile int&>() == "volatile int&");
  CHECK(type_string<volatile int&&>() == "volatile int&&");
#endif

#if defined(_MSC_VER) && !defined(__clang__)
  CHECK(type_string<test_type_string::struct_test>() ==
        "test_type_string::struct_test");
  CHECK(type_string<const test_type_string::struct_test>() ==
        "const struct test_type_string::struct_test");
  CHECK(type_string<test_type_string::class_test>() ==
        "test_type_string::class_test");
  CHECK(type_string<const test_type_string::class_test>() ==
        "const class test_type_string::class_test");
  CHECK(type_string<test_type_string::union_test>() ==
        "test_type_string::union_test");
  CHECK(type_string<const test_type_string::union_test>() ==
        "const union test_type_string::union_test");
#else
  CHECK(type_string<test_type_string::struct_test>() ==
        "test_type_string::struct_test");
  CHECK(type_string<const test_type_string::struct_test>() ==
        "const test_type_string::struct_test");
  CHECK(type_string<test_type_string::class_test>() ==
        "test_type_string::class_test");
  CHECK(type_string<const test_type_string::class_test>() ==
        "const test_type_string::class_test");
  CHECK(type_string<test_type_string::union_test>() ==
        "test_type_string::union_test");
  CHECK(type_string<const test_type_string::union_test>() ==
        "const test_type_string::union_test");
#endif
}

DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char** argv) { return doctest::Context(argc, argv).run(); }
DOCTEST_MSVC_SUPPRESS_WARNING_POP
