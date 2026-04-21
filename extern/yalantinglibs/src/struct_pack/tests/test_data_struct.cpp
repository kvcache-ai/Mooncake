#include <bitset>
#include <memory>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
using namespace struct_pack;

template <typename T>
void test_container(T &v) {
  auto ret = serialize(v);
  T v1{};
  auto ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v == v1);

  v.clear();
  v1.clear();
  ret = serialize(v);
  ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v1.empty());
  CHECK(v == v1);
}

TEST_CASE("testing sequence containers") {
  SUBCASE("test vector") {
    std::vector<person> v{{20, "tom"}};
    test_container(v);
  }
  SUBCASE("test list") {
    std::list<person> v{{20, "tom"}};
    test_container(v);
  }
  SUBCASE("test deque") {
    std::deque<person> v{{20, "tom"}};
    test_container(v);
  }
}

TEST_CASE("testing associative containers") {
  SUBCASE("test int map") {
    std::map<int, person> v{{1, {20, "tom"}}, {2, {22, "jerry"}}};
    test_container(v);
  }

  SUBCASE("test string map") {
    std::map<std::string, person> v{{"aa", {20, "tom"}}, {"bb", {22, "jerry"}}};
    test_container(v);
  }

  SUBCASE("test multimap") {
    std::multimap<int, person> v{{1, {20, "tom"}}, {2, {22, "jerry"}}};
    test_container(v);

    std::multimap<int, person> v1{{1, {20, "tom"}}, {1, {22, "jerry"}}};
    test_container(v1);

    std::multimap<int, person> v2{
        {1, {20, "tom"}}, {1, {22, "jerry"}}, {3, {22, "jack"}}};
    test_container(v2);

    std::multimap<std::string, person> v3{{"aa", {20, "tom"}},
                                          {"bb", {22, "jerry"}}};
    test_container(v3);

    std::multimap<std::string, person> v4{
        {"dd", {20, "tom"}}, {"aa", {22, "jerry"}}, {"aa", {20, "jack"}}};
    test_container(v4);
  }

  SUBCASE("test set") {
    std::set<int> v{1, 2};
    test_container(v);

    std::set<std::string> v2{"aa", "bb"};
    test_container(v2);

    std::set<person> v3{{20, "tom"}, {22, "jerry"}};
    test_container(v3);
  }

  SUBCASE("test multiset") {
    std::multiset<int> v{1, 2, 1, 2};
    test_container(v);

    std::multiset<std::string> v2{"aa", "bb", "aa"};
    test_container(v2);

    std::multiset<person> v3{{20, "tom"}, {22, "jerry"}, {20, "jack"}};
    test_container(v3);
  }
}

TEST_CASE("testing unordered associative containers") {
  SUBCASE("test int map") {
    std::unordered_map<int, person> v{{1, {20, "tom"}}, {2, {22, "jerry"}}};
    test_container(v);
  }

  SUBCASE("test string map") {
    std::unordered_map<std::string, person> v{{"aa", {20, "tom"}},
                                              {"bb", {22, "jerry"}}};
    test_container(v);
  }

  SUBCASE("test multimap") {
    std::unordered_map<int, person> v{{1, {20, "tom"}}, {2, {22, "jerry"}}};
    test_container(v);

    std::unordered_map<int, person> v1{{1, {20, "tom"}}, {1, {22, "jerry"}}};
    test_container(v1);

    std::unordered_map<int, person> v2{
        {1, {20, "tom"}}, {1, {22, "jerry"}}, {3, {22, "jack"}}};
    test_container(v2);

    std::unordered_map<std::string, person> v3{{"aa", {20, "tom"}},
                                               {"bb", {22, "jerry"}}};
    test_container(v3);

    std::unordered_map<std::string, person> v4{
        {"dd", {20, "tom"}}, {"aa", {22, "jerry"}}, {"aa", {20, "jack"}}};
    test_container(v4);
  }
}

// We should not inherit from stl container, this case just for testing.
template <typename T>
struct my_vector : public std::vector<T> {};

template <typename Key, typename Value>
struct my_map : public std::map<Key, Value> {};

TEST_CASE("testing nonstd containers") {
  SUBCASE("test custom vector") {
    my_vector<int> v;
    v.push_back(1);
    v.push_back(2);
    test_container(v);

    my_vector<std::string> v1;
    v1.push_back("aa");
    v1.push_back("bb");
    test_container(v1);

    my_vector<person> v2;
    v2.push_back({20, "tom"});
    test_container(v2);
  }

  SUBCASE("test custom map") {
    my_map<int, person> v;
    v.emplace(1, person{20, "tom"});
    v.emplace(2, person{22, "jerry"});
    test_container(v);
  }
}
template <typename T>
void test_tuple_like(T &v) {
  auto ret = serialize(v);

  T v1{};
  auto ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v == v1);
}

TEST_CASE("testing tuple") {
  std::tuple<int, std::string> v = std::make_tuple(1, "hello");
  test_tuple_like(v);

  std::tuple<int, std::string> v1{};
  test_tuple_like(v1);

  std::tuple<int, std::string, person> v2{1, "aa", {20, "tom"}};
  test_tuple_like(v2);
}

TEST_CASE("test std::pair") {
  std::pair<int, std::string> v{1, "hello"};
  test_tuple_like(v);

  std::pair<std::string, person> v1{"aa", {20, "tom"}};
  test_tuple_like(v1);

  std::pair<std::string, person> v2{};
  test_tuple_like(v2);
}

TEST_CASE("testing std::array") {
  std::array<int, 3> v{1, 2, 3};
  test_tuple_like(v);

  std::array<std::string, 2> v1{"tom", "jerry"};
  test_tuple_like(v1);

  std::array<person, 2> v2{person{20, "tom"}, {22, "jerry"}};
  test_tuple_like(v2);

  std::array<person, 2> v3{};
  test_tuple_like(v3);
}
#if __cplusplus >= 202002L
TEST_CASE("test_trivial_copy_tuple") {
  tuplet::tuple tp = tuplet::make_tuple(1, 2);

  constexpr auto count = detail::members_count<decltype(tp)>();
  static_assert(count == 2);

  static_assert(std::is_same_v<decltype(tp), tuplet::tuple<int, int>>);
  static_assert(!std::is_same_v<decltype(tp), std::tuple<int, int>>);

  static_assert(std::is_same_v<decltype(detail::get_types<decltype(tp)>()),
                               tuplet::tuple<int, int>>);
  static_assert(!std::is_same_v<decltype(detail::get_types<decltype(tp)>()),
                                std::tuple<int, int>>);
  static_assert(get_type_code<decltype(tp)>() !=
                get_type_code<std::tuple<int, int>>());
  [[maybe_unused]] constexpr auto i = get_type_literal<decltype(tp)>();
  static_assert(get_type_literal<decltype(tp)>() !=
                get_type_literal<std::tuple<int, int>>());

  auto buf = serialize(tp);

  std::tuple<int, int> v{};
  auto ec = deserialize_to(v, buf);
  CHECK(ec);

  decltype(tp) tp1;
  auto ec2 = deserialize_to(tp1, buf);
  CHECK(!ec2);
  CHECK(tp == tp1);
}

struct test_obj {
  int id;
  std::string str;
  tuplet::tuple<int, std::string> tp;
  int d;
};

TEST_CASE("test_trivial_copy_tuple in an object") {
  test_obj obj{1, "hello", {2, "tuple"}, 3};
  auto buf = serialize(obj);

  test_obj obj1;
  auto ec = deserialize_to(obj1, buf);
  CHECK(!ec);
  CHECK(obj.tp == obj1.tp);
}
#endif
template <typename T>
void test_c_array(T &v) {
  auto ret = serialize(v);

  T v1{};
  auto ec = deserialize_to(v1, ret.data(), ret.size());
  REQUIRE(!ec);

  auto size = std::extent_v<T>;
  for (decltype(size) i = 0; i < size; ++i) {
    CHECK(v[i] == v1[i]);
  }
}

TEST_CASE("testing c array") {
  int v[3] = {1, 2, 3};
  test_c_array(v);
  int v1[2] = {};
  test_c_array(v1);

  std::string v3[2] = {"hello", "world"};
  test_c_array(v3);

  person v4[2] = {{20, "tom"}, {22, "jerry"}};
  test_c_array(v4);
}

enum class enum_i8 : int8_t { hello, hi };
enum class enum_i32 : int32_t { hello, hi };

TEST_CASE("testing enum") {
  {
    enum_i8 e{enum_i8::hi}, e2;
    auto ret = serialize(e);
    std::size_t sz;
    auto ec = deserialize_to(e2, ret.data(), ret.size(), sz);
    CHECK(!ec);
    CHECK(sz == ret.size());
    CHECK(e == e2);
  }
  {
    enum_i8 e{enum_i8::hi};
    auto ret = serialize(e);
    auto ec = deserialize<enum_i32>(ret.data(), ret.size());
    CHECK(!ec);
    if (!ec) {
      CHECK(ec.error() == struct_pack::errc::invalid_buffer);
    }
  }
  {
    constexpr auto code_enum_i8 = get_type_code<enum_i8>();
    constexpr auto code_enum_i32 = get_type_code<enum_i32>();
    constexpr auto code_i8 = get_type_code<int8_t>();
    constexpr auto code_i32 = get_type_code<int32_t>();
    static_assert(code_enum_i8 != code_enum_i32);
    static_assert(code_enum_i8 == code_i8);
    static_assert(code_enum_i32 == code_i32);
  }
}

TEST_CASE("testing fundamental types") {
  {
    std::array ar = {get_type_code<int8_t>(),   get_type_code<int16_t>(),
                     get_type_code<int32_t>(),  get_type_code<int64_t>(),
                     get_type_code<uint8_t>(),  get_type_code<uint16_t>(),
                     get_type_code<uint32_t>(), get_type_code<uint64_t>(),
                     get_type_code<char>(),     get_type_code<wchar_t>(),
                     get_type_code<char16_t>(), get_type_code<char32_t>(),
                     get_type_code<float>(),    get_type_code<double>()};
    std::sort(ar.begin(), ar.end());
    CHECK(std::unique(ar.begin(), ar.end()) == ar.end());
  }
  {
#ifdef __cpp_lib_char8_t
    static_assert(get_type_literal<char>() == get_type_literal<char8_t>());
#endif
    static_assert(get_type_literal<signed char>() ==
                  get_type_literal<int8_t>());
    static_assert(get_type_literal<unsigned char>() ==
                  get_type_literal<uint8_t>());
  }
}

TEST_CASE("test variant") {
  {
    constexpr auto variant_i = get_type_code<std::variant<int>>();
    constexpr auto variant_i_f = get_type_code<std::variant<int, float>>();
    constexpr auto i = get_type_code<int>();
    constexpr auto tuple_i = get_type_code<std::tuple<int>>();
    static_assert(variant_i != variant_i_f);
    static_assert(variant_i != i);
    static_assert(variant_i != tuple_i);
  }
  {
    std::variant<int, double> var = 1.4, var2;
    auto ret = struct_pack::serialize(var);
    auto ec = struct_pack::deserialize_to(var2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(var2 == var);
  }
  {
    std::variant<int, std::monostate> var = std::monostate{}, var2;
    auto ret = struct_pack::serialize(var);
    auto ec = struct_pack::deserialize_to(var2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(var2 == var);
  }
  {
    std::variant<int, int, int, std::monostate, double, double, double> var{
        std::in_place_index_t<1>{}, 2},
        var2;
    auto ret = struct_pack::serialize(var);
    auto ec = struct_pack::deserialize_to(var2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(var2 == var);
    CHECK(var2.index() == 1);
  }
  {
    std::variant<std::monostate, std::string> var{"hello"}, var2;
    auto ret = struct_pack::serialize(var);
    auto ec = struct_pack::deserialize_to(var2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(var2 == var);
  }
  {
    std::tuple<int, std::variant<int, double>, double,
               std::variant<int, double>, double>
        var = {1, 2.0, 3.0, 42, 5.0};
    auto ret = struct_pack::serialize(var);
    auto res = struct_pack::get_field<decltype(var), 3>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == std::variant<int, double>(42));
  }
  { auto ret = struct_pack::serialize(std::tuple<std::monostate>{}); }
  {
    std::variant<
        int8_t, int8_t, int8_t, int8_t, int8_t, int8_t, int8_t, int8_t, int8_t,
        int8_t, int16_t, int16_t, int16_t, int16_t, int16_t, int16_t, int16_t,
        int16_t, int16_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t,
        int32_t, int32_t, int32_t, int64_t, int64_t, int64_t, int64_t, int64_t,
        int64_t, int64_t, int64_t, int64_t, std::string, std::monostate,
        std::unordered_map<int, std::variant<std::monostate, double, int>>>
        big_variant =
            {std::unordered_map<int, std::variant<std::monostate, double, int>>{
                {1, 1}, {2, 0.2}, {-1, std::monostate{}}}},
        big_variant2;
    auto ret = struct_pack::serialize(big_variant);
    auto ec = struct_pack::deserialize_to(big_variant2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(big_variant2 == big_variant);
  }
}

void init_tree(my_tree *tree, int depth = 0) {
  tree->value = rand();
  tree->name = std::to_string(tree->value);
  tree->left_child = nullptr;
  tree->right_child = nullptr;
  if (depth > 10) {
    return;
  }
  if (rand() % 10 != 0) {
    tree->left_child = std::make_unique<my_tree>();
    init_tree(tree->left_child.get(), depth + 1);
  }
  if (rand() % 10 != 0) {
    tree->right_child = std::make_unique<my_tree>();
    init_tree(tree->right_child.get(), depth + 1);
  }
}

struct test_unique_ptr {
  int a;
  std::vector<std::unique_ptr<test_unique_ptr>> b;
  friend bool operator==(const test_unique_ptr &a, const test_unique_ptr &b) {
    if (a.a != b.a)
      return false;
    if (a.b.size() != b.b.size())
      return false;
    for (size_t i = 0; i < a.b.size(); ++i) {
      if (a.b[i] == nullptr && b.b[i] == nullptr) {
      }
      else if (a.b[i] != nullptr && b.b[i] != nullptr) {
        if (*a.b[i] == *b.b[i]) {
        }
        else {
          return false;
        }
      }
      else {
        return false;
      }
    }
    return true;
  }
};

TEST_CASE("test unique_ptr") {
  SUBCASE("nullptr") {
    auto buffer = struct_pack::serialize(std::unique_ptr<int>{nullptr});
    auto ret = deserialize<std::unique_ptr<int>>(buffer);
    CHECK(ret.has_value());
    CHECK(ret->get() == nullptr);
  }
  SUBCASE("normal int") {
    auto buffer = struct_pack::serialize(std::make_unique<int>(42));
    auto ret = deserialize<std::unique_ptr<int>>(buffer);
    CHECK(ret.has_value());
    CHECK(*ret.value() == 42);
  }
  SUBCASE("ptr2string") {
    auto buffer =
        struct_pack::serialize(std::make_unique<std::string>("Hello"));
    auto ret = deserialize<std::unique_ptr<std::string>>(buffer);
    CHECK(ret.has_value());
    CHECK(*ret.value() == "Hello");
  }
  SUBCASE("ptr2person") {
    auto buffer = struct_pack::serialize(
        std::make_unique<person>(person{24, std::string{"name24"}}));
    auto ret = deserialize<std::unique_ptr<person>>(buffer);
    CHECK(ret.has_value());
    CHECK(*ret.value() == person{24, "name24"});
  }

  SUBCASE("ptr2optional") {
    auto buffer = struct_pack::serialize(
        std::make_unique<person>(person{24, std::string{"name24"}}));
    auto ret = deserialize<std::optional<person>>(buffer);
    CHECK(ret.has_value());
    CHECK(*ret.value() == person{24, "name24"});
  }
  SUBCASE("optional2ptr") {
    auto buffer =
        struct_pack::serialize(std::optional<person>{person{24, "name24"}});
    auto ret = deserialize<std::unique_ptr<person>>(buffer);
    CHECK(ret.has_value());
    CHECK(*ret.value() == person{24, "name24"});
  }
  SUBCASE("error template param") {
    auto buffer = struct_pack::serialize(
        std::tuple<std::unique_ptr<int>>{std::make_unique<int>(24)});
    auto ret = deserialize<std::unique_ptr<float>>(buffer);
    REQUIRE(!ret);
    CHECK(ret.error() == struct_pack::errc::invalid_buffer);
  }
  SUBCASE("test list") {
    my_list list_head, *now = &list_head;
    for (int i = 0; i < 100; ++i) {
      now->name = std::to_string(i);
      now->value = i;
      if (i + 1 < 100) {
        now->next = std::make_unique<my_list>();
        now = now->next.get();
      }
      else {
        now->next = nullptr;
      }
    }
    auto buffer = struct_pack::serialize(list_head);
    auto result = struct_pack::deserialize<my_list>(buffer);
    auto literal = struct_pack::get_type_literal<my_list>();
    std::string_view sv{literal.data(), literal.size()};
    using struct_pack::detail::type_id;
    CHECK(sv == std::string{(char)type_id::struct_t, (char)type_id::optional_t,
                            (char)type_id::circle_flag, (char)129,
                            (char)type_id::string_t, (char)type_id::char_8_t,
                            (char)type_id::int32_t,
                            (char)type_id::type_end_flag});
    CHECK(result == list_head);
  }
  SUBCASE("test list(unique_ptr)") {
    auto list_head = std::make_unique<my_list>();
    my_list *now = list_head.get();
    for (int i = 0; i < 100; ++i) {
      now->name = std::to_string(i);
      now->value = i;
      if (i + 1 < 100) {
        now->next = std::make_unique<my_list>();
        now = now->next.get();
      }
      else {
        now->next = nullptr;
      }
    }
    auto buffer = struct_pack::serialize(list_head);
    auto result = struct_pack::deserialize<std::unique_ptr<my_list>>(buffer);
    auto literal = struct_pack::get_type_literal<std::unique_ptr<my_list>>();
    std::string_view sv{literal.data(), literal.size()};
    using struct_pack::detail::type_id;
    CHECK(sv == std::string{(char)type_id::optional_t, (char)type_id::struct_t,
                            (char)type_id::circle_flag, (char)129,
                            (char)type_id::string_t, (char)type_id::char_8_t,
                            (char)type_id::int32_t,
                            (char)type_id::type_end_flag});
    CHECK(*result.value() == *result.value());
  }
  SUBCASE("test list2") {
    auto list_head = std::make_unique<my_list2>();
    my_list2 *now = list_head.get();
    for (int i = 0; i < 100; ++i) {
      now->list.name = std::to_string(i);
      now->list.value = i;
      if (i + 1 < 100) {
        now->list.next = std::make_unique<my_list2>();
        now = now->list.next.get();
      }
      else {
        now->list.next = nullptr;
      }
    }
    auto buffer = struct_pack::serialize(list_head);
    auto result = struct_pack::deserialize<std::unique_ptr<my_list2>>(buffer);
    auto literal = struct_pack::get_type_literal<std::unique_ptr<my_list2>>();
    std::string_view sv{literal.data(), literal.size()};
    using struct_pack::detail::type_id;
    CHECK(sv == std::string{(char)type_id::optional_t, (char)type_id::struct_t,
                            (char)type_id::struct_t, (char)type_id::circle_flag,
                            (char)130, (char)type_id::string_t,
                            (char)type_id::char_8_t, (char)type_id::int32_t,
                            (char)type_id::type_end_flag,
                            (char)type_id::type_end_flag});
    CHECK(*result.value() == *result.value());
  }
  SUBCASE("test tree") {
    auto literal = struct_pack::get_type_literal<my_tree>();
    auto sv = std::string_view{literal.data(), literal.size()};
    using struct_pack::detail::type_id;
    CHECK(sv == std::string{
                    (char)type_id::struct_t, (char)type_id::optional_t,
                    (char)type_id::circle_flag, (char)129,
                    (char)type_id::optional_t, (char)type_id::circle_flag,
                    (char)129, (char)type_id::string_t, (char)type_id::char_8_t,
                    (char)type_id::int32_t, (char)type_id::type_end_flag});
    my_tree tree;
    init_tree(&tree);
    auto buffer = struct_pack::serialize(tree);
    auto result = struct_pack::deserialize<my_tree>(buffer);
    CHECK(tree == result);
  }
  SUBCASE("test unique_array") {
    static_assert(
        struct_pack::detail::unique_ptr<std::unique_ptr<int[]>> == false,
        "We don't support unique_ptr to array now.");
  }
  SUBCASE("test unique_ptr self-reference") {
    test_unique_ptr p;
    p.a = 123;
    p.b.push_back(std::make_unique<test_unique_ptr>(test_unique_ptr{456}));
    p.b.back()->a = 1456;
    p.b.back()->b.push_back(nullptr);

    p.b.push_back(std::make_unique<test_unique_ptr>(test_unique_ptr{789}));
    p.b.back()->a = 1789;
    p.b.back()->b.push_back(
        std::make_unique<test_unique_ptr>(test_unique_ptr{11789}));

    p.b.push_back(std::make_unique<test_unique_ptr>(test_unique_ptr{123}));
    p.b.back()->a = 1123;
    p.b.back()->b.push_back(
        std::make_unique<test_unique_ptr>(test_unique_ptr{11123}));
    p.b.back()->b.push_back(
        std::make_unique<test_unique_ptr>(test_unique_ptr{12123}));

    auto buffer = struct_pack::serialize(p);
    auto p2 = struct_pack::deserialize<test_unique_ptr>(buffer);
    CHECK(p2.has_value());
    CHECK(p2.value() == p);
  }
}

TEST_CASE("test bitset") {
  SUBCASE("test serialize size") {
    std::bitset<255> ar;
    constexpr auto sz =
        struct_pack::get_needed_size<struct_pack::sp_config::DISABLE_TYPE_INFO>(
            ar);
    static_assert(sz.size() == 36);
  }
  SUBCASE("test serialize/deserialize") {
    std::bitset<255> ar;
    ar.set();
    auto buffer = struct_pack::serialize(ar);
    auto result = struct_pack::deserialize<std::bitset<255>>(buffer);
    CHECK(result.has_value());
    CHECK(result.value() == ar);
  }
  SUBCASE("test type check") {
    std::bitset<255> ar;
    auto buffer = struct_pack::serialize(ar);
    auto result = struct_pack::deserialize<std::bitset<256>>(buffer);
    CHECK(result.has_value() == false);
  }
  SUBCASE("test trivial copy") {
    std::array<std::bitset<255>, 10> ar;
    static_assert(
        struct_pack::detail::is_trivial_serializable<decltype(ar)>::value);
  }
}