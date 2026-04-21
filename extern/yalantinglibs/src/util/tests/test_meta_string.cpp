#include <ylt/util/type_traits.h>

#include <ylt/util/magic_names.hpp>
#include <ylt/util/meta_string.hpp>

namespace refvalue::tests {
constexpr void meta_string_tests() {
  // Fundamental tests.
  constexpr meta_string str1{"Hello world!"};
  constexpr meta_string str2{"Hello world!"};
  constexpr meta_string str3{"Hello WORLD!"};

  constexpr size_t pos = str1.find('o');
  static_assert(pos == 4);
  constexpr size_t pos1 = str1.rfind('e');
  static_assert(pos1 == 1);

  constexpr size_t pos2 = str1.find(' ');
  constexpr auto str = str1.substr<pos2 + 1>();
  static_assert(str == "world!");

  constexpr auto append_str = str1 + " Tom" + " and Jerry";
  static_assert(append_str == "Hello world! Tom and Jerry");

  constexpr auto append_str1 = "Good " + str1;
  static_assert(append_str1 == "Good Hello world!");

  static_assert(str1 == str2);
  static_assert(str2 != str3);
  static_assert(str1 > str3);
  static_assert(str1 >= str3);
  static_assert(str3 < str2);
  static_assert(str3 <= str2);
  static_assert(str1.size() == str2.size());
  static_assert(str1.size() == str3.size());
  static_assert(str1.size() == 12);
  static_assert(static_cast<std::string_view>(str1) == "Hello world!");
  static_assert(meta_string{""}.empty());
  static_assert(str1.front() == 'H');
  static_assert(str1.back() == '!');
  static_assert(std::string_view{str1.c_str(), str1.size()} == "Hello world!");
  static_assert(std::string_view{str1.c_str()} == "Hello world!");
  static_assert(std::string_view{str1.data(), str1.size()} == "Hello world!");
  static_assert(std::string_view{str1.data()} == "Hello world!");
  static_assert(meta_string{"nice to meet you!"}.contains('!'));
  static_assert(meta_string{"nice to meet you!"}.contains("meet"));
  static_assert(str1 + str2 == meta_string{"Hello world!Hello world!"});

  // Tests of remove_char, remove, split_of and split.
  constexpr meta_string str4{"Poor guy,eligible,incognito"};
  constexpr meta_string str5{"Poor guy{abc}eligible{abc}incognito"};
  constexpr meta_string str6{
      "12345, Mean, Bad, 12345, Naughty, 12345, Possy, Fancy"};
  constexpr meta_string str7{"null"};
  constexpr auto no_letter_o = remove_char_v<str4, 'o'>;
  constexpr auto no_12345 = remove_v<str6, "12345">;
  constexpr auto split_of_space_and_comma = split_of_v<str4, ", ">;
  constexpr auto split_by_string = split_v<str5, "{abc}">;
  constexpr auto no_123456_nop = remove_v<str7, "123456">;

  static_assert(no_letter_o == meta_string{"Pr guy,eligible,incgnit"});
  static_assert(no_12345 ==
                meta_string{", Mean, Bad, , Naughty, , Possy, Fancy"});
  static_assert(split_of_space_and_comma.size() == 4);
  static_assert(split_of_space_and_comma[0] == "Poor");
  static_assert(split_of_space_and_comma[1] == "guy");
  static_assert(split_of_space_and_comma[2] == "eligible");
  static_assert(split_of_space_and_comma[3] == "incognito");

  static_assert(split_by_string.size() == 3);
  static_assert(split_by_string[0] == "Poor guy");
  static_assert(split_by_string[1] == "eligible");
  static_assert(split_by_string[2] == "incognito");

  static_assert(no_123456_nop == meta_string{"null"});

  // Tests of arbitrary constructions.
  constexpr meta_string subobject1{"This meta_string object"};
  constexpr meta_string subobject2{"is constructed by several subobjects"};
  constexpr meta_string subobject3{"which are concatenated together."};
  [[maybe_unused]] constexpr meta_string str_by_subobjects{
      subobject1, subobject2, subobject3};

  [[maybe_unused]] constexpr meta_string str_by_literal{
      "This meta_string object is constructed by a string literal."};

  [[maybe_unused]] constexpr meta_string str_by_char_sequence{'C', 'H', 'A',
                                                              'R'};
  constexpr std::string_view fragment1{"This meta_string object"};
  constexpr std::string_view fragment2{"Tis constructed by several spans"};
  constexpr std::string_view fragment3{"which are concatenated together."};
  [[maybe_unused]] constexpr meta_string str_by_spans{
      std::span<const char, fragment1.size()>{fragment1},
      std::span<const char, fragment2.size()>{fragment2},
      std::span<const char, fragment3.size()>{fragment3}};
}

void top_fun_1(int, long, double) {}

namespace ns1::ns2::ns3 {
void fun1() {}

#ifdef _MSC_VER
void** __vectorcall fun2(int, long, double) { return nullptr; }
#endif

using fun_ptr_type_1 = decltype(&top_fun_1);

template <typename T>
struct some_class {
  static int class_fun1(int, fun_ptr_type_1) { return {}; };

  static std::string_view class_fun2(long, const char*) noexcept { return {}; }
};

template <typename T, typename U>
static int func_template(T, signed char, float*, U) {
  return {};
}

constexpr void magic_names_tests() {
  // Fundamental tests.
  static_assert(qualified_name_of_v<&top_fun_1> ==
                meta_string{"refvalue::tests::top_fun_1"});
  static_assert(qualified_name_of_v<&ns1::ns2::ns3::fun1> ==
                meta_string{"refvalue::tests::ns1::ns2::ns3::fun1"});
  static_assert(
      qualified_name_of_v<&some_class<int>::class_fun1> ==
      meta_string{
          "refvalue::tests::ns1::ns2::ns3::some_class<int>::class_fun1"});
  static_assert(
      qualified_name_of_v<&some_class<int>::class_fun2> ==
      meta_string{
          "refvalue::tests::ns1::ns2::ns3::some_class<int>::class_fun2"});

#if 0
#ifdef _MSC_VER
  static_assert(qualified_name_of_v<&func_template<int, double>> ==
                meta_string{"refvalue::tests::ns1::ns2::ns3::func_template"});
#elif !defined(__APPLE__)
  static_assert(qualified_name_of_v<&func_template<int, double>> ==
                meta_string{"refvalue::tests::ns1::ns2::ns3::func_template"});
#endif

  static_assert(name_of_v<&top_fun_1> == meta_string{"top_fun_1"});
  static_assert(name_of_v<&ns1::ns2::ns3::fun1> == meta_string{"fun1"});
  static_assert(name_of_v<&some_class<int>::class_fun1> ==
                meta_string{"some_class<int>::class_fun1"});
  static_assert(name_of_v<&some_class<int>::class_fun2> ==
                meta_string{"some_class<int>::class_fun2"});

#ifdef _MSC_VER
  static_assert(name_of_v<&func_template<int, double>> ==
                meta_string{"func_template"});
#elif !defined(__APPLE__)
  static_assert(name_of_v<&func_template<int, double>> ==
                meta_string{"func_template"});
#endif
#endif
}

void testSimpleFunction(const char*) {}

template <class T>
struct Plus {
  T operator()(T a, T b) const noexcept { return a + b; }
};

constexpr void function_traits_test_return_type() {
  using namespace util;
  static_assert(std::is_same_v<function_return_type_t<int(int)>, int>);
  auto f1 = []() {
  };
  static_assert(std::is_same_v<function_return_type_t<decltype(f1)>, void>);
  auto f2 = []() mutable {
    return 42;
  };
  static_assert(std::is_same_v<function_return_type_t<decltype(f2)>, int>);
  struct Add {
    int operator()(int a, int b) { return a + b; }
    constexpr std::string_view get() { return "Hello World"; }
  };
  Add a;
  static_assert(std::is_same_v<function_return_type_t<Add>, int>);
  static_assert(std::is_same_v<function_return_type_t<Add&>, int>);
  static_assert(std::is_same_v<function_return_type_t<Add&&>, int>);
  // Add&
  static_assert(std::is_same_v<function_return_type_t<decltype((a))>, int>);
  // Add&&
  static_assert(
      std::is_same_v<function_return_type_t<decltype(std::move(a))>, int>);

  // void(&)(const char*)
  static_assert(
      std::is_same_v<function_return_type_t<decltype((testSimpleFunction))>,
                     void>);
  // void(const char*)
  static_assert(
      std::is_same_v<function_return_type_t<decltype(testSimpleFunction)>,
                     void>);
  // void(*)(const char*)
  static_assert(
      std::is_same_v<function_return_type_t<decltype(&testSimpleFunction)>,
                     void>);
  static_assert(std::is_same_v<function_return_type_t<Plus<double>>, double>);
  static_assert(std::is_same_v<function_return_type_t<decltype(&Add::get)>,
                               std::string_view>);
}

constexpr void function_traits_test_last_parameters_type() {
  using namespace util;
  static_assert(std::is_same_v<last_parameters_type_t<int(int&&)>, int>);
  auto f1 = [](int&) {
  };
  static_assert(std::is_same_v<last_parameters_type_t<decltype(f1)>, int>);
  static_assert(
      std::is_same_v<last_parameters_type_t<decltype(testSimpleFunction)>,
                     const char*>);
  static_assert(std::is_same_v<last_parameters_type_t<Plus<double>>, double>);
}

}  // namespace ns1::ns2::ns3
}  // namespace refvalue::tests

int main() {}
