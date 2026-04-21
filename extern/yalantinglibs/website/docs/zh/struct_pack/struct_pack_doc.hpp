/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "ylt/struct_pack/struct_pack_impl.hpp"

namespace struct_pack {

/*!
 * \defgroup struct_pack struct_pack
 *
 * \brief 高性能，head-only，直接解析C++结构体的二进制序列化库。
 *
 */

/*!
 * \ingroup struct_pack
 * \struct expected
 * \brief `deserialize`函数的返回值类型
 *
 * 当启用C++23标准且标准库已实现`std::expected`时，该类是`std::expected`的别名。否则该类将使用内部的实现代码来模拟。
 *
 * 该类型的详细文档与介绍请见[cpprefence](https://en.cppreference.com/w/cpp/utility/expected)
 *
 * 例如：
 @code{.cpp}
  int test() {
    person p{20, "tom"};
    auto buffer = struct_pack::serialize(p);
    struct_pack::expected<person, struct_pack::err_code> p2 =
        struct_pack::deserialize<person>(buffer.data(), buffer.size());
    assert(p2);
    assert(p == p2.value());
  }
 @endcode
 *
 */

/*!
 * @ingroup struct_pack
 * @struct trivial_view
 * @brief `trivial_view<T>` 是一个平凡结构体的视图，在类型系统上等价于`T`。
 *
 * 其作用是减少赋值/反序列化过程中的内存拷贝。
 * 例如，假如有一个巨大的结构体`Data`
 @code{.cpp}
  struct Data {
    int x[10000],y[10000],z[10000];
  };
 @endcode
 * 假设有协议：
 @code{.cpp}
  struct Proto {
    std::string name;
    Data data;
  };
  void serialzie(std::string_view name, Data& data) {
    Proto proto={.name = name, .data = data};
    //从已有的数据构造对象时，需要花费大量时间拷贝
    auto buffer = struct_pack::serialize(proto);
    auto result = struct_pack::deserialize<Proto>(data);
    //反序列化时，需要花费大量时间拷贝
    assert(result->name == name && result->data == data);
  }
 @endcode
 *
 * 可以发现，构造/反序列化时拷贝代价很大。
 * 如何解决？我们可以改用视图：
 @code{.cpp}
  struct ProtoView {
    std::string_view name;
    struct_pack::trivial_view<Data> data;
  };
  void serialzie(std::string_view name, Data& data) {
    ProtoView proto={.name = name, .data = data};
    //从已有的数据构造对象时，可以做到zero-copy
    auto buffer = struct_pack::serialize(proto);
    auto result = struct_pack::deserialize<ProtoView>(data);
    //反序列化是zero-copy的。
    assert(result->name == name && result->data.get() == data);
  }
 @endcode
 * `trivial_view<T>`在类型系统中等价于`T`，因此，序列化其中一者再反序列化到另外一者是合法的。
 @code{.cpp}
  void serialzie(Proto& proto) {
    auto buffer = struct_pack::serialize(proto);
    auto result = struct_pack::deserialize<ProtoView>(data);
    //反序列化是zero-copy的。
    assert(result->name == name && result->data.get() == data);
  }
 @endcode
 *
 * @tparam T `trivial_view`指向的类型，`T`必须是一个平凡的结构体
 */
template <typename T>
struct trivial_view {
 private:
  const T *ref;

 public:
  /**
   * @brief 构造一个指向对象`t`的`trivial_view`
   *
   * @param t
   */
  trivial_view(const T *t);
  /**
   * @brief 构造一个指向对象`t`的`trivial_view`
   *
   * @param t
   */
  trivial_view(const T &t);
  trivial_view(const trivial_view &) = default;
  /**
   * @brief 构造一个指向空对象的`trivial_view`
   *
   */
  trivial_view() : ref(nullptr){};
  trivial_view &operator=(const trivial_view &) = default;
  /**
   * @brief `trivial_view`指向的对象的类型，`value_type`必须是一个平凡的结构体

   *
   */
  using value_type = T;

  /**
   * @brief 设置`trivial_view`指向的对象
   *
   * @param obj
   */
  void set(const T &obj);
  /**
   * @brief 返回`trivial_vie`w指向的对象`T`的常引用
   *
   * @return const T&
   */
  const T &get() const;
  /**
   * @brief 允许`trivial_view`通过运算符`->`访问指向对象`T`
   *
   * @return const T*
   */
  const T *operator->() const;
};

/*!
 * \ingroup struct_pack
 * \struct compatible
 * \brief 兼容字段类型
 *
 * @tparam T 兼容字段的类型
 * @tparam version 兼容字段的版本号
 *
 * 这个类使用上类似于`std::optional<T>`，但其语义是添加一个能够保持向前兼容的字段。
 *
 * 例如：
 *
 @code{.cpp}
  struct person_v1 {
    int age;
    std::string name;
  };

  struct person_v2 {
    int age;
    std::string name;
    struct_pack::compatible<std::size_t, 20210101> id; //20210101为版本号
  };
 @endcode
 *
 * `struct_pack::compatible`可以为空值，从而保证向前和向后的兼容性。
 * 例如，序列化`person_v2`，然后将其按照`person_v1`来反序列化，多余的字段在反序列化时将会被直接忽略。
 * 反过来说，序列化`person_v1`，再将其按照`person_v2`来反序列化，则解析出的`person_v2`中的`compatibale`字段均为空值。
 *
 * `compatible`字段的版本号可以省略，默认版本号为0。
 *
 @code{.cpp}
  person_v2 p2{20, "tom", 114.1, "tom"};
  auto buf = struct_pack::serialize(p2);

  person_v1 p1;
  // deserialize person_v2 as person_v1
  auto ec = struct_pack::deserialize_to(p1, buf.data(), buf.size());
  CHECK(!ec);

  auto buf1 = struct_pack::serialize(p1);
  person_v2 p3;
  // deserialize person_v1 as person_v2
  auto ec = struct_pack::deserialize_to(p3, buf1.data(), buf1.size());
  CHECK(!ec);
 @endcode
 *
 * 升级接口时应注意保持<b>版本号递增</b> 原则：
 *
 * 1.新增加的`compatible`字段的版本号，应该大于上一次增加的`compatible`字段的版本号。
 *
 * 2.不得移除原来的任何一个`compatible`字段（移除普通字段也会出错，但这种错误是可以被类型校验检查出来的）
 *
 * 假如违反这一原则，那么可能会引发未定义行为。
 * 例如：
 *
 @code{.cpp}
  struct loginRequest_V1
  {
    string user_name;
    string pass_word;
    struct_pack::compatible<string> verification_code;
  };

  struct loginRequest_V2
  {
    string user_name;
    string pass_word;
    struct_pack::compatible<network::ip> ip_address;
  };

  auto data=struct_pack::serialize(loginRequest_V1{});
  loginRequest_V2 req;
  //该行为是未定义的！
  struct_pack::deserialize_to(req, data);
 @endcode
 *
 @code{.cpp}
  struct loginRequest_V1
  {
    string user_name;
    string pass_word;
    struct_pack::compatible<string, 20210101> verification_code;
  };

  struct loginRequest_V2
  {
    string user_name;
    string pass_word;
    struct_pack::compatible<string, 20210101> verification_code;
    struct_pack::compatible<network::ip, 20000101> ip_address;
  };

  auto data=struct_pack::serialize(loginRequest_V1{});
  loginRequest_V2 req;
  //该行为是未定义的！
  struct_pack::deserialize_to(req, data);
 @endcode
 */

template <typename T, uint64_t version>
struct compatible : public std::optional<T> {
  constexpr compatible() = default;
  constexpr compatible(const compatible &other) = default;
  constexpr compatible(compatible &&other) = default;
  constexpr compatible(std::optional<T> &&other)
      : std::optional<T>(std::move(other)){};
  constexpr compatible(const std::optional<T> &other)
      : std::optional<T>(other){};
  constexpr compatible &operator=(const compatible &other) = default;
  constexpr compatible &operator=(compatible &&other) = default;
  using std::optional<T>::optional;
  friend bool operator==(const compatible<T, version> &self,
                         const compatible<T, version> &other);
  static constexpr uint64_t version_number = version;
};

/*!
 * \ingroup struct_pack
 * \struct string_literal
 * \brief 编译期字符串类型
 *
 * \tparam CharType 字符类型
 * \tparam Size 字符串长度
 * 该类用于表示一个编译期的字符串类型，是函数`struct_pack::get_type_literal`的返回值，该字符串以'\0'结尾
 *
 * 样例代码：
 *
 @code{.cpp}
  auto str = struct_pack::get_type_literal<int, int, short>();
  CHECK(str.size() == 5);
  string_literal<char, 5> val{{(char)-2, 1, 1, 7, (char)-1}};
  //                         {struct_begin,int32_t,int32_t,int16_t,struct_end};
  CHECK(str == val);
 @endcode
 */

template <typename CharType, std::size_t Size>
struct string_literal {
  constexpr string_literal() = default;
  /**
   * @brief 从`string_view`构造`string_literal`类型
   *
   */
  constexpr string_literal(std::basic_string_view<CharType> str);
  /**
   * @brief 从数组构造`string_literal`类型
   *
   */
  constexpr string_literal(const CharType (&value)[Size + 1]);
  /**
   * @brief 返回字符串的长度
   *
   * @return constexpr std::size_t
   */
  constexpr std::size_t size() const;
  /**
   * @brief 判断字符串是否为空字符串
   *
   * @return true
   * @return false
   */
  constexpr bool empty() const;
  /**
   * @brief 获取下标对应的字符
   *
   * @param sz
   * @return constexpr CharType&
   */
  constexpr CharType &operator[](std::size_t sz);
  /**
   * @brief 获取下标对应的字符
   *
   * @param sz
   * @return constexpr const char&
   */
  constexpr const char &operator[](std::size_t sz) const;

  /**
   * @brief 返回一个C-style（以'\0'结尾）的字符串指针
   *
   * @return constexpr const CharType*
   */
  constexpr const CharType *data() const;

  /**
   * @brief 判断两个字符串是否不相等
   *
   * @tparam Size2
   * @param other
   * @return true
   * @return false
   */
  template <std::size_t Size2>
  constexpr bool operator!=(const string_literal<CharType, Size2> &other) const;

  /**
   * @brief 判断两个字符串是否相等
   *
   * @tparam Size2
   * @param other
   * @return true
   * @return false
   */
  template <std::size_t Size2>
  constexpr bool operator==(const string_literal<CharType, Size2> &other) const;

  /**
   * @brief 拼接两个字符串
   *
   * @tparam Size2
   * @param other
   * @return string_literal<CharType, Size + Size2> constexpr 返回拼接后的字符串
   */
  template <size_t Size2>
  string_literal<CharType, Size + Size2> constexpr operator+(
      string_literal<CharType, Size2> other) const;

 private:
  CharType ar[Size + 1];
};

/*!
 * \ingroup struct_pack
 * \struct err_code
 * \brief
 * struct_pack的错误码，存储了一个枚举值`struct_pack::errc`，可用于判断序列化是否成功。
 */

struct err_code {
 public:
  struct_pack::errc ec;
  /**
   * @brief err_code的默认构造函数，默认情况下无错误
   *
   */
  err_code() noexcept;
  /**
   * @brief 通过错误值枚举`struct_pack::errc`构造错误码
   *
   * @param ec 错误值枚举，`struct_pack::errc`类型
   */
  err_code(struct_pack::errc ec) noexcept;
  err_code &operator=(errc ec);
  err_code(const err_code &err_code) noexcept;
  err_code &operator=(const err_code &o) noexcept;
  /**
   * @brief 比较错误码是否相同
   *
   * @param o 另外一个错误码
   * @return true 两个错误码相同
   * @return false 两个错误码不同
   */
  bool operator==(const err_code &o) const noexcept { return ec == o.ec; }
  /**
   * @brief 比较错误码是否不同
   *
   * @param o 另外一个错误码
   * @return true 两个错误码不同
   * @return false 两个错误码相同
   */
  bool operator!=(const err_code &o);
  /**
   * @brief 判断是否有错误
   *
   * @return true 有错误
   * @return false 无错误
   */
  operator bool() const noexcept { return ec != errc::ok; }
  /**
   * @brief 将错误码转换为整数
   *
   * @return int
   */
  int val() const noexcept;
  /**
   * @brief 返回错误码对应的错误消息
   *
   * @return std::string_view
   */
  std::string_view message() const noexcept;
};

/**
 * \ingroup struct_pack
 * @brief struct_pack的错误值枚举
 *
 * struct_pack的错误值枚举，各枚举值解释如下：
 * 1. ok，代表无错误。
 * 2. no_buffer_space，缓冲区耗尽，未能完成所有字段的反序列化。
 * 3. invalid_buffer，读取到非法数据，无法将数据反序列化到指定类型。
 * 4. hash_conflict,
 * 侦测到哈希冲突，该错误仅在序列化数据中存在完整类型信息时才可能出现。代表尽管类型校验哈希码相同，但反序列化的目的类型和数据的实际类型依然不同，两类型的校验值之间发生了哈希冲突。
 * 5.
 * invalid_width_of_container_length，容器长度的位数不合法，例如，当字符串/容器长度大于2的32次方时，序列化数据的二进制格式使用64位的容器长度来存储数据，将该数据保存到文件中，并在32位系统上反序列化，即会返回该错误，因为32位系统不支持如此之大的数据。
 */
enum class errc {
  ok = 0,
  no_buffer_space,
  invalid_buffer,
  hash_conflict,
  invalid_width_of_container_length,
};

/**
 * \ingroup struct_pack
 * @brief struct_pack的配置
 *
 * struct_pack的编译期配置，用户可通过或运算组合各枚举值，以决定struct_pack的序列化行为。
 *
 * 具体说明详见：[序列化设置](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_tips.html#%E5%BA%8F%E5%88%97%E5%8C%96%E8%AE%BE%E7%BD%AE)
 *
 */
enum sp_config : uint64_t {
  DEFAULT = 0,

  DISABLE_TYPE_INFO = 0b1,
  ENABLE_TYPE_INFO = 0b10,
  DISABLE_ALL_META_INFO = 0b11,

  ENCODING_WITH_VARINT = 0b100,
  USE_FAST_VARINT = 0b1000
};

/*!
 * \ingroup struct_pack
 * \brief 手动标注结构体成员个数
 *
 * @tparam T 待纠正的结构体
 *
 * 某些特殊情况下，struct_pack可能无法正确计算结构体内的元素个数并导致编译期错误。
 * 此时请特化模板`struct_pack::members_count`，并手动标明结构体内元素的个数。
 *
 * 样例代码：
 @code{.cpp}
  struct bug_member_count_struct1 {
  int i;
  std::string j;
  double k = 3;
  };

  struct bug_member_count_struct2 : bug_member_count_struct1 {};

  template <>
  constexpr size_t struct_pack::members_count<bug_member_count_struct2> = 3;
 @endcode
 */
template <typename T>
constexpr std::size_t members_count;

/*!
 * \ingroup struct_pack
 * @brief 构造`std::error_code`
 * @param err error code.
 * @return std::error_code
 *
 * 通过`struct_pack::errc`构造`std::error_code`
 */
inline std::error_code make_error_code(struct_pack::errc err);

/*!
 * \ingroup struct_pack
 * @brief 获取错误消息
 * @param err
 * @return std::string 错误消息
 *
 * 本函数获取struct_pack::errc对应的错误消息。
 */

inline std::string error_message(struct_pack::errc err);

/*!
 * \ingroup struct_pack
 * @brief 获取类型校验码
 * @tparam Args
 * @return 编译期计算出的类型名
 *
 *
返回Args的31位类型校验码。当传入的参数多于一个时，返回类型`std::tuple<Args...>`的校验码
 *
 * 样例代码：
 *
 @code{.cpp}
  static_assert(
      get_type_code<std::deque<int>>() == get_type_code<std::vector<int>>(),
      "different class accord with container concept should get same MD5");
  static_assert(
      get_type_code<std::map<int,int>>() != get_type_code<std::vector<int>>(),
      "different class accord with different concept should get different MD5");
 @endcode
 *
 * @tparam Args
 * @return 编译期计算出的类型哈希校验码。
 */
template <typename... Args>
constexpr std::uint32_t get_type_code();

/*!
 * \ingroup struct_pack
 * 本函数返回编译期计算出的类型名，并按`struct_pack::string_literal<char,N>`类型返回。
 * 当传入的参数多于一个时，返回类型`std::tuple<T...>`的类型名。
 *
 * 样例代码：
 *
 @code{.cpp}
  auto str = get_type_literal<tuple<int, int, short>>();
  CHECK(str.size() == 5);
  string_literal<char, 5> val{{(char)-2, 1, 1, 7, (char)-1}};
  //{struct_begin,int32_t,int32_t,int16_t,struct_end};
  CHECK(str == val);
 @endcode
 */
template <typename... Args>
constexpr decltype(auto) get_type_literal();

/*!
 * \ingroup struct_pack
 *
 * @brief 获取序列化长度
 *
 * @tparam Args
 * @param args
 * @return serialize_buffer_size 序列化所需的buffer的长度。
 *
 * 用于预先分配好合适长度的内存，通常配合`struct_pack::serialize_to`函数使用。
 * 如果类型允许，该计算可能在编译期完成。
 *
 * 样例代码：
 *
 @code{.cpp}
  std::map<std::string, std::string> obj{{"hello", "struct pack"}};
  auto size=struct_pack::get_needed_size(obj);
  auto array=std::make_unique<char[]>(size);
  struct_pack::serialize_to(array.get(),size);
 @endcode
 */
template <typename... Args>
[[nodiscard]] constexpr struct_pack::serialize_buffer_size get_needed_size(
    const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief 将对象序列化并写入到给定的流/缓冲区。
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Writer
 输出流/缓冲区容器类型，对于缓冲区容器需要满足`detail::struct_pack_buffer`约束，可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 ，对于流需要满足`struct_pack::writer_t`约束的类型(如`std::ostream`)
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param writer 输出缓冲区
 * @param args 待序列化的对象
 *
 * 需要注意的是，该函数不会清空写入的容器/流，而是会将数据附加到缓冲区尾部。
 *
 * 样例代码：
 * @code{.cpp}
  std::string buffer = "The next line is struct_pack data.\n";
  struct_pack::serialize_to(buffer, p);
   @endcode
 * @code{.cpp}
  std::ofstream writer("struct_pack_demo.data",
                        std::ofstream::out | std::ofstream::binary);
  struct_pack::serialize_to(writer, p);
   @endcode
 *
 *
 */
template <uint64_t conf = sp_config::DEFAULT, typename Writer, typename... Args>
void serialize_to(Writer &writer, const Args &...args);

/*!
 * \ingroup struct_pack
 *
 * @brief 将对象序列化到给定的内存地址
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param buffer 内存首地址
 * @param len 序列化所需长度，该长度可通过`struct_pack::get_needed_size`计算得到
 * @param args 待序列化的对象
 *
 * 样例代码：
 * @code{.cpp}
  auto size = struct_pack::get_needed_size(p);
  auto array = std::make_unique<char[]>(size);
  struct_pack::serialize_to(array.get(), size, p);
   @endcode
 */
template <uint64_t conf = sp_config::DEFAULT, typename... Args>
void serialize_to(char *buffer, serialize_buffer_size len, const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief 将序列化结果保存到给定缓冲区尾部，并在序列化结果的头部预留一段字节。
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Buffer
 * 序列化缓冲区类型，需要满足`detail::struct_pack_buffer`约束，缓冲区可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param buffer 输出缓冲区
 * @param offset 头部偏移的空闲字节长度
 * @param args 待序列化的对象
 *
 * 需要注意的是，出于性能优化考虑，预留的字节可能未被初始化，不应假设其被初始化为0。
 */
template <uint64_t conf = sp_config::DEFAULT, detail::struct_pack_buffer Buffer,
          typename... Args>
void serialize_to_with_offset(Buffer &buffer, std::size_t offset,
                              const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief 序列化对象并返回结果
 *
 * @tparam Buffer
 * 需要序列化的对象类型，默认为`std::vector<char>`，需要满足`detail::struct_pack_buffer`约束，可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param args 待序列化的对象
 * @return Buffer 返回序列化的结果
 *
 * 序列化对象并返回结果
 @code{.cpp}
 person p{20, "tom"};
 auto buffer = struct_pack::serialize(p);
 @endcode
 *
 */
template <detail::struct_pack_buffer Buffer = std::vector<char>,
          typename... Args>
[[nodiscard]] Buffer serialize(const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief 将序列化结果保存到容器并返回，同时在序列化结果的头部预留一段字节。
 *
 * @tparam Buffer
 * 序列化的容器（缓冲区）类型，需要满足`detail::struct_pack_buffer`约束，可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param offset 头部偏移的空闲字节长度
 * @param args 待序列化的对象
 * @return Buffer 将结果保存为容器类型，按值返回。
 */
template <
    detail::struct_pack_buffer Buffer = std::vector<char> typename... Args>
[[nodiscard]] Buffer serialize_with_offset(std::size_t offset,
                                           const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief 按给定配置序列化对象并返回结果
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Buffer
 * 需要序列化的对象类型，默认为std::vector<char>，需要满足`detail::struct_pack_buffer`约束，可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param args 待序列化的对象
 * @return Buffer 返回序列化的结果
 */
template <uint64_t conf, detail::struct_pack_buffer Buffer = std::vector<char>,
          typename... Args>
[[nodiscard]] Buffer serialize(const Args &...args);

/**
 * \ingroup struct_pack
 *
 * @brief
 * 按给定配置将序列化结果保存到容器并返回，同时在序列化结果的头部预留一段字节。
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Buffer
 * 序列化的容器（缓冲区）类型，需要满足`detail::struct_pack_buffer`约束，可以为内存连续的字节容器(如`std::vector`,`std::string`)。
 * @tparam Args
 * 需要序列化的对象类型。当传入多个序列化对象时，函数会将其打包合并，按`std::tuple`类型的格式序列化。
 * @param offset 头部偏移的空闲字节长度
 * @param args 待序列化的对象
 * @return Buffer 将结果保存为容器类型，按值返回。
 */
template <uint64_t conf, detail::struct_pack_buffer Buffer = std::vector<char>,
          typename... Args>
[[nodiscard]] Buffer serialize_with_offset(std::size_t offset,
                                           const Args &...args);

/**
 * \ingroup struct_pack
 * @brief 从视图中反序列化目的对象
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam T 对象的类型T
 * @tparam Args
 * 对象的类型Args,当Args不为空时，会将数据按`std::tuple<T,Args...>`格式将数据反序列化保存到参数中。
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * 约束，如std::string_view
 * @param t 待反序列化的对象t
 * @param v 存有struct_pack序列化数据的视图
 * @param args 待反序列化的对象args
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 * 样例代码：
 @code{.cpp}
  person p{20, "tom"};
  auto buffer = struct_pack::serialize(p);
  person p2;
  auto ec = struct_pack::deserialize_to(p2, buffer);
  assert(!ec);
  assert(p == p2);
 @endcode
 */
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const View &v,
                                                   Args &...args);

/**
 * \ingroup struct_pack
 * @brief 将内存中的数据反序列化到目的对象
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam T 对象的类型T
 * @tparam Args
 * 对象的类型Args,当Args不为空时，会将数据按`std::tuple<T,Args...>`格式将数据反序列化保存到参数中。
 * @param t 待反序列化的对象t
 * @param data 起始地址
 * @param size 数据的长度
 * @param args 待反序列化的对象args
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 * 样例代码：
 @code{.cpp}
  person p{20, "tom"};
  auto buffer = struct_pack::serialize(p);
  person p2;
  auto ec = struct_pack::deserialize_to(p2, buffer.data(), buffer.size());
  assert(!ec);
  assert(p == p2);
 @endcode
 */
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args>
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const char *data,
                                                   size_t size, Args &...args);
/**
 * \ingroup struct_pack
 * @brief 将输入流中的数据反序列化到目的对象
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam T 对象的类型T
 * @tparam Args
 * 对象的类型Args,当Args不为空时，会将数据按`std::tuple<T,Args...>`格式将数据反序列化保存到参数中。
 * @tparam Reader 输入流类型Reader，该类型需要满足约束`struct_pack::reader_t`
 * @param t 待反序列化的对象t
 * @param reader 输入流
 * @param args
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败。
 *
 * 当反序列化失败时，t的值可能被部分修改。
 */
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::reader_t Reader>
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, Reader &reader,
                                                   Args &...args);

/**
 * \ingroup struct_pack
 * @brief 从视图中反序列化目的对象，反序列化时跳过开头的若干字节
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam T 对象的类型T
 * @tparam Args
 * 对象的类型Args,当Args不为空时，会将数据按`std::tuple<T,Args...>`格式将数据反序列化保存到参数中。
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param t 待反序列化的对象t
 * @param v 存有struct_pack序列化数据的视图
 * @param offset 跳过开头字节的长度
 * @param args 待反序列化的对象args
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 */
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] struct_pack::err_code deserialize_to_with_offset(T &t,
                                                               const View &v,
                                                               size_t &offset,
                                                               Args &...args);

/**
 * \ingroup struct_pack
 * @brief 将内存中的数据反序列化到目的对象，反序列化时跳过开头的若干字节
 *
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam T 对象的类型T
 * @tparam Args
 * 对象的类型Args,当Args不为空时，会将数据按`std::tuple<T,Args...>`格式将数据反序列化保存到参数中。
 * @param t 待反序列化的对象t
 * @param data 起始地址
 * @param size 数据长度
 * @param offset 跳过开头字节的长度
 * @param args 待反序列化的对象args
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 */
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args>
[[nodiscard]] struct_pack::err_code deserialize_to_with_offset(
    T &t, const char *data, size_t size, size_t &offset, Args &...args);

/**
 * \ingroup struct_pack
 * @brief 反序列化视图中的数据，并按值返回
 *
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 存有struct_pack序列化数据的视图
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  auto p2 = struct_pack::deserialize<person>(buffer);
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 *
 */
template <typename... Args, struct_pack::detail::deserialize_view View>
[[nodiscard]] auto deserialize(const View &v);

/**
 * \ingroup struct_pack
 *
 * @brief 反序列化内存中的数据，并按值返回
 *
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @param data 起始地址
 * @param size 数据长度
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  auto p2 = struct_pack::deserialize<person>(buffer.data(), buffer.size());
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 *
 */
template <typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size);

/**
 * \ingroup struct_pack
 * @brief 反序列化输入流中的数据，并按值返回
 *
 * @tparam Args
 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam Reader 输入流类型Reader，该类型需要满足约束`struct_pack::reader_t`
 * @param v 输入流
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  std::ifstream ifs("struct_pack_demo.data",
                     std::ofstream::in | std::ofstream::binary);
  auto p2 = struct_pack::deserialize<person>(ifs);
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 */
template <typename... Args, struct_pack::reader_t Reader>
[[nodiscard]] auto deserialize(Reader &v);

/**
 * \ingroup struct_pack
 * @brief 反序列化视图中的数据，并按值返回，并在参数中返回消耗的数据长度。
 *
 * @tparam Args
 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 存有struct_pack序列化数据的视图
 * @param consume_len 出参，保存消耗的数据长度。
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 当错误发生时，consume_len会被设为0。
 */
template <typename... Args, struct_pack::detail::deserialize_view View>
[[nodiscard]] auto deserialize(const View &v, size_t &consume_len);

/**
 * \ingroup struct_pack
 * @brief 反序列化内存中的数据，并按值返回，并在参数中返回消耗的数据长度。
 *
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @param data 起始地址
 * @param size 数据长度
 * @param consume_len 出参，保存消耗的数据长度。
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 当错误发生时，consume_len会被设为0。
 */
template <typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size,
                               size_t &consume_len);

/**
 * \ingroup struct_pack
 * @brief 按给定配置反序列化视图中的数据，并按值返回
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 存有struct_pack序列化数据的视图
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  auto p2 = struct_pack::deserialize<person>(buffer);
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 *
 */
template <uint64_t conf, typename... Args,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] auto deserialize(const View &v);

/**
 * \ingroup struct_pack
 *
 * @brief 按给定配置反序列化内存中的数据，并按值返回
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @param data 起始地址
 * @param size 数据长度
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  auto p2 = struct_pack::deserialize<person>(buffer.data(), buffer.size());
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 *
 */
template <uint64_t conf, typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size);

/**
 * \ingroup struct_pack
 * @brief 按给定配置反序列化输入流中的数据，并按值返回
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam Reader 输入流类型Reader，该类型需要满足约束`struct_pack::reader_t`
 * @param v 输入流
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 @code{.cpp}
  person p;
  std::ifstream ifs("struct_pack_demo.data",
                     std::ofstream::in | std::ofstream::binary);
  auto p2 = struct_pack::deserialize<person>(ifs);
  assert(p2);  // check if no error
  assert(p2.value() == p2); // check if value equal.
 @endcode
 */
template <uint64_t conf, typename... Args, struct_pack::reader_t Reader>
[[nodiscard]] auto deserialize(Reader &v);

/**
 * \ingroup struct_pack
 * @brief
 按给定配置反序列化视图中的数据，并按值返回，并在参数中返回消耗的数据长度。
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 存有struct_pack序列化数据的视图
 * @param consume_len 出参，保存消耗的数据长度。
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 当错误发生时，consume_len会被设为0。
 */
template <uint64_t conf, typename... Args,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] auto deserialize(const View &v, size_t &consume_len);

/**
 * \ingroup struct_pack
 * @brief
 * 按给定配置反序列化内存中的数据，并按值返回，并在参数中返回消耗的数据长度。
 *
 * @tparam conf 显式指定的序列化配置，详见`struct_pack::sp_config`
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @param data 起始地址
 * @param size 数据长度
 * @param consume_len 出参，保存消耗的数据长度。
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 当错误发生时，consume_len会被设为0。
 */
template <uint64_t conf, typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size,
                               size_t &consume_len);

/**
 * \ingroup struct_pack
 *
 * @brief 从视图中反序列化目的对象并保存到返回值，反序列化时跳过开头的若干字节
 *
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 存有struct_pack序列化数据的视图
 * @param offset 反序列化起始位置的偏移量
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 */
template <typename... Args, struct_pack::detail::deserialize_view View>
[[nodiscard]] auto deserialize_with_offset(const View &v, size_t &offset);

/**
 * \ingroup struct_pack
 * @brief
 * 从内存中反序列化目的对象并保存到返回值，反序列化时跳过开头的若干字节
 *
 * @tparam Args
 * 反序列化对象的类型，至少应填写一个，当填写多个时，按`std::tuple<Args...>`类型反序列化
 * @param data 起始地址
 * @param size 数据长度
 * @param offset 反序列化起始位置的偏移量
 * @return struct_pack::expected<Args, struct_pack::err_code>
 * 类型，当Args有多个参数时，返回值类型为`struct_pack::expected<std::tuple<Args...>,
 * struct_pack::err_code>`。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码：
 *
 @code{.cpp}
 * std::string hi = "Hello struct_pack";
 * std::vector<char> ho;
 * for (int i = 0; i < 100; ++i) {
 *   struct_pack::serialize_to(ho, hi + std::to_string(i));
 * }
 * for (size_t i = 0, offset = 0; i < 100; ++i) {
 *   auto ret = struct_pack::deserialize_with_offset<std::string>(ho, offset);
 *   CHECK(ret.has_value());
 *   CHECK(ret.value == hi + std::to_string(i));
 * }
 @endcode
 *
 */
template <typename... Args>
[[nodiscard]] auto deserialize_with_offset(const char *data, size_t size,
                                           size_t &offset);

/**
 *
 * \ingroup struct_pack
 * @brief 从视图中反序列化一个字段并保存到目的对象
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Field 字段类型
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param dst 目的对象
 * @param v 数据视图
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 *
 * 样例代码
 @code{.cpp}
  person p{20, "tom"};
  auto buffer = serialize(p);
  int age;
  auto ec = get_field_to<person, 0>(age, buffer);
  CHECK(!ec);
  CHECK(age == 20);
 @endcode
 *
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field, struct_pack::detail::deserialize_view View>
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, const View &v);
/**
 * \ingroup struct_pack
 * @brief 从内存中反序列化一个字段并保存到目的对象
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Field 字段类型
 * @param dst 目的对象
 * @param v 数据视图
 * @param data 起始地址
 * @param size 数据长度
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 * 样例代码
 @code{.cpp}
  person p{20, "tom"};
  auto buffer = serialize(p);
  std::string name;
  auto ec = get_field_to<person, 1>(name, buffer.data(), buffer.size());
  CHECK(!ec);
  CHECK(name == "tom");
 @endcode
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field>
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, const char *data,
                                                 size_t size);

/**
 * \ingroup struct_pack
 * @brief 从输入流中反序列化一个字段并保存到目的对象
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Field 字段类型
 * @tparam Reader 输入流类型，需要满足`struct_pack::reader_t`约束
 * @param dst 目的对象
 * @param reader 输入流
 * @return struct_pack::err_code
 * 返回错误码，如果返回值不等于`struct_pack::errc::ok`，说明反序列化失败
 *
 * 当反序列化失败时，t的值可能被部分修改。
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field, struct_pack::reader_t Reader>
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, Reader &reader);

/**
 * \ingroup struct_pack
 * @brief 从视图中反序列化一个字段并返回
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 视图
 * @return struct_pack::expected<TI, struct_pack::err_code>
 * 类型，其中TI代表T的第I个字段。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码
 @code{.cpp}
  person p{20, "tom"};
  auto buffer = serialize(p);
  std::string name;
  auto result = get_field_to<person, 1>(name, buffer);
  CHECK(result);
  CHECK(result.value() == "tom");
 @endcode
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] auto get_field(const View &v);

/**
 * \ingroup struct_pack
 * @brief 从内存中反序列化一个字段并返回
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @param data 起始地址
 * @param size 数据长度
 * @return struct_pack::expected<TI, struct_pack::err_code>
 * 类型，其中TI代表T的第I个字段。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 样例代码
  @code{.cpp}
  person p{20, "tom"};
  auto buffer = serialize(p);
  std::string name;
  auto result = get_field_to<person, 1>(name, buffer.data(), buffer.size());
  CHECK(result);
  CHECK(result.value() == "tom");
  @endcode
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT>
[[nodiscard]] auto get_field(const char *data, size_t size);

/**
 * \ingroup struct_pack
 * @brief 从输入流中反序列化一个字段并返回
 *
 * @tparam T 反序列化对象的类型
 * @tparam I 反序列化对象的第I个字段（从0开始计数）
 * @tparam conf 序列化配置，详见`struct_pack::sp_config`
 * @tparam Reader 输入流类型，需要满足`struct_pack::reader_t`约束
 * @param reader 输入流
 * @return struct_pack::expected<TI, struct_pack::err_code>
 * 类型，其中TI代表T的第I个字段。该类型存储了反序列化结果或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 */
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          struct_pack::reader_t Reader>
[[nodiscard]] auto get_field(Reader &reader);

/**
 * \ingroup struct_pack
 * @brief 从视图中反序列化派生类到基类的指针
 *
 * @tparam BaseClass 基类类型
 * @tparam DerivedClasses 所有可能的派生类类型
 * @tparam View 视图类型，需满足`struct_pack::detail::deserialize_view`约束
 * @param v 视图
 * @return struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
 * 类型，该类型存储了反序列化结果（`std::unique_ptr<BaseClass>`）或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 本函数用于在不知道派生类具体类型的情况下，将其反序列化到基类的指针。
 *
 * 样例代码
  @code{.cpp}
  //    base
  //   /   |
  //  obj1 obj2
  //  |
  //  obj3
  auto ret = struct_pack::serialize(obj3{});
  auto result =
      struct_pack::deserialize_derived_class<base, obj1, obj2, obj3>(ret);
  assert(result.has_value());   // check deserialize ok
  std::unique_ptr<base> ptr = std::move(result.value());
  assert(ptr != nullptr);
  @endcode
 */
template <typename BaseClass, typename... DerivedClasses,
          struct_pack::detail::deserialize_view View>
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(const View &v);

/**
 * \ingroup struct_pack
 * @brief 从内存中反序列化派生类到基类的指针
 *
 * @tparam BaseClass 基类类型
 * @tparam DerivedClasses 所有可能的派生类类型
 * @param data 起始地址
 * @param size 数据长度
 * @return struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
 * 类型，该类型存储了反序列化结果（`std::unique_ptr<BaseClass>`）或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 *
 * 本函数用于在不知道派生类具体类型的情况下，将其反序列化到基类的指针。
 *
 * 样例代码
  @code{.cpp}
  //    base
  //   /   |
  //  obj1 obj2
  //  |
  //  obj3
  auto ret = struct_pack::serialize(obj3{});
  auto result =
      struct_pack::deserialize_derived_class<base, obj1, obj2, obj3>(ret.data(),
 ret.size());
  assert(result.has_value());   // check deserialize ok
  std::unique_ptr<base> ptr = std::move(result.value());
  assert(ptr != nullptr);
  @endcode
 */
template <typename BaseClass, typename... DerivedClasses>
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(const char *data, size_t size);

/**
 * \ingroup struct_pack
 * @brief 从输入流中反序列化派生类到基类的指针
 *
 * @tparam BaseClass 基类类型
 * @tparam DerivedClasses 所有可能的派生类类型
 * @tparam Reader 输入流类型，需要满足`struct_pack::reader_t`约束
 * @param reader
 * @return struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
 * 类型，该类型存储了反序列化结果（`std::unique_ptr<BaseClass>`）或`struct_pack::err_code`类型的错误码。详见`struct_pack::expected`
 */
template <typename BaseClass, typename... DerivedClasses,
          struct_pack::reader_t Reader>
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(Reader &reader);

}  // namespace struct_pack
