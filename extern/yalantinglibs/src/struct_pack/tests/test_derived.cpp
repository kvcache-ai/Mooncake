#include "test_derived.hpp"
namespace test1 {
struct Base {
  int id = 17;
  Base(){};
  static struct_pack::expected<std::unique_ptr<Base>, struct_pack::err_code>
  deserialize(std::string_view sv);
  virtual std::string get_name() const { return "Base"; };
  friend bool operator==(const Base& a, const Base& b) { return a.id == b.id; }
  virtual ~Base(){};
};
YLT_REFL(Base, id);
struct foo : public Base {
  std::string hello = "1";
  std::string hi = "2";
  virtual std::string get_name() const override { return "foo"; }
  friend bool operator==(const foo& a, const foo& b) {
    return a.id == b.id && a.hello == b.hello && a.hi == b.hi;
  }
};
YLT_REFL(foo, id, hello, hi);
struct foo2 : public Base {
  std::string hello = "1";
  std::string hi = "2";
  virtual std::string get_name() const override { return "foo2"; }
  friend bool operator==(const foo2& a, const foo2& b) {
    return a.id == b.id && a.hello == b.hello && a.hi == b.hi;
  }
  static constexpr int struct_pack_id = 114514;
};
YLT_REFL(foo2, id, hello, hi);
struct foo3 : public Base {
  std::string hello = "1";
  std::string hi = "2";
  virtual std::string get_name() const override { return "foo3"; }
  friend bool operator==(const foo3& a, const foo3& b) {
    return a.id == b.id && a.hello == b.hello && a.hi == b.hi;
  }
};
YLT_REFL(foo3, id, hello, hi);
struct foo4 : public Base {
  std::string hello = "1";
  std::string hi = "2";
  virtual std::string get_name() const override { return "foo4"; }
  friend bool operator==(const foo4& a, const foo4& b) {
    return a.id == b.id && a.hello == b.hello && a.hi == b.hi;
  }
};
constexpr int struct_pack_id(foo4*) { return 112233211; }
YLT_REFL(foo4, id, hello, hi);
struct bar : public Base {
  int oh = 1;
  int no = 2;
  virtual std::string get_name() const override { return "bar"; }
  friend bool operator==(const bar& a, const bar& b) {
    return a.id == b.id && a.oh == b.oh && a.no == b.no;
  }
};
YLT_REFL(bar, id, oh, no);
struct gua : Base {
  std::string a = "Hello";
  int b = 1;
  virtual std::string get_name() const override { return "gua"; }
  friend bool operator==(const gua& a, const gua& b) {
    return a.id == b.id && a.a == b.a && a.b == b.b;
  }
};
YLT_REFL(gua, id, a, b);

struct_pack::expected<std::unique_ptr<Base>, struct_pack::err_code>
Base::deserialize(std::string_view sv) {
  return struct_pack::deserialize_derived_class<Base, bar, foo, gua, foo2,
                                                foo4>(sv);
}
}  // namespace test1
TEST_CASE("testing derived") {
  using namespace std::string_literals;
  using namespace test1;
  static_assert(struct_pack::detail::has_user_defined_id_ADL<foo4>);
  Base base;
  foo f;
  foo2 f2;
  foo4 f4;
  bar b;
  gua g;
  std::vector vecs{struct_pack::serialize<std::string>(f),
                   struct_pack::serialize<std::string>(b),
                   struct_pack::serialize<std::string>(g),
                   struct_pack::serialize<std::string>(f2),
                   struct_pack::serialize<std::string>(f4),
                   struct_pack::serialize<std::string>(base)};
  auto f1 = Base::deserialize(vecs[0]);
  auto b1 = Base::deserialize(vecs[1]);
  auto g1 = Base::deserialize(vecs[2]);
  auto f21 = Base::deserialize(vecs[3]);
  auto f41 = Base::deserialize(vecs[4]);
  auto base1 = Base::deserialize(vecs[5]);
  auto& i = *(foo*)(f1->get());
  CHECK(i == f);
  CHECK(*(foo2*)(f21->get()) == f2);
  CHECK(*(bar*)(b1->get()) == b);
  CHECK(*(gua*)(g1->get()) == g);
  CHECK(*(base1->get()) == base);
  std::vector<std::pair<std::unique_ptr<Base>, std::string>> vec;
  vec.emplace_back(std::move(f1.value()), "foo");
  vec.emplace_back(std::move(f21.value()), "foo2");
  vec.emplace_back(std::move(b1.value()), "bar");
  vec.emplace_back(std::move(g1.value()), "gua");
  vec.emplace_back(std::move(f41.value()), "foo4");
  vec.emplace_back(std::move(base1.value()), "Base");
  for (const auto& e : vec) {
    CHECK(e.first->get_name() == e.second);
  }
}

TEST_CASE("test hash collision") {
  using namespace test1;
  static_assert(
      struct_pack::detail::MD5_set<
          std::tuple<foo, bar, foo2, gua, foo3, foo4>>::has_hash_collision !=
      0);
}

TEST_CASE("test unique_ptr<Base>") {
  using namespace test2;
  static_assert(struct_pack::detail::is_base_class<base>);
  std::vector<std::unique_ptr<base>> vec;
  vec.emplace_back(std::make_unique<derived4>());
  vec.emplace_back(std::make_unique<derived3>());
  vec.emplace_back(std::make_unique<derived2>());
  vec.emplace_back(std::make_unique<derived1>());
  vec.emplace_back(std::make_unique<base>());
  auto buffer = struct_pack::serialize(vec);
  auto res =
      struct_pack::deserialize<std::vector<std::unique_ptr<base>>>(buffer);
  CHECK(res);
  auto& vec2 = res.value();
  CHECK(vec2.size() == vec.size());
  for (std::size_t i = 0; i < vec.size(); ++i) {
    CHECK(vec[i]->get_name() == vec2[i]->get_name());
  }
}

TEST_CASE("test vector<unique_ptr<Base>> with virtual base") {
  using namespace test3;
  static_assert(struct_pack::detail::is_base_class<base>);
  std::vector<std::unique_ptr<base>> vec;
  vec.emplace_back(std::make_unique<derived4>());
  vec.emplace_back(std::make_unique<derived3>());
  vec.emplace_back(std::make_unique<derived2>());
  vec.emplace_back(std::make_unique<derived1>());
  auto buffer = struct_pack::serialize(vec);
  auto res =
      struct_pack::deserialize<std::vector<std::unique_ptr<base>>>(buffer);
  CHECK(res);
  auto& vec2 = res.value();
  CHECK(vec2.size() == vec.size());
  for (std::size_t i = 0; i < vec.size(); ++i) {
    CHECK(vec[i]->get_name() == vec2[i]->get_name());
  }
}

TEST_CASE("test unique_ptr<Base> with virtual base") {
  using namespace test3;
  std::unique_ptr<base> ptr = std::make_unique<derived4>();
  auto buffer2 = struct_pack::serialize<std::string>(ptr);
  auto res2 = struct_pack::deserialize<std::unique_ptr<base>>(buffer2);
  CHECK(res2);
  CHECK(res2.value()->get_name() == std::make_unique<derived4>()->get_name());
}

namespace derived_class_contain_another_derived_class {

struct base {
  virtual uint32_t get_struct_pack_id() const = 0;
  virtual std::string get_name() const = 0;
  static std::unique_ptr<base> deserialize(std::string& serialized);
  virtual ~base(){};
};

struct derived1 : public base {
  int b;
  virtual uint32_t get_struct_pack_id() const override;
  std::string get_name() const override { return "derived1"; }
};
YLT_REFL(derived1, b);

struct derived2 : public base {
  std::string c;
  std::unique_ptr<derived1> child;
  virtual uint32_t get_struct_pack_id() const override;
  std::string get_name() const override { return "derived2"; }
};

YLT_REFL(derived2, c, child);

STRUCT_PACK_DERIVED_IMPL(base, derived1, derived2);

std::unique_ptr<base> base::deserialize(std::string& serialized) {
  return struct_pack::deserialize_derived_class<base, derived1, derived2>(
             serialized)
      .value();
}

}  // namespace derived_class_contain_another_derived_class

TEST_CASE("test derived class contain by other derived class") {
  using namespace derived_class_contain_another_derived_class;
  {
    auto serialized = struct_pack::serialize<std::string>(derived1{});
    auto x = base::deserialize(serialized);
    REQUIRE(x);
    CHECK(x->get_name() == "derived1");
  }
  {
    auto serialized = struct_pack::serialize<std::string>(derived2{});
    auto x = base::deserialize(serialized);
    REQUIRE(x);
    CHECK(x->get_name() == "derived2");
  }
}
