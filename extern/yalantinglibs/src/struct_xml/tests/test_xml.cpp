#include <deque>
#include <iostream>
#include <iterator>
#include <list>
#include <optional>
#include <vector>

#include "doctest.h"
#include "ylt/struct_xml/xml_reader.h"
#include "ylt/struct_xml/xml_writer.h"

struct Owner_t {
  std::string ID;
  std::string DisplayName;
  auto operator==(const Owner_t &rhs) const {
    return ID == rhs.ID && DisplayName == rhs.DisplayName;
  }
};
YLT_REFL(Owner_t, ID, DisplayName);
struct Contents {
  std::string Key;
  std::string LastModified;
  std::string ETag;
  std::string Type;
  uint32_t Size;
  std::string StorageClass;
  Owner_t Owner;

  auto operator==(const Contents &rhs) const {
    return Key == rhs.Key && LastModified == rhs.LastModified &&
           ETag == rhs.ETag && Type == rhs.Type && Size == rhs.Size &&
           StorageClass == rhs.StorageClass && Owner == rhs.Owner;
  }
};
YLT_REFL(Contents, Key, LastModified, ETag, Type, Size, StorageClass, Owner);

TEST_CASE("simple test") {
  Contents contents{"key", "ddd", "ccc", "aaa", 123, "aaa", {"bbb", "sss"}};
  {
    std::string ss;
    iguana::to_xml(contents, ss);

    Contents contents2;
    iguana::from_xml(contents2, ss);
    CHECK(contents == contents2);
  }
  {
    std::string ss;
    iguana::to_xml<true>(contents, ss);
    std::cout << ss;
    Contents contents2;
    iguana::from_xml(contents2, ss);
    CHECK(contents == contents2);
  }
}

template <class T>
struct Haha {
  std::string msg;
  YLT_REFL(Haha, msg);
};

template <typename T>
inline constexpr std::string_view get_alias_struct_name(Haha<T> *) {
  return "haha";
}

TEST_CASE("template class test") {
  Haha<int> ha{"test"};
  std::string ss;
  iguana::to_xml(ha, ss);
  std::cout << ss << "\n";

  Haha<int> ha1{};
  iguana::from_xml(ha1, ss);
  CHECK(ha1.msg == ha.msg);
}

struct optional_t {
  int a;
  std::optional<int> b;
  std::optional<std::string> c;
  bool d;
  char e;
};
YLT_REFL(optional_t, a, b, c, d, e);

struct list_t {
  std::vector<optional_t> list;
  int id;
};
YLT_REFL(list_t, list, id);

TEST_CASE("test vector") {
  auto validator = [](list_t l) {
    CHECK(l.list[0].a == 1);
    CHECK(*l.list[0].b == 2);
    CHECK(!l.list[0].c);
    CHECK(l.list[0].d == false);
    CHECK(l.list[0].e == 'o');

    CHECK(l.list[1].a == 3);
    CHECK(*l.list[1].b == 4);
    CHECK(!l.list[1].c);
    CHECK(l.list[1].d == true);
    CHECK(l.list[1].e == 'k');
  };
  list_t l;
  l.list.push_back(optional_t{1, 2, {}, 0, 'o'});
  l.list.push_back(optional_t{3, 4, {}, 1, 'k'});

  {
    std::string ss;
    iguana::to_xml(l, ss);

    list_t l1;
    iguana::from_xml(l1, ss);
    validator(l1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(l, ss);

    list_t l1;
    iguana::from_xml(l1, ss);
    validator(l1);
  }
}

struct test_arr_t {
  std::vector<iguana::xml_attr_t<int, std::map<std::string_view, int>>> item;
};
YLT_REFL(test_arr_t, item);

TEST_CASE("test vector with attr") {
  auto validator = [](test_arr_t &array) {
    auto &arr = array.item;
    CHECK(arr[0].attr()["index"] == 0);
    CHECK(arr[1].attr()["index"] == 1);
    CHECK(arr[2].attr()["index"] == 2);
    CHECK(arr[3].attr()["index"] == 3);
    CHECK(arr[0].value() == 1);
    CHECK(arr[1].value() == 2);
    CHECK(arr[2].value() == 3);
    CHECK(arr[3].value() == 4);
  };
  std::string str = R"(
  <test_arr_t size="4">
    <item index="0">1</item>
    <item index="1">2</item>
    <item index="2">3</item>
    <item index="3">4 </item>
  </test_arr_t>
  )";
  test_arr_t arr;
  iguana::from_xml(arr, str);
  validator(arr);

  {
    std::string ss;
    iguana::to_xml(arr, ss);
    test_arr_t arr1;
    iguana::from_xml(arr1, ss);
    validator(arr1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(arr, ss);
    test_arr_t arr1;
    iguana::from_xml(arr1, ss);
    validator(arr1);
  }
}

enum class enum_status {
  start,
  stop,
};
struct child_t {
  int key1;
  int key2;
};
YLT_REFL(child_t, key1, key2);
struct some_type_t {
  std::vector<float> price;
  std::optional<std::string> description;
  std::unique_ptr<child_t> child;
  bool hasdescription;
  char c;
  std::optional<double> d_v;
  std::string name;
  std::string_view addr;
  enum_status status;
};
YLT_REFL(some_type_t, price, description, child, hasdescription, c, d_v, name,
         addr, status);

TEST_CASE("test parse_done") {
  std::string str = R"(
<child>
  <key1>10</key1>
</child>
  )";
  child_t c;
  iguana::from_xml(c, str);
  CHECK(c.key1 == 10);
}

TEST_CASE("test some type") {
  auto validator_some_type = [](const some_type_t &s) {
    CHECK(s.price[0] == 1.23f);
    CHECK(s.price[1] == 3.25f);
    CHECK(s.price[2] == 9.57f);
    CHECK(*s.description == "Some description");
    CHECK(s.child->key1 == 10);
    CHECK(s.child->key2 == 20);
    CHECK(s.hasdescription == true);
    CHECK(s.c == 'X');
    CHECK(*s.d_v == 3.14159);
    CHECK(s.name == "张三");
    CHECK(s.addr == "123 Main St");
    CHECK(s.status == enum_status::stop);
  };
  std::string str = R"(
<?xml version="1.0" encoding="UTF-8"?>
<some_type_t a="b" b="c">
	<price>1.23</price>
  <?myapp instruction?>
	<price>3.25</price>
  <![CDATA[ node2</p>]]>
	<price>9.57</price>
	<description>Some description </description>
	<child>
		<key1>10</key1>
		<key2>20</key2>
	</child>
	<hasdescription>true</hasdescription>
	<c>X</c>
	<d_v>3.14159</d_v>
	<name>张三</name>
	<addr>123 Main St</addr>
	<status>1</status>
</some_type_t>
)";
  some_type_t st;
  iguana::from_xml(st, str);
  validator_some_type(st);

  {
    std::string ss;
    iguana::to_xml(st, ss);

    some_type_t st1;
    iguana::from_xml(st1, ss);
    validator_some_type(st1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(st, ss);
    std::cout << ss;
    some_type_t st1;
    iguana::from_xml(st1, ss);
    validator_some_type(st1);
  }
}

struct book_t {
  std::string title;
  std::string author;
};
YLT_REFL(book_t, title, author);
struct library {
  iguana::xml_attr_t<book_t> book;
};
YLT_REFL(library, book);
TEST_CASE("test library with attr") {
  auto validator = [](library lib) {
    CHECK(lib.book.attr()["id"] == "1234");
    CHECK(lib.book.attr()["language"] == "en");
    CHECK(lib.book.attr()["edition"] == "1");
    CHECK(lib.book.value().title == "Harry Potter and the Philosopher's Stone");
    CHECK(lib.book.value().author == "J.K. Rowling");
  };
  std::string str = R"(
  <?xml version="1.0" encoding="UTF-8"?>
  <library name="UESTC library">
    <book id="1234" language="en" edition="1">
      <title>Harry Potter and the Philosopher's Stone</title>
      <author>J.K. Rowling</author>
    </book>
  </library>
)";
  {
    library lib;
    iguana::from_xml(lib, str);
    validator(lib);
  }
  {
    iguana::xml_attr_t<library> lib;
    iguana::from_xml(lib, str);
    CHECK(lib.attr()["name"] == "UESTC library");
    validator(lib.value());

    {
      std::string ss;
      iguana::to_xml(lib, ss);
      iguana::xml_attr_t<library> lib1;
      iguana::from_xml(lib1, ss);
      validator(lib1.value());
      std::cout << ss << std::endl;
    }
    {
      std::string ss;
      iguana::to_xml<true>(lib, ss);
      iguana::xml_attr_t<library> lib1;
      iguana::from_xml(lib1, ss);
      validator(lib1.value());
      std::cout << ss << std::endl;
    }
  }
}

struct package_t {
  iguana::xml_attr_t<std::optional<std::string_view>> version;
  iguana::xml_attr_t<std::string_view> changelog;
};
YLT_REFL(package_t, version, changelog);
TEST_CASE("test example package") {
  auto validator = [](iguana::xml_attr_t<package_t> package) {
    CHECK(package.attr()["name"] == "apr-util-ldap");
    CHECK(package.attr()["arch"] == "x86_64");
    auto &p = package.value();
    CHECK(p.version.attr()["epoch"] == "0");
    CHECK(p.version.attr()["ver"] == "1.6.1");
    CHECK(p.version.attr()["rel"] == "6.el8");
    CHECK(p.changelog.attr()["author"] == "Lubo");
    CHECK(p.changelog.attr()["date"] == "1508932800");
    CHECK(p.changelog.value() == "new version 1.6.1");
  };
  std::string str = R"(
    <?xml version="1.0" encoding="UTF-8"?>
    <package_t name="apr-util-ldap" arch="x86_64">
      <version epoch="0" ver="1.6.1" rel="6.el8"/>
      <changelog author="Lubo" date="1508932800">
      new version 1.6.1</changelog>
    </package_t>
  )";
  iguana::xml_attr_t<package_t> package;
  iguana::from_xml(package, str);
  validator(package);

  {
    std::string ss;
    iguana::to_xml(package, ss);
    iguana::xml_attr_t<package_t> package1;
    iguana::from_xml(package1, ss);
    validator(package1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(package, ss);
    iguana::xml_attr_t<package_t> package1;
    iguana::from_xml(package1, ss);
    validator(package1);
  }
}

struct description_t {
  iguana::xml_cdata_t<std::string> cdata;
};
YLT_REFL(description_t, cdata);
struct node_t {
  std::string title;
  description_t description;
  iguana::xml_cdata_t<> cdata;
};
YLT_REFL(node_t, title, description, cdata);
TEST_CASE("test example cdata") {
  auto validator = [](node_t node) {
    CHECK(node.title == "what's the cdata");
    CHECK(node.cdata.value() == "<p>this is a  cdata node</p>");
    CHECK(node.description.cdata.value() ==
          "<p>nest cdata node1 and </p>node2</p>");
  };
  std::string str = R"(
    <?xml version="1.0" encoding="UTF-8"?>
    <node_t>
      <title>what's the cdata</title>
      <description>
        <![CDATA[<p>nest cdata node1 and </p>]]>
        <!-- This is a comment -->
        <![CDATA[ node2</p>]]>
      </description>
      <!DOCTYPE test node>
      <?myapp instruction?>
      <!-- <price>3.25</price> -->
      <!-- <price a="b"/> -->
      <![CDATA[<p>this is a  cdata node</p>]]>
    </node_t>
  )";
  // only parse cdata node
  node_t node;
  iguana::from_xml(node, str);
  validator(node);

  {
    std::string ss;
    iguana::to_xml(node, ss);
    node_t node1;
    iguana::from_xml(node1, ss);
    validator(node1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(node, ss);
    node_t node1;
    iguana::from_xml(node1, ss);
    validator(node1);
  }
}

struct test_exception_t {
  std::unique_ptr<int> a;
  bool b;
  char c;
};
YLT_REFL(test_exception_t, a, b, c);
TEST_CASE("test exception") {
  {
    std::string str = "<root> <a>d3</a> </root>";
    test_exception_t t;
    CHECK_THROWS(iguana::from_xml(t, str));
  }
  {
    std::string str = "<root> <b>TURE</b> </root>";
    test_exception_t t;
    CHECK_THROWS(iguana::from_xml(t, str));
  }
  {
    std::string str = "<root> <c>ab</c> </root>";
    test_exception_t t;
    CHECK_THROWS(iguana::from_xml(t, str));
  }
  {
    std::string str = "<root> <d>ab</d> </root>";
    test_exception_t t;
    CHECK_THROWS(iguana::from_xml(t, str));
  }
  {
    test_exception_t t;
    t.b = true;
    t.c = 'a';
    std::string ss;
    iguana::to_xml(t, ss);
  }
}

struct city_t {
  iguana::xml_attr_t<long> area;
  iguana::xml_cdata_t<> cd;
  std::string name;
};
YLT_REFL(city_t, area, cd, name);
struct cities_t {
  std::vector<city_t> city;
};
YLT_REFL(cities_t, city);
struct province {
  iguana::xml_attr_t<long> area;
  iguana::xml_cdata_t<> cd;
  std::unique_ptr<cities_t> cities;
  std::string capital;
};
YLT_REFL(province, area, cd, cities, capital);

TEST_CASE("test province example") {
  auto validator = [](province &p) {
    CHECK(p.capital == "Chengdu");
    CHECK(p.cd.value() == "sichuan <>");
    CHECK(p.area.value() == 485000);
    CHECK(p.area.attr()["unit"] == "km^2");
    CHECK(p.cities->city[0].name == "Chengdu City");
    CHECK(p.cities->city[0].cd.value() == "chengdu <>");
    CHECK(p.cities->city[0].area.attr()["unit"] == "km^2");
    CHECK(p.cities->city[0].area.value() == 14412);

    CHECK(p.cities->city[1].name == "Suining City");
    CHECK(p.cities->city[1].cd.value() == "Suining <>");
    CHECK(p.cities->city[1].area.attr()["unit"] == "km^2");
    CHECK(p.cities->city[1].area.value() == 20788);
  };
  std::string str = R"(
<!-- this is a <province xml> -->
<province name="Sichuan Province">
  <capital>Chengdu</capital>
  <![CDATA[ sichuan <> ]]>
  <area unit="km^2">485000</area>
  <cities>
    <city>
      <name>Chengdu City</name>
      <![CDATA[ chengdu <> ]]>
      <area unit="km^2">14412</area>
    </city>
    <city>
      <name>Suining City</name>
      <![CDATA[ Suining <> ]]>
      <area unit="km^2">20788</area>
    </city>
    <!-- <city a="b" /> -->
    <!-- More cities -->
  </cities>
</province>
  )";
  province p;
  iguana::from_xml(p, str);
  validator(p);

  {
    std::string ss;
    iguana::to_xml(p, ss);
    province p1;
    iguana::from_xml(p1, str);
    validator(p1);
  }
  {
    std::string ss;
    iguana::to_xml<true>(p, ss);
    province p1;
    iguana::from_xml(p1, str);
    validator(p1);
  }
}

TEST_CASE("test get_number") {
  std::string str = "3.14";
  CHECK(iguana::get_number<float>(str) == 3.14f);
}

struct some_book {
  std::string_view title;
  std::string_view author;
};
YLT_REFL(some_book, title, author);
REQUIRED(some_book, title, author);

TEST_CASE("test required filed") {
  some_book book{"book", "tom"};
  std::string xml_str;
  iguana::to_xml(book, xml_str);

  std::cout << xml_str << "\n";

  std::string s1 = R"(<book_t><title>book</title></book_t>)";
  some_book b;
  CHECK_THROWS_AS(iguana::from_xml(b, s1), std::invalid_argument);

  std::string s2 = R"(<book_t><author>tom</author></book_t>)";
  CHECK_THROWS_AS(iguana::from_xml(b, s2), std::invalid_argument);
}

enum class Fruit {
  APPLE = 9999,
  BANANA = -4,
  ORANGE = 10,
  MANGO = 99,
  CHERRY = 7,
  GRAPE = 100000
};
enum class Color {
  BULE = 10,
  RED = 15,
};
enum class Status { stop = 10, start };
namespace iguana {
template <>
struct enum_value<Fruit> {
  constexpr static std::array<int, 6> value = {9999, -4, 10, 99, 7, 100000};
};

}  // namespace iguana
struct test_enum_t {
  Fruit a;
  Fruit b;
  Fruit c;
  Fruit d;
  Fruit e;
  Fruit f;
  Color g;
  Color h;
};
YLT_REFL(test_enum_t, a, b, c, d, e, f, g, h);

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)

TEST_CASE("test enum") {
  auto validator = [](test_enum_t e) {
    CHECK(e.a == Fruit::APPLE);
    CHECK(e.b == Fruit::BANANA);
    CHECK(e.c == Fruit::ORANGE);
    CHECK(e.d == Fruit::MANGO);
    CHECK(e.e == Fruit::CHERRY);
    CHECK(e.f == Fruit::GRAPE);
    CHECK(e.g == Color::BULE);
    CHECK(e.h == Color::RED);
  };
  std::string str = R"(
<?xml version="1.0" encoding="UTF-8" ?>
<test_enum_t>
  <a>APPLE</a>
  <b>BANANA</b>
  <c>ORANGE</c>
  <d>MANGO</d>
  <e>CHERRY</e>
  <f>GRAPE</f>
  <g>10</g>
  <h>15</h>
</test_enum_t>
  )";
  test_enum_t e;
  iguana::from_xml(e, str);
  validator(e);

  std::string ss;
  iguana::to_xml(e, ss);
  std::cout << ss << std::endl;
  test_enum_t e1;
  iguana::from_xml(e1, ss);
  validator(e1);
}

enum class State { STOP = 10, START };
namespace iguana {
template <>
struct enum_value<State> {
  constexpr static std::array<int, 1> value = {10};
};
}  // namespace iguana

struct enum_exception_t {
  State a;
  State b;
};
YLT_REFL(enum_exception_t, a, b);

TEST_CASE("enum exception") {
  std::string str = R"(
<enum_exception_t>
  <a>START</a>
  <b>STOP</b>
</enum_exception_t>
  )";
  {
    enum_exception_t e;
    CHECK_THROWS(iguana::from_xml(e, str));
  }
  {
    enum_exception_t e;
    std::string ss;
    e.a = State::START;
    e.b = State::STOP;
    CHECK_THROWS(iguana::to_xml(e, ss));
  }
}
#endif

class some_object {
  int id;
  std::string name;

 public:
  some_object() = default;
  some_object(int i, std::string str) : id(i), name(str) {}
  int get_id() const { return id; }
  std::string get_name() const { return name; }
  YLT_REFL(some_object, id, name);
};

TEST_CASE("test inner reflection") {
  some_object obj{20, "tom"};
  std::string str;
  iguana::to_xml(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_xml(obj1, str);
  CHECK(obj1.get_id() == 20);
  CHECK(obj1.get_name() == "tom");
}

struct Contents_t {
  std::unique_ptr<std::vector<std::unique_ptr<int>>> vec;
  std::shared_ptr<std::vector<std::unique_ptr<int>>> vec_s;
  std::string b;
};
YLT_REFL(Contents_t, vec, vec_s, b);

TEST_CASE("test smart_ptr") {
  std::string str = R"(
<Contents_t>
	<vec>42</vec>
	<vec>21</vec>
	<vec_s>15</vec_s>
	<vec_s>16</vec_s>
	<b>test</b>
</Contents_t>
  )";
  auto validator = [](Contents_t &cont) {
    CHECK(cont.b == "test");
    CHECK(*(*cont.vec)[0] == 42);
    CHECK(*(*cont.vec)[1] == 21);
    CHECK(*(*cont.vec_s)[0] == 15);
    CHECK(*(*cont.vec_s)[1] == 16);
  };

  Contents_t cont;
  iguana::from_xml(cont, str);
  validator(cont);

  std::string ss;
  iguana::to_xml(cont, ss);
  Contents_t cont1;
  iguana::from_xml(cont1, ss);
  validator(cont1);
}

TEST_CASE("test throw while parsing an illegal number") {
  {
    std::string str = R"(
  <Contents_t>
    <vec>42A</vec>
    <vec_s>15</vec_s>
    <b>test</b>
  </Contents_t>
    )";
    Contents_t cont;
    CHECK_THROWS(iguana::from_xml(cont, str));
  }
  {
    std::string str = R"(
<Contents_t>
	<vec>42</vec>
	<vec_s>15.7</vec_s>
	<b>test</b>
</Contents_t>
  )";
    Contents_t cont;
    CHECK_THROWS(iguana::from_xml(cont, str));
  }
}

struct next_obj_t {
  int x;
  int y;
};
YLT_REFL(next_obj_t, x, y);

inline constexpr std::string_view get_alias_struct_name(next_obj_t *) {
  return "next";
}

inline constexpr auto get_alias_field_names(next_obj_t *) {
  return std::array{ylt::reflection::field_alias_t{"w", 0},
                    ylt::reflection::field_alias_t{"h", 1}};
}

struct out_object {
  std::unique_ptr<int> id;
  std::string_view name;
  next_obj_t obj;
  YLT_REFL(out_object, id, name, obj);
};

inline constexpr std::string_view get_alias_struct_name(out_object *) {
  return "qi";
}

inline constexpr auto get_alias_field_names(out_object *) {
  return std::array{ylt::reflection::field_alias_t{"w", 0},
                    ylt::reflection::field_alias_t{"h", 1}};
}

TEST_CASE("test alias") {
  out_object m{std::make_unique<int>(20), "tom", {21, 42}};
  std::string xml_str;
  iguana::to_xml(m, xml_str);

  out_object m1;
  iguana::from_xml(m1, xml_str);
  CHECK(m1.name == "tom");
  CHECK(m1.obj.x == 21);
  CHECK(m1.obj.y == 42);
}

struct text_t {
  using escape_attr_t =
      iguana::xml_attr_t<std::string, std::map<std::string_view, std::string>>;
  escape_attr_t ID;
  std::string DisplayName;
};
YLT_REFL(text_t, ID, DisplayName);
TEST_CASE("test escape") {
  using text_attr_type =
      iguana::xml_attr_t<text_t, std::map<std::string_view, std::string>>;
  {
    std::string str = R"(
    <text_t description="&quot;&lt;'&#x5c0f;&#24378;'&gt;&quot;">
      <ID ID'msg='{"msg&apos;reply": "it&apos;s ok"}'>&amp;&lt;&gt;</ID>
      <DisplayName>&#x5c0f;&#24378;</DisplayName>
    </text_t>
    )";

    auto validator = [](const text_attr_type &text) {
      auto v = text.value();
      auto attr = text.attr();
      CHECK(attr["description"] == R"("<'小强'>")");
      CHECK(v.ID.value() == R"(&<>)");
      CHECK(v.ID.attr()["ID'msg"] == R"({"msg'reply": "it's ok"})");
      CHECK(v.DisplayName == "小强");
    };
    text_attr_type text;
    iguana::from_xml(text, str);
    validator(text);
    std::string ss;
    iguana::to_xml<true>(text, ss);

    text_attr_type text1;
    iguana::from_xml(text1, ss);
    validator(text1);
  }
  {
    std::string str = R"(
    <text_t description=ABC>
      <ID ID'msg='{"msg&apos;reply": "it&apos;s ok"}'>&amp;&lt;&gt;</ID>
      <DisplayName>&#x5c0f;&#24378;</DisplayName>
    </text_t>
    )";
    text_attr_type text;
    CHECK_THROWS(iguana::from_xml(text, str));
  }
}
