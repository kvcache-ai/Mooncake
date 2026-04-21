#undef THROW_UNKNOWN_KEY
#define XML_ATTR_USE_APOS
#define XML_ESCAPE_UNICODE
#include <deque>
#include <iostream>
#include <iterator>
#include <list>
#include <optional>
#include <vector>

#include "doctest.h"
#include "ylt/struct_xml/xml_reader.h"
#include "ylt/struct_xml/xml_writer.h"

enum class enum_status {
  paid,
  unpaid,
};
struct order_t {
  enum_status status;
  float total;
};
YLT_REFL(order_t, status, total);

TEST_CASE("test unkonwn key") {
  auto validator = [](order_t od) {
    CHECK(od.status == enum_status::unpaid);
    CHECK(od.total == 65.0f);
  };
  std::string str = R"(
<order_t>
  <orderID>12345</orderID>
  <status>1</status>
  <phone />
  <customer>
    <firstName>John</firstName>
    <![CDATA[ node2</p>]]>
    <lastName>Doe</lastName>
    <address>
      <street>123 Main St</street>
      <city>Anytown</city>
      <state>CA</state>
      <zip>12345</zip>
    </address>
  </customer>
  <items>
    <item>
      <productID>67890</productID>
      <name>Widget A</name>
      <quantity>2</quantity>
      <price>10.00</price>
    </item>
    <item>
      <productID>98765</productID>
      <name>Widget B</name>
      <quantity>3</quantity>
      <price>15.00</price>
    </item>
  </items>
  <![CDATA[ node2</p>]]>
  <total>65.00</total>
</order_t>
  )";
  order_t od;
  iguana::from_xml(od, str);
  validator(od);

  std::string ss;
  iguana::to_xml(od, ss);
  order_t od1;
  iguana::from_xml(od1, ss);
  validator(od1);
}

TEST_CASE("test exception") {
  std::string str = R"(
<order_t>
  <orderID>12345</orderID>
  <c>a</c>
  <e>
</order_t>
)";
  order_t od;
  CHECK_THROWS(iguana::from_xml(od, str));
}

struct text_t {
  using escape_attr_t =
      iguana::xml_attr_t<std::string, std::map<std::string_view, std::string>>;
  escape_attr_t ID;
  std::string DisplayName;
};
YLT_REFL(text_t, ID, DisplayName);
TEST_CASE("test escape") {
  {
    std::string str = R"(
    <text_t description="&quot;&lt;'&#x5c0f;&#24378;'&gt;&quot;">
      <ID ID'msg='{"msg&apos;reply": "it&apos;s ok"}'>&amp;&lt;&gt;</ID>
      <DisplayName>&#x5c0f;&#24378;</DisplayName>
    </text_t>
    )";
    using text_attr_t =
        iguana::xml_attr_t<text_t, std::map<std::string_view, std::string>>;
    auto validator = [](const text_attr_t &text) {
      auto v = text.value();
      auto attr = text.attr();
      CHECK(attr["description"] == R"("<'小强'>")");
      CHECK(v.ID.value() == R"(&<>)");
      CHECK(v.ID.attr()["ID'msg"] == R"({"msg'reply": "it's ok"})");
      CHECK(v.DisplayName == "小强");
    };
    text_attr_t text;
    iguana::from_xml(text, str);
    validator(text);
    std::string ss;
    iguana::to_xml<true>(text, ss);
    std::cout << ss << std::endl;

    text_attr_t text1;
    iguana::from_xml(text1, ss);
    validator(text1);
  }
}
