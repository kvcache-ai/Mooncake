#include <cmath>
#define DOCTEST_CONFIG_IMPLEMENT
#include <chrono>
#include <random>
using namespace std::chrono_literals;

#include "doctest.h"
#include "ylt/metric.hpp"

using namespace ylt;
using namespace ylt::metric;

struct metrc_tag {};

struct test_tag {};
TEST_CASE("test serialize with dynamic labels only") {
  auto verify_serialize = [](const std::string& str) {
    CHECK(str.find("method=\"GET\"") != std::string::npos);
    CHECK(str.find("url=\"/\"") != std::string::npos);
  };

  auto verify_serialize_json = [](const std::string& str) {
    CHECK(str.find("\"method\":\"GET\"") != std::string::npos);
    CHECK(str.find("\"url\":\"/\"") != std::string::npos);
  };

  {
    dynamic_counter_2t d("test_counter", "help",
                         std::array<std::string, 2>{"method", "url"});
    d.inc({"GET", "/"});
    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }

  {
    dynamic_summary_2 d("test_summary", "help",
                        {5.23, 10.54, 20.0, 50.0, 100.0},
                        std::array<std::string, 2>{"method", "url"});
    d.observe({"GET", "/"}, 23);
    d.observe({"POST", "/"}, 50);

    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }

  {
    dynamic_histogram_2t d("test_histogram", "help",
                           {5.23, 10.54, 20.0, 50.0, 100.0},
                           std::array<std::string, 2>{"method", "url"});
    d.observe({"GET", "/"}, 23);
    d.observe({"POST", "/"}, 50);

    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }
}

TEST_CASE("test serialize with dynamic hybrid labels") {
  auto staticMetric = std::map<std::string, std::string>(
      {{"instance_id", "1.0.0.1"}, {"cluster_id", "demo"}});

  auto verify_serialize = [](const std::string& str) {
    CHECK(str.find("instance_id=\"1.0.0.1\"") != std::string::npos);
    CHECK(str.find("cluster_id=\"demo\"") != std::string::npos);
    CHECK(str.find("method=\"GET\"") != std::string::npos);
    CHECK(str.find("url=\"/\"") != std::string::npos);
  };

  auto verify_serialize_json = [](const std::string& str) {
    CHECK(str.find("\"instance_id\":\"1.0.0.1\"") != std::string::npos);
    CHECK(str.find("\"cluster_id\":\"demo\"") != std::string::npos);
    CHECK(str.find("\"method\":\"GET\"") != std::string::npos);
    CHECK(str.find("\"url\":\"/\"") != std::string::npos);
  };

  {
    dynamic_counter_2t d("test_counter", "help", staticMetric,
                         std::array<std::string, 2>{"method", "url"});
    d.inc({"GET", "/"});
    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }

  {
    dynamic_summary_2 d("test_summary", "help",
                        {5.23, 10.54, 20.0, 50.0, 100.0}, staticMetric,
                        std::array<std::string, 2>{"method", "url"});
    d.observe({"GET", "/"}, 23);
    d.observe({"POST", "/"}, 50);

    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }

  {
    dynamic_histogram_2t d("test_histogram", "help",
                           {5.23, 10.54, 20.0, 50.0, 100.0}, staticMetric,
                           std::array<std::string, 2>{"method", "url"});
    d.observe({"GET", "/"}, 23);
    d.observe({"POST", "/"}, 50);

    std::string str;
    d.serialize(str);
    verify_serialize(str);
    std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    d.serialize_to_json(str_json);
    verify_serialize_json(str_json);
    std::cout << str_json << "\n";
#endif
  }
}

TEST_CASE("summary test zero") {
  std::map<std::string, std::string> customMap = {};
  summary_t summary{"test_summary",
                    "summary help",
                    {0.5, 0.9, 0.95, 0.99},
                    customMap,
                    std::chrono::seconds(2)};
  summary.observe(1);
  std::string str;
  summary.serialize_to_json(str);

  basic_dynamic_summary<2> dy_summary(
      std::string("test_summary"), std::string("summary help"),
      std::vector<double>{0.5, 0.9, 0.95, 0.99},
      std::array<std::string, 2>{"method", "url"}, std::chrono::seconds(1));
  dy_summary.observe({"GET", "test"}, 10);
  std::string str2;
  dy_summary.serialize_to_json(str2);

  std::cout << str << "\n";
  std::cout << str2 << "\n";
  double sum{};
  uint64_t count{};
  summary.get_rates(sum, count);
  CHECK(sum > 0);
  CHECK(count > 0);
  sum = 0;
  count = 0;
  dy_summary.get_rates({"GET", "test"}, sum, count);
  CHECK(sum > 0);
  CHECK(count > 0);

  std::this_thread::sleep_for(3s);

  str.clear();
  str2.clear();

  summary.serialize_to_json(str);
  dy_summary.serialize_to_json(str2);

  std::cout << str << "\n";
  std::cout << str2 << "\n";
  sum = 0;
  count = 0;
  summary.get_rates(sum, count);
  CHECK(sum == 0);
  CHECK(count == 0);
}

TEST_CASE("serialize zero") {
  {
    std::string str;
    counter_t c("test", "");
    c.inc(0);
    c.serialize_to_json(str);
    CHECK(!str.empty());
    str.clear();
    gauge_t g("test1", "");
    g.dec(0);
    g.serialize_to_json(str);
    CHECK(!str.empty());
  }
  {
    std::string str;
    counter_t c("test", "");
    c.inc(-1);
    c.serialize_to_json(str);
    CHECK(str.empty());
    c.inc(0);
    c.serialize_to_json(str);
    CHECK(!str.empty());
  }
  {
    std::string str;
    counter_t c("test", "");
    c.inc(2);
    c.update(0);  // now zero
    c.serialize_to_json(str);
    CHECK(!str.empty());
    str.clear();
    gauge_t g("test1", "");
    g.inc(2);
    g.dec(2);  // now zero
    g.serialize_to_json(str);
    CHECK(!str.empty());
  }

  counter_t c("test", "");
  gauge_t g("test1", "");
  std::string str;
  c.serialize_to_json(str);
  CHECK(str.empty());
  g.serialize_to_json(str);
  CHECK(str.empty());
  c.inc();
  c.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
  g.inc();
  g.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
  c.update(0);
  c.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
  // dec to zero
  g.dec();
  g.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();

  dynamic_counter_1t c1("test", "", {"url"});
  c1.serialize(str);
  CHECK(str.empty());
  c1.update({"/test"}, 0);
  c1.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();

  dynamic_gauge_1t g1("test", "", {"url"});
  g1.serialize(str);
  CHECK(str.empty());
  c1.inc({"/test"});
  c1.update({"/test"}, 0);
  c1.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
  g1.inc({"/test"});
  g1.dec({"/test"});
  g1.serialize(str);
  CHECK(!str.empty());
  str.clear();

  c1.update({"/test"}, 0);
  c1.serialize(str);
  CHECK(!str.empty());
  str.clear();

  g1.dec({"/test"});
  g1.serialize(str);
  CHECK(!str.empty());
  str.clear();

  c1.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
  g1.serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();

  histogram_t h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0});
  h.serialize(str);
  CHECK(str.empty());
  h.serialize_to_json(str);
  CHECK(str.empty());
  h.observe(23);
  h.serialize(str);
  CHECK(!str.empty());
  str.clear();

  std::map<std::string, std::string> customMap = {};
  auto summary = std::make_shared<summary_t>(
      "test", "help", std::vector{0.5, 0.9, 0.95, 0.99}, customMap);
  summary->serialize(str);
  CHECK(str.empty());
  summary->serialize_to_json(str);
  CHECK(str.empty());
  summary->observe(0);
  summary->serialize(str);
  CHECK(!str.empty());
  str.clear();
  summary->serialize_to_json(str);
  CHECK(!str.empty());
  str.clear();
}

TEST_CASE("test metric manager") {
  auto c = std::make_shared<counter_t>("test1", "");
  auto g = std::make_shared<gauge_t>("test2", "");
  auto inst_s = static_metric_manager<metrc_tag>::instance();
  static_metric_manager<metrc_tag>::instance()->register_metric(c);
  static_metric_manager<metrc_tag>::instance()->register_metric(g);
  auto pair = inst_s->create_metric_static<counter_t>("test1", "");
  CHECK(pair.first == std::errc::invalid_argument);
  auto v1 = inst_s->get_metric_by_label({});
  CHECK(v1.size() == 2);
  auto v2 = inst_s->get_metric_by_name("test1");
  CHECK(v2 != nullptr);

  v2 = inst_s->get_metric_by_name("test111");
  CHECK(v2 == nullptr);

  c->inc();
  g->inc();

  inst_s->create_metric_static<counter_t>(
      "test_counter", "", std::map<std::string, std::string>{{"url", "/"}});
  auto ms = inst_s->filter_metrics_by_label_value(std::regex("/"));
  CHECK(ms.size() == 1);

  {
    std::string str = inst_s->serialize_static();
    std::cout << str << "\n";

    std::string json = inst_s->serialize_to_json_static();
    std::cout << json << "\n";
  }

  {
    metric_filter_options options;
    options.name_regex = ".*test.*";
    auto v5 = inst_s->filter_metrics_static(options);
    CHECK(v5.size() == 3);
    options.label_regex = "url";
    auto v6 = inst_s->filter_metrics_static(options);
    CHECK(v6.size() == 1);
  }

  auto dc = std::make_shared<dynamic_counter_t>(
      std::string("test3"), std::string(""),
      std::array<std::string, 2>{"url", "code"});
  dynamic_metric_manager<metrc_tag>::instance()->register_metric(dc);
  auto inst_d = dynamic_metric_manager<metrc_tag>::instance();
  auto pair1 = inst_d->create_metric_dynamic<dynamic_counter_t>(
      std::string("test3"), std::string(""), std::array<std::string, 2>{});
  CHECK(pair1.first == std::errc::invalid_argument);
  dc->inc({"/", "200"});

  {
    std::string str = inst_d->serialize_dynamic();
    std::cout << str << "\n";

    std::string json = inst_d->serialize_to_json_dynamic();
    std::cout << json << "\n";

    using root_manager = metric_collector_t<static_metric_manager<metrc_tag>,
                                            dynamic_metric_manager<metrc_tag>>;
    str = root_manager::serialize();
    std::cout << str << "\n";
    json = root_manager::serialize_to_json();
    std::cout << json << "\n";
  }

  auto v3 = inst_d->get_metric_by_label({{"url", "/"}, {"code", "200"}});
  CHECK(v3.size() == 1);

  auto v4 = inst_d->get_metric_by_label_name({"url", "code"});
  CHECK(v4.size() == 1);

  inst_d->remove_metric(dc);
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric(dc->str_name());
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric(std::vector<std::shared_ptr<dynamic_metric>>{dc});
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric({dc->str_name()});
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric_by_label({{"code", "400"}});
  CHECK(inst_d->metric_count() == 1);
  inst_d->remove_metric_by_label({{"code", "200"}});
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_label_value({{"code", "400"}});
  CHECK(inst_d->metric_count() == 1);
  inst_d->remove_label_value({{"code", "200"}});
  CHECK(dc->label_value_count() == 0);
  dc->inc({"/", "200"});

  CHECK(dc->label_value_count() == 1);
  inst_d->remove_label_value({{"url", "/"}});
  CHECK(dc->label_value_count() == 0);
  dc->inc({"/", "200"});

  CHECK(dc->label_value_count() == 1);
  inst_d->remove_label_value({{"url", "/"}, {"code", "200"}});
  CHECK(dc->label_value_count() == 0);
  dc->inc({"/", "200"});

  inst_d->remove_metric_by_label_name(std::vector<std::string>{"url", "code"});
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric_by_label_name("url");
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  inst_d->remove_metric_by_label_name("code");
  CHECK(inst_d->metric_count() == 0);
  inst_d->register_metric(dc);

  auto pair2 = inst_d->create_metric_dynamic<dynamic_counter_t>(
      "test4", "", std::array<std::string, 2>{"method", "code"});

  metric_filter_options options;
  options.name_regex = ".*test.*";
  auto v5 = inst_d->filter_metrics_dynamic(options);
  CHECK(v5.size() == 2);
  options.label_regex = "method";
  auto v6 = inst_d->filter_metrics_dynamic(options);
  CHECK(v6.size() == 1);

  options.label_value_regex = "200";

  auto v7 = inst_d->filter_metrics_dynamic(options);
  CHECK(v7.size() == 0);

  pair2.second->inc({"200"});
  auto v8 = inst_d->filter_metrics_dynamic(options);
  CHECK(v8.size() == 1);
}

struct remove_tag {};

TEST_CASE("test metric manager remove") {
  auto dc = std::make_shared<dynamic_counter_t>(
      std::string("test3"), std::string(""),
      std::array<std::string, 2>{"url", "code"});
  dc->inc(std::array<std::string, 2>{"/", "200"});
  dynamic_metric_manager<remove_tag>::instance()->register_metric(dc);
  auto inst_d = dynamic_metric_manager<remove_tag>::instance();
  std::map<std::string, std::string> map{
      {"url", "/"}, {"code", "200"}, {"method", "GET"}};
  CHECK(inst_d->metric_count() == 1);
  inst_d->remove_metric_by_label(map);
  CHECK(inst_d->metric_count() == 1);

  std::vector<std::shared_ptr<metric_t>> filtered_metrics;
  metric_filter_options options;
  options.name_regex = ".*counter.*";
  manager_helper::filter_by_label_name(filtered_metrics, dc, options);
  CHECK(filtered_metrics.empty());

  options.label_regex = ".*counter.*";
  manager_helper::filter_by_label_name(filtered_metrics, dc, options);
  CHECK(filtered_metrics.empty());

  options.label_regex = "url";
  manager_helper::filter_by_label_name(filtered_metrics, dc, options);
  CHECK(filtered_metrics.size() == 1);
  filtered_metrics.clear();
  options.label_regex = "code";
  manager_helper::filter_by_label_name(filtered_metrics, dc, options);
  CHECK(filtered_metrics.size() == 1);

  std::map<std::string, std::string> map1{{"url", "/test"}, {"code", "200"}};
  inst_d->remove_metric_by_label(map1);
  CHECK(inst_d->metric_count() == 1);
  std::map<std::string, std::string> map2{{"url", "/"}, {"code", "400"}};
  inst_d->remove_metric_by_label(map2);
  CHECK(inst_d->metric_count() == 1);
  std::map<std::string, std::string> map3{{"url1", "/"}, {"code", "200"}};
  inst_d->remove_metric_by_label(map3);
  CHECK(inst_d->metric_count() == 1);
  std::map<std::string, std::string> map4{{"url", "/"}, {"code", "200"}};
  inst_d->remove_metric_by_label(map4);
  CHECK(inst_d->metric_count() == 0);

  inst_d->register_metric(dc);
  CHECK(inst_d->metric_count() == 1);

  std::map<std::string, std::string> map5{{"url", "/get"}};
  inst_d->remove_metric_by_label(map5);
  CHECK(inst_d->metric_count() == 1);

  std::map<std::string, std::string> map6{{"url1", "/"}};
  inst_d->remove_metric_by_label(map6);
  CHECK(inst_d->metric_count() == 1);

  std::map<std::string, std::string> map7{{"url", "/"}};
  inst_d->remove_metric_by_label(map7);
  CHECK(inst_d->metric_count() == 0);
}

TEST_CASE("test dynamic counter") {
  basic_dynamic_counter<int64_t, 2> c("test", "", {"url", "code"});
  c.inc({"/", "200"});
  c.inc({"/test", "200"});
  auto v1 = c.value({"/", "200"});
  auto v2 = c.value({"/test", "200"});
  CHECK(v1 == 1);
  CHECK(v2 == 1);

  {
    std::string str;
    c.serialize(str);
    std::cout << str << "\n";

    std::string json;
    c.serialize_to_json(json);
    std::cout << json << "\n";
  }

  basic_dynamic_counter<int64_t, 0> c1("test1", "", {});
  c1.inc({});
  auto v3 = c1.value({});
  CHECK(v3 == 1);

  {
    std::string str;
    c1.serialize(str);
    std::cout << str << "\n";

    std::string json;
    c1.serialize_to_json(json);
    std::cout << json << "\n";
  }

  basic_dynamic_gauge<int64_t, 1> g("test_gauge", "", {"url"});
  g.inc({"/"});
  CHECK(g.value({"/"}) == 1);

  g.dec({"/"});
  CHECK(g.value({"/"}) == 0);

  basic_dynamic_gauge<int64_t, 0> g1("test_gauge1", "", {});
  g1.inc({});
  CHECK(g1.value({}) == 1);
  g1.dec({});
  CHECK(g1.value({}) == 0);

  dynamic_gauge_t g2("test_g2", "", {"url", "code"});
  g2.inc({"/", "200"});
  CHECK(g2.value({"/", "200"}) == 1);

  auto count = g2.label_value_count();
  g2.remove_label_value({{"url", "/"}, {"code", "200"}, {"method", "GET"}});
  auto count1 = g2.label_value_count();
  CHECK(count == count1);
  g2.remove_label_value({{"url1", "/"}, {"code1", "200"}});
  count1 = g2.label_value_count();
  CHECK(count == count1);
}

TEST_CASE("test static counter") {
  basic_static_counter<int64_t> c("test", "");
  c.inc();
  c.inc();
  auto v = c.value();
  CHECK(v == 2);

  {
    std::string str;
    c.serialize(str);
    std::cout << str << "\n";

    std::string json;
    c.serialize_to_json(json);
    std::cout << json << "\n";
  }

  basic_static_counter<int64_t> c1(
      "test", "",
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  c1.inc();
  c1.inc();
  auto v1 = c1.value();
  CHECK(v1 == 2);

  {
    std::string str;
    c1.serialize(str);
    std::cout << str << "\n";

    std::string json;
    c1.serialize_to_json(json);
    std::cout << json << "\n";
  }

  basic_static_gauge<int64_t> g("test", "");
  g.inc();
  g.inc();
  auto v3 = g.value();
  CHECK(v3 == 2);
  g.dec();
  CHECK(g.value() == 1);

  basic_static_gauge<int64_t> g1("test", "",
                                 std::map<std::string, std::string>{});
  g1.inc();
  g1.inc();
  auto v4 = g1.value();
  CHECK(v4 == 2);
  g1.dec();
  CHECK(g1.value() == 1);
}

TEST_CASE("test static histogram") {
  {
    histogram_t h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0});
    h.observe(23);
    auto counts = h.get_bucket_counts();
    CHECK(counts[3]->value() == 1);
    h.observe(42);
    CHECK(counts[3]->value() == 2);
    h.observe(60);
    CHECK(counts[4]->value() == 1);
    h.observe(120);
    CHECK(counts[5]->value() == 1);
    h.observe(1);
    CHECK(counts[0]->value() == 1);

    std::string str;
    h.serialize(str);
    std::cout << str;

    std::string json;
    h.serialize_to_json(json);
    std::cout << json << "\n";
  }

  {
    histogram_t h(
        "test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
        std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
    h.observe(23);
    auto counts = h.get_bucket_counts();
    CHECK(counts[3]->value() == 1);
    h.observe(42);
    CHECK(counts[3]->value() == 2);
    h.observe(60);
    CHECK(counts[4]->value() == 1);
    h.observe(120);
    CHECK(counts[5]->value() == 1);
    h.observe(1);
    CHECK(counts[0]->value() == 1);

    std::string str;
    h.serialize(str);
    std::cout << str;

    std::string json;
    h.serialize_to_json(json);
    std::cout << json << "\n";
  }

  {
    histogram_t h(
        "test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
        std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});

    std::string str;
    h.serialize(str);
    std::cout << str;

    std::string json;
    h.serialize_to_json(json);
    std::cout << json << "\n";
  }
}

TEST_CASE("test dynamic histogram") {
  dynamic_histogram_t h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
                        {"method", "url"});
  h.observe({"GET", "/"}, 23);
  auto counts = h.get_bucket_counts();
  CHECK(counts[3]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 42);
  CHECK(counts[3]->value({"GET", "/"}) == 2);
  h.observe({"GET", "/"}, 60);
  CHECK(counts[4]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 120);
  CHECK(counts[5]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 1);
  CHECK(counts[0]->value({"GET", "/"}) == 1);

  h.observe({"POST", "/"}, 23);
  CHECK(counts[3]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 42);
  CHECK(counts[3]->value({"POST", "/"}) == 2);
  h.observe({"POST", "/"}, 60);
  CHECK(counts[4]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 120);
  CHECK(counts[5]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 1);
  CHECK(counts[0]->value({"POST", "/"}) == 1);

  std::string str;
  h.serialize(str);
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  h.serialize_to_json(str_json);
  std::cout << str_json << "\n";
#endif
}

struct my_tag {};
using my_manager = static_metric_manager<my_tag>;

auto g_pair = my_manager::instance()
                  -> create_metric_static<counter_t>("test_g_counter", "");

TEST_CASE("test no label") {
  {
    std::map<std::string, std::string> customMap = {};
    auto summary = std::make_shared<summary_t>(
        "test", "help", std::vector{0.5, 0.9, 0.95, 0.99}, customMap);
    summary->observe(100);
    auto result = summary->get_rates();
    for (auto& e : result) {
      CHECK(e == 100);
    }

    double sum{};
    result = summary->get_rates(sum);
    CHECK(sum == 100);

    uint64_t count{};
    result = summary->get_rates(count);
    CHECK(count == 1);
  }

  {
    std::map<std::string, std::string> customMap = {};
    auto summary = std::make_shared<summary_t>(
        "test", "help", std::vector{0.95, 0.5, 0.99, 0.9}, customMap);
    summary->observe(100);
    auto result = summary->get_rates();
    for (auto& e : result) {
      CHECK(e == 100);
    }
  }
  {
    std::map<std::string, std::string> customMap = {};
    auto summary = std::make_shared<summary_t>(
        "test", "help", std::vector<double>{}, customMap);
    std::string str;
    summary->serialize(str);
    CHECK(str.empty());

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    summary->serialize_to_json(str_json);
    CHECK(str_json.empty());
#endif
  }
  auto g_counter = g_pair.second;
  g_counter->inc();
  CHECK(g_counter->value() == 1);
  {
    gauge_t g{"test_gauge", "help", 10};
    g.inc();
    g.inc();

    std::string str;
    g.serialize(str);
    CHECK(str.find("test_gauge 2") != std::string::npos);

    g.dec();
    CHECK(g.value() == 1);

    counter_t c{"test_counter", "help", 10};
    c.inc();
    c.inc();
    std::string str1;
    c.serialize(str1);
    CHECK(str1.find("test_counter 2") != std::string::npos);

    auto r = c.reset();
    CHECK(r == 2);
    CHECK(c.value() == 0);

    r = g.update(10);
    CHECK(r == 1);
    CHECK(g.value() == 10);
  }
  {
    metric_t m{};
    auto name = m.metric_name();
    CHECK(name == "unknown");

    m.clean_expired_label();

    std::string str;
    m.serialize(str);
    CHECK(str.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
    m.serialize_to_json(str);
    CHECK(str.empty());
#endif
  }
  {
    counter_t c("get_count", "get counter");
    CHECK(c.metric_type() == MetricType::Counter);
    CHECK(c.help() == "get counter");
    CHECK(c.labels_name().empty());
    c.inc();
    CHECK(c.value() == 1);
    c.inc();
    CHECK(c.value() == 2);
    c.inc(0);

    CHECK(c.value() == 2);

    c.update(10);
    CHECK(c.value() == 10);

    c.update(0);
    CHECK(c.value() == 0);
  }
}

TEST_CASE("test with atomic") {
  counter_t c(
      "get_count", "get counter",
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});

  c.inc();
  c.inc(2);
  CHECK(c.value() == 3);
  c.update(10);
  CHECK(c.value() == 10);

  bool r = c.has_label_value("GET");
  CHECK(r);
  r = c.has_label_value("POST");
  CHECK(!r);

  r = c.has_label_value(std::vector<std::string>{"GET", "/"});
  CHECK(r);
  r = c.has_label_value(std::vector<std::string>{"GET"});
  CHECK(!r);

  c.remove_label_value(
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  r = c.has_label_value(std::vector<std::string>{"GET", "/"});
  CHECK(r);

  gauge_t g(
      "get_qps", "get qps",
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  g.inc();
  g.inc(2);
  CHECK(g.value() == 3);
  g.dec();
  g.dec(1);
  CHECK(g.value() == 1);

  std::string str;
  c.serialize(str);
  std::cout << str;
  std::string str1;
  g.serialize(str1);
  std::cout << str1;
  CHECK(str.find("} 10") != std::string::npos);
  CHECK(str1.find("} 1") != std::string::npos);

  {
    gauge_t g("get_qps", "get qps",
              std::map<std::string, std::string>{{"method", "POST"},
                                                 {"url", "/test"}});
    g.inc();
    g.inc();
    CHECK(g.value() == 2);
    CHECK(g.value() == 2);
    g.dec();
    CHECK(g.value() == 1);
    CHECK(g.value() == 1);
    g.dec();
    CHECK(g.value() == 0);
    CHECK(g.value() == 0);
  }
}

TEST_CASE("test counter with dynamic labels value") {
  {
    auto c = std::make_shared<dynamic_counter_t>(
        "get_count", "get counter",
        std::array<std::string, 2>{"method", "code"});
    CHECK(c->name() == "get_count");
    auto g = std::make_shared<dynamic_gauge_t>(
        std::string("get_count"), std::string("get counter"),
        std::array<std::string, 2>{"method", "code"});
    CHECK(g->name() == "get_count");
    CHECK(g->metric_name() == "gauge");
  }

  {
    dynamic_counter_t c(std::string("get_count"), std::string("get counter"),
                        std::array<std::string, 2>{"method", "code"});
    CHECK(c.labels_name() == std::vector<std::string>{"method", "code"});
    c.inc({"GET", "200"}, 1);
    CHECK(c.value({"GET", "200"}) == 1);
    c.inc({"GET", "200"}, 2);
    CHECK(c.value({"GET", "200"}) == 3);

    std::string str;
    c.serialize(str);
    std::cout << str;
    CHECK(str.find("# TYPE get_count counter") != std::string::npos);
    CHECK(str.find("get_count{method=\"GET\",code=\"200\"} 3") !=
          std::string::npos);

    c.update({"GET", "200"}, 20);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(c.value({"GET", "200"}) == 20);
  }
}

TEST_CASE("test gauge") {
  {
    gauge_t g("get_count", "get counter");
    CHECK(g.metric_type() == MetricType::Gauge);
    CHECK(g.labels_name().empty());
    g.inc();
    CHECK(g.value() == 1);
    g.inc();
    CHECK(g.value() == 2);
    g.inc(0);

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    g.serialize_to_json(str_json);
    std::cout << str_json << "\n";
    CHECK(str_json.find("\"value\":2") != std::string::npos);
#endif

    g.dec();
    CHECK(g.value() == 1);
    g.dec();
    CHECK(g.value() == 0);
  }

  {
    dynamic_gauge_3t g("get_count", "get counter", {"method", "code", "url"});
    CHECK(g.labels_name() == std::vector<std::string>{"method", "code", "url"});
    // method, status code, url
    g.inc({"GET", "200", "/"}, 1);
    CHECK(g.value({"GET", "200", "/"}) == 1);
    g.inc({"GET", "200", "/"}, 2);
    CHECK(g.value({"GET", "200", "/"}) == 3);

    g.inc({"POST", "200", "/"}, 4);

#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string str_json;
    g.serialize_to_json(str_json);
    std::cout << str_json << "\n";
    std::cout << str_json.size() << std::endl;
    CHECK(str_json.size() == 191);
#endif

    std::string str;
    g.serialize(str);
    std::cout << str;
    CHECK(str.find("# TYPE get_count gauge") != std::string::npos);
    CHECK(str.find("get_count{method=\"GET\",code=\"200\",url=\"/\"} 3") !=
          std::string::npos);

    g.dec({"GET", "200", "/"}, 1);
    CHECK(g.value({"GET", "200", "/"}) == 2);
    g.dec({"GET", "200", "/"}, 2);
    CHECK(g.value({"GET", "200", "/"}) == 0);
  }
}

TEST_CASE("test histogram") {
  histogram_t h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0});
  h.observe(23);
  auto counts = h.get_bucket_counts();
  CHECK(counts[3]->value() == 1);
  h.observe(42);
  CHECK(counts[3]->value() == 2);
  h.observe(60);
  CHECK(counts[4]->value() == 1);
  h.observe(120);
  CHECK(counts[5]->value() == 1);
  h.observe(1);
  CHECK(counts[0]->value() == 1);
  std::string str;
  h.serialize(str);
  std::cout << str << "\n";
  CHECK(str.find("test_count") != std::string::npos);
  CHECK(str.find("test_sum") != std::string::npos);
  CHECK(str.find("test_bucket{le=\"5.23") != std::string::npos);
  CHECK(str.find("test_bucket{le=\"+Inf\"}") != std::string::npos);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  h.serialize_to_json(str_json);
  std::cout << str_json << "\n";
  CHECK(str_json.find("\"5.23\":1") != std::string::npos);
#endif
}

TEST_CASE("test summary") {
  summary_t summary{"test_summary", "summary help", {0.5, 0.9, 0.95, 0.99}};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 50; i++) {
    summary.observe(distr(gen));
  }
  std::string str;
  summary.serialize(str);
  std::cout << str;
  double sum;
  uint64_t cnt;
  summary.get_rates(sum, cnt);
  CHECK(cnt == 50);
  CHECK(sum > 0);
  CHECK(str.find("test_summary") != std::string::npos);
  CHECK(str.find("test_summary_count") != std::string::npos);
  CHECK(str.find("test_summary_sum") != std::string::npos);
  CHECK(str.find("test_summary{quantile=\"") != std::string::npos);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  summary.serialize_to_json(str_json);
  std::cout << str_json << "\n";
#endif
}

TEST_CASE("test summary with INF") {
  summary_t summary{"test_summary", "summary help", {0.5, 0.9, 0.95, 0.99}};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 50; i++) {
    summary.observe(INFINITY);
  }
  std::string str;
  summary.serialize(str);
  std::cout << str;
  double sum;
  uint64_t cnt;
  summary.get_rates(sum, cnt);
  CHECK(cnt == 50);
  CHECK(sum < 1e99);
  CHECK(str.find("test_summary") != std::string::npos);
  CHECK(str.find("test_summary_count") != std::string::npos);
  CHECK(str.find("test_summary_sum") != std::string::npos);
  CHECK(str.find("test_summary{quantile=\"") != std::string::npos);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  summary.serialize_to_json(str_json);
  std::cout << str_json << "\n";
  std::cout << str_json.size() << std::endl;
  CHECK(str_json.size() == 205);
#endif
}

TEST_CASE("test summary with NAN") {
  summary_t summary{"test_summary", "summary help", {0.5, 0.9, 0.95, 0.99}};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 50; i++) {
    summary.observe(NAN);
  }
  std::string str;
  summary.serialize(str);
  std::cout << str;
  double sum;
  uint64_t cnt;
  summary.get_rates(sum, cnt);
  CHECK(cnt == 50);
  CHECK(sum < 1e99);
  CHECK(str.find("test_summary") != std::string::npos);
  CHECK(str.find("test_summary_count") != std::string::npos);
  CHECK(str.find("test_summary_sum") != std::string::npos);
  CHECK(str.find("test_summary{quantile=\"") != std::string::npos);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  summary.serialize_to_json(str_json);
  std::cout << str_json << "\n";
  std::cout << str_json.size() << std::endl;
  CHECK(str_json.size() == 205);
#endif
}

TEST_CASE("test summary with illegal quantities") {
  summary_t summary{
      "test_summary", "summary help", {-1, 0.9, 0.5, 0.9, 0, 0.95, 1.1}};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 100; i++) {
    summary.observe(i);
  }
  std::string str;
  summary.serialize(str);
  std::cout << str;
  double sum;
  uint64_t cnt;
  auto result = summary.get_rates(sum, cnt);
  CHECK(cnt == 100);
  CHECK(sum > 0);
  CHECK(str.find("test_summary") != std::string::npos);
  CHECK(str.find("test_summary_count") != std::string::npos);
  CHECK(str.find("test_summary_sum") != std::string::npos);
  CHECK(str.find("test_summary{quantile=\"") != std::string::npos);
  CHECK(result[0] < 0);
  CHECK(result[1] == 0);
  CHECK(result[result.size() - 1] > result[result.size() - 2]);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  summary.serialize_to_json(str_json);
  std::cout << str_json << "\n";
  std::cout << str_json.size() << std::endl;
  CHECK(str_json.size() == 186);
#endif
}

TEST_CASE("test summary with many quantities") {
  std::vector<double> q;
  for (int i = 0; i <= 1000; ++i) {
    q.push_back(1.0 * i / 1000);
  }
  summary_t summary{"test_summary", "summary help", q};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 50; i++) {
    summary.observe(i);
  }
  std::string str;
  summary.serialize(str);
  // std::cout << str;
  double sum;
  uint64_t cnt;
  auto result = summary.get_rates(sum, cnt);
  CHECK(cnt == 50);
  CHECK(sum > 0);
  CHECK(str.find("test_summary") != std::string::npos);
  CHECK(str.find("test_summary_count") != std::string::npos);
  CHECK(str.find("test_summary_sum") != std::string::npos);
  CHECK(str.find("test_summary{quantile=\"") != std::string::npos);
  CHECK(result[1] == result[2]);
  CHECK(result[result.size() - 2] == result[result.size() - 3]);

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  summary.serialize_to_json(str_json);
  std::cout << str_json << "\n";
  std::cout << str_json.size() << std::endl;
  CHECK(str_json.size() == 10818);
#endif
}

TEST_CASE("test summary refresh") {
  summary_t summary{"test_summary", "summary help", {0.5, 0.9, 0.95, 1.1}, 1s};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  std::string str;
  summary.serialize_to_json(str);
  CHECK(str.size() == 0);
  str = "";
  summary.serialize(str);
  CHECK(str.size() == 0);

  std::this_thread::sleep_for(1001ms);
  summary.serialize_to_json(str);
  CHECK(str.size() == 0);
  str = "";
  summary.serialize(str);
  CHECK(str.size() == 0);

  for (int i = 0; i < 50; i++) {
    summary.observe(i);
  }

  double sum;
  uint64_t cnt;
  summary.get_rates(sum, cnt);
  CHECK(cnt == 50);
  std::this_thread::sleep_for(1s);
  summary.get_rates(sum, cnt);
  CHECK(cnt == 0);
  for (int i = 0; i < 50; i++) {
    summary.observe(i);
  }
  std::this_thread::sleep_for(500ms);
  for (int i = 0; i < 10; i++) {
    summary.observe(i);
  }
  summary.get_rates(sum, cnt);
  CHECK(cnt == 60);
  std::this_thread::sleep_for(500ms);
  summary.get_rates(sum, cnt);
  CHECK(cnt == 10);
  std::this_thread::sleep_for(500ms);
  summary.get_rates(sum, cnt);
  summary.serialize_to_json(str);
  CHECK(str.size() > 0);
  str = "";
  summary.serialize(str);
  CHECK(str.size() > 0);
}

TEST_CASE("test dynamic summary refresh") {
  auto summary = std::make_shared<dynamic_summary_1>(
      "jindosdk_testSummary_dynamic", "help", std::vector{0.5, 0.9, 0.95, 1.1},
      std::array{std::string{"bucket"}}, 1s);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  std::string str;
  summary->serialize_to_json(str);
  CHECK(str.size() == 0);
  str = "";
  summary->serialize(str);
  CHECK(str.size() == 0);

  std::this_thread::sleep_for(1001ms);
  summary->serialize_to_json(str);
  CHECK(str.size() == 0);
  str = "";
  summary->serialize(str);
  CHECK(str.size() == 0);

  for (int i = 0; i < 50; i++) {
    summary->observe(std::array{std::string{"1"}}, i);
  }

  double sum;
  uint64_t cnt;
  summary->get_rates(std::array{std::string{"1"}}, sum, cnt);
  CHECK(cnt == 50);
  std::this_thread::sleep_for(1s);
  summary->get_rates(std::array{std::string{"1"}}, sum, cnt);
  CHECK(cnt == 0);
  for (int i = 0; i < 50; i++) {
    summary->observe(std::array{std::string{"1"}}, i);
  }
  std::this_thread::sleep_for(500ms);
  for (int i = 0; i < 10; i++) {
    summary->observe(std::array{std::string{"1"}}, i);
  }
  summary->get_rates(std::array{std::string{"1"}}, sum, cnt);
  CHECK(cnt == 60);
  std::this_thread::sleep_for(500ms);
  summary->get_rates(std::array{std::string{"1"}}, sum, cnt);
  CHECK(cnt == 10);
  std::this_thread::sleep_for(500ms);
  summary->get_rates(std::array{std::string{"1"}}, sum, cnt);
  summary->serialize_to_json(str);
  CHECK(str.size() > 0);
  str = "";
  summary->serialize(str);
  CHECK(str.size() > 0);
}

TEST_CASE("test register metric") {
  auto c = std::make_shared<counter_t>(std::string("get_count"),
                                       std::string("get counter"));
  default_static_metric_manager::instance()->register_metric(c);
  CHECK_FALSE(default_static_metric_manager::instance()->register_metric(c));

  auto g = std::make_shared<gauge_t>(std::string("get_guage_count"),
                                     std::string("get counter"));
  default_static_metric_manager::instance()->register_metric(g);

  auto map1 = default_static_metric_manager::instance()->metric_map();
  for (auto& [k, v] : map1) {
    bool r = k == "get_count" || k == "get_guage_count";
    break;
  }

  CHECK(default_static_metric_manager::instance()->metric_count() >= 2);

  c->inc();
  g->inc();

  auto map = default_static_metric_manager::instance()->metric_map();
  CHECK(map["get_count"]->as<counter_t>()->value() == 1);
  CHECK(map["get_guage_count"]->as<gauge_t>()->value() == 1);

  auto s = default_static_metric_manager::instance()->serialize_static();
  std::cout << s << "\n";
  CHECK(s.find("get_count 1") != std::string::npos);
  CHECK(s.find("get_guage_count 1") != std::string::npos);

  auto m =
      default_static_metric_manager::instance()->get_metric_static<counter_t>(
          "get_count");
  CHECK(m->as<counter_t>()->value() == 1);

  auto m1 =
      default_static_metric_manager::instance()->get_metric_static<gauge_t>(
          "get_guage_count");
  CHECK(m1->as<gauge_t>()->value() == 1);

  auto m2 =
      default_static_metric_manager::instance()->get_metric_static<counter_t>(
          "not_exist");
  CHECK(m2 == nullptr);
}

template <size_t id>
struct test_id_t {};

TEST_CASE("test remove metric and serialize metrics") {
  using metric_mgr = dynamic_metric_manager<test_id_t<1>>;

  metric_mgr::instance()->create_metric_dynamic<dynamic_counter_2t>(
      "test_counter", "", std::array<std::string, 2>{});
  metric_mgr::instance()->create_metric_dynamic<dynamic_counter_2t>(
      "test_counter2", "", std::array<std::string, 2>{});

  size_t count = metric_mgr::instance()->metric_count();
  CHECK(count == 2);

  metric_mgr::instance()->remove_metric("test_counter");
  count = metric_mgr::instance()->metric_count();
  CHECK(count == 1);

  metric_mgr::instance()->remove_metric(std::vector<std::string>{});
  count = metric_mgr::instance()->metric_count();
  CHECK(count == 1);

  std::shared_ptr<dynamic_metric> ptr = nullptr;
  metric_mgr::instance()->remove_metric(ptr);
  count = metric_mgr::instance()->metric_count();
  CHECK(count == 1);

  metric_mgr::instance()->remove_metric(
      std::vector<std::shared_ptr<dynamic_metric>>{});
  count = metric_mgr::instance()->metric_count();
  CHECK(count == 1);

  metric_mgr::instance()->remove_metric("test_counter2");
  count = metric_mgr::instance()->metric_count();
  CHECK(count == 0);

  using metric_mgr2 = static_metric_manager<test_id_t<2>>;
  auto c = metric_mgr2::instance()->create_metric_static<counter_t>(
      "test_static_counter", "");
  auto c2 = metric_mgr2::instance()->create_metric_static<counter_t>(
      "test_static_counter2", "");
  c.second->inc();
  c2.second->inc();

#ifdef CINATRA_ENABLE_METRIC_JSON
  auto s = metric_mgr2::instance()->serialize_to_json_static();
  std::cout << s << "\n";
#endif
  count = metric_mgr2::instance()->metric_count();
  CHECK(count == 2);
}

TEST_CASE("test filter metrics static") {
  using metric_mgr = static_metric_manager<test_id_t<3>>;
  auto c = metric_mgr::instance()->create_metric_static<counter_t>(
      "test_static_counter", "",
      std::map<std::string, std::string>{{"method", "GET"}});
  auto c2 = metric_mgr::instance()->create_metric_static<counter_t>(
      "test_static_counter2", "",
      std::map<std::string, std::string>{{"url", "/"}});
  c.second->inc();
  c2.second->inc();

  metric_filter_options options;
  options.name_regex = ".*counter.*";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.size() == 2);

    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_static_counter") != std::string::npos);
    std::cout << s << "\n";
  }

  options.label_regex = ".*ur.*";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_static_counter2") != std::string::npos);
    std::cout << s << "\n";
  }

  options.name_regex = "no_such_name";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.empty());
    auto s = manager_helper::serialize(metrics);
    CHECK(s.empty());
  }

  options = {};
  options.label_regex = "no_such_label";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.empty());
    auto s = manager_helper::serialize(metrics);
    CHECK(s.empty());
  }

  {
#ifdef CINATRA_ENABLE_METRIC_JSON
    std::vector<std::shared_ptr<metric_t>> vec{};
    auto s = manager_helper::serialize_to_json(vec);
    CHECK(s == "[]");
    auto c = std::make_shared<counter_t>(std::string("get_count"),
                                         std::string("get counter"));
    vec.push_back(c);
    s = manager_helper::serialize_to_json(vec);
    CHECK(s == "[]");
#endif
  }

  // don't filter
  options = {};
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.size() == 2);
  }

  // black
  options.label_regex = ".*ur.*";
  options.is_white = false;
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_static_counter") != std::string::npos);
    CHECK(s.find("test_static_counter2") == std::string::npos);
  }

  options = {};
  options.label_regex = ".*ur.*";
  options.is_white = false;
  {
    auto metrics = metric_mgr::instance()->filter_metrics_static(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_static_counter") != std::string::npos);
    CHECK(s.find("method") != std::string::npos);
    CHECK(s.find("test_static_counter2") == std::string::npos);
    CHECK(s.find("url") == std::string::npos);
  }
}

TEST_CASE("test filter metrics dynamic") {
  using metric_mgr = dynamic_metric_manager<test_id_t<4>>;
  auto [ec, c] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_1t>(
          "test_dynamic_counter", "", std::array<std::string, 1>{"method"});
  auto [ec2, c2] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_1t>(
          "test_dynamic_counter2", "", std::array<std::string, 1>{"url"});
  c->inc({"GET"});
  c->inc({"POST"});
  c2->inc({"/"});
  c2->inc({"/test"});

  metric_filter_options options;
  options.name_regex = ".*counter.*";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 2);

    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_dynamic_counter") != std::string::npos);
    std::cout << s << "\n";
  }

  options.label_regex = ".*ur.*";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_dynamic_counter2") != std::string::npos);
    std::cout << s << "\n";
  }

  options.name_regex = "no_such_name";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.empty());
    auto s = manager_helper::serialize(metrics);
    CHECK(s.empty());
  }

  options = {};
  options.label_regex = "no_such_label";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.empty());
    auto s = manager_helper::serialize(metrics);
    CHECK(s.empty());
  }

  // don't filter
  options = {};
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 2);
  }

  // black
  options.label_regex = ".*ur.*";
  options.is_white = false;
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_dynamic_counter") != std::string::npos);
    CHECK(s.find("test_dynamic_counter2") == std::string::npos);
  }

  options = {};
  options.label_regex = ".*ur.*";
  options.is_white = false;
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 1);
    auto s = manager_helper::serialize(metrics);
    CHECK(s.find("test_dynamic_counter") != std::string::npos);
    CHECK(s.find("method") != std::string::npos);
    CHECK(s.find("test_dynamic_counter2") == std::string::npos);
    CHECK(s.find("url") == std::string::npos);
  }
}

TEST_CASE("test get metric by static labels and label") {
  using metric_mgr = static_metric_manager<test_id_t<9>>;
  metric_mgr::instance()->create_metric_static<counter_t>(
      "http_req_test", "",
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  metric_mgr::instance()->create_metric_static<gauge_t>(
      "http_req_test1", "",
      std::map<std::string, std::string>{{"method", "POST"}, {"url", "/"}});
  metric_mgr::instance()->create_metric_static<counter_t>(
      "http_req_test2", "",
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/test"}});

  auto v = metric_mgr::instance()->get_metric_by_label(
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/test"}});
  CHECK(v[0]->name() == "http_req_test2");

  v = metric_mgr::instance()->get_metric_by_label(
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  CHECK(v[0]->name() == "http_req_test");

  auto [ec, h1] = metric_mgr::instance()->create_metric_static<histogram_t>(
      "http_req_static_hist", "help",
      std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});

  h1->observe(23);

  std::string str1;
  h1->serialize(str1);
  std::cout << str1;
  CHECK(str1.find("method=\"GET\",url=\"/\",le=") != std::string::npos);

  auto map =
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}};
  auto [ec1, s1] = metric_mgr::instance()->create_metric_static<summary_t>(
      "http_req_static_summary", "help", std::vector{0.5, 0.9, 0.95, 0.99},
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  s1->observe(23);

  auto vec = metric_mgr::instance()->get_metric_by_label(map);
  CHECK(vec.size() == 3);

  {
    using metric_mgr2 = static_metric_manager<test_id_t<19>>;
    auto [ec, s2] = metric_mgr2::instance()->create_metric_static<summary_t>(
        "http_req_static_summary2", "help", std::vector{0.5, 0.9, 0.95, 0.99},
        map);
    s2->observe(23);

    auto vec = metric_mgr2::instance()->get_metric_by_label(map);
    CHECK(vec.size() == 1);
  }
}

TEST_CASE("test get metric by dynamic labels") {
  using metric_mgr = dynamic_metric_manager<test_id_t<10>>;
  auto [ec, c] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static", "", std::array<std::string, 2>{"method", "code"});

  auto [ec1, c1] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static1", "", std::array<std::string, 2>{"method", "code"});

  auto [ec2, c2] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static2", "", std::array<std::string, 2>{"method", "code"});

  auto [ec3, c3] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static3", "", std::array<std::string, 2>{"method", "code"});

  c->inc({"POST", "200"});
  c1->inc({"GET", "200"});
  c2->inc({"POST", "301"});
  c3->inc({"POST", "400"});

  auto [ec4, c4] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static4", "", std::array<std::string, 2>{"host", "url"});

  auto [ec5, c5] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "http_req_static5", "", std::array<std::string, 2>{"host", "url"});

  c4->inc({"shanghai", "/"});
  c5->inc({"shanghai", "/test"});

  auto vec =
      metric_mgr::instance()->filter_metrics_by_label_value(std::regex("POST"));
  CHECK(vec.size() == 3);

  vec =
      metric_mgr::instance()->filter_metrics_by_label_value(std::regex("GET"));
  CHECK(vec.size() == 1);

  vec = metric_mgr::instance()->filter_metrics_by_label_value(
      std::regex("shanghai"));
  CHECK(vec.size() == 2);

  vec = metric_mgr::instance()->filter_metrics_by_label_value(std::regex("/"));
  CHECK(vec.size() == 1);

  vec = metric_mgr::instance()->filter_metrics_by_label_value(
      std::regex("/test"));
  CHECK(vec.size() == 1);

  vec = metric_mgr::instance()->filter_metrics_by_label_value(
      std::regex("/none"));
  CHECK(vec.size() == 0);

  vec =
      metric_mgr::instance()->filter_metrics_by_label_value(std::regex("HEAD"));
  CHECK(vec.size() == 0);

  auto [ec6, h1] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_histogram_2t>(
          "http_req_static_hist", "help",
          std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
          std::array<std::string, 2>{"method", "url"});

  h1->observe({"GET", "/"}, 23);

  auto [ec7, s1] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_summary_2>(
          "http_req_static_summary", "help", std::vector{0.5, 0.9, 0.95, 0.99},
          std::array<std::string, 2>{"method", "url"});
  s1->observe({"GET", "/"}, 23);

  vec =
      metric_mgr::instance()->filter_metrics_by_label_value(std::regex("GET"));
  CHECK(vec.size() >= 2);

  auto str = metric_mgr::instance()->serialize_dynamic();
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  auto json_str = metric_mgr::instance()->serialize_to_json_dynamic();
  std::cout << json_str << "\n";
#endif
}

TEST_CASE("test histogram serialize with dynamic labels") {
  dynamic_histogram_2t h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
                         std::array<std::string, 2>{"method", "url"});
  h.observe({"GET", "/"}, 23);
  auto counts = h.get_bucket_counts();
  CHECK(counts[3]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 42);
  CHECK(counts[3]->value({"GET", "/"}) == 2);
  h.observe({"GET", "/"}, 60);
  CHECK(counts[4]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 120);
  CHECK(counts[5]->value({"GET", "/"}) == 1);
  h.observe({"GET", "/"}, 1);
  CHECK(counts[0]->value({"GET", "/"}) == 1);

  h.observe({"POST", "/"}, 23);
  CHECK(counts[3]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 42);
  CHECK(counts[3]->value({"POST", "/"}) == 2);
  h.observe({"POST", "/"}, 60);
  CHECK(counts[4]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 120);
  CHECK(counts[5]->value({"POST", "/"}) == 1);
  h.observe({"POST", "/"}, 1);
  CHECK(counts[0]->value({"POST", "/"}) == 1);

  std::string str;
  h.serialize(str);
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  h.serialize_to_json(str_json);
  std::cout << str_json << "\n";
#endif
}

TEST_CASE("test histogram serialize with static labels default") {
  histogram_t h(
      "test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  h.observe(23);
  auto counts = h.get_bucket_counts();
  CHECK(counts[3]->value() == 1);
  h.observe(42);
  CHECK(counts[3]->value() == 2);
  h.observe(60);
  CHECK(counts[4]->value() == 1);
  h.observe(120);
  CHECK(counts[5]->value() == 1);
  h.observe(1);
  CHECK(counts[0]->value() == 1);
}

TEST_CASE("test histogram serialize with static labels") {
  histogram_t h(
      "test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}});
  h.observe(23);
  auto counts = h.get_bucket_counts();
  CHECK(counts[3]->value() == 1);
  h.observe(42);
  CHECK(counts[3]->value() == 2);
  h.observe(60);
  CHECK(counts[4]->value() == 1);
  h.observe(120);
  CHECK(counts[5]->value() == 1);
  h.observe(1);
  CHECK(counts[0]->value() == 1);

  std::string str;
  h.serialize(str);
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string str_json;
  h.serialize_to_json(str_json);
  std::cout << str_json << "\n";
#endif
}

TEST_CASE("test summary with dynamic labels") {
  basic_dynamic_summary<2> summary{"test_summary",
                                   "summary help",
                                   {0.5, 0.9, 0.95, 0.99},
                                   {"method", "url"}};
  std::random_device rd;
  std::mt19937 gen(rd());
  for (int i = 0; i < 100; i++) {
    summary.observe({"GET", "/"}, i);
    summary.observe({"POST", "/test"}, i);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  double sum;
  uint64_t count;

  auto rates = summary.get_rates({"GET", "/"}, sum, count);
  std::cout << rates.size() << "\n";

  std::string str;
  summary.serialize(str);
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string json_str;
  summary.serialize_to_json(json_str);
  CHECK(json_str.size() == 305);
  std::cout << json_str << "\n";
#endif

  {
    basic_dynamic_summary<2> summary{"test_summary",
                                     "summary help",
                                     {0.9, 0.5, 0.95, 0.99},
                                     {"method", "url"}};
#ifdef CINATRA_ENABLE_METRIC_JSON
    std::string json_str1;
    summary.serialize_to_json(json_str1);
    CHECK(json_str1.empty());
#endif
  }
}

TEST_CASE("test summary with static labels") {
  summary_t summary{
      "test_summary",
      "summary help",
      {0.5, 0.9, 0.95, 0.99},
      std::map<std::string, std::string>{{"method", "GET"}, {"url", "/"}}};
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(1, 100);
  for (int i = 0; i < 50; i++) {
    summary.observe(distr(gen));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  double sum;
  uint64_t count;
  auto rates = summary.get_rates(sum, count);
  std::cout << rates.size() << "\n";

  auto rates1 = summary.get_rates(sum, count);
  CHECK(rates == rates1);

  std::string str;
  summary.serialize(str);
  std::cout << str;

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string json_str;
  summary.serialize_to_json(json_str);
  std::cout << json_str << "\n";
#endif
}

TEST_CASE("test serialize with emptry metrics") {
  std::string s1;

  auto h1 = std::make_shared<dynamic_histogram_1t>(
      "get_count2", "help", std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
      std::array<std::string, 1>{"method"});
  h1->serialize(s1);
  CHECK(s1.empty());

#ifdef CINATRA_ENABLE_METRIC_JSON
  h1->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  h1->observe({"GET"}, 0);
  h1->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  h1->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  auto h2 = std::make_shared<histogram_t>(
      "get_count2", "help",
      std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0});
  h2->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  h2->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  auto h3 = std::make_shared<histogram_t>(
      "get_count2", "help", std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
      std::map<std::string, std::string>{{"method", "/"}});
  h3->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  h3->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  auto c1 = std::make_shared<counter_t>(std::string("get_count"),
                                        std::string("get counter"));
  c1->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  c1->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  auto c2 = std::make_shared<counter_t>(
      std::string("get_count"), std::string("get counter"),
      std::map<std::string, std::string>{{"method", "GET"}});
  c2->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  c2->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  auto c3 = std::make_shared<dynamic_counter_1t>(
      std::string("get_count"), std::string("get counter"),
      std::array<std::string, 1>{"method"});
  c3->serialize(s1);
  CHECK(s1.empty());
#ifdef CINATRA_ENABLE_METRIC_JSON
  c3->serialize_to_json(s1);
  CHECK(s1.empty());
#endif

  {
    std::string str;
    h1->observe({"POST"}, 1);
    bool r = h1->has_label_value("POST");
    CHECK(r);
    r = h1->has_label_value("HEAD");
    CHECK(!r);
    r = h1->has_label_value(std::vector<std::string>{"POST"});
    CHECK(r);
    r = h1->has_label_value(std::vector<std::string>{"HEAD"});
    CHECK(!r);
    h1->serialize(str);
    CHECK(!str.empty());
    str.clear();
#ifdef CINATRA_ENABLE_METRIC_JSON
    h1->serialize_to_json(str);
    CHECK(!str.empty());
#endif
  }

  {
    std::string str;
    h2->observe(1);
    h2->serialize(str);
    CHECK(!str.empty());
    str.clear();
#ifdef CINATRA_ENABLE_METRIC_JSON
    h1->serialize_to_json(str);
    CHECK(!str.empty());
#endif
  }

  {
    std::string str;
    c1->inc();
    c1->serialize(str);
    CHECK(!str.empty());
    str.clear();
#ifdef CINATRA_ENABLE_METRIC_JSON
    c1->serialize_to_json(str);
    CHECK(!str.empty());
#endif
  }

  {
    std::string str;
    c2->inc();
    c2->serialize(str);
    CHECK(!str.empty());
    str.clear();
#ifdef CINATRA_ENABLE_METRIC_JSON
    c2->serialize_to_json(str);
    CHECK(!str.empty());
#endif
  }

  {
    std::string str;
    c3->inc({"POST"});
    c3->serialize(str);
    CHECK(!str.empty());
    str.clear();
#ifdef CINATRA_ENABLE_METRIC_JSON
    c3->serialize_to_json(str);
    CHECK(!str.empty());
#endif
  }
}

TEST_CASE("test serialize with multiple threads") {
  {
    dynamic_histogram_d h("test", "help", {5.23, 10.54, 20.0, 50.0, 100.0},
                          std::array<std::string, 2>{"url", "code"});
    h.observe({"/", "code"}, 23);
  }
  auto c = std::make_shared<dynamic_counter_1t>(
      std::string("get_count"), std::string("get counter"),
      std::array<std::string, 1>{"method"});
  auto g = std::make_shared<dynamic_counter_1t>(
      std::string("get_count1"), std::string("get counter"),
      std::array<std::string, 1>{"method"});

  auto h1 = std::make_shared<dynamic_histogram_1t>(
      "get_count2", "help", std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
      std::array<std::string, 1>{"method"});

  auto c1 = std::make_shared<dynamic_counter_1t>(
      std::string("get_count3"), std::string("get counter"),
      std::array<std::string, 1>{"method"});

  using test_metric_manager = dynamic_metric_manager<test_id_t<20>>;

  test_metric_manager::instance()->register_metric({c, g, h1, c1});

  c->inc({"POST"}, 1);
  g->inc({"GET"}, 1);
  h1->observe({"HEAD"}, 1);

  auto s = test_metric_manager::instance()->serialize_dynamic();
  std::cout << s;
  CHECK(!s.empty());
  CHECK(s.find("get_count") != std::string::npos);
  CHECK(s.find("get_count1") != std::string::npos);
  CHECK(s.find("get_count2") != std::string::npos);
  CHECK(s.find("get_count3") == std::string::npos);

#ifdef CINATRA_ENABLE_METRIC_JSON
  auto json = test_metric_manager::instance()->serialize_to_json_dynamic();
  std::cout << json << "\n";
  CHECK(!json.empty());
  CHECK(json.find("get_count") != std::string::npos);
  CHECK(json.find("get_count1") != std::string::npos);
  CHECK(json.find("get_count2") != std::string::npos);
#endif
}

#if defined(__GNUC__)
TEST_CASE("test system metric") {
  start_system_metric();

  auto s = system_metric_manager::instance()->serialize_static();
  std::cout << s;
  CHECK(!s.empty());

#ifdef CINATRA_ENABLE_METRIC_JSON
  auto json = system_metric_manager::instance()->serialize_to_json_static();
  std::cout << json << "\n";
  CHECK(!json.empty());
#endif

  using metric_manager = dynamic_metric_manager<test_id_t<21>>;
  auto c =
      metric_manager::instance()->create_metric_dynamic<dynamic_counter_1t>(
          std::string("test_qps"), "", std::array<std::string, 1>{"url"});
  c.second->inc({"/"}, 42);
  using root = metric_collector_t<metric_manager, default_static_metric_manager,
                                  system_metric_manager>;
  s.clear();
  s = root::serialize();
  std::cout << s;
  CHECK(!s.empty());

#ifdef CINATRA_ENABLE_METRIC_JSON
  json.clear();
  json = root::serialize_to_json();
  std::cout << json << "\n";
  CHECK(!json.empty());
#endif
}

TEST_CASE("test metric capacity") {
  std::cout << ylt::metric::metric_t::g_user_metric_count << "\n";
  using test_metric_manager = dynamic_metric_manager<test_id_t<21>>;
  set_metric_capacity(ylt::metric::metric_t::g_user_metric_count + 2);
  auto c = test_metric_manager::instance()
               ->create_metric_dynamic<dynamic_counter_1t>(
                   std::string("counter"), "", std::array<std::string, 1>{});
  CHECK(c.second != nullptr);
  auto c1 = test_metric_manager::instance()
                ->create_metric_dynamic<dynamic_counter_1t>(
                    std::string("counter1"), "", std::array<std::string, 1>{});
  CHECK(c1.second != nullptr);
  auto c2 = test_metric_manager::instance()
                ->create_metric_dynamic<dynamic_counter_1t>(
                    std::string("counter2"), "", std::array<std::string, 1>{});
  CHECK(c2.second == nullptr);
  set_metric_capacity(10000000);

  auto process_memory_resident =
      system_metric_manager::instance()->get_metric_static<gauge_t>(
          "ylt_process_memory_resident");
  std::cout << (int64_t)process_memory_resident->value() << "\n";

  auto process_memory_virtual =
      system_metric_manager::instance()->get_metric_static<gauge_t>(
          "ylt_process_memory_virtual");
  std::cout << (int64_t)process_memory_virtual->value() << "\n";
}
#endif

TEST_CASE("test remove dynamic metric") {
  using test_metric_manager = dynamic_metric_manager<test_id_t<22>>;
  auto pair = test_metric_manager::instance()
                  ->create_metric_dynamic<dynamic_counter_1t>(
                      std::string("counter"), "", std::array<std::string, 1>{});
  CHECK(pair.second != nullptr);
  auto pair1 =
      test_metric_manager::instance()
          ->create_metric_dynamic<dynamic_counter_1t>(
              std::string("counter1"), "", std::array<std::string, 1>{});
  CHECK(pair1.second != nullptr);
  auto pair2 =
      test_metric_manager::instance()
          ->create_metric_dynamic<dynamic_counter_1t>(
              std::string("counter2"), "", std::array<std::string, 1>{});
  CHECK(pair2.second != nullptr);

  auto c = pair.second;
  auto c1 = pair1.second;
  auto c2 = pair2.second;

  test_metric_manager::instance()->remove_metric(c);
  CHECK(test_metric_manager::instance()->metric_count() == 2);
  test_metric_manager::instance()->remove_metric(c1);
  CHECK(test_metric_manager::instance()->metric_count() == 1);
  test_metric_manager::instance()->remove_metric(c2);
  CHECK(test_metric_manager::instance()->metric_count() == 0);

  test_metric_manager::instance()->register_metric({c, c1, c2});
  CHECK(test_metric_manager::instance()->metric_count() == 3);
  test_metric_manager::instance()->remove_metric("counter");
  CHECK(test_metric_manager::instance()->metric_count() == 2);
  test_metric_manager::instance()->remove_metric(
      std::vector<std::string>{"counter1", "counter2"});
  CHECK(test_metric_manager::instance()->metric_count() == 0);

  test_metric_manager::instance()->register_metric({c, c1, c2});
  CHECK(test_metric_manager::instance()->metric_count() == 3);
  test_metric_manager::instance()->remove_metric({c1, c2});
  CHECK(test_metric_manager::instance()->metric_count() == 1);
  auto r = test_metric_manager::instance()->register_metric({c, c1});
  CHECK(!r);
  CHECK(test_metric_manager::instance()->metric_count() == 1);

  r = test_metric_manager::instance()->register_metric({c1, c});
  CHECK(!r);
  CHECK(test_metric_manager::instance()->metric_count() == 2);
}

TEST_CASE("testFilterMetricsDynamicWithMultiLabel") {
  using metric_mgr = dynamic_metric_manager<test_id_t<31>>;
  auto [ec, c] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "test_dynamic_counter", "",
          std::array<std::string, 2>{"method", "bucket"});
  auto [ec2, c2] =
      metric_mgr::instance()->create_metric_dynamic<dynamic_counter_t>(
          "test_dynamic_counter2", "",
          std::array<std::string, 2>{"url", "bucket"});
  c->inc({"GET", "bucket1"});
  c->inc({"POST", "bucket2"});
  c2->inc({"/", "bucket1"});
  c2->inc({"/test", "bucket3"});

  auto counter = metric_mgr::instance()->get_metric_dynamic<dynamic_counter_t>(
      "test_dynamic_counter");
  CHECK(counter != nullptr);

  metric_filter_options options;
  options.name_regex = ".*counter.*";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 2);

    auto s = metric_mgr::instance()->serialize(metrics);
    CHECK(s.find("test_dynamic_counter") != std::string::npos);
    std::cout << s << "\n";
  }

  options.label_regex = "bucket";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 2);
    auto s = metric_mgr::instance()->serialize(metrics);
    CHECK(s.find("test_dynamic_counter2") != std::string::npos);
    std::cout << s << "\n";
  }

  options.label_regex = "method";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 1);
    auto s = metric_mgr::instance()->serialize(metrics);
    CHECK(s.find("test_dynamic_counter") != std::string::npos);
    std::cout << s << "\n";
  }

  options.label_regex = "url";
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 1);
    auto s = metric_mgr::instance()->serialize(metrics);
    CHECK(s.find("test_dynamic_counter2") != std::string::npos);
    std::cout << s << "\n";
  }

  // black
  options.label_regex = ".*bucket.*";
  options.is_white = false;
  {
    auto metrics = metric_mgr::instance()->filter_metrics_dynamic(options);
    CHECK(metrics.size() == 0);
  }
}

TEST_CASE("test metric manager clean expired label") {
  set_label_max_age(std::chrono::seconds(1), std::chrono::seconds(1));
  auto inst = dynamic_metric_manager<test_tag>::instance();
  auto pair = inst->create_metric_dynamic<dynamic_counter_1t>(
      std::string("some_counter"), "", std::array<std::string, 1>{"url"});
  auto summary = std::make_shared<basic_dynamic_summary<2>>(
      std::string("test_summary"), std::string("summary help"),
      std::vector<double>{0.5, 0.9, 0.95, 0.99},
      std::array<std::string, 2>{"method", "url"});
  auto h = std::make_shared<dynamic_histogram_t>(
      std::string("test"), std::string("help"),
      std::vector<double>{5.23, 10.54, 20.0, 50.0, 100.0},
      std::array<std::string, 2>{"method", "url"});
  inst->register_metric(summary);
  inst->register_metric(h);
  auto c = pair.second;
  c->inc({"/"});
  c->inc({"/test"});
  summary->observe({"GET", "test"}, 10);
  h->observe({"GET", "test"}, 10);
  CHECK(c->label_value_count() == 2);
  CHECK(summary->label_value_count() == 1);
  CHECK(h->label_value_count() == 1);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  c->inc({"/index"});
  size_t count = c->label_value_count();
  if (count != 1) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  CHECK(count == 1);
  auto ct1 = summary->label_value_count();
  CHECK(ct1 == 0);
  auto ct2 = h->label_value_count();
  CHECK(ct2 == 0);
}

TEST_CASE("test remove label value") {
  dynamic_counter_t counter("test", "",
                            std::array<std::string, 2>{"url", "code"});
  counter.inc({"/", "200"});
  counter.inc({"/", "400"});
  counter.inc({"/index", "200"});
  counter.inc({"/test", "404"});
  CHECK(counter.has_label_name("url"));
  CHECK(counter.has_label_name(std::vector<std::string>{"url", "code"}));

  CHECK(counter.has_label_value("/"));
  CHECK(counter.has_label_value("/index"));
  CHECK(counter.has_label_value("/test"));

  CHECK(!counter.has_label_value(std::vector<std::string>{"/"}));
  bool r = counter.has_label_value(std::vector<std::string>{"/index"});
  CHECK(!counter.has_label_value(std::vector<std::string>{"/index"}));
  CHECK(!counter.has_label_value(std::vector<std::string>{"/test"}));

  CHECK(!counter.has_label_value(std::vector<std::string>{"/", "test"}));
  CHECK(!counter.has_label_value(std::vector<std::string>{"/", "200", "test"}));
  CHECK(!counter.has_label_value(std::vector<std::string>{}));
}

TEST_CASE("test static summary with 0 and 1 quantiles") {
  {
    ylt::metric::summary_t s("test", "help", {0, 1});
    for (uint64_t i = 0; i < 100ull; ++i) {
      s.observe(1);
    }
    auto result = s.get_rates();
    CHECK(result[0] == 1);
    CHECK(result[1] == 1);
  }
  {
    ylt::metric::summary_t s("test", "help", {0, 1});
    auto result = s.get_rates();
    CHECK(result[0] == 0);
    CHECK(result[1] == 0);
  }
}

DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char** argv) { return doctest::Context(argc, argv).run(); }
DOCTEST_MSVC_SUPPRESS_WARNING_POP