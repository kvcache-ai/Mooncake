#include <chrono>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"
#include "doctest.h"
#include "ylt/standalone/cinatra/coro_http_client.hpp"
#include "ylt/standalone/cinatra/coro_http_server.hpp"

using namespace cinatra;
using namespace std::chrono_literals;

// UnifiedIPFilter class for comprehensive IP filtering
class UnifiedIPFilter {
 public:
  enum class FilterType {
    SINGLE_IP,  // Single IP address
    CIDR,       // CIDR network
    RANGE,      // IP range
    REGEX       // Regular expression
  };

  struct FilterRule {
    FilterType type;
    std::string pattern;
    uint32_t start_ip = 0;
    uint32_t end_ip = 0;
    uint8_t prefix_len = 0;
    std::regex regex_pattern;

    FilterRule(FilterType t, const std::string& p) : type(t), pattern(p) {
      if (type == FilterType::REGEX) {
        regex_pattern = std::regex(pattern);
      }
    }
  };

 private:
  std::vector<FilterRule> allow_rules_;
  std::vector<FilterRule> deny_rules_;
  bool default_allow_ = true;

  // Convert IP string to 32-bit integer
  uint32_t ip_to_uint32(const std::string& ip) const {
    uint32_t result = 0;
    int parts[4];
    if (sscanf(ip.c_str(), "%d.%d.%d.%d", &parts[0], &parts[1], &parts[2],
               &parts[3]) == 4) {
      for (int i = 0; i < 4; i++) {
        if (parts[i] < 0 || parts[i] > 255)
          return 0;
        result = (result << 8) | parts[i];
      }
    }
    return result;
  }

  // Extract IP address string from endpoint
  std::string extract_ip(const asio::ip::tcp::endpoint& endpoint) const {
    return endpoint.address().to_string();
  }

  // Parse CIDR format
  bool parse_cidr(const std::string& cidr, uint32_t& network,
                  uint8_t& prefix_len) const {
    size_t slash_pos = cidr.find('/');
    if (slash_pos == std::string::npos)
      return false;

    std::string ip_str = cidr.substr(0, slash_pos);
    std::string prefix_str = cidr.substr(slash_pos + 1);

    network = ip_to_uint32(ip_str);
    if (network == 0)
      return false;

    prefix_len = std::stoi(prefix_str);
    if (prefix_len > 32)
      return false;

    // Apply network mask
    uint32_t mask = (0xFFFFFFFF << (32 - prefix_len));
    network &= mask;

    return true;
  }

  // Parse IP range format
  bool parse_range(const std::string& range, uint32_t& start_ip,
                   uint32_t& end_ip) const {
    size_t dash_pos = range.find('-');
    if (dash_pos == std::string::npos)
      return false;

    std::string start_str = range.substr(0, dash_pos);
    std::string end_str = range.substr(dash_pos + 1);

    start_ip = ip_to_uint32(start_str);
    end_ip = ip_to_uint32(end_str);

    return start_ip != 0 && end_ip != 0 && start_ip <= end_ip;
  }

  // Check if IP matches the rule
  bool matches_rule(const FilterRule& rule, const std::string& ip) const {
    uint32_t ip_uint = ip_to_uint32(ip);
    if (ip_uint == 0)
      return false;

    switch (rule.type) {
      case FilterType::SINGLE_IP:
        return ip == rule.pattern;

      case FilterType::CIDR: {
        uint32_t mask = (0xFFFFFFFF << (32 - rule.prefix_len));
        return (ip_uint & mask) == rule.start_ip;
      }

      case FilterType::RANGE:
        return ip_uint >= rule.start_ip && ip_uint <= rule.end_ip;

      case FilterType::REGEX:
        return std::regex_match(ip, rule.regex_pattern);
    }
    return false;
  }

 public:
  UnifiedIPFilter(bool default_allow = true) : default_allow_(default_allow) {}

  // Add allow rule
  void add_allow_rule(FilterType type, const std::string& pattern) {
    FilterRule rule(type, pattern);

    if (type == FilterType::CIDR) {
      if (!parse_cidr(pattern, rule.start_ip, rule.prefix_len)) {
        throw std::invalid_argument("Invalid CIDR format: " + pattern);
      }
    }
    else if (type == FilterType::RANGE) {
      if (!parse_range(pattern, rule.start_ip, rule.end_ip)) {
        throw std::invalid_argument("Invalid range format: " + pattern);
      }
    }

    allow_rules_.push_back(std::move(rule));
  }

  // Add deny rule
  void add_deny_rule(FilterType type, const std::string& pattern) {
    FilterRule rule(type, pattern);

    if (type == FilterType::CIDR) {
      if (!parse_cidr(pattern, rule.start_ip, rule.prefix_len)) {
        throw std::invalid_argument("Invalid CIDR format: " + pattern);
      }
    }
    else if (type == FilterType::RANGE) {
      if (!parse_range(pattern, rule.start_ip, rule.end_ip)) {
        throw std::invalid_argument("Invalid range format: " + pattern);
      }
    }

    deny_rules_.push_back(std::move(rule));
  }

  // Main filtering function
  bool operator()(const asio::ip::tcp::endpoint& endpoint) const {
    std::string ip = extract_ip(endpoint);

    // First check deny rules
    for (const auto& rule : deny_rules_) {
      if (matches_rule(rule, ip)) {
        return false;
      }
    }

    // Then check allow rules
    for (const auto& rule : allow_rules_) {
      if (matches_rule(rule, ip)) {
        return true;
      }
    }

    // If no matching rules, return default behavior
    return default_allow_;
  }

  // Clear all rules
  void clear() {
    allow_rules_.clear();
    deny_rules_.clear();
  }

  // Get rule count
  size_t allow_rule_count() const { return allow_rules_.size(); }
  size_t deny_rule_count() const { return deny_rules_.size(); }
};

TEST_CASE("test coro_http_server client_filter basic functionality") {
  coro_http_server server(1, 9001);

  // Set a simple handler
  server.set_http_handler<GET>(
      "/test", [](coro_http_request& req, coro_http_response& resp) {
        resp.set_status_and_content(status_type::ok, "success");
      });

  // Test filter that allows all connections
  server.client_filter([](const asio::ip::tcp::endpoint&) {
    return true;
  });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9001/test");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "success");

  server.stop();
}

TEST_CASE("test coro_http_server client_filter deny all") {
  coro_http_server server(1, 9002);

  server.set_http_handler<GET>(
      "/test", [](coro_http_request& req, coro_http_response& resp) {
        resp.set_status_and_content(status_type::ok, "success");
      });

  // Set filter that denies all connections
  server.client_filter([](const asio::ip::tcp::endpoint&) {
    return false;
  });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9002/test");
  // Connection should be rejected, status code should not be 200
  CHECK(result.status != 200);

  server.stop();
}

TEST_CASE("test coro_http_server client_filter with specific IP") {
  coro_http_server server(1, 9003);

  server.set_http_handler<GET>(
      "/test", [](coro_http_request& req, coro_http_response& resp) {
        resp.set_status_and_content(status_type::ok, "success");
      });

  // Only allow connections from 127.0.0.1
  server.client_filter([](const asio::ip::tcp::endpoint& endpoint) {
    return endpoint.address().to_string() == "127.0.0.1";
  });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9003/test");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "success");

  server.stop();
}

TEST_CASE("test UnifiedIPFilter single IP") {
  UnifiedIPFilter filter(false);  // Default deny

  // Add allow rules
  filter.add_allow_rule(UnifiedIPFilter::FilterType::SINGLE_IP, "127.0.0.1");
  filter.add_allow_rule(UnifiedIPFilter::FilterType::SINGLE_IP,
                        "192.168.1.100");

  // Create test endpoints
  asio::ip::tcp::endpoint allowed_ep(
      asio::ip::address::from_string("127.0.0.1"), 12345);
  asio::ip::tcp::endpoint allowed_ep2(
      asio::ip::address::from_string("192.168.1.100"), 12345);
  asio::ip::tcp::endpoint denied_ep(
      asio::ip::address::from_string("192.168.1.1"), 12345);

  CHECK(filter(allowed_ep) == true);
  CHECK(filter(allowed_ep2) == true);
  CHECK(filter(denied_ep) == false);
}

TEST_CASE("test UnifiedIPFilter CIDR") {
  UnifiedIPFilter filter(false);  // Default deny

  // Add CIDR rules
  filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "192.168.1.0/24");
  filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "10.0.0.0/8");

  // Test 192.168.1.0/24 network
  asio::ip::tcp::endpoint allowed_ep1(
      asio::ip::address::from_string("192.168.1.1"), 12345);
  asio::ip::tcp::endpoint allowed_ep2(
      asio::ip::address::from_string("192.168.1.254"), 12345);
  asio::ip::tcp::endpoint denied_ep1(
      asio::ip::address::from_string("192.168.2.1"), 12345);

  // Test 10.0.0.0/8 network
  asio::ip::tcp::endpoint allowed_ep3(
      asio::ip::address::from_string("10.1.1.1"), 12345);
  asio::ip::tcp::endpoint allowed_ep4(
      asio::ip::address::from_string("10.255.255.255"), 12345);
  asio::ip::tcp::endpoint denied_ep2(asio::ip::address::from_string("11.0.0.1"),
                                     12345);

  CHECK(filter(allowed_ep1) == true);
  CHECK(filter(allowed_ep2) == true);
  CHECK(filter(denied_ep1) == false);
  CHECK(filter(allowed_ep3) == true);
  CHECK(filter(allowed_ep4) == true);
  CHECK(filter(denied_ep2) == false);
}

TEST_CASE("test UnifiedIPFilter IP range") {
  UnifiedIPFilter filter(false);  // Default deny

  // Add IP range rules
  filter.add_allow_rule(UnifiedIPFilter::FilterType::RANGE,
                        "192.168.1.10-192.168.1.20");
  filter.add_allow_rule(UnifiedIPFilter::FilterType::RANGE,
                        "10.0.0.1-10.0.0.100");

  // Test IPs within range
  asio::ip::tcp::endpoint allowed_ep1(
      asio::ip::address::from_string("192.168.1.10"), 12345);
  asio::ip::tcp::endpoint allowed_ep2(
      asio::ip::address::from_string("192.168.1.15"), 12345);
  asio::ip::tcp::endpoint allowed_ep3(
      asio::ip::address::from_string("192.168.1.20"), 12345);
  asio::ip::tcp::endpoint allowed_ep4(
      asio::ip::address::from_string("10.0.0.50"), 12345);

  // Test IPs outside range
  asio::ip::tcp::endpoint denied_ep1(
      asio::ip::address::from_string("192.168.1.9"), 12345);
  asio::ip::tcp::endpoint denied_ep2(
      asio::ip::address::from_string("192.168.1.21"), 12345);
  asio::ip::tcp::endpoint denied_ep3(
      asio::ip::address::from_string("10.0.0.101"), 12345);

  CHECK(filter(allowed_ep1) == true);
  CHECK(filter(allowed_ep2) == true);
  CHECK(filter(allowed_ep3) == true);
  CHECK(filter(allowed_ep4) == true);
  CHECK(filter(denied_ep1) == false);
  CHECK(filter(denied_ep2) == false);
  CHECK(filter(denied_ep3) == false);
}

TEST_CASE("test UnifiedIPFilter regex") {
  UnifiedIPFilter filter(false);  // Default deny

  // Add regex rules
  filter.add_allow_rule(UnifiedIPFilter::FilterType::REGEX,
                        R"(192\.168\.1\.\d+)");
  filter.add_allow_rule(UnifiedIPFilter::FilterType::REGEX, R"(127\.0\.0\.1)");

  // Test matching IPs
  asio::ip::tcp::endpoint allowed_ep1(
      asio::ip::address::from_string("192.168.1.1"), 12345);
  asio::ip::tcp::endpoint allowed_ep2(
      asio::ip::address::from_string("192.168.1.255"), 12345);
  asio::ip::tcp::endpoint allowed_ep3(
      asio::ip::address::from_string("127.0.0.1"), 12345);

  // Test non-matching IPs
  asio::ip::tcp::endpoint denied_ep1(
      asio::ip::address::from_string("192.168.2.1"), 12345);
  asio::ip::tcp::endpoint denied_ep2(asio::ip::address::from_string("10.0.0.1"),
                                     12345);

  CHECK(filter(allowed_ep1) == true);
  CHECK(filter(allowed_ep2) == true);
  CHECK(filter(allowed_ep3) == true);
  CHECK(filter(denied_ep1) == false);
  CHECK(filter(denied_ep2) == false);
}

TEST_CASE("test UnifiedIPFilter deny rules priority") {
  UnifiedIPFilter filter(true);  // Default allow

  // Add allow rule
  filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "192.168.1.0/24");

  // Add deny rule (higher priority)
  filter.add_deny_rule(UnifiedIPFilter::FilterType::SINGLE_IP, "192.168.1.100");

  // Test: 192.168.1.100 is in allowed network but explicitly denied by deny
  // rule
  asio::ip::tcp::endpoint denied_ep(
      asio::ip::address::from_string("192.168.1.100"), 12345);
  asio::ip::tcp::endpoint allowed_ep(
      asio::ip::address::from_string("192.168.1.1"), 12345);

  CHECK(filter(denied_ep) == false);  // Denied by deny rule
  CHECK(filter(allowed_ep) == true);  // Allowed by allow rule
}

TEST_CASE("test coro_http_server with UnifiedIPFilter integration") {
  coro_http_server server(1, 9004);

  server.set_http_handler<GET>(
      "/test", [](coro_http_request& req, coro_http_response& resp) {
        resp.set_status_and_content(status_type::ok, "success");
      });

  // Create UnifiedIPFilter instance
  auto filter = std::make_shared<UnifiedIPFilter>(false);  // Default deny
  filter->add_allow_rule(UnifiedIPFilter::FilterType::SINGLE_IP, "127.0.0.1");
  filter->add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "192.168.1.0/24");

  // Set filter
  server.client_filter([filter](const asio::ip::tcp::endpoint& endpoint) {
    return (*filter)(endpoint);
  });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9004/test");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "success");

  server.stop();
}

TEST_CASE("test UnifiedIPFilter error handling") {
  UnifiedIPFilter filter;

  // Test invalid CIDR format
  CHECK_THROWS_AS(filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR,
                                        "192.168.1.0/33"),
                  std::invalid_argument);
  CHECK_THROWS_AS(
      filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "invalid_ip/24"),
      std::invalid_argument);
  CHECK_THROWS_AS(
      filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "192.168.1.0"),
      std::invalid_argument);

  // Test invalid range format
  CHECK_THROWS_AS(filter.add_allow_rule(UnifiedIPFilter::FilterType::RANGE,
                                        "192.168.1.20-192.168.1.10"),
                  std::invalid_argument);
  CHECK_THROWS_AS(filter.add_allow_rule(UnifiedIPFilter::FilterType::RANGE,
                                        "invalid_ip-192.168.1.10"),
                  std::invalid_argument);
  CHECK_THROWS_AS(
      filter.add_allow_rule(UnifiedIPFilter::FilterType::RANGE, "192.168.1.10"),
      std::invalid_argument);
}

TEST_CASE("test UnifiedIPFilter rule management") {
  UnifiedIPFilter filter;

  CHECK(filter.allow_rule_count() == 0);
  CHECK(filter.deny_rule_count() == 0);

  filter.add_allow_rule(UnifiedIPFilter::FilterType::SINGLE_IP, "127.0.0.1");
  filter.add_allow_rule(UnifiedIPFilter::FilterType::CIDR, "192.168.1.0/24");
  filter.add_deny_rule(UnifiedIPFilter::FilterType::SINGLE_IP, "192.168.1.100");

  CHECK(filter.allow_rule_count() == 2);
  CHECK(filter.deny_rule_count() == 1);

  filter.clear();
  CHECK(filter.allow_rule_count() == 0);
  CHECK(filter.deny_rule_count() == 0);
}

TEST_CASE("test coro_http_server client_filter with multiple connections") {
  coro_http_server server(1, 9005);

  server.set_http_handler<GET>(
      "/test", [](coro_http_request& req, coro_http_response& resp) {
        resp.set_status_and_content(status_type::ok, "success");
      });

  std::atomic<int> connection_count{0};

  // Set filter to count connections
  server.client_filter(
      [&connection_count](const asio::ip::tcp::endpoint& endpoint) {
        connection_count++;
        return endpoint.address().to_string() == "127.0.0.1";
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  // Create multiple client connections
  std::vector<std::thread> threads;
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([i]() {
      coro_http_client client{};
      auto result = client.get("http://127.0.0.1:9005/test");
      CHECK(result.status == 200);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify filter was called multiple times
  CHECK(connection_count.load() >= 5);

  server.stop();
}