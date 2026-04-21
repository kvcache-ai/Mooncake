/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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

#include <async_simple/coro/SyncAwait.h>

#include <asio/ip/network_v4.hpp>
#include <asio/ip/network_v6.hpp>
#include <chrono>
#include <regex>
#include <thread>
#include <unordered_set>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "doctest.h"
#include "rpc_api.hpp"

using namespace coro_rpc;
using namespace std::chrono_literals;

// Test RPC function
std::string test_filter_rpc() { return "Hello from filtered RPC server!"; }

// Simple CIDR network representation for testing
struct cidr_network {
  asio::ip::address network_addr;
  int prefix_length;
  bool is_v4;

  cidr_network(const std::string& cidr_str) {
    size_t slash_pos = cidr_str.find('/');
    if (slash_pos == std::string::npos) {
      throw std::invalid_argument("Invalid CIDR format");
    }

    std::string ip_part = cidr_str.substr(0, slash_pos);
    std::string prefix_part = cidr_str.substr(slash_pos + 1);

    network_addr = asio::ip::address::from_string(ip_part);
    prefix_length = std::stoi(prefix_part);
    is_v4 = network_addr.is_v4();
  }

  bool contains(const asio::ip::address& addr) const {
    if (is_v4 != addr.is_v4()) {
      return false;
    }

    if (is_v4) {
      auto network_v4 = network_addr.to_v4().to_uint();
      auto addr_v4 = addr.to_v4().to_uint();
      uint32_t mask = ~((1u << (32 - prefix_length)) - 1);
      return (network_v4 & mask) == (addr_v4 & mask);
    }
    else {
      auto network_bytes = network_addr.to_v6().to_bytes();
      auto addr_bytes = addr.to_v6().to_bytes();

      int full_bytes = prefix_length / 8;
      int remaining_bits = prefix_length % 8;

      // Check full bytes
      if (std::memcmp(network_bytes.data(), addr_bytes.data(), full_bytes) !=
          0) {
        return false;
      }

      // Check remaining bits
      if (remaining_bits > 0 && full_bytes < 16) {
        uint8_t mask = ~((1u << (8 - remaining_bits)) - 1);
        return (network_bytes[full_bytes] & mask) ==
               (addr_bytes[full_bytes] & mask);
      }

      return true;
    }
  }
};

// Unified IP Filter class for testing
class UnifiedIPFilter {
 private:
  std::unordered_set<asio::ip::address> single_ips_;
  std::vector<cidr_network> cidr_networks_;
  std::vector<std::pair<asio::ip::address, asio::ip::address>> ip_ranges_;
  std::vector<std::regex> regex_patterns_;

 public:
  bool add_rule(const std::string& rule) {
    if (rule.empty()) {
      return false;
    }

    // Check CIDR format
    if (rule.find('/') != std::string::npos) {
      return add_cidr(rule);
    }

    // Check IP range format
    size_t dash_pos = rule.find(" - ");
    if (dash_pos != std::string::npos) {
      std::string start_ip = rule.substr(0, dash_pos);
      std::string end_ip = rule.substr(dash_pos + 3);
      return add_ip_range(start_ip, end_ip);
    }

    // Check regex format
    const std::string regex_chars = ".*+?^$()[]{}|\\";
    bool has_regex_char = false;
    for (char c : rule) {
      if (regex_chars.find(c) != std::string::npos) {
        has_regex_char = true;
        break;
      }
    }

    if (has_regex_char) {
      return add_regex_pattern(rule);
    }

    // Default: single IP
    return add_single_ip(rule);
  }

  bool operator()(const asio::ip::tcp::endpoint& endpoint) {
    return is_allowed(endpoint.address());
  }

  bool is_allowed(const asio::ip::address& ip_addr) {
    // Check single IPs
    if (single_ips_.find(ip_addr) != single_ips_.end()) {
      return true;
    }

    // Check CIDR networks
    for (const auto& network : cidr_networks_) {
      if (network.contains(ip_addr)) {
        return true;
      }
    }

    // Check IP ranges
    for (const auto& range : ip_ranges_) {
      if (is_ip_in_range(ip_addr, range.first, range.second)) {
        return true;
      }
    }

    // Check regex patterns
    std::string ip_str = ip_addr.to_string();
    for (const auto& pattern : regex_patterns_) {
      if (std::regex_match(ip_str, pattern)) {
        return true;
      }
    }

    return false;
  }

  void clear() {
    single_ips_.clear();
    cidr_networks_.clear();
    ip_ranges_.clear();
    regex_patterns_.clear();
  }

  size_t size() const {
    return single_ips_.size() + cidr_networks_.size() + ip_ranges_.size() +
           regex_patterns_.size();
  }

 private:
  bool add_single_ip(const std::string& ip_str) {
    try {
      auto addr = asio::ip::address::from_string(ip_str);
      single_ips_.insert(addr);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  }

  bool add_cidr(const std::string& cidr_str) {
    try {
      cidr_networks_.emplace_back(cidr_str);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  }

  bool add_ip_range(const std::string& start_ip, const std::string& end_ip) {
    try {
      auto start_addr = asio::ip::address::from_string(start_ip);
      auto end_addr = asio::ip::address::from_string(end_ip);

      if (start_addr.is_v4() != end_addr.is_v4()) {
        return false;
      }

      ip_ranges_.emplace_back(start_addr, end_addr);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  }

  bool add_regex_pattern(const std::string& pattern) {
    try {
      regex_patterns_.emplace_back(pattern);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  }

  bool is_ip_in_range(const asio::ip::address& ip,
                      const asio::ip::address& start,
                      const asio::ip::address& end) {
    if (ip.is_v4() && start.is_v4() && end.is_v4()) {
      auto ip_v4 = ip.to_v4().to_uint();
      auto start_v4 = start.to_v4().to_uint();
      auto end_v4 = end.to_v4().to_uint();
      return ip_v4 >= start_v4 && ip_v4 <= end_v4;
    }
    else if (ip.is_v6() && start.is_v6() && end.is_v6()) {
      auto ip_bytes = ip.to_v6().to_bytes();
      auto start_bytes = start.to_v6().to_bytes();
      auto end_bytes = end.to_v6().to_bytes();
      return std::memcmp(ip_bytes.data(), start_bytes.data(), 16) >= 0 &&
             std::memcmp(ip_bytes.data(), end_bytes.data(), 16) <= 0;
    }
    return false;
  }
};

TEST_CASE("test client_filter basic functionality") {
  SUBCASE("test simple lambda filter") {
    coro_rpc_server server(1, 8820);
    server.register_handler<test_filter_rpc>();

    // Set up filter to only allow localhost
    server.client_filter([](const asio::ip::tcp::endpoint& endpoint) -> bool {
      std::string client_ip = endpoint.address().to_string();
      return client_ip == "127.0.0.1" || client_ip == "::1";
    });

    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");

    // Test allowed connection
    coro_rpc_client client(coro_io::get_global_executor());
    auto ec = syncAwait(client.connect("127.0.0.1", "8820"));
    CHECK_MESSAGE(!ec, ec.message());

    auto ret = syncAwait(client.call<test_filter_rpc>());
    CHECK_MESSAGE(ret.has_value(), ret.error().msg);
    CHECK(ret.value() == "Hello from filtered RPC server!");

    server.stop();
  }

  SUBCASE("test filter rejection") {
    coro_rpc_server server(1, 8821);
    server.register_handler<test_filter_rpc>();

    // Set up filter to reject all connections
    server.client_filter([](const asio::ip::tcp::endpoint& endpoint) -> bool {
      return false;  // Reject all
    });

    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");

    // Give server time to start
    std::this_thread::sleep_for(10ms);

    // Test rejected connection - connection should be closed immediately
    coro_rpc_client client(coro_io::get_global_executor());
    auto ec = syncAwait(client.connect("127.0.0.1", "8821"));
    // Connection might succeed but be closed immediately, or fail to connect
    // Either way, any RPC call should fail
    if (!ec) {
      auto ret = syncAwait(client.call<test_filter_rpc>());
      CHECK_MESSAGE(!ret.has_value(),
                    "Expected call to fail due to filter rejection");
    }

    server.stop();
  }
}

TEST_CASE("test UnifiedIPFilter class") {
  SUBCASE("test single IP filtering") {
    UnifiedIPFilter filter;
    CHECK(filter.add_rule("127.0.0.1"));
    CHECK(filter.add_rule("192.168.1.100"));
    CHECK(filter.size() == 2);

    auto localhost = asio::ip::address::from_string("127.0.0.1");
    auto allowed_ip = asio::ip::address::from_string("192.168.1.100");
    auto denied_ip = asio::ip::address::from_string("8.8.8.8");

    CHECK(filter.is_allowed(localhost));
    CHECK(filter.is_allowed(allowed_ip));
    CHECK(!filter.is_allowed(denied_ip));
  }

  SUBCASE("test CIDR network filtering") {
    UnifiedIPFilter filter;
    CHECK(filter.add_rule("192.168.1.0/24"));
    CHECK(filter.size() == 1);

    auto ip_in_network = asio::ip::address::from_string("192.168.1.50");
    auto ip_out_network = asio::ip::address::from_string("192.168.2.50");

    CHECK(filter.is_allowed(ip_in_network));
    CHECK(!filter.is_allowed(ip_out_network));
  }

  SUBCASE("test IP range filtering") {
    UnifiedIPFilter filter;
    CHECK(filter.add_rule("10.0.0.1 - 10.0.0.100"));
    CHECK(filter.size() == 1);

    auto ip_in_range = asio::ip::address::from_string("10.0.0.50");
    auto ip_out_range = asio::ip::address::from_string("10.0.0.200");

    CHECK(filter.is_allowed(ip_in_range));
    CHECK(!filter.is_allowed(ip_out_range));
  }

  SUBCASE("test regex pattern filtering") {
    UnifiedIPFilter filter;
    CHECK(filter.add_rule(R"(192\.168\.1\.[0-9]+)"));
    CHECK(filter.size() == 1);

    auto matching_ip = asio::ip::address::from_string("192.168.1.123");
    auto non_matching_ip = asio::ip::address::from_string("192.168.2.123");

    CHECK(filter.is_allowed(matching_ip));
    CHECK(!filter.is_allowed(non_matching_ip));
  }

  SUBCASE("test mixed filtering rules") {
    UnifiedIPFilter filter;
    CHECK(filter.add_rule("127.0.0.1"));                   // Single IP
    CHECK(filter.add_rule("192.168.1.0/24"));              // CIDR
    CHECK(filter.add_rule("10.0.0.1 - 10.0.0.100"));       // Range
    CHECK(filter.add_rule(R"(172\.16\.[0-9]+\.[0-9]+)"));  // Regex
    CHECK(filter.size() == 4);

    // Test each type
    CHECK(filter.is_allowed(
        asio::ip::address::from_string("127.0.0.1")));  // Single IP
    CHECK(filter.is_allowed(
        asio::ip::address::from_string("192.168.1.50")));  // CIDR
    CHECK(filter.is_allowed(
        asio::ip::address::from_string("10.0.0.25")));  // Range
    CHECK(filter.is_allowed(
        asio::ip::address::from_string("172.16.1.100")));  // Regex

    // Test denied IP
    CHECK(!filter.is_allowed(asio::ip::address::from_string("8.8.8.8")));
  }
}

TEST_CASE("test RPC server with UnifiedIPFilter") {
  SUBCASE("test server with unified filter") {
    coro_rpc_server server(1, 8822);
    server.register_handler<test_filter_rpc>();

    UnifiedIPFilter filter;
    filter.add_rule("127.0.0.1");
    filter.add_rule("::1");

    server.client_filter(filter);

    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");

    // Test connection
    coro_rpc_client client(coro_io::get_global_executor());
    auto ec = syncAwait(client.connect("127.0.0.1", "8822"));
    CHECK_MESSAGE(!ec, ec.message());

    auto ret = syncAwait(client.call<test_filter_rpc>());
    CHECK_MESSAGE(ret.has_value(), ret.error().msg);
    CHECK(ret.value() == "Hello from filtered RPC server!");

    server.stop();
  }
}

TEST_CASE("test filter exception handling") {
  SUBCASE("test filter with exception") {
    coro_rpc_server server(1, 8824);
    server.register_handler<test_filter_rpc>();

    // Set up filter that throws exception
    server.client_filter([](const asio::ip::tcp::endpoint& endpoint) -> bool {
      std::string client_ip = endpoint.address().to_string();
      if (client_ip == "127.0.0.1") {
        return true;  // Allow localhost
      }
      throw std::runtime_error("Filter exception test");
    });

    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");

    // Test connection - should work for localhost
    coro_rpc_client client(coro_io::get_global_executor());
    auto ec = syncAwait(client.connect("127.0.0.1", "8824"));
    CHECK_MESSAGE(!ec, ec.message());

    auto ret = syncAwait(client.call<test_filter_rpc>());
    CHECK_MESSAGE(ret.has_value(), ret.error().msg);

    server.stop();
  }
}