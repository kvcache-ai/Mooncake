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

#include <iostream>
#include <string>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_io/coro_io.hpp>

using namespace cinatra;

// Certificate paths - use SM2 certs installed with Tongsuo in CI
const std::string CERT_PATH = "/usr/local/tongsuo/ssl/certs/sm2/";
const std::string CLIENT_SIGN_CERT =
    CERT_PATH + "client_sign.crt";  // SM2 client signing certificate
const std::string CLIENT_SIGN_KEY =
    CERT_PATH + "client_sign.key";  // SM2 client signing private key
const std::string CLIENT_ENC_CERT =
    CERT_PATH + "client_enc.crt";  // SM2 client encryption certificate
const std::string CLIENT_ENC_KEY =
    CERT_PATH + "client_enc.key";  // SM2 client encryption private key
const std::string CA_CERT = CERT_PATH + "chain-ca.crt";  // CA certificate

// Single certificate paths for RFC 8998 TLS 1.3 + GM mode
const std::string CLIENT_GM_CERT =
    CERT_PATH + "client_sign.crt";  // GM single certificate
const std::string CLIENT_GM_KEY =
    CERT_PATH + "client_sign.key";  // GM single private key

// Test one-way authentication NTLS client (server certificate only)
async_simple::coro::Lazy<void> test_ntls_http_client_one_way() {
  std::cout << "\n=== One-way Authentication NTLS Client (8801) ==="
            << std::endl;

  coro_http_client client{};

  // Enable NTLS protocol (simple mode, server authentication only)
  client.set_ntls_schema(true);

  std::cout << "NTLS HTTP Client initialized (one-way auth mode)" << std::endl;

  // Send GET request to root endpoint
  auto response = co_await client.async_get("https://localhost:8801/");
  if (response.net_err) {
    std::cout << "GET / request failed: " << response.net_err.message()
              << std::endl;
  }
  else {
    std::cout << "GET / Response status: " << response.status << std::endl;
    std::cout << "GET / Response body: " << response.resp_body << std::endl;
  }

  // Send POST request to echo endpoint
  auto post_response = co_await client.async_post(
      "https://localhost:8801/echo", "Test message", req_content_type::text);
  if (post_response.net_err) {
    std::cout << "POST /echo request failed: "
              << post_response.net_err.message() << std::endl;
  }
  else {
    std::cout << "POST /echo Response status: " << post_response.status
              << std::endl;
    std::cout << "POST /echo Response body: " << post_response.resp_body
              << std::endl;
  }
}

// Test mutual authentication NTLS client (client certificate required)
async_simple::coro::Lazy<void> test_ntls_http_client_mutual_auth() {
  std::cout << "\n=== Mutual Authentication NTLS Client (8802) ==="
            << std::endl;

  coro_http_client client{};

  // Initialize NTLS client with mutual authentication
  bool init_ok = client.init_ntls_client(
      CLIENT_SIGN_CERT,       // Client SM2 signing certificate
      CLIENT_SIGN_KEY,        // Client SM2 signing private key
      CLIENT_ENC_CERT,        // Client SM2 encryption certificate
      CLIENT_ENC_KEY,         // Client SM2 encryption private key
      CA_CERT,                // CA certificate
      asio::ssl::verify_peer  // Verify server certificate
  );

  if (!init_ok) {
    std::cout << "Failed to initialize NTLS client with mutual authentication"
              << std::endl;
    co_return;
  }

  std::cout << "NTLS HTTP Client initialized (mutual authentication mode)"
            << std::endl;

  // Send GET request to root endpoint (requires client certificate)
  auto response = co_await client.async_get("https://localhost:8802/");
  if (response.net_err) {
    std::cout << "GET / request failed: " << response.net_err.message()
              << std::endl;
  }
  else {
    std::cout << "GET / Response status: " << response.status << std::endl;
    std::cout << "GET / Response body: " << response.resp_body << std::endl;
  }

  // Send POST request to echo endpoint (requires client certificate)
  auto post_response = co_await client.async_post(
      "https://localhost:8802/echo", "Test message with client certificate",
      req_content_type::text);
  if (post_response.net_err) {
    std::cout << "POST /echo request failed: "
              << post_response.net_err.message() << std::endl;
  }
  else {
    std::cout << "POST /echo Response status: " << post_response.status
              << std::endl;
    std::cout << "POST /echo Response body: " << post_response.resp_body
              << std::endl;
  }
}

// Test one-way authentication TLS 1.3 + GM client (single certificate)
async_simple::coro::Lazy<void> test_tls13_gm_client_one_way() {
  std::cout << "\n=== TLS 1.3 + GM One-way Authentication Client (8803) ==="
            << std::endl;

  coro_http_client client{};

  // Initialize TLS 1.3 + GM client for one-way authentication
  // Use empty certificates for one-way mode, but specify TLS 1.3 + GM mode
  bool init_ok = client.init_ntls_tls13_gm_client(
      "",                      // No client certificate needed for one-way
      "",                      // No client key needed for one-way
      CA_CERT,                 // CA certificate to verify server
      asio::ssl::verify_peer,  // Verify server certificate
      "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"  // TLS 1.3 GM cipher suites
  );

  if (!init_ok) {
    std::cout << "Failed to initialize TLS 1.3 + GM one-way client"
              << std::endl;
    co_return;
  }

  std::cout << "TLS 1.3 + GM HTTP Client initialized (one-way auth mode)"
            << std::endl;

  // Send GET request to root endpoint
  auto response = co_await client.async_get("https://localhost:8803/");
  if (response.net_err) {
    std::cout << "GET / request failed: " << response.net_err.message()
              << std::endl;
  }
  else {
    std::cout << "GET / Response status: " << response.status << std::endl;
    std::cout << "GET / Response body: " << response.resp_body << std::endl;
  }

  // Send POST request to echo endpoint
  auto post_response = co_await client.async_post("https://localhost:8803/echo",
                                                  "TLS 1.3 + GM test message",
                                                  req_content_type::text);
  if (post_response.net_err) {
    std::cout << "POST /echo request failed: "
              << post_response.net_err.message() << std::endl;
  }
  else {
    std::cout << "POST /echo Response status: " << post_response.status
              << std::endl;
    std::cout << "POST /echo Response body: " << post_response.resp_body
              << std::endl;
  }
}

// Test mutual authentication TLS 1.3 + GM client (single certificate)
async_simple::coro::Lazy<void> test_tls13_gm_client_mutual_auth() {
  std::cout << "\n=== TLS 1.3 + GM Mutual Authentication Client (8804) ==="
            << std::endl;

  coro_http_client client{};

  // Initialize TLS 1.3 + GM client with mutual authentication (single
  // certificate)
  bool init_ok = client.init_ntls_tls13_gm_client(
      CLIENT_GM_CERT,                    // Client GM single certificate
      CLIENT_GM_KEY,                     // Client GM single private key
      CA_CERT,                           // CA certificate
      asio::ssl::verify_peer,            // Verify server certificate
      "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"  // TLS 1.3 GM cipher suites
  );

  if (!init_ok) {
    std::cout
        << "Failed to initialize TLS 1.3 + GM client with mutual authentication"
        << std::endl;
    co_return;
  }

  std::cout
      << "TLS 1.3 + GM HTTP Client initialized (mutual authentication mode)"
      << std::endl;

  // Send GET request to root endpoint (requires client certificate)
  auto response = co_await client.async_get("https://localhost:8804/");
  if (response.net_err) {
    std::cout << "GET / request failed: " << response.net_err.message()
              << std::endl;
  }
  else {
    std::cout << "GET / Response status: " << response.status << std::endl;
    std::cout << "GET / Response body: " << response.resp_body << std::endl;
  }

  // Send POST request to echo endpoint (requires client certificate)
  auto post_response = co_await client.async_post(
      "https://localhost:8804/echo", "TLS 1.3 + GM mutual auth test message",
      req_content_type::text);
  if (post_response.net_err) {
    std::cout << "POST /echo request failed: "
              << post_response.net_err.message() << std::endl;
  }
  else {
    std::cout << "POST /echo Response status: " << post_response.status
              << std::endl;
    std::cout << "POST /echo Response body: " << post_response.resp_body
              << std::endl;
  }
}

int main() {
  std::cout << "Starting NTLS HTTP Client Examples..." << std::endl;
  std::cout << "Testing both TLCP and TLS 1.3 + GM protocols" << std::endl;

  try {
    std::cout << "TLCP Dual Certificate Mode Tests" << std::endl;

    // Test TLCP one-way authentication client (port 8801)
    async_simple::coro::syncAwait(test_ntls_http_client_one_way());

    // Test TLCP mutual authentication client (port 8802)
    async_simple::coro::syncAwait(test_ntls_http_client_mutual_auth());

    std::cout << "TLS 1.3 + GM Single Certificate Mode Tests" << std::endl;

    // Test TLS 1.3 + GM one-way authentication client (port 8803)
    async_simple::coro::syncAwait(test_tls13_gm_client_one_way());

    // Test TLS 1.3 + GM mutual authentication client (port 8804)
    async_simple::coro::syncAwait(test_tls13_gm_client_mutual_auth());

  } catch (const std::exception& e) {
    std::cout << "Exception: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "NTLS HTTP Client Examples completed." << std::endl;
  return 0;
}