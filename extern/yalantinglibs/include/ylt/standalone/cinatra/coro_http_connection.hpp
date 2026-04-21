#pragma once
#include <async_simple/Executor.h>
#include <async_simple/coro/SyncAwait.h>

#include <asio/buffer.hpp>
#include <system_error>
#include <thread>

#include "asio/dispatch.hpp"
#include "asio/streambuf.hpp"
#include "async_simple/coro/Lazy.h"
#include "cinatra/cinatra_log_wrapper.hpp"
#include "cinatra/response_cv.hpp"
#include "cookie.hpp"
#include "coro_http_request.hpp"
#include "coro_http_router.hpp"
#include "define.h"
#include "http_parser.hpp"
#include "multipart.hpp"
#include "session_manager.hpp"
#include "sha1.hpp"
#include "string_resize.hpp"
#include "websocket.hpp"
#ifdef CINATRA_ENABLE_GZIP
#include "gzip.hpp"
#endif
#include "ylt/coro_io/coro_file.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/socket_wrapper.hpp"

namespace cinatra {
struct websocket_result {
  std::error_code ec;
  ws_frame_type type;
  std::string_view data;
  bool eof;
};

class coro_http_connection
    : public std::enable_shared_from_this<coro_http_connection> {
 public:
  template <typename executor_t>
  coro_http_connection(executor_t *executor,
                       coro_io::socket_wrapper_t &&socket_wrapper,
                       coro_http_router &router)
      : executor_(executor),
        socket_wrapper_(std::move(socket_wrapper)),
        router_(router),
        request_(parser_, this),
        response_(this) {
    buffers_.reserve(3);
  }

  ~coro_http_connection() { close(); }

#ifdef CINATRA_ENABLE_SSL
  bool init_ssl(
      std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket &>> ssl_stream) {
    if (ssl_stream) {
      socket_wrapper_.ssl_stream() = std::move(ssl_stream);
      return true;
    }
    return false;
  }
  bool init_ssl(const std::string &cert_file, const std::string &key_file,
                std::string passwd) {
    unsigned long ssl_options = asio::ssl::context::default_workarounds |
                                asio::ssl::context::no_sslv2 |
                                asio::ssl::context::single_dh_use;
    try {
      ssl_ctx_ =
          std::make_unique<asio::ssl::context>(asio::ssl::context::sslv23);

      ssl_ctx_->set_options(ssl_options);
      if (!passwd.empty()) {
        ssl_ctx_->set_password_callback([pwd = std::move(passwd)](auto, auto) {
          return pwd;
        });
      }

      std::error_code ec;
      if (fs::exists(cert_file, ec)) {
        ssl_ctx_->use_certificate_chain_file(std::move(cert_file));
      }

      if (fs::exists(key_file, ec)) {
        ssl_ctx_->use_private_key_file(std::move(key_file),
                                       asio::ssl::context::pem);
      }

      socket_wrapper_.ssl_stream() =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              *socket_wrapper_.socket(), *ssl_ctx_);
    } catch (const std::exception &e) {
      CINATRA_LOG_ERROR << "init ssl failed, reason: " << e.what();
      return false;
    }
    return true;
  }

#ifdef YLT_ENABLE_NTLS
  /*!
   * NTLS mode enumeration for HTTP connection
   */
  enum class ntls_mode {
    tlcp_dual_cert,  //!< GB/T 38636-2020 TLCP with dual certificates (signing +
                     //!< encryption)
    tls13_single_cert  //!< RFC 8998 TLS 1.3 + GM with single certificate
  };

  /*!
   * Initialize NTLS SSL context with dual certificates (Tongsuo native API)
   */
  // bool init_ntls(const std::string &sign_cert_file,
  //               const std::string &sign_key_file,
  //               const std::string &enc_cert_file,
  //               const std::string &enc_key_file,
  //               const std::string &ca_cert_file = "",
  //               int verify_mode = asio::ssl::verify_none,
  //               std::string passwd = "") {
  //  tcp_with_ntls_config ntls_config_;
  //  ntls_config_.sign_cert_file = sign_cert_file;
  //
  //}
  bool init_ntls(const auto &ntls_config, std::string passwd) {
    try {
      SSL_CTX *ctx = NULL;
      // Configure based on NTLS mode
      if (ntls_config.mode == ntls_mode::tls13_single_cert) {
        // Create SSL context with TLCP server method
        ssl_ctx_ =
            std::make_unique<asio::ssl::context>(SSL_CTX_new(TLS_method()));

        // Enable NTLS using Tongsuo native API
        ctx = ssl_ctx_->native_handle();
        if (!ctx) {
          CINATRA_LOG_ERROR << "SSL_CTX native_handle is null";
          return false;
        }
        // RFC 8998 TLS 1.3 + GM single certificate mode
        CINATRA_LOG_INFO
            << "Configuring RFC 8998 TLS 1.3 + GM single certificate mode";

        // Enable strict SM TLS 1.3 (Tongsuo)
        SSL_CTX_enable_sm_tls13_strict(ctx);

        CINATRA_LOG_INFO << "TLS 1.3 strict mode enabled successfully";

        // Enforce TLS 1.3 only
        if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
          CINATRA_LOG_ERROR << "Failed to set minimum TLS version to 1.3";
          return false;
        }
        if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
          CINATRA_LOG_ERROR << "Failed to set maximum TLS version to 1.3";
          return false;
        }

        // Set TLS 1.3 GM cipher suites (align with s_client)
        std::string cipher_suites = ntls_config.cipher_suites.empty()
                                        ? "TLS_SM4_GCM_SM3"
                                        : ntls_config.cipher_suites;
        if (SSL_CTX_set_ciphersuites(ctx, cipher_suites.c_str()) != 1) {
          unsigned long err = ::ERR_get_error();
          CINATRA_LOG_ERROR << "Failed to set TLS 1.3 GM cipher suites '"
                            << cipher_suites
                            << "': " << ::ERR_error_string(err, nullptr);
          return false;
        }
        CINATRA_LOG_INFO << "TLS 1.3 GM cipher suites set to: "
                         << cipher_suites;

        //// Set supported curves for SM2 (strict mode requires only SM2)
        // if (SSL_CTX_set1_curves_list(ctx, "SM2:X25519:prime256v1") != 1) {
        //  unsigned long err = ::ERR_get_error();
        //  CINATRA_LOG_ERROR << "Failed to set SM2 curves: "
        //                    << ::ERR_error_string(err, nullptr);
        //  return false;
        //}
        CINATRA_LOG_INFO << "SM2 curves configured for strict mode";
      }
      else {
        // Create SSL context with TLCP server method
        ssl_ctx_ =
            std::make_unique<asio::ssl::context>(SSL_CTX_new(NTLS_method()));

        // Enable NTLS using Tongsuo native API
        ctx = ssl_ctx_->native_handle();
        if (!ctx) {
          CINATRA_LOG_ERROR << "SSL_CTX native_handle is null";
          return false;
        }

        // Enable NTLS mode for Tongsuo
        SSL_CTX_enable_ntls(ctx);
        CINATRA_LOG_INFO << "NTLS mode enabled successfully";

        // GB/T 38636-2020 TLCP dual certificate mode (default)
        CINATRA_LOG_INFO
            << "Configuring GB/T 38636-2020 TLCP dual certificate mode";

        // Set TLCP cipher suites
        std::string cipher_suites =
            ntls_config.cipher_suites.empty()
                ? "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3"
                : ntls_config.cipher_suites;
        if (SSL_CTX_set_cipher_list(ctx, cipher_suites.c_str()) != 1) {
          unsigned long err = ::ERR_get_error();
          CINATRA_LOG_WARNING << "Failed to set TLCP cipher suites '"
                              << cipher_suites
                              << "': " << ::ERR_error_string(err, nullptr);
        }
        else {
          CINATRA_LOG_INFO << "TLCP cipher suites set to: " << cipher_suites;
        }
      }

      // Set context options
      ssl_ctx_->set_options(asio::ssl::context::default_workarounds |
                            asio::ssl::context::no_sslv2 |
                            asio::ssl::context::single_dh_use);
      if (!passwd.empty()) {
        ssl_ctx_->set_password_callback([pwd = std::move(passwd)](auto, auto) {
          return pwd;
        });
      }
      namespace fs = std::filesystem;
      std::error_code file_ec;

      // Load certificates based on NTLS mode
      if (ntls_config.mode == ntls_mode::tls13_single_cert) {
        // RFC 8998 TLS 1.3 + GM single certificate mode
        auto gm_cert_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.gm_cert_file)
                : fs::path(ntls_config.base_path) / ntls_config.gm_cert_file;
        auto gm_key_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.gm_key_file)
                : fs::path(ntls_config.base_path) / ntls_config.gm_key_file;

        // Load single GM certificate
        if (fs::exists(gm_cert_path, file_ec)) {
          if (SSL_CTX_use_certificate_file(ctx, gm_cert_path.string().c_str(),
                                           SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR
                << "failed to load GM certificate: " << gm_cert_path.string()
                << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded GM certificate: "
                           << gm_cert_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "GM certificate file not found: "
                            << gm_cert_path.string();
          return false;
        }

        // Load single GM private key
        if (fs::exists(gm_key_path, file_ec)) {
          if (SSL_CTX_use_PrivateKey_file(ctx, gm_key_path.string().c_str(),
                                          SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR
                << "failed to load GM private key: " << gm_key_path.string()
                << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded GM private key: " << gm_key_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "GM private key file not found: "
                            << gm_key_path.string();
          return false;
        }
      }
      else {
        // GB/T 38636-2020 TLCP dual certificate mode
        auto sign_cert_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.sign_cert_file)
                : fs::path(ntls_config.base_path) / ntls_config.sign_cert_file;
        auto sign_key_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.sign_key_file)
                : fs::path(ntls_config.base_path) / ntls_config.sign_key_file;
        auto enc_cert_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.enc_cert_file)
                : fs::path(ntls_config.base_path) / ntls_config.enc_cert_file;
        auto enc_key_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.enc_key_file)
                : fs::path(ntls_config.base_path) / ntls_config.enc_key_file;

        // Load SM2 signing certificate and key
        if (fs::exists(sign_cert_path, file_ec)) {
          if (SSL_CTX_use_sign_certificate_file(ctx,
                                                sign_cert_path.string().c_str(),
                                                SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR << "failed to load SM2 signing certificate: "
                              << sign_cert_path.string()
                              << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded SM2 signing certificate: "
                           << sign_cert_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "SM2 signing certificate file not found: "
                            << sign_cert_path.string();
          return false;
        }

        if (fs::exists(sign_key_path, file_ec)) {
          if (SSL_CTX_use_sign_PrivateKey_file(
                  ctx, sign_key_path.string().c_str(), SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR << "failed to load SM2 signing private key: "
                              << sign_key_path.string()
                              << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded SM2 signing private key: "
                           << sign_key_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "SM2 signing key file not found: "
                            << sign_key_path.string();
          return false;
        }

        // Load SM2 encryption certificate and key
        if (fs::exists(enc_cert_path, file_ec)) {
          if (SSL_CTX_use_enc_certificate_file(
                  ctx, enc_cert_path.string().c_str(), SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR << "failed to load SM2 encryption certificate: "
                              << enc_cert_path.string()
                              << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded SM2 encryption certificate: "
                           << enc_cert_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "SM2 encryption certificate file not found: "
                            << enc_cert_path.string();
          return false;
        }

        if (fs::exists(enc_key_path, file_ec)) {
          if (SSL_CTX_use_enc_PrivateKey_file(
                  ctx, enc_key_path.string().c_str(), SSL_FILETYPE_PEM) != 1) {
            unsigned long err = ::ERR_get_error();
            CINATRA_LOG_ERROR << "failed to load SM2 encryption private key: "
                              << enc_key_path.string()
                              << ", err: " << ::ERR_error_string(err, nullptr);
            return false;
          }
          CINATRA_LOG_INFO << "loaded SM2 encryption private key: "
                           << enc_key_path.string();
        }
        else {
          CINATRA_LOG_ERROR << "SM2 encryption key file not found: "
                            << enc_key_path.string();
          return false;
        }
      }
      // Load CA certificate if provided
      if (!ntls_config.ca_cert_file.empty()) {
        auto ca_cert_path =
            ntls_config.base_path.empty()
                ? fs::path(ntls_config.ca_cert_file)
                : fs::path(ntls_config.base_path) / ntls_config.ca_cert_file;
        if (fs::exists(ca_cert_path, file_ec)) {
          asio::error_code ec;
          ssl_ctx_->load_verify_file(ca_cert_path.string(), ec);
          if (ec) {
            CINATRA_LOG_WARNING
                << "failed to load CA certificate: " << ca_cert_path.string()
                << ", error: " << ec.message();
          }
        }
      }

      // Set verification mode
      asio::error_code ec;
      if (ntls_config.enable_client_verify) {
        ssl_ctx_->set_verify_mode(asio::ssl::verify_peer, ec);
      }
      else {
        ssl_ctx_->set_verify_mode(asio::ssl::verify_none, ec);
      }
      if (ec) {
        CINATRA_LOG_WARNING << "failed to set verify mode: " << ec.message();
      }

      // Create SSL stream
      socket_wrapper_.ssl_stream() =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              *socket_wrapper_.socket(), *ssl_ctx_);

      CINATRA_LOG_INFO << "NTLS server context initialized successfully";
      return true;
    } catch (const std::exception &e) {
      CINATRA_LOG_ERROR << "NTLS context init error: " << e.what();
      return false;
    }
  }
#endif  // YLT_ENABLE_NTLS
#endif

  void add_head(std::string_view msg) {
    head_buf_.sputn(msg.data(), msg.size());
  }
  async_simple::coro::Lazy<void> start(bool has_shake = false) {
    std::chrono::system_clock::time_point start{};
    std::chrono::system_clock::time_point mid{};
    while (true) {
#ifdef CINATRA_ENABLE_SSL
      if (socket_wrapper_.use_ssl() && !has_shake) {
        auto ec = co_await coro_io::async_handshake(
            socket_wrapper_.ssl_stream(), asio::ssl::stream_base::server);
        if (ec) {
          CINATRA_LOG_ERROR << "handle_shake error: " << ec.message();
          close();
          break;
        }

        has_shake = true;
      }
#endif
      auto [ec, size] = co_await async_read_until(head_buf_, TWO_CRCF);
      if (ec) {
        if (ec != asio::error::eof) {
          CINATRA_LOG_WARNING << "read http header error: " << ec.message();
        }

        close();
        break;
      }

      const char *data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      int head_len = parser_.parse_request(data_ptr, size, 0);
      if (head_len <= 0) {
        CINATRA_LOG_ERROR << "parse http header error";
        response_.set_status_and_content(status_type::bad_request,
                                         "invalid http protocol");
        co_await reply();
        close();
        break;
      }

      if (parser_.body_len() > max_http_body_len_ || parser_.body_len() < 0)
          [[unlikely]] {
        CINATRA_LOG_ERROR << "invalid http content length: "
                          << parser_.body_len();
        response_.set_status_and_content(status_type::bad_request,
                                         "invalid http content length");
        co_await reply();
        close();
        break;
      }

      head_buf_.consume(size);
      keep_alive_ = check_keep_alive();

      auto type = request_.get_content_type();

      if (type != content_type::chunked && type != content_type::multipart) {
        size_t body_len = (size_t)parser_.body_len();
        if (body_len == 0) {
          if (parser_.method() == "GET"sv) {
            if (request_.is_upgrade()) {
#ifdef CINATRA_ENABLE_GZIP
              if (request_.is_support_compressed()) {
                is_client_ws_compressed_ = true;
              }
              else {
                is_client_ws_compressed_ = false;
              }
#endif
              // websocket
              build_ws_handshake_head();
              bool ok = co_await reply(true);  // response ws handshake
              if (!ok) {
                close();
                break;
              }
              response_.set_delay(true);
            }
          }
        }
        else if (body_len <= head_buf_.size()) {
          if (body_len > 0) {
            detail::resize(body_, body_len);
            auto data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
            memcpy(body_.data(), data_ptr, body_len);
            head_buf_.consume(head_buf_.size());
          }
        }
        else {
          size_t part_size = head_buf_.size();
          size_t size_to_read = body_len - part_size;
          auto data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
          detail::resize(body_, body_len);
          memcpy(body_.data(), data_ptr, part_size);
          head_buf_.consume(part_size);

          auto [ec, size] = co_await async_read(
              asio::buffer(body_.data() + part_size, size_to_read),
              size_to_read);
          if (ec) {
            CINATRA_LOG_ERROR << "async_read error: " << ec.message();
            close();
            break;
          }
        }
      }

      std::string_view key = {
          parser_.method().data(),
          parser_.method().length() + 1 + parser_.url().length()};

      std::string decode_key;
      if (parser_.url().find('%') != std::string_view::npos) {
        decode_key = code_utils::url_decode(key);
        key = decode_key;
      }

      if (!body_.empty()) {
        request_.set_body(body_);
      }

      if (auto handler = router_.get_handler(key); handler) {
        router_.route(handler, request_, response_, key);
      }
      else {
        if (auto coro_handler = router_.get_coro_handler(key); coro_handler) {
          co_await router_.route_coro(coro_handler, request_, response_, key);
        }
        else {
          bool is_exist = false;
          bool is_coro_exist = false;
          bool is_matched_regex_router = false;
          std::function<void(coro_http_request & req,
                             coro_http_response & resp)>
              handler;
          std::string method_str{parser_.method()};
          std::string url_path = method_str;
          url_path.append(" ").append(parser_.url());
          std::tie(is_exist, handler, request_.params_) =
              router_.get_router_tree()->get(url_path, method_str);
          if (is_exist) {
            if (handler) {
              (handler)(request_, response_);
            }
            else {
              response_.set_status(status_type::not_found);
            }
          }
          else {
            std::function<async_simple::coro::Lazy<void>(
                coro_http_request & req, coro_http_response & resp)>
                coro_handler;

            std::tie(is_coro_exist, coro_handler, request_.params_) =
                router_.get_coro_router_tree()->get_coro(url_path, method_str);

            if (is_coro_exist) {
              if (coro_handler) {
                co_await coro_handler(request_, response_);
              }
              else {
                response_.set_status(status_type::not_found);
              }
            }
            else {
              // coro regex router
              auto coro_regex_handlers = router_.get_coro_regex_handlers();
              if (coro_regex_handlers.size() != 0) {
                for (auto &pair : coro_regex_handlers) {
                  std::string coro_regex_key{key};

                  if (std::regex_match(coro_regex_key, request_.matches_,
                                       std::get<0>(pair))) {
                    auto coro_handler = std::get<1>(pair);
                    if (coro_handler) {
                      co_await coro_handler(request_, response_);
                      is_matched_regex_router = true;
                    }
                  }
                }
              }
              // regex router
              if (!is_matched_regex_router) {
                auto regex_handlers = router_.get_regex_handlers();
                if (regex_handlers.size() != 0) {
                  for (auto &pair : regex_handlers) {
                    std::string regex_key{key};
                    if (std::regex_match(regex_key, request_.matches_,
                                         std::get<0>(pair))) {
                      auto handler = std::get<1>(pair);
                      if (handler) {
                        (handler)(request_, response_);
                        is_matched_regex_router = true;
                      }
                    }
                  }
                }
              }
              // radix route -> radix coro route -> regex coro -> regex ->
              // default -> not found
              if (!is_matched_regex_router) {
                if (default_handler_) {
                  co_await default_handler_(request_, response_);
                }
                else {
                  // not found
                  response_.set_status(status_type::not_found);
                }
              }
            }
          }
        }
      }

      if (!response_.get_delay()) {
        if (head_buf_.size()) {
          if (type == content_type::multipart ||
              type == content_type::chunked) {
            if (response_.content().empty())
              response_.set_status_and_content(
                  status_type::not_implemented,
                  "mutipart handler not implemented or incorrect implemented");
            co_await reply();
            close();
            CINATRA_LOG_ERROR
                << "mutipart handler not implemented or incorrect implemented"
                << ec.message();
            break;
          }
          else if (parser_.method()[0] != 'G' && parser_.method()[0] != 'H') {
            // handle pipeling, only support GET and HEAD method now.
            response_.set_status_and_content(status_type::method_not_allowed,
                                             "method not allowed");
            co_await reply();
          }
          else {
            resp_str_.reserve(512);
            response_.build_resp_str(resp_str_);

            while (true) {
              size_t left_size = head_buf_.size();
              auto next_data_ptr =
                  asio::buffer_cast<const char *>(head_buf_.data());
              std::string_view left_content{next_data_ptr, left_size};
              size_t pos = left_content.find(TWO_CRCF);
              if (pos == std::string_view::npos) {
                break;
              }
              http_parser parser;
              int head_len = parser.parse_request(next_data_ptr, left_size, 0);
              if (head_len <= 0) {
                CINATRA_LOG_ERROR << "parse http header error";
                response_.set_status_and_content(status_type::bad_request,
                                                 "invalid http protocol");
                co_await reply();
                close();
                break;
              }

              head_buf_.consume(pos + TWO_CRCF.length());

              std::string_view next_key = {
                  parser.method().data(),
                  parser.method().length() + 1 + parser.url().length()};

              coro_http_request req(parser, this);
              coro_http_response resp(this);
              resp.need_date_head(response_.need_date());
              if (auto handler = router_.get_handler(next_key); handler) {
                router_.route(handler, req, resp, key);
              }
              else {
                if (auto coro_handler = router_.get_coro_handler(next_key);
                    coro_handler) {
                  co_await router_.route_coro(coro_handler, req, resp, key);
                }
                else {
                  resp.set_status(status_type::not_found);
                }
              }

              resp.build_resp_str(resp_str_);
            }

            auto [write_ec, _] = co_await async_write(asio::buffer(resp_str_));
            if (write_ec) {
              CINATRA_LOG_ERROR << "async_write error: " << write_ec.message();
              close();
              co_return;
            }
          }
          head_buf_.consume(head_buf_.size());
        }
        else {
          handle_session_for_response();
          co_await reply();
        }
      }

      if (!keep_alive_) {
        // now in io thread, so can close socket immediately.
        close();
      }

      response_.clear();
      request_.clear();
      buffers_.clear();
      body_.clear();
      resp_str_.clear();
      multi_buf_ = true;
      if (need_shrink_every_time_) {
        body_.shrink_to_fit();
      }
    }
  }

  async_simple::coro::Lazy<bool> reply(bool need_to_bufffer = true) {
    std::error_code ec;
    size_t size;
    if (multi_buf_) {
      if (need_to_bufffer) {
        response_.to_buffers(buffers_, chunk_size_str_);
      }
      std::tie(ec, size) = co_await async_write(buffers_);
    }
    else {
      if (need_to_bufffer) {
        response_.build_resp_str(resp_str_);
      }
      std::tie(ec, size) = co_await async_write(asio::buffer(resp_str_));
    }

    if (ec) {
      CINATRA_LOG_INFO << "async_write error: " << ec.message();
      close();
      co_return false;
    }

    co_return true;
  }

  std::string local_address() {
    std::string addr;
    set_address_impl(addr, false);
    return addr;
  }

  std::string remote_address() {
    if (!remote_addr_.empty()) {
      return remote_addr_;
    }
    set_address_impl(remote_addr_);
    return remote_addr_;
  }

  size_t available() {
    std::error_code ec{};
    return socket_wrapper_.socket()->available(ec);
  }

  void set_multi_buf(bool r) { multi_buf_ = r; }

  void set_default_handler(
      std::function<async_simple::coro::Lazy<void>(
          coro_http_request &, coro_http_response &)> &handler) {
    default_handler_ = handler;
  }

  void set_max_http_body_size(int64_t max_size) {
    max_http_body_len_ = max_size;
  }

#ifdef INJECT_FOR_HTTP_SEVER_TEST
  void set_write_failed_forever(bool r) { write_failed_forever_ = r; }

  void set_read_failed_forever(bool r) { read_failed_forever_ = r; }
#endif

  async_simple::coro::Lazy<bool> write_data(std::string_view message) {
    std::vector<asio::const_buffer> buffers;
    buffers.push_back(asio::buffer(message));
    auto [ec, _] = co_await async_write(buffers);
    if (ec) {
      CINATRA_LOG_ERROR << "async_write error: " << ec.message();
      close();
      co_return false;
    }

    co_return true;
  }

  async_simple::coro::Lazy<bool> begin_chunked() {
    response_.set_delay(true);
    response_.set_status(status_type::ok);
    co_return co_await reply();
  }

  async_simple::coro::Lazy<bool> write_chunked(std::string_view chunked_data,
                                               bool eof = false) {
    response_.set_delay(true);
    buffers_.clear();
    to_chunked_buffers(buffers_, chunk_size_str_, chunked_data, eof);
    co_return co_await reply(false);
  }

  async_simple::coro::Lazy<bool> end_chunked() {
    co_return co_await write_chunked("", true);
  }

  async_simple::coro::Lazy<bool> begin_multipart(
      std::string_view boundary = "", std::string_view content_type = "") {
    response_.set_delay(true);
    response_.set_status(status_type::ok);
    if (boundary.empty()) {
      boundary = BOUNDARY;
    }
    if (content_type.empty()) {
      content_type = "multipart/form-data";
    }

    std::string str{content_type};
    str.append("; ").append("boundary=").append(boundary);
    response_.add_header("Content-Type", str);
    response_.set_boundary(boundary);
    co_return co_await reply();
  }

  async_simple::coro::Lazy<bool> write_multipart(
      std::string_view part_data, std::string_view content_type) {
    response_.set_delay(true);
    buffers_.clear();
    std::string part_head = "--";
    part_head.append(response_.get_boundary()).append(CRCF);
    part_head.append("Content-Type: ").append(content_type).append(CRCF);
    part_head.append("Content-Length: ")
        .append(std::to_string(part_data.size()))
        .append(TWO_CRCF);

    buffers_.push_back(asio::buffer(part_head));
    buffers_.push_back(asio::buffer(part_data));
    buffers_.push_back(asio::buffer(CRCF));

    auto [ec, _] = co_await async_write(buffers_);
    co_return !ec;
  }

  async_simple::coro::Lazy<bool> end_multipart() {
    response_.set_delay(true);
    buffers_.clear();
    std::string multipart_end = "--";
    multipart_end.append(response_.get_boundary()).append("--").append(CRCF);
    auto [ec, _] = co_await async_write(asio::buffer(multipart_end));
    co_return !ec;
  }

  async_simple::coro::Lazy<chunked_result> read_chunked() {
    if (head_buf_.size() > 0) {
      const char *data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      chunked_buf_.sputn(data_ptr, head_buf_.size());
      head_buf_.consume(head_buf_.size());
    }

    chunked_result result{};
    std::error_code ec{};
    size_t size = 0;

    if (std::tie(ec, size) = co_await async_read_until(chunked_buf_, CRCF);
        ec) {
      result.ec = ec;
      close();
      co_return result;
    }

    size_t buf_size = chunked_buf_.size();
    size_t additional_size = buf_size - size;
    const char *data_ptr = asio::buffer_cast<const char *>(chunked_buf_.data());
    std::string_view size_str(data_ptr, size - CRCF.size());
    size_t chunk_size;
    auto [ptr, err] = std::from_chars(
        size_str.data(), size_str.data() + size_str.size(), chunk_size, 16);
    if (err != std::errc{}) {
      CINATRA_LOG_ERROR << "bad chunked size";
      result.ec = std::make_error_code(std::errc::invalid_argument);
      co_return result;
    }

    chunked_buf_.consume(size);

    if (additional_size < size_t(chunk_size + 2)) {
      // not a complete chunk, read left chunk data.
      size_t size_to_read = chunk_size + 2 - additional_size;
      if (std::tie(ec, size) = co_await async_read(chunked_buf_, size_to_read);
          ec) {
        result.ec = ec;
        close();
        co_return result;
      }
    }

    if (chunk_size == 0) {
      // all finished, no more data
      chunked_buf_.consume(chunked_buf_.size());
      result.eof = true;
      co_return result;
    }

    data_ptr = asio::buffer_cast<const char *>(chunked_buf_.data());
    result.data = std::string_view{data_ptr, (size_t)chunk_size};
    chunked_buf_.consume(chunk_size + CRCF.size());

    co_return result;
  }

  async_simple::coro::Lazy<std::error_code> write_websocket(
      std::string_view msg, opcode op = opcode::text, bool eof = true) {
    std::vector<asio::const_buffer> buffers;
    std::string_view header;
#ifdef CINATRA_ENABLE_GZIP
    std::string dest_buf;
    if (is_client_ws_compressed_ && msg.size() > 0) {
      if (!cinatra::gzip_codec::deflate(msg, dest_buf)) {
        CINATRA_LOG_ERROR << "compress data error, data: " << msg;
        co_return std::make_error_code(std::errc::protocol_error);
      }

      header = ws_.encode_ws_header(dest_buf.length(), op, eof, true, false);
      buffers.push_back(asio::buffer(header));
      buffers.push_back(asio::buffer(dest_buf));
    }
    else {
#endif
      header = ws_.encode_ws_header(msg.length(), op, eof, false, false);
      buffers.push_back(asio::buffer(header));
      buffers.push_back(asio::buffer(msg));
#ifdef CINATRA_ENABLE_GZIP
    }
#endif
    auto [ec, sz] = co_await async_write(buffers);
    co_return ec;
  }

  async_simple::coro::Lazy<websocket_result> read_websocket() {
    auto [ec, ws_hd_size] = co_await async_read(head_buf_, SHORT_HEADER);
    websocket_result result{ec};
    if (ec) {
      close();
      co_return result;
    }

    while (true) {
      const char *data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      auto status = ws_.parse_header(data_ptr, ws_.len_bytes());
      if (status == ws_header_status::complete) {
        ws_.reset_len_bytes();
        head_buf_.consume(head_buf_.size());
        std::span<char> payload{};
        auto payload_length = ws_.payload_length();

        if (max_part_size_ != 0 && payload_length > max_part_size_) {
          std::string close_reason = "message_too_big";
          std::string close_msg = ws_.format_close_payload(
              close_code::too_big, close_reason.data(), close_reason.size());
          co_await write_websocket(close_msg, opcode::close);
          close();
          result.ec = std::error_code(asio::error::message_size,
                                      asio::error::get_system_category());
          break;
        }

        if (payload_length > 0) {
          detail::resize(body_, payload_length);
          auto [ec, read_sz] =
              co_await async_read(asio::buffer(body_), payload_length);
          if (ec) {
            close();
            result.ec = ec;
            break;
          }
          payload = body_;
        }

        ws_frame_type type = ws_.parse_payload(payload);

        switch (type) {
          case cinatra::ws_frame_type::WS_ERROR_FRAME:
            close();
            result.ec = std::make_error_code(std::errc::protocol_error);
            break;
          case cinatra::ws_frame_type::WS_OPENING_FRAME:
            continue;
          case ws_frame_type::WS_INCOMPLETE_TEXT_FRAME:
          case ws_frame_type::WS_INCOMPLETE_BINARY_FRAME:
            result.eof = false;
            result.data = {payload.data(), payload.size()};
            break;
          case cinatra::ws_frame_type::WS_TEXT_FRAME:
          case cinatra::ws_frame_type::WS_BINARY_FRAME: {
#ifdef CINATRA_ENABLE_GZIP
            if (!gzip_compress(payload, result)) {
              break;
            }
#endif
            result.eof = true;
            result.data = {payload.data(), payload.size()};
          } break;
          case cinatra::ws_frame_type::WS_CLOSE_FRAME: {
#ifdef CINATRA_ENABLE_GZIP
            if (!gzip_compress(payload, result)) {
              break;
            }
#endif
            close_frame close_frame =
                ws_.parse_close_payload(payload.data(), payload.size());
            result.eof = true;
            result.data = {close_frame.message, close_frame.length};

            std::string close_msg = ws_.format_close_payload(
                close_code::normal, close_frame.message, close_frame.length);

            co_await write_websocket(close_msg, opcode::close);
            close();
          } break;
          case cinatra::ws_frame_type::WS_PING_FRAME: {
            result.data = {payload.data(), payload.size()};
            auto ec = co_await write_websocket(result.data, opcode::pong);
            if (ec) {
              close();
              result.ec = ec;
            }
          } break;
          case cinatra::ws_frame_type::WS_PONG_FRAME: {
            result.data = {payload.data(), payload.size()};
            auto ec = co_await write_websocket(result.data, opcode::ping);
            result.ec = ec;
          } break;
          default:
            break;
        }

        result.type = type;
        co_return result;
      }
      else if (status == ws_header_status::incomplete) {
        auto [ec, sz] = co_await async_read(head_buf_, ws_.left_header_len());
        if (ec) {
          close();
          result.ec = ec;
          break;
        }
        continue;
      }
      else {
        close();
        result.ec = std::make_error_code(std::errc::protocol_error);
        co_return result;
      }
    }

    co_return result;
  }

#ifdef CINATRA_ENABLE_GZIP
  bool gzip_compress(std::span<char> &payload, websocket_result &result) {
    if (is_client_ws_compressed_) {
      inflate_str_.clear();
      if (!cinatra::gzip_codec::inflate({payload.data(), payload.size()},
                                        inflate_str_)) {
        CINATRA_LOG_ERROR << "compress data error";
        result.ec = std::make_error_code(std::errc::protocol_error);
        return false;
      }
      payload = inflate_str_;
    }
    return true;
  }
#endif

  auto &tcp_socket() { return *socket_wrapper_.socket(); }

  uint64_t conn_id() const { return conn_id_; }

  void set_quit_callback(std::function<void(const uint64_t &conn_id)> callback,
                         uint64_t conn_id) {
    quit_cb_ = std::move(callback);
    conn_id_ = conn_id;
  }

  void set_ws_max_size(uint64_t max_size) { max_part_size_ = max_size; }

  void set_shrink_to_fit(bool r) {
    need_shrink_every_time_ = r;
    response_.set_shrink_to_fit(r);
  }

#ifdef INJECT_FOR_HTTP_SEVER_TEST
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
  async_write_failed() {
    co_return std::make_pair(std::make_error_code(std::errc::io_error), 0);
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
  async_read_failed() {
    co_return std::make_pair(std::make_error_code(std::errc::io_error), 0);
  }
#endif

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read(
      AsioBuffer &&buffer, size_t size_to_read) noexcept {
#ifdef INJECT_FOR_HTTP_SEVER_TEST
    if (read_failed_forever_) {
      return async_read_failed();
    }
#endif
    set_last_time();
#ifdef CINATRA_ENABLE_SSL
    if (socket_wrapper_.use_ssl()) {
      return coro_io::async_read(*socket_wrapper_.ssl_stream(), buffer,
                                 size_to_read);
    }
    else {
#endif
      return coro_io::async_read(*socket_wrapper_.socket(), buffer,
                                 size_to_read);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write(
      AsioBuffer &&buffer) {
#ifdef INJECT_FOR_HTTP_SEVER_TEST
    if (write_failed_forever_) {
      return async_write_failed();
    }
#endif
    set_last_time();
#ifdef CINATRA_ENABLE_SSL
    if (socket_wrapper_.use_ssl()) {
      return coro_io::async_write(*socket_wrapper_.ssl_stream(), buffer);
    }
    else {
#endif
      return coro_io::async_write(*socket_wrapper_.socket(), buffer);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read_until(
      AsioBuffer &buffer, asio::string_view delim) noexcept {
    set_last_time();
#ifdef CINATRA_ENABLE_SSL
    if (socket_wrapper_.use_ssl()) {
      return coro_io::async_read_until(*socket_wrapper_.ssl_stream(), buffer,
                                       delim);
    }
    else {
#endif
      return coro_io::async_read_until(*socket_wrapper_.socket(), buffer,
                                       delim);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  void set_last_time() {
    if (checkout_timeout_) {
      last_rwtime_ = std::chrono::system_clock::now();
    }
  }

  std::chrono::system_clock::time_point get_last_rwtime() {
    return last_rwtime_;
  }

  auto get_executor() { return executor_; }

  void close(bool need_cb = true) {
    if (has_closed_) {
      return;
    }
    if (socket_wrapper_.socket()) {
      asio::dispatch(socket_wrapper_.socket()->get_executor(),
                     [this, need_cb, self = shared_from_this()] {
                       std::error_code ec;
                       socket_wrapper_.socket()->shutdown(
                           asio::socket_base::shutdown_both, ec);
                       socket_wrapper_.socket()->close(ec);
                       if (need_cb && quit_cb_) {
                         quit_cb_(conn_id_);
                       }
                       has_closed_ = true;
                     });
    }
  }

  void set_check_timeout(bool r) { checkout_timeout_ = r; }

  void handle_session_for_response() {
    if (request_.has_session()) {
      auto session =
          session_manager::get().get_session(request_.get_cached_session_id());
      if (session != nullptr && session->get_need_set_to_client()) {
        response_.add_cookie(session->get_session_cookie());
        session->set_need_set_to_client(false);
      }
    }
  }

 private:
  bool check_keep_alive() {
    if (parser_.has_close()) {
      return false;
    }
    return true;
  }

  void build_ws_handshake_head() {
    uint8_t sha1buf[20], key_src[60];
    char accept_key[29];

    std::memcpy(key_src, request_.get_header_value("sec-websocket-key").data(),
                24);
    std::memcpy(key_src + 24, ws_guid, 36);
    sha1_context ctx;
    init(ctx);
    update(ctx, key_src, sizeof(key_src));
    finish(ctx, sha1buf);

    code_utils::base64_encode(accept_key, sha1buf, sizeof(sha1buf), 0);

    response_.set_status_and_content(status_type::switching_protocols, "");

    response_.add_header("Upgrade", "WebSocket");
    response_.add_header("Connection", "Upgrade");
    response_.add_header("Sec-WebSocket-Accept", std::string(accept_key, 28));
    auto protocal_str = request_.get_header_value("sec-websocket-protocol");
#ifdef CINATRA_ENABLE_GZIP
    if (is_client_ws_compressed_) {
      response_.add_header("Sec-WebSocket-Extensions",
                           "permessage-deflate; client_no_context_takeover");
    }
#endif
    if (!protocal_str.empty()) {
      response_.add_header("Sec-WebSocket-Protocol", std::string(protocal_str));
    }
  }

  void set_address_impl(std::string &address, bool remote = true) {
    if (has_closed_) {
      return;
    }

    std::error_code ec;
    auto pt = remote ? socket_wrapper_.socket()->remote_endpoint(ec)
                     : socket_wrapper_.socket()->local_endpoint(ec);
    if (ec) {
      return;
    }
    address = pt.address().to_string(ec);
    if (ec) {
      return;
    }
    address.append(":").append(std::to_string(pt.port()));
  }

 private:
  friend class multipart_reader_t<coro_http_connection>;
  coro_io::ExecutorWrapper<> *executor_;
  coro_io::socket_wrapper_t socket_wrapper_;
  coro_http_router &router_;
  asio::streambuf head_buf_;
  std::string body_;
  asio::streambuf chunked_buf_;
  http_parser parser_;
  bool keep_alive_;
  coro_http_request request_;
  coro_http_response response_;
  std::vector<asio::const_buffer> buffers_;
  std::atomic<bool> has_closed_{false};
  uint64_t conn_id_{0};
  std::function<void(const uint64_t &conn_id)> quit_cb_ = nullptr;
  bool checkout_timeout_ = false;
  std::atomic<std::chrono::system_clock::time_point> last_rwtime_ =
      std::chrono::system_clock::now();
  uint64_t max_part_size_ = 8 * 1024 * 1024;
  std::string resp_str_;

#ifdef CINATRA_ENABLE_GZIP
  bool is_client_ws_compressed_ = false;
  std::string inflate_str_;
#endif

  websocket ws_;
#ifdef CINATRA_ENABLE_SSL
  std::unique_ptr<asio::ssl::context> ssl_ctx_ = nullptr;
#endif
  bool need_shrink_every_time_ = false;
  bool multi_buf_ = true;
  std::function<async_simple::coro::Lazy<void>(coro_http_request &,
                                               coro_http_response &)>
      default_handler_ = nullptr;
  std::string chunk_size_str_;
  std::string remote_addr_;
  int64_t max_http_body_len_ = 0;
#ifdef INJECT_FOR_HTTP_SEVER_TEST
  bool write_failed_forever_ = false;
  bool read_failed_forever_ = false;
#endif
};
}  // namespace cinatra
