#pragma once
#include <atomic>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "asio/dispatch.hpp"
#include "asio/error.hpp"
#include "asio/ip/tcp.hpp"
#include "asio/streambuf.hpp"
#include "async_simple/Future.h"
#include "async_simple/Unit.h"
#include "async_simple/coro/FutureAwaiter.h"
#include "async_simple/coro/Lazy.h"
#ifdef CINATRA_ENABLE_GZIP
#include "gzip.hpp"
#endif
#ifdef CINATRA_ENABLE_BROTLI
#include "brzip.hpp"
#endif
#include "cinatra_log_wrapper.hpp"
#include "error.hpp"
#include "http_parser.hpp"
#include "multipart.hpp"
#include "picohttpparser.h"
#include "response_cv.hpp"
#include "string_resize.hpp"
#include "uri.hpp"
#include "websocket.hpp"
#include "ylt/coro_io/coro_file.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"

namespace coro_io {
template <typename T, typename U>
class client_pool;
}
namespace cinatra {
template <class, class = void>
struct is_stream : std::false_type {};

template <class T>
struct is_stream<
    T, std::void_t<decltype(std::declval<T>().read(nullptr, 0),
                            std::declval<T>().async_read(nullptr, 0))>>
    : std::true_type {};

template <class T>
constexpr bool is_stream_v = is_stream<T>::value;

template <class, class = void>
struct is_span : std::false_type {};

template <class T>
struct is_span<T, std::void_t<decltype(std::declval<T>().data(),
                                       std::declval<T>().size())>>
    : std::true_type {};

template <class T>
constexpr bool is_span_v = is_span<T>::value;

template <class, class = void>
struct is_smart_ptr : std::false_type {};

template <class T>
struct is_smart_ptr<
    T, std::void_t<decltype(std::declval<T>().get(), *std::declval<T>(),
                            is_stream_v<typename T::element_type>)>>
    : std::true_type {};

template <class T>
constexpr bool is_stream_ptr_v = is_smart_ptr<T>::value || std::is_pointer_v<T>;

struct http_header;

struct resp_data {
  std::error_code net_err;
  int status = 0;
  bool eof = false;
  std::string_view resp_body;
  std::span<http_header> resp_headers;
#ifdef BENCHMARK_TEST
  uint64_t total = 0;
#endif
};

template <typename String = std::string>
struct req_context {
  req_content_type content_type = req_content_type::none;
  std::string req_header; /*header string*/
  String content;         /*body*/
  coro_io::coro_file *resp_body_stream = nullptr;
};

struct multipart_t {
  std::string filename;
  std::string content;
  size_t size = 0;
};

struct read_result {
  std::span<char> buf;
  bool eof;
  std::error_code err;
};

enum class upload_type_t { with_length, chunked, multipart };

class coro_http_client : public std::enable_shared_from_this<coro_http_client> {
 public:
  struct config {
    std::optional<std::chrono::steady_clock::duration> conn_timeout_duration;
    std::optional<std::chrono::steady_clock::duration> req_timeout_duration;
    std::string sec_key;
    size_t max_single_part_size;
    std::string proxy_host;
    std::string proxy_port;
    std::string proxy_auth_username;
    std::string proxy_auth_passwd;
    std::string proxy_auth_token;
    bool enable_tcp_no_delay;
#ifdef CINATRA_ENABLE_SSL
    bool use_ssl =
        false;  // if set use_ssl true, cinatra will add https automaticlly.
#ifdef YLT_ENABLE_NTLS
    bool use_ntls =
        false;  // if set use_ntls true, cinatra will use NTLS/TLCP protocol.
#endif          // YLT_ENABLE_NTLS
#endif
  };

  coro_http_client(asio::io_context::executor_type executor)
      : executor_wrapper_(executor),
        timer_(&executor_wrapper_),
        socket_(std::make_shared<socket_t>(executor)),
        head_buf_(socket_->head_buf_),
        chunked_buf_(socket_->chunked_buf_),
        create_tp_(std::chrono::steady_clock::now()) {}

  coro_http_client(
      coro_io::ExecutorWrapper<> *executor = coro_io::get_global_executor())
      : coro_http_client(executor->get_asio_executor()) {}

  bool init_config(const config &conf) {
    config_ = conf;
    if (conf.conn_timeout_duration.has_value()) {
      set_conn_timeout(*conf.conn_timeout_duration);
    }
    if (conf.req_timeout_duration.has_value()) {
      set_req_timeout(*conf.req_timeout_duration);
    }
    if (!conf.sec_key.empty()) {
      set_ws_sec_key(conf.sec_key);
    }
    if (conf.max_single_part_size > 0) {
      set_max_single_part_size(conf.max_single_part_size);
    }
    if (!conf.proxy_host.empty()) {
      set_proxy_basic_auth(conf.proxy_host, conf.proxy_port);
    }
    if (!conf.proxy_auth_username.empty()) {
      set_proxy_basic_auth(conf.proxy_auth_username, conf.proxy_auth_passwd);
    }
    if (!conf.proxy_auth_token.empty()) {
      set_proxy_bearer_token_auth(conf.proxy_auth_token);
    }
    if (conf.enable_tcp_no_delay) {
      enable_tcp_no_delay_ = conf.enable_tcp_no_delay;
    }
#ifdef CINATRA_ENABLE_SSL
    set_ssl_schema(conf.use_ssl);
#ifdef YLT_ENABLE_NTLS
    set_ntls_schema(conf.use_ntls);
#endif  // YLT_ENABLE_NTLS
#endif
    return true;
  }

  ~coro_http_client() { close(); }

  auto get_create_time_point() const noexcept {
    return std::chrono::steady_clock::now();
  }

  void close() {
    if (socket_ == nullptr || socket_->has_closed_)
      return;

    asio::dispatch(executor_wrapper_.get_asio_executor(), [socket = socket_] {
      close_socket(*socket);
    });
  }

  coro_io::ExecutorWrapper<> &get_executor() { return executor_wrapper_; }

  const config &get_config() { return config_; }

#ifdef CINATRA_ENABLE_SSL
  bool init_ssl(int verify_mode, const std::string &base_path,
                const std::string &cert_file, const std::string &sni_hostname) {
    if (has_init_ssl_) {
      return true;
    }

    try {
#ifdef YLT_ENABLE_NTLS
      if (use_ntls_) {
        ssl_ctx_ = std::make_unique<asio::ssl::context>(
            SSL_CTX_new(NTLS_client_method()));

        // Enable NTLS mode for Tongsuo
        SSL_CTX_enable_ntls(ssl_ctx_->native_handle());

        // Set NTLS cipher suites
        if (!SSL_CTX_set_cipher_list(
                ssl_ctx_->native_handle(),
                "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3")) {
          CINATRA_LOG_WARNING << "failed to set NTLS cipher suites";
        }
      }
      else {
        CINATRA_LOG_ERROR << "NTLS is not supported in this build.";
        return false;
      }
#else
      ssl_ctx_ =
          std::make_unique<asio::ssl::context>(asio::ssl::context::sslv23);
#endif  // YLT_ENABLE_NTLS
      auto full_cert_file = std::filesystem::path(base_path).append(cert_file);
      if (std::filesystem::exists(full_cert_file)) {
        ssl_ctx_->load_verify_file(full_cert_file.string());
      }
      else {
        if (!base_path.empty() || !cert_file.empty())
          return false;
      }

      if (base_path.empty() && cert_file.empty()) {
        ssl_ctx_->set_default_verify_paths();
      }

      ssl_ctx_->set_verify_mode(verify_mode);

      socket_->ssl_stream_ =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              socket_->impl_, *ssl_ctx_);

      // ssl_ctx_.add_certificate_authority(asio::buffer(CA_PEM));
      if (!sni_hostname.empty()) {
        ssl_ctx_->set_verify_callback(
            asio::ssl::host_name_verification(sni_hostname));

        if (need_set_sni_host_) {
          // Set SNI Hostname (many hosts need this to handshake successfully)
          SSL_set_tlsext_host_name(socket_->ssl_stream_->native_handle(),
                                   sni_hostname.c_str());
        }
      }

      has_init_ssl_ = true;
    } catch (std::exception &e) {
      CINATRA_LOG_ERROR << "init ssl failed: " << e.what();
      return false;
    }
    return true;
  }

  [[nodiscard]] bool init_ssl(int verify_mode = asio::ssl::verify_none,
                              std::string full_path = "",
                              const std::string &sni_hostname = "") {
    std::string base_path;
    std::string cert_file;
    if (full_path.empty()) {
      base_path = "";
      cert_file = "";
    }
    else {
      base_path = full_path.substr(0, full_path.find_last_of('/'));
      cert_file = full_path.substr(full_path.find_last_of('/') + 1);
    }
    return init_ssl(verify_mode, base_path, cert_file, sni_hostname);
  }
#endif

  // return body_, the user will own body's lifetime.
  std::string release_buf() {
    if (body_.empty()) {
      return std::move(resp_chunk_str_);
    }
    return std::move(body_);
  }

#ifdef CINATRA_ENABLE_GZIP
  void set_ws_deflate(bool enable_ws_deflate) {
    enable_ws_deflate_ = enable_ws_deflate;
  }
#endif

  /*!
   * Connect server
   *
   * only make socket connet(or handshake) to the host
   *
   * @param uri server address
   * @param eps endpoints of resolve result. if eps is not nullptr and vector is
   * empty, it will return the endpoints that, else if vector is not empty, it
   * will use the eps to skill resolve and connect to server directly.
   * @return resp_data
   */
  async_simple::coro::Lazy<resp_data> connect(
      std::string uri, std::vector<asio::ip::tcp::endpoint> *eps = nullptr) {
    resp_data data{};
    bool no_schema = !has_schema(uri);
    std::string append_uri;
    if (no_schema) {
#ifdef CINATRA_ENABLE_SSL
      if (is_ssl_schema_)
        append_uri.append("https://").append(uri);
      else
#endif
        append_uri.append("http://").append(uri);
    }

    auto [ok, u] = handle_uri(data, no_schema ? append_uri : uri);
    if (!ok) {
      co_return resp_data{std::make_error_code(std::errc::protocol_error), 404};
    }
    {
      if (u.is_websocket()) {
        // build websocket http header
        add_header("Upgrade", "websocket");
        add_header("Connection", "Upgrade");
        if (ws_sec_key_.empty()) {
          ws_sec_key_ = "s//GYHa/XO7Hd2F2eOGfyA==";  // provide a random string.
        }
        add_header("Sec-WebSocket-Key", ws_sec_key_);
        add_header("Sec-WebSocket-Version", "13");
#ifdef CINATRA_ENABLE_GZIP
        if (enable_ws_deflate_)
          add_header("Sec-WebSocket-Extensions",
                     "permessage-deflate; client_max_window_bits");
#endif
        req_context<> ctx{};
        data = co_await async_request(std::move(uri), http_method::GET,
                                      std::move(ctx));

#ifdef CINATRA_ENABLE_GZIP
        if (enable_ws_deflate_) {
          for (auto c : data.resp_headers) {
            if (c.name == "Sec-WebSocket-Extensions") {
              if (c.value.find("permessage-deflate;") != std::string::npos) {
                is_server_support_ws_deflate_ = true;
              }
              else {
                is_server_support_ws_deflate_ = false;
              }
              break;
            }
          }
        }
#endif
        co_return data;
      }
      data = co_await connect(u, eps);
    }
    if (socket_->is_timeout_) {
      co_return resp_data{make_error_code(http_errc::connect_timeout), 404};
    }
    if (!data.net_err) {
      data.status = 200;
    }
    co_return data;
  }

  bool has_closed() { return socket_->has_closed_; }

  [[nodiscard]] std::size_t get_pipeline_size() const noexcept { return 0; }

  const auto &get_headers() { return req_headers_; }

  void set_headers(std::unordered_map<std::string, std::string> req_headers) {
    req_headers_ = std::move(req_headers);
  }

  bool add_header(std::string key, std::string val) {
    if (key.empty())
      return false;

    req_headers_[key] = std::move(val);

    return true;
  }

  void set_ws_sec_key(std::string sec_key) { ws_sec_key_ = std::move(sec_key); }

  /**
   * @brief Set the max http body size object, in default it's INT64_MAX
   *
   * @param max_size
   */
  void set_max_http_body_size(int64_t max_size) {
    max_http_body_len_ = max_size;
  }

  size_t available() {
    std::error_code ec{};
    return socket_->impl_.available(ec);
  }

  async_simple::coro::Lazy<resp_data> read_websocket() {
    auto time_out_guard =
        timer_guard(this, req_timeout_duration_, "websocket timer");
    co_return co_await async_read_ws();
  }

  async_simple::coro::Lazy<resp_data> write_websocket(
      const char *data, opcode op = opcode::text) {
    std::string str(data);
    co_return co_await write_websocket(str, op);
  }

  async_simple::coro::Lazy<resp_data> write_websocket(
      const char *data, size_t size, opcode op = opcode::text) {
    std::string str(data, size);
    co_return co_await write_websocket(str, op);
  }

  async_simple::coro::Lazy<resp_data> write_websocket(
      std::string_view data, opcode op = opcode::text) {
    std::string str(data);
    co_return co_await write_websocket(str, op);
  }

  async_simple::coro::Lazy<resp_data> write_websocket(
      std::string &data, opcode op = opcode::text) {
    co_return co_await write_websocket(std::span<char>(data), op);
  }

  async_simple::coro::Lazy<resp_data> write_websocket(
      std::string &&data, opcode op = opcode::text) {
    co_return co_await write_websocket(std::span<char>(data), op);
  }

  async_simple::coro::Lazy<void> write_ws_frame(std::span<char> msg,
                                                websocket ws, opcode op,
                                                resp_data &data,
                                                bool eof = true) {
    auto header = ws.encode_frame(msg, op, eof, enable_ws_deflate_);
    std::vector<asio::const_buffer> buffers{
        asio::buffer(header), asio::buffer(msg.data(), msg.size())};

    auto [ec, sz] = co_await async_write(buffers);
    if (ec) {
      data.net_err = ec;
      data.status = 404;
    }
  }

#ifdef CINATRA_ENABLE_GZIP
  void gzip_compress(std::string_view source, std::string &dest_buf,
                     std::span<char> &span, resp_data &data) {
    if (enable_ws_deflate_ && is_server_support_ws_deflate_) {
      if (cinatra::gzip_codec::deflate(source, dest_buf)) {
        span = dest_buf;
      }
      else {
        CINATRA_LOG_ERROR << "compress data error, data: " << source;
        data.net_err = std::make_error_code(std::errc::protocol_error);
        data.status = 404;
      }
    }
  }
#endif

  template <typename Source>
  async_simple::coro::Lazy<resp_data> write_websocket(
      Source source, opcode op = opcode::text) {
    resp_data data{};

    websocket ws{};
    std::string close_str;
    if (op == opcode::close) {
      if constexpr (is_span_v<Source>) {
        close_str = ws.format_close_payload(close_code::normal, source.data(),
                                            source.size());
        source = {close_str.data(), close_str.size()};
      }
    }

    std::span<char> span{};
    if constexpr (is_span_v<Source>) {
      span = {source.data(), source.size()};
#ifdef CINATRA_ENABLE_GZIP
      std::string dest_buf;
      if (enable_ws_deflate_) {
        gzip_compress({source.data(), source.size()}, dest_buf, span, data);
      }
#endif
      co_await write_ws_frame(span, ws, op, data, true);
    }
    else {
      while (true) {
        auto result = co_await source();
        span = {result.buf.data(), result.buf.size()};
#ifdef CINATRA_ENABLE_GZIP
        std::string dest_buf;
        if (enable_ws_deflate_) {
          gzip_compress({result.buf.data(), result.buf.size()}, dest_buf, span,
                        data);
        }
#endif
        co_await write_ws_frame(span, ws, op, data, result.eof);

        if (result.eof || data.status == 404) {
          break;
        }
      }
    }

    co_return data;
  }

  async_simple::coro::Lazy<resp_data> write_websocket_close(
      std::string msg = "") {
    co_return co_await write_websocket(std::move(msg), opcode::close);
  }

#ifdef BENCHMARK_TEST
  void set_bench_stop() { stop_bench_ = true; }
#endif

  async_simple::coro::Lazy<resp_data> async_patch(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_request(std::move(uri), cinatra::http_method::PATCH,
                         cinatra::req_context<>{}, std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_options(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_request(std::move(uri), cinatra::http_method::OPTIONS,
                         cinatra::req_context<>{}, std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_trace(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_request(std::move(uri), cinatra::http_method::TRACE,
                         cinatra::req_context<>{}, std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_head(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_request(std::move(uri), cinatra::http_method::HEAD,
                         cinatra::req_context<>{}, std::move(headers));
  }

  // CONNECT example.com HTTP/1.1
  async_simple::coro::Lazy<resp_data> async_http_connect(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_request(std::move(uri), cinatra::http_method::CONNECT,
                         cinatra::req_context<>{}, std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_get(
      std::string uri,
      std::unordered_map<std::string, std::string> headers = {}) {
    resp_data data{};
    req_context<> ctx{};
    data = co_await async_request(std::move(uri), http_method::GET,
                                  std::move(ctx), std::move(headers));
#ifdef BENCHMARK_TEST
    data.total = total_len_;
#endif
    if (redirect_uri_.empty() || !is_redirect(data)) {
      co_return data;
    }
    else {
      if (enable_follow_redirect_)
        data = co_await async_request(std::move(redirect_uri_),
                                      http_method::GET, std::move(ctx));
      co_return data;
    }
  }

  resp_data get(std::string uri,
                std::unordered_map<std::string, std::string> headers = {}) {
    return async_simple::coro::syncAwait(
        async_get(std::move(uri), std::move(headers)));
  }

  resp_data post(std::string uri, std::string content,
                 req_content_type content_type,
                 std::unordered_map<std::string, std::string> headers = {}) {
    return async_simple::coro::syncAwait(async_post(
        std::move(uri), std::move(content), content_type, std::move(headers)));
  }

  async_simple::coro::Lazy<resp_data> async_post(
      std::string uri, std::string content, req_content_type content_type,
      std::unordered_map<std::string, std::string> headers = {}) {
    req_context<> ctx{content_type, "", std::move(content)};
    return async_request(std::move(uri), http_method::POST, std::move(ctx),
                         std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_delete(
      std::string uri, std::string content, req_content_type content_type,
      std::unordered_map<std::string, std::string> headers = {}) {
    req_context<> ctx{content_type, "", std::move(content)};
    return async_request(std::move(uri), http_method::DEL, std::move(ctx),
                         std::move(headers));
  }

  async_simple::coro::Lazy<resp_data> async_put(
      std::string uri, std::string content, req_content_type content_type,
      std::unordered_map<std::string, std::string> headers = {}) {
    req_context<> ctx{content_type, "", std::move(content)};
    return async_request(std::move(uri), http_method::PUT, std::move(ctx),
                         std::move(headers));
  }

  bool add_str_part(std::string name, std::string content) {
    size_t size = content.size();
    return form_data_
        .emplace(std::move(name), multipart_t{"", std::move(content), size})
        .second;
  }

  bool add_file_part(std::string name, std::string filename) {
    if (form_data_.find(name) != form_data_.end()) {
      CINATRA_LOG_WARNING << "name already exist: " << name;
      return false;
    }

    std::error_code ec;
    bool r = std::filesystem::exists(filename, ec);
    if (!r || ec) {
      if (ec) {
        CINATRA_LOG_WARNING << ec.message();
      }
      CINATRA_LOG_WARNING << "file not exists, "
                          << std::filesystem::current_path().string();
      return false;
    }

    size_t file_size = std::filesystem::file_size(filename);
    form_data_.emplace(std::move(name),
                       multipart_t{std::move(filename), "", file_size});
    return true;
  }

  void set_max_single_part_size(size_t size) { max_single_part_size_ = size; }

  struct timer_guard {
    timer_guard(coro_http_client *self,
                std::chrono::steady_clock::duration duration, std::string msg)
        : self(self), dur_(duration) {
      self->socket_->is_timeout_ = false;

      if (duration.count() >= 0) {
        self->timeout(self->timer_, duration, std::move(msg))
            .start([](auto &&) {
            });
      }
      return;
    }
    ~timer_guard() {
      if (dur_.count() > 0 && self->socket_->is_timeout_ == false) {
        std::error_code ignore_ec;
        self->timer_.cancel(ignore_ec);
      }
    }
    coro_http_client *self;
    std::chrono::steady_clock::duration dur_;
  };

  async_simple::coro::Lazy<resp_data> async_download(std::string uri,
                                                     std::string filename,
                                                     std::string range = "") {
    resp_data data{};
    coro_io::coro_file file;
    file.open(filename, std::ios::trunc | std::ios::out);
    if (!file.is_open()) {
      data.net_err = std::make_error_code(std::errc::no_such_file_or_directory);
      data.status = 404;
      co_return data;
    }

    req_context<> ctx{};
    if (range.empty()) {
      add_header("Transfer-Encoding", "chunked");
      ctx = {req_content_type::none, "", "", &file};
    }
    else {
      std::string req_str = "Range: bytes=";
      req_str.append(range).append(CRCF);
      ctx = {req_content_type::none, std::move(req_str), {}, &file};
    }

    data = co_await async_request(std::move(uri), http_method::GET,
                                  std::move(ctx));

    co_return data;
  }

  resp_data download(std::string uri, std::string filename,
                     std::string range = "") {
    return async_simple::coro::syncAwait(
        async_download(std::move(uri), std::move(filename), std::move(range)));
  }

  bool is_body_in_out_buf() const { return !out_buf_.empty(); }

  void reset() {
    if (!has_closed()) {
      close_socket(*socket_);
    }

    socket_->impl_ = asio::ip::tcp::socket{executor_wrapper_.context()};
    if (!socket_->impl_.is_open()) {
      std::error_code ec;
      socket_->impl_.open(asio::ip::tcp::v4(), ec);
      if (ec) {
        CINATRA_LOG_WARNING << "client reset socket failed, reason: "
                            << ec.message();
        return;
      }
    }

    socket_->has_closed_ = true;
#ifdef CINATRA_ENABLE_SSL
    need_set_sni_host_ = true;
    if (has_init_ssl_) {
      socket_->ssl_stream_ = nullptr;
      socket_->ssl_stream_ =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              socket_->impl_, *ssl_ctx_);
      has_init_ssl_ = false;
    }
#endif
#ifdef BENCHMARK_TEST
    total_len_ = 0;
#endif

    // clear
    head_buf_.consume(head_buf_.size());
    chunked_buf_.consume(chunked_buf_.size());
    resp_chunk_str_.clear();
  }

  std::string_view get_host() { return host_; }

  std::string_view get_port() { return port_; }

 private:
  async_simple::coro::Lazy<void> send_file_copy_with_chunked(
      std::string_view source, std::error_code &ec) {
    std::string file_data;
    detail::resize(file_data, max_single_part_size_);
    coro_io::coro_file file{};
    file.open(source, std::ios::in);
    if (!file.is_open()) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      co_return;
    }
    while (!file.eof()) {
      auto [rd_ec, rd_size] =
          co_await file.async_read(file_data.data(), file_data.size());
      std::vector<asio::const_buffer> bufs;
      std::string size_str;
      cinatra::to_chunked_buffers(bufs, size_str, {file_data.data(), rd_size},
                                  file.eof());
      std::size_t size;
      if (std::tie(ec, size) = co_await async_write(bufs); ec) {
        break;
      }
    }
  }

  async_simple::coro::Lazy<void> send_file_copy_with_length(
      std::string_view source, std::error_code &ec, std::size_t length,
      std::size_t offset) {
    if (length <= 0) {
      co_return;
    }
    std::string file_data;
    detail::resize(file_data, (std::min)(max_single_part_size_, length));
    coro_io::coro_file file{};
    file.open(source, std::ios::in);
    if (!file.is_open()) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      co_return;
    }
    file.seek(offset, std::ios::cur);
    std::size_t size;
    while (length > 0) {
      if (std::tie(ec, size) = co_await file.async_read(
              file_data.data(), (std::min)(file_data.size(), length));
          ec) {
        // bad request, file may smaller than content-length
        break;
      }
      length -= size;
      if (length > 0 && file.eof()) {
        // bad request, file may smaller than content-length
        ec = std::make_error_code(std::errc::invalid_argument);
        break;
      }
      if (std::tie(ec, size) =
              co_await async_write(asio::buffer(file_data.data(), size));
          ec) {
        break;
      }
    }
  }
#ifdef __linux__
  struct fd_guard {
    int fd;
    fd_guard(const char *file_path) : fd(::open(file_path, O_RDONLY)) {}
    ~fd_guard() {
      if (fd >= 0) {
        ::close(fd);
      }
    }
  };
  async_simple::coro::Lazy<void> send_file_no_copy_with_length(
      const std::filesystem::path &source, std::error_code &ec,
      std::size_t length, std::size_t offset) {
    fd_guard guard(source.c_str());
    if (guard.fd < 0) [[unlikely]] {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      co_return;
    }
    std::size_t actual_len = 0;
    std::tie(ec, actual_len) = co_await coro_io::async_sendfile(
        socket_->impl_, guard.fd, offset, length);
    if (ec) [[unlikely]] {
      co_return;
    }
    if (actual_len != length) [[unlikely]] {
      // bad request, file is smaller than content-length
      ec = std::make_error_code(std::errc::invalid_argument);
      co_return;
    }
  }
  async_simple::coro::Lazy<void> send_file_no_copy_with_chunked(
      const std::filesystem::path &source, std::error_code &ec) {
    fd_guard guard(source.c_str());
    if (guard.fd < 0) [[unlikely]] {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      co_return;
    }
    off_t now_position = 0,
          max_position = std::filesystem::file_size(source, ec);
    if (ec) {
      co_return;
    }
    size_t len =
        std::min<size_t>(max_single_part_size_, max_position - now_position);
    // send chunked
    std::array<char, 24> chunked_buffer;
    std::size_t sz;
    std::tie(ec, sz) = co_await async_write(
        asio::buffer(get_chuncked_buffers<true, false>(len, chunked_buffer)));
    if (ec) [[unlikely]] {
      co_return;
    }
    do {
      std::size_t actual_len = 0;
      std::tie(ec, actual_len) = co_await coro_io::async_sendfile(
          socket_->impl_, guard.fd, now_position, len);
      if (ec) [[unlikely]] {
        co_return;
      }
      if (actual_len != len) [[unlikely]] {
        // bad request, file is smaller than content-length
        ec = std::make_error_code(std::errc::invalid_argument);
        co_return;
      }
      if (now_position += actual_len; now_position < max_position) {
        len = std::min<size_t>(max_single_part_size_,
                               max_position - now_position);
        std::tie(ec, sz) = co_await async_write(asio::buffer(
            get_chuncked_buffers<false, false>(len, chunked_buffer)));
        if (ec) {
          co_return;
        }
      }
      else [[unlikely]] {
        std::tie(ec, sz) = co_await async_write(asio::buffer(
            get_chuncked_buffers<false, true>(len, chunked_buffer)));
        if (ec) {
          co_return;
        }
        break;
      }
    } while (true);
  }
#endif
  template <typename stream>
  static std::size_t getRemainingBytes(stream &file) {
    auto current_pos = file.tellg();
    file.seekg(0, std::ios::end);
    auto end_pos = file.tellg();
    auto remaining_bytes = end_pos - current_pos;
    file.seekg(current_pos);
    return remaining_bytes;
  }

  template <typename Source>
  void check_source(resp_data &data, Source &source) {
    if constexpr (is_stream_ptr_v<Source>) {
      if (!source) {
        data = resp_data{
            std::make_error_code(std::errc::no_such_file_or_directory), 404};
      }
    }
    else if constexpr (std::is_same_v<Source, std::string> ||
                       std::is_same_v<Source, std::string_view>) {
      if (!std::filesystem::exists(source)) {
        data = resp_data{
            std::make_error_code(std::errc::no_such_file_or_directory), 404};
      }
    }
  }

  void handle_upload_header_with_multipart() {
    size_t content_len = multipart_content_len();
    add_header("Content-Length", std::to_string(content_len));
  }

  void handle_upload_header_with_chunked(
      std::unordered_map<std::string, std::string> &headers) {
    if (!resp_chunk_str_.empty()) {
      resp_chunk_str_.clear();
    }

    if (headers.empty()) {
      add_header("Transfer-Encoding", "chunked");
    }
    else {
      headers.emplace("Transfer-Encoding", "chunked");
    }
  }

  template <typename Source>
  int64_t handle_upload_header_with_length(
      resp_data &data, Source &source,
      std::unordered_map<std::string, std::string> &headers, uint64_t offset,
      int64_t content_length) {
    if (content_length < 0) {
      if constexpr (is_stream_ptr_v<Source>) {
        content_length = getRemainingBytes(*source);
      }
      else if constexpr (std::is_same_v<Source, std::string> ||
                         std::is_same_v<Source, std::string_view>) {
        content_length = std::filesystem::file_size(source);
      }
      else {
        CINATRA_LOG_ERROR
            << "user should set content-length before calling async_upload "
               "when source is user-defined function.";
        data =
            resp_data{std::make_error_code(std::errc::invalid_argument), 404};
        return content_length;
      }
      content_length -= offset;
      if (content_length < 0) {
        CINATRA_LOG_ERROR << "the offset is larger than the end of file";
        data =
            resp_data{std::make_error_code(std::errc::invalid_argument), 404};
        return content_length;
      }
    }

    assert(content_length >= 0);
    char buf[32];
    auto [ptr, _] = std::to_chars(buf, buf + 32, content_length);
    if (headers.empty()) {
      add_header("Content-Length", std::string(buf, ptr - buf));
    }
    else {
      headers.emplace("Content-Length", std::string_view(buf, ptr - buf));
    }
    return content_length;
  }

  async_simple::coro::Lazy<void> send_fstream_with_multipart(
      std::error_code &ec) {
    resp_data data{};
    for (auto &[key, part] : form_data_) {
      data = co_await send_single_part(key, part);

      if (data.net_err) {
        ec = data.net_err;
        co_return;
      }
    }

    std::string last_part;
    size_t size = 0;
    last_part.append("--").append(BOUNDARY).append("--").append(CRCF);
    if (std::tie(ec, size) = co_await async_write(asio::buffer(last_part));
        ec) {
      co_return;
    }
  }

  template <typename Source>
  async_simple::coro::Lazy<void> send_fstream_with_chunked(
      Source &source, std::error_code &ec) {
    size_t size = 0;
    std::string file_data;
    detail::resize(file_data, max_single_part_size_);
    while (!source->eof()) {
      size_t rd_size =
          source->read(file_data.data(), file_data.size()).gcount();
      std::vector<asio::const_buffer> bufs;
      std::string size_str;
      cinatra::to_chunked_buffers(bufs, size_str, {file_data.data(), rd_size},
                                  source->eof());
      if (std::tie(ec, size) = co_await async_write(bufs); ec) {
        break;
      }
    }
  }

  template <typename Source>
  async_simple::coro::Lazy<void> send_fstream_with_length(
      Source &source, std::error_code &ec, uint64_t offset,
      int64_t content_length) {
    size_t size = 0;
    source->seekg(offset, std::ios::cur);
    std::string file_data;
    detail::resize(file_data, std::min<std::size_t>(max_single_part_size_,
                                                    content_length));
    while (content_length > 0 && !source->eof()) {
      size_t rd_size =
          source
              ->read(file_data.data(),
                     std::min<size_t>(content_length, file_data.size()))
              .gcount();
      if (std::tie(ec, size) =
              co_await async_write(asio::buffer(file_data.data(), rd_size));
          ec) {
        break;
      }
      content_length -= rd_size;
    }
    if (!ec && content_length > 0) {
      // bad request, file is smaller than content-length
      ec = std::make_error_code(std::errc::invalid_argument);
    }
  }

  template <typename Source>
  async_simple::coro::Lazy<void> send_sink_with_chunked(Source &source,
                                                        std::error_code &ec) {
    size_t size = 0;
    while (true) {
      auto result = co_await source();
      std::vector<asio::const_buffer> bufs;
      std::string size_str;
      cinatra::to_chunked_buffers(
          bufs, size_str, {result.buf.data(), result.buf.size()}, result.eof);
      if (std::tie(ec, size) = co_await async_write(bufs); ec) {
        break;
      }
      if (result.eof) {
        break;
      }
    }
  }

  template <typename Source>
  async_simple::coro::Lazy<void> send_sink_with_length(Source &source,
                                                       std::error_code &ec,
                                                       int64_t content_length) {
    size_t size = 0;
    while (true) {
      auto result = co_await source();
      if (std::tie(ec, size) = co_await async_write(asio::buffer(
              result.buf.data(),
              std::min<std::size_t>(content_length, result.buf.size())));
          ec) {
        break;
      }
      content_length -= size;
      if (content_length <= 0) {
        break;
      }
      else if (result.eof) [[unlikely]] {
        // bad request, file is smaller than content-length
        ec = std::make_error_code(std::errc::invalid_argument);
        break;
      }
    }
  }

  async_simple::coro::Lazy<bool> reconnect(resp_data &data, uri_t u) {
    data = co_await connect(u);
    if (socket_->is_timeout_) {
      data = resp_data{make_error_code(http_errc::connect_timeout), 404};
    }
    if (data.net_err) {
      co_return false;
    }

    co_return true;
  }

  void handle_upload_timeout_error(std::error_code &ec) {
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
    if (write_header_timeout_ || write_payload_timeout_ || read_timeout_) {
      socket_->is_timeout_ = true;
    }
#endif
    if (socket_->is_timeout_) {
      ec = make_error_code(http_errc::request_timeout);
    }
  }

  template <upload_type_t upload_type, typename S, typename Source>
  async_simple::coro::Lazy<resp_data> async_upload_impl(
      S uri, http_method method, Source source /* file */,
      req_content_type content_type = req_content_type::text,
      std::unordered_map<std::string, std::string> headers = {},
      uint64_t offset = 0 /*file offset*/,
      int64_t content_length = -1 /*upload size*/) {
    std::error_code ec{};
    size_t size = 0;
    bool is_keep_alive = true;
    req_context<> ctx{content_type};
    resp_data data{};

    out_buf_ = {};

    std::shared_ptr<void> guard(nullptr, [&, this](auto) {
      if (!req_headers_.empty()) {
        req_headers_.clear();
      }
    });

    auto [ok, u] = handle_uri(data, uri);
    if (!ok) {
      co_return resp_data{std::make_error_code(std::errc::protocol_error), 404};
    }

    if constexpr (upload_type != upload_type_t::multipart) {
      check_source(data, source);
      if (data.status != 0) {
        co_return data;
      }
    }

    if constexpr (upload_type == upload_type_t::with_length) {
      content_length = handle_upload_header_with_length(data, source, headers,
                                                        offset, content_length);
      if (data.status != 0) {
        co_return data;
      }
    }
    else if constexpr (upload_type == upload_type_t::chunked) {
      handle_upload_header_with_chunked(headers);
    }
    else if constexpr (upload_type == upload_type_t::multipart) {
      handle_upload_header_with_multipart();
    }

    std::string header_str =
        build_request_header(u, method, ctx, true, std::move(headers));

    if (socket_->has_closed_) {
      if (bool r = co_await reconnect(data, u); !r) {
        co_return data;
      }
    }

    auto time_guard = timer_guard(this, req_timeout_duration_, "request timer");
    std::tie(ec, size) = co_await async_write(asio::buffer(header_str));
    if (ec) {
      handle_upload_timeout_error(ec);
      co_return resp_data{ec, 404};
    }

    constexpr bool is_stream_file = is_stream_ptr_v<Source>;
    if constexpr (is_stream_file) {
      if constexpr (upload_type == upload_type_t::with_length) {
        co_await send_fstream_with_length(source, ec, offset, content_length);
      }
      else if constexpr (upload_type == upload_type_t::chunked) {
        co_await send_fstream_with_chunked(source, ec);
      }
    }
    else if constexpr (std::is_enum_v<Source>) {  // only for multipart
      co_await send_fstream_with_multipart(ec);
    }
    else if constexpr (std::is_same_v<Source, std::string> ||
                       std::is_same_v<Source, std::string_view>) {
#ifdef __linux__
#ifdef CINATRA_ENABLE_SSL
      if (!has_init_ssl_) {
#endif
        if constexpr (upload_type == upload_type_t::with_length) {
          co_await send_file_no_copy_with_length(std::filesystem::path{source},
                                                 ec, content_length, offset);
        }
        else if constexpr (upload_type == upload_type_t::chunked) {
          co_await send_file_no_copy_with_chunked(std::filesystem::path{source},
                                                  ec);
        }
#ifdef CINATRA_ENABLE_SSL
      }
      else {
        if constexpr (upload_type == upload_type_t::with_length) {
          co_await send_file_copy_with_length(source, ec, content_length,
                                              offset);
        }
        else if constexpr (upload_type == upload_type_t::chunked) {
          co_await send_file_copy_with_chunked(source, ec);
        }
      }
#endif
#else
      if constexpr (upload_type == upload_type_t::with_length) {
        co_await send_file_copy_with_length(source, ec, content_length, offset);
      }
      else if constexpr (upload_type == upload_type_t::chunked) {
        co_await send_file_copy_with_chunked(source, ec);
      }
#endif
    }
    else {
      if constexpr (upload_type == upload_type_t::with_length) {
        co_await send_sink_with_length(source, ec, content_length);
      }
      else if constexpr (upload_type == upload_type_t::chunked) {
        co_await send_sink_with_chunked(source, ec);
      }
    }
    if (ec) {
      handle_upload_timeout_error(ec);
      co_return resp_data{ec, 404};
    }

    data = co_await handle_read(ec, size, is_keep_alive, std::move(ctx),
                                http_method::POST);
    if (ec) {
      handle_upload_timeout_error(ec);
    }
    handle_result(data, ec, is_keep_alive);
    co_return data;
  }

 public:
  // send file with length
  template <typename S, typename Source>
  async_simple::coro::Lazy<resp_data> async_upload(
      S uri, http_method method, Source source /* file */,
      uint64_t offset = 0 /*file offset*/,
      int64_t content_length = -1 /*upload size*/,
      req_content_type content_type = req_content_type::text,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_upload_impl<upload_type_t::with_length>(
        std::move(uri), method, std::move(source), content_type,
        std::move(headers), offset, content_length);
  }

  // send file with chunked
  template <typename S, typename Source>
  async_simple::coro::Lazy<resp_data> async_upload_chunked(
      S uri, http_method method, Source source,
      req_content_type content_type = req_content_type::text,
      std::unordered_map<std::string, std::string> headers = {}) {
    return async_upload_impl<upload_type_t::chunked>(
        std::move(uri), method, std::move(source), content_type,
        std::move(headers));
  }

  // send multipart data, should call add_file_part or add_str_part firstly.
  async_simple::coro::Lazy<resp_data> async_upload_multipart(std::string uri) {
    if (form_data_.empty()) {
      CINATRA_LOG_WARNING << "no multipart";
      co_return resp_data{std::make_error_code(std::errc::invalid_argument),
                          404};
    }

    co_return co_await async_upload_impl<upload_type_t::multipart>(
        std::move(uri), http_method::POST, upload_type_t::multipart,
        req_content_type::multipart);
  }

  async_simple::coro::Lazy<resp_data> async_upload_multipart(
      std::string uri, std::string name, std::string filename) {
    if (!add_file_part(std::move(name), std::move(filename))) {
      CINATRA_LOG_WARNING << "open file failed or duplicate test names";
      co_return resp_data{std::make_error_code(std::errc::invalid_argument),
                          404};
    }
    co_return co_await async_upload_multipart(std::move(uri));
  }

  template <typename S, typename String>
  async_simple::coro::Lazy<resp_data> async_request(
      S uri, http_method method, req_context<String> ctx,
      std::unordered_map<std::string, std::string> headers = {},
      std::span<char> out_buf = {}) {
    if (!resp_chunk_str_.empty()) {
      resp_chunk_str_.clear();
    }
    if (!body_.empty()) {
      body_.clear();
    }

    out_buf_ = out_buf;

    std::shared_ptr<int> guard(nullptr, [this](auto) {
      if (!req_headers_.empty()) {
        req_headers_.clear();
      }
    });

    resp_data data{};

    std::error_code ec{};
    size_t size = 0;
    bool is_keep_alive = true;

    do {
      uri_t u;
      std::string append_uri;

      if (socket_->has_closed_ || (!uri.empty() && uri[0] != '/')) {
        bool no_schema = !has_schema(uri);

        if (no_schema) {
#ifdef CINATRA_ENABLE_SSL
          if (is_ssl_schema_) {
            append_uri.append("https://").append(uri);
          }
          else
#endif
          {
            append_uri.append("http://").append(uri);
          }
        }
        bool ok = false;
        std::tie(ok, u) = handle_uri(data, no_schema ? append_uri : uri);
        if (!ok) {
          break;
        }
      }
      else {
        u.path = uri;
      }
      if (socket_->has_closed_) {
        data = co_await connect(u);
        if (data.status != 0) {
          co_return data;
        }
      }

      std::vector<asio::const_buffer> vec;
      std::string req_head_str =
          build_request_header(u, method, ctx, false, std::move(headers));

      bool has_body = !ctx.content.empty();
      if (has_body) {
        vec.push_back(asio::buffer(req_head_str));
        vec.push_back(asio::buffer(ctx.content.data(), ctx.content.size()));
      }

#ifdef CORO_HTTP_PRINT_REQ_HEAD
      CINATRA_LOG_DEBUG << req_head_str;
#endif
      auto guard = timer_guard(this, req_timeout_duration_, "request timer");
      if (has_body) {
        std::tie(ec, size) = co_await async_write(vec);
      }
      else {
        std::tie(ec, size) = co_await async_write(asio::buffer(req_head_str));
      }
      if (ec) {
        break;
      }
      data =
          co_await handle_read(ec, size, is_keep_alive, std::move(ctx), method);
    } while (0);
    if (ec && socket_->is_timeout_) {
      ec = make_error_code(http_errc::request_timeout);
    }
    handle_result(data, ec, is_keep_alive);
    co_return data;
  }

  async_simple::coro::Lazy<std::error_code> handle_shake() {
#ifdef CINATRA_ENABLE_SSL
    if (!has_init_ssl_) {
      bool r = init_ssl(asio::ssl::verify_none, "", host_);
      if (!r) {
        co_return std::make_error_code(std::errc::invalid_argument);
      }
    }

    if (socket_->ssl_stream_ == nullptr) {
      co_return std::make_error_code(std::errc::not_a_stream);
    }

    auto ec = co_await coro_io::async_handshake(socket_->ssl_stream_,
                                                asio::ssl::stream_base::client);
    if (ec) {
      CINATRA_LOG_ERROR << "handle failed " << ec.message();
    }
    co_return ec;
#else
    // please open CINATRA_ENABLE_SSL before request https!
    co_return std::make_error_code(std::errc::protocol_error);
#endif
  }

#ifdef INJECT_FOR_HTTP_CLIENT_TEST
  async_simple::coro::Lazy<std::error_code> async_write_raw(
      std::string_view data) {
    auto [ec, _] = co_await async_write(asio::buffer(data));
    co_return ec;
  }

  async_simple::coro::Lazy<resp_data> async_read_raw(
      http_method method, bool clear_buffer = false) {
    if (clear_buffer) {
      body_.clear();
    }

    char buf[1024];
    std::error_code ec{};
    size_t size{};
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl_) {
      std::tie(ec, size) = co_await coro_io::async_read_some(
          *socket_->ssl_stream_, asio::buffer(buf, 1024));
    }
    else {
#endif
      std::tie(ec, size) = co_await coro_io::async_read_some(
          socket_->impl_, asio::buffer(buf, 1024));
#ifdef CINATRA_ENABLE_SSL
    }
#endif
    body_.append(buf, size);

    co_return resp_data{ec, {}, {}, body_};
  }
#endif

  inline void set_proxy(const std::string &host, const std::string &port) {
    proxy_host_ = host;
    proxy_port_ = port;
  }

  inline void set_proxy_basic_auth(const std::string &username,
                                   const std::string &password) {
    proxy_basic_auth_username_ = username;
    proxy_basic_auth_password_ = password;
  }

  inline void set_proxy_bearer_token_auth(const std::string &token) {
    proxy_bearer_token_auth_token_ = token;
  }

  inline void enable_auto_redirect(bool enable_follow_redirect) {
    enable_follow_redirect_ = enable_follow_redirect;
  }

#ifdef CINATRA_ENABLE_SSL
  void set_ssl_schema(bool r) { is_ssl_schema_ = r; }
#ifdef YLT_ENABLE_NTLS
  void set_ntls_schema(bool r) { use_ntls_ = r; }

  /*!
   * Initialize NTLS client with dual certificates
   */
  bool init_ntls_client(const std::string &sign_cert_file,
                        const std::string &sign_key_file,
                        const std::string &enc_cert_file,
                        const std::string &enc_key_file,
                        const std::string &ca_cert_file = "",
                        int verify_mode = asio::ssl::verify_none,
                        const std::string &passwd = "") {
    if (has_init_ssl_) {
      return true;
    }

    try {
      ssl_ctx_ = std::make_unique<asio::ssl::context>(
          SSL_CTX_new(NTLS_client_method()));

      // Enable NTLS mode for Tongsuo
      SSL_CTX_enable_ntls(ssl_ctx_->native_handle());

      // Set NTLS cipher suites
      if (!SSL_CTX_set_cipher_list(ssl_ctx_->native_handle(),
                                   "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3")) {
        CINATRA_LOG_WARNING << "failed to set NTLS cipher suites";
      }

      if (!passwd.empty()) {
        ssl_ctx_->set_password_callback([pwd = std::move(passwd)](auto, auto) {
          return pwd;
        });
      }

      // Load client certificates if provided (for mutual authentication)
      if (!sign_cert_file.empty() && !sign_key_file.empty()) {
        if (!SSL_CTX_use_sign_certificate_file(ssl_ctx_->native_handle(),
                                               sign_cert_file.c_str(),
                                               SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR << "failed to load client SM2 signing certificate";
          return false;
        }

        if (!SSL_CTX_use_sign_PrivateKey_file(ssl_ctx_->native_handle(),
                                              sign_key_file.c_str(),
                                              SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR << "failed to load client SM2 signing private key";
          return false;
        }
      }

      if (!enc_cert_file.empty() && !enc_key_file.empty()) {
        if (!SSL_CTX_use_enc_certificate_file(ssl_ctx_->native_handle(),
                                              enc_cert_file.c_str(),
                                              SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR
              << "failed to load client SM2 encryption certificate";
          return false;
        }

        if (!SSL_CTX_use_enc_PrivateKey_file(ssl_ctx_->native_handle(),
                                             enc_key_file.c_str(),
                                             SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR
              << "failed to load client SM2 encryption private key";
          return false;
        }
      }

      // Load CA certificate if provided
      if (!ca_cert_file.empty()) {
        if (!SSL_CTX_load_verify_locations(ssl_ctx_->native_handle(),
                                           ca_cert_file.c_str(), nullptr)) {
          CINATRA_LOG_WARNING << "failed to load CA certificate";
        }
      }

      ssl_ctx_->set_verify_mode(verify_mode);

      socket_->ssl_stream_ =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              socket_->impl_, *ssl_ctx_);

      has_init_ssl_ = true;
      use_ntls_ = true;
      return true;
    } catch (std::exception &e) {
      CINATRA_LOG_ERROR << "init NTLS client failed: " << e.what();
      return false;
    }
  }
  /*!
   * Initialize NTLS client with TLS 1.3 + GM single certificate mode (RFC 8998)
   */
  bool init_ntls_tls13_gm_client(
      const std::string &gm_cert_file = "", const std::string &gm_key_file = "",
      const std::string &ca_cert_file = "",
      int verify_mode = asio::ssl::verify_none,
      const std::string &cipher_suites = "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3",
      const std::string &passwd = "") {
    if (has_init_ssl_) {
      return true;
    }

    try {
      ssl_ctx_ = std::make_unique<asio::ssl::context>(
          SSL_CTX_new(TLS_client_method()));

      // Configure TLS 1.3 + GM mode (RFC 8998)
      SSL_CTX_set_min_proto_version(ssl_ctx_->native_handle(), TLS1_3_VERSION);
      SSL_CTX_set_max_proto_version(ssl_ctx_->native_handle(), TLS1_3_VERSION);

      // Enable strict SM TLS 1.3 mode (Tongsuo specific)
      SSL_CTX_enable_sm_tls13_strict(ssl_ctx_->native_handle());

      // Set TLS 1.3 GM cipher suites
      if (!SSL_CTX_set_ciphersuites(ssl_ctx_->native_handle(),
                                    cipher_suites.c_str())) {
        CINATRA_LOG_WARNING << "failed to set TLS 1.3 GM cipher suites";
      }

      // Set GM signature algorithms (required for SM TLS 1.3 strict mode)
      if (!SSL_CTX_set1_sigalgs_list(ssl_ctx_->native_handle(), "sm2sig_sm3")) {
        CINATRA_LOG_WARNING << "failed to set GM signature algorithms";
      }

      // Set GM curves (required for SM TLS 1.3 strict mode)
      if (!SSL_CTX_set1_curves_list(ssl_ctx_->native_handle(), "SM2")) {
        CINATRA_LOG_WARNING << "failed to set GM curves";
      }

      if (!passwd.empty()) {
        ssl_ctx_->set_password_callback([pwd = std::move(passwd)](auto, auto) {
          return pwd;
        });
      }

      // Load client certificate if provided (for mutual authentication)
      if (!gm_cert_file.empty() && !gm_key_file.empty()) {
        if (!SSL_CTX_use_certificate_file(ssl_ctx_->native_handle(),
                                          gm_cert_file.c_str(),
                                          SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR << "failed to load client GM certificate";
          return false;
        }

        if (!SSL_CTX_use_PrivateKey_file(ssl_ctx_->native_handle(),
                                         gm_key_file.c_str(),
                                         SSL_FILETYPE_PEM)) {
          CINATRA_LOG_ERROR << "failed to load client GM private key";
          return false;
        }
      }

      // Load CA certificate if provided
      if (!ca_cert_file.empty()) {
        if (!SSL_CTX_load_verify_locations(ssl_ctx_->native_handle(),
                                           ca_cert_file.c_str(), nullptr)) {
          CINATRA_LOG_WARNING << "failed to load CA certificate";
        }
      }

      ssl_ctx_->set_verify_mode(verify_mode);

      socket_->ssl_stream_ =
          std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
              socket_->impl_, *ssl_ctx_);

      has_init_ssl_ = true;
      use_ntls_ = true;
      return true;
    } catch (std::exception &e) {
      CINATRA_LOG_ERROR << "init TLS 1.3 + GM client failed: " << e.what();
      return false;
    }
  }

#endif  // YLT_ENABLE_NTLS
#endif

  std::string get_redirect_uri() { return redirect_uri_; }

  bool is_redirect(resp_data &data) {
    if (data.status > 299 && data.status <= 399)
      return true;
    return false;
  }

  void set_conn_timeout(std::chrono::steady_clock::duration timeout_duration) {
    conn_timeout_duration_ = timeout_duration;
  }

  void set_req_timeout(std::chrono::steady_clock::duration timeout_duration) {
    req_timeout_duration_ = timeout_duration;
  }

  void set_chunked_callback(
      std::function<async_simple::coro::Lazy<void>(std::string_view)> cb) {
    chunked_cb_ = std::move(cb);
  }

  bool has_chunked_callback() const { return chunked_cb_ != nullptr; }

#ifdef CINATRA_ENABLE_SSL
  void enable_sni_hostname(bool r) { need_set_sni_host_ = r; }
#endif

  template <typename T, typename U>
  friend class coro_io::client_pool;

 private:
  struct socket_t {
    asio::ip::tcp::socket impl_;
    std::atomic<bool> has_closed_ = true;
    std::atomic<bool> is_timeout_ = false;
    asio::streambuf head_buf_;
    asio::streambuf chunked_buf_;
#ifdef CINATRA_ENABLE_SSL
    std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket &>> ssl_stream_;
#endif
    template <typename ioc_t>
    socket_t(ioc_t &&ioc) : impl_(std::forward<ioc_t>(ioc)) {}
  };
  static bool is_ok(const resp_data &data) noexcept {
    return data.net_err == std::error_code{};
  }

  template <typename S>
  std::pair<bool, uri_t> handle_uri(resp_data &data, const S &uri) {
    uri_t u;
    if (!u.parse_from(uri.data())) {
      CINATRA_LOG_WARNING
          << uri
          << ", the url is not right, maybe need to encode the url firstly";
      data.net_err = std::make_error_code(std::errc::protocol_error);
      data.status = 404;
      return {false, {}};
    }

    if (u.host.front() == '[') {  // for ipv6
      if (u.host.size() > 2)
        u.host = u.host.substr(1, u.host.size() - 2);
    }

    // construct proxy request uri
    construct_proxy_uri(u);

    return {true, u};
  }

  void construct_proxy_uri(uri_t &u) {
    if (!proxy_host_.empty() && !proxy_port_.empty()) {
      if (!proxy_request_uri_.empty())
        proxy_request_uri_.clear();
      if (u.get_port() == "80") {
        proxy_request_uri_.append("http://").append(u.get_host()).append(":80");
      }
      else if (u.get_port() == "443") {
        proxy_request_uri_.append("https://")
            .append(u.get_host())
            .append(":443");
      }
      else {
        // all be http
        proxy_request_uri_.append("http://")
            .append(u.get_host())
            .append(":")
            .append(u.get_port());
      }
      proxy_request_uri_.append(u.get_path());
      u.path = std::string_view(proxy_request_uri_);
    }
  }

  std::string build_request_header(
      const uri_t &u, http_method method, const auto &ctx,
      bool already_has_len = false,
      std::unordered_map<std::string, std::string> headers = {}) {
    std::string req_str(method_name(method));

    req_str.append(" ").append(u.get_path());
    if (!u.query.empty()) {
      req_str.append("?").append(u.query);
    }

    if (!headers.empty()) {
      req_headers_ = std::move(headers);
      req_str.append(" HTTP/1.1\r\n");
    }
    else {
      if (req_headers_.find("Host") == req_headers_.end()) {
        req_str.append(" HTTP/1.1\r\nHost:").append(u.host).append("\r\n");
      }
      else {
        req_str.append(" HTTP/1.1\r\n");
      }
    }

    auto type_str = get_content_type_str(ctx.content_type);
    if (!type_str.empty()) {
      if (ctx.content_type == req_content_type::multipart) {
        type_str.append(BOUNDARY);
      }
      req_headers_["Content-Type"] = std::move(type_str);
    }

    bool has_connection = false;
    // add user headers
    if (!req_headers_.empty()) {
      for (auto &pair : req_headers_) {
        if (pair.first == "Connection") {
          has_connection = true;
        }
        req_str.append(pair.first)
            .append(": ")
            .append(pair.second)
            .append("\r\n");
      }
    }

    if (!has_connection) {
      req_str.append("Connection: keep-alive\r\n");
    }

    if (!proxy_basic_auth_username_.empty() &&
        !proxy_basic_auth_password_.empty()) {
      std::string basic_auth_str = "Proxy-Authorization: Basic ";
      std::string basic_base64_str = base64_encode(
          proxy_basic_auth_username_ + ":" + proxy_basic_auth_password_);
      req_str.append(basic_auth_str).append(basic_base64_str).append(CRCF);
    }

    if (!proxy_bearer_token_auth_token_.empty()) {
      std::string bearer_token_str = "Proxy-Authorization: Bearer ";
      req_str.append(bearer_token_str)
          .append(proxy_bearer_token_auth_token_)
          .append(CRCF);
    }

    if (!ctx.req_header.empty())
      req_str.append(ctx.req_header);
    size_t content_len = ctx.content.size();
    bool should_add_len = false;
    if (content_len > 0) {
      should_add_len = true;
    }
    else {
      if ((method == http_method::POST || method == http_method::PUT) &&
          ctx.content_type != req_content_type::multipart) {
        should_add_len = true;
      }
    }

    if (req_headers_.find("Content-Length") != req_headers_.end()) {
      should_add_len = false;
    }

    if (already_has_len) {
      should_add_len = false;
    }

    if (should_add_len) {
      char buf[32];
      auto [ptr, ec] = std::to_chars(buf, buf + 32, content_len);
      req_str.append("Content-Length: ")
          .append(std::string_view(buf, ptr - buf))
          .append("\r\n");
    }

    req_str.append("\r\n");
    return req_str;
  }

  std::error_code handle_header(resp_data &data, http_parser &parser,
                                size_t header_size) {
    // parse header
    const char *data_ptr = asio::buffer_cast<const char *>(head_buf_.data());

    int parse_ret = parser.parse_response(data_ptr, header_size, 0);
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
    if (parse_failed_forever_) {
      parse_ret = -1;
    }
#endif
    if (parse_ret < 0) [[unlikely]] {
      head_buf_.consume(head_buf_.size());
      return std::make_error_code(std::errc::protocol_error);
    }

    if (parser_.body_len() > max_http_body_len_ || parser_.body_len() < 0)
        [[unlikely]] {
      CINATRA_LOG_ERROR << "invalid http content length: "
                        << parser_.body_len();
      head_buf_.consume(head_buf_.size());
      return std::make_error_code(std::errc::invalid_argument);
    }

    head_buf_.consume(header_size);  // header size
    data.resp_headers = parser.get_headers();
    data.status = parser.status();
    return {};
  }

  template <typename String>
  async_simple::coro::Lazy<resp_data> handle_read(std::error_code &ec,
                                                  size_t &size,
                                                  bool &is_keep_alive,
                                                  req_context<String> ctx,
                                                  http_method method) {
    resp_data data{};
    do {
      if (std::tie(ec, size) = co_await async_read_until(head_buf_, TWO_CRCF);
          ec) {
        break;
      }

      ec = handle_header(data, parser_, size);
      if (ec) {
        break;
      }

      is_keep_alive = parser_.keep_alive();
      if (method == http_method::HEAD) {
        co_return data;
      }

      bool is_out_buf = false;

      bool is_ranges = parser_.is_resp_ranges();
      if (is_ranges) {
        is_keep_alive = true;
      }
      if (parser_.is_chunked()) {
        out_buf_ = {};
        is_keep_alive = true;
        if (head_buf_.size() > 0) {
          const char *data_ptr =
              asio::buffer_cast<const char *>(head_buf_.data());
          chunked_buf_.sputn(data_ptr, head_buf_.size());
          head_buf_.consume(head_buf_.size());
        }
        ec = co_await handle_chunked(data, std::move(ctx));
        break;
      }

      if (parser_.is_multipart()) {
        out_buf_ = {};
        is_keep_alive = true;
        if (head_buf_.size() > 0) {
          const char *data_ptr =
              asio::buffer_cast<const char *>(head_buf_.data());
          chunked_buf_.sputn(data_ptr, head_buf_.size());
          head_buf_.consume(head_buf_.size());
        }
        ec = co_await handle_multipart(data, std::move(ctx));
        break;
      }

      redirect_uri_.clear();
      bool is_redirect = parser_.is_location();
      if (is_redirect)
        redirect_uri_ = parser_.get_header_value("Location");

      if (!parser_.get_header_value("Content-Encoding").empty()) {
        if (parser_.get_header_value("Content-Encoding").find("gzip") !=
            std::string_view::npos)
          encoding_type_ = content_encoding::gzip;
        else if (parser_.get_header_value("Content-Encoding").find("deflate") !=
                 std::string_view::npos)
          encoding_type_ = content_encoding::deflate;
        else if (parser_.get_header_value("Content-Encoding").find("br") !=
                 std::string_view::npos)
          encoding_type_ = content_encoding::br;
      }
      else {
        encoding_type_ = content_encoding::none;
      }

      size_t content_len = (size_t)parser_.body_len();
#ifdef BENCHMARK_TEST
      total_len_ = parser_.total_len();
#endif

      is_out_buf = !out_buf_.empty();
      if (is_out_buf) {
        if (content_len > 0 && out_buf_.size() < content_len) {
          out_buf_ = {};
          is_out_buf = false;
        }
      }

      if (content_len <= head_buf_.size()) {
        // Now get entire content, additional data will discard.
        // copy body.
        if (content_len > 0) {
          auto data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
          if (is_out_buf) {
            memcpy(out_buf_.data(), data_ptr, content_len);
          }
          else {
            detail::resize(body_, content_len);
            memcpy(body_.data(), data_ptr, content_len);
          }
          head_buf_.consume(head_buf_.size());
        }
        co_await handle_entire_content(data, content_len, is_ranges, ctx);
        break;
      }

      // read left part of content.
      size_t part_size = head_buf_.size();
      size_t size_to_read = content_len - part_size;

      auto data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      if (is_out_buf) {
        memcpy(out_buf_.data(), data_ptr, part_size);
      }
      else {
        detail::resize(body_, content_len);
        memcpy(body_.data(), data_ptr, part_size);
      }

      head_buf_.consume(part_size);

      if (is_out_buf) {
        if (std::tie(ec, size) = co_await async_read(
                asio::buffer(out_buf_.data() + part_size, size_to_read),
                size_to_read);
            ec) {
          break;
        }
      }
      else {
        if (std::tie(ec, size) = co_await async_read(
                asio::buffer(body_.data() + part_size, size_to_read),
                size_to_read);
            ec) {
          break;
        }
      }

      // Now get entire content, additional data will discard.
      co_await handle_entire_content(data, content_len, is_ranges, ctx);
    } while (0);

    if (!resp_chunk_str_.empty()) {
      data.resp_body =
          std::string_view{resp_chunk_str_.data(), resp_chunk_str_.size()};
    }

    co_return data;
  }

  async_simple::coro::Lazy<void> handle_entire_content(resp_data &data,
                                                       size_t content_len,
                                                       bool is_ranges,
                                                       auto &ctx) {
    if (content_len > 0) {
      const char *data_ptr;
      if (head_buf_.size() == 0) {
        if (out_buf_.empty()) {
          data_ptr = body_.data();
        }
        else {
          data_ptr = out_buf_.data();
        }
      }
      else {
        data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      }

      if (is_ranges) {
        if (ctx.resp_body_stream) {
          auto [ec, size] = co_await ctx.resp_body_stream->async_write(
              {data_ptr, content_len});
          if (ec) {
            data.net_err = ec;
            co_return;
          }
        }
      }

      std::string_view reply(data_ptr, content_len);
#ifdef CINATRA_ENABLE_GZIP
      if (encoding_type_ == content_encoding::gzip) {
        uncompressed_str_.clear();
        bool r = gzip_codec::uncompress(reply, uncompressed_str_);
        if (r)
          data.resp_body = uncompressed_str_;
        else
          data.resp_body = reply;
      }
      else if (encoding_type_ == content_encoding::deflate) {
        uncompressed_str_.clear();
        bool r = gzip_codec::inflate(reply, uncompressed_str_);
        if (r)
          data.resp_body = uncompressed_str_;
        else
          data.resp_body = reply;
      }
#endif
#if defined(CINATRA_ENABLE_BROTLI) && defined(CINATRA_ENABLE_GZIP)
      else if (encoding_type_ == content_encoding::br)
#endif
#if defined(CINATRA_ENABLE_BROTLI) && !defined(CINATRA_ENABLE_GZIP)
        if (encoding_type_ == content_encoding::br)
#endif
#ifdef CINATRA_ENABLE_BROTLI
        {
          uncompressed_str_.clear();
          bool r = br_codec::brotli_decompress(reply, uncompressed_str_);
          if (r)
            data.resp_body = uncompressed_str_;
          else
            data.resp_body = reply;
        }
#endif
#if defined(CINATRA_ENABLE_BROTLI) || defined(CINATRA_ENABLE_GZIP)
        else
#endif
          data.resp_body = reply;

      head_buf_.consume(content_len);
    }
    data.eof = (head_buf_.size() == 0);
  }

  void handle_result(resp_data &data, std::error_code ec, bool is_keep_alive) {
    if (ec) {
      close_socket(*socket_);
      data.net_err = ec;
      data.status = 404;
#ifdef BENCHMARK_TEST
      if (!stop_bench_) {
        CINATRA_LOG_DEBUG << ec.message();
      }
#endif
    }
    else {
      if (!is_keep_alive) {
        close_socket(*socket_);
      }
    }
  }

  template <typename String>
  async_simple::coro::Lazy<std::error_code> handle_multipart(
      resp_data &data, req_context<String> ctx) {
    std::error_code ec{};
    std::string boundary = std::string{parser_.get_boundary()};
    multipart_reader_t multipart(this);
    while (true) {
      auto part_head = co_await multipart.read_part_head(boundary);
      if (part_head.ec) {
        co_return part_head.ec;
      }

      auto part_body = co_await multipart.read_part_body(boundary);

      if (ctx.resp_body_stream) {
        size_t size;
        std::tie(ec, size) =
            co_await ctx.resp_body_stream->async_write(part_body.data);
      }
      else {
        resp_chunk_str_.append(part_body.data.data(), part_body.data.size());
      }

      if (part_body.ec) {
        co_return part_body.ec;
      }

      if (part_body.eof) {
        break;
      }
    }
    co_return ec;
  }

  template <typename String>
  async_simple::coro::Lazy<std::error_code> handle_chunked(
      resp_data &data, req_context<String> ctx) {
    std::error_code ec{};
    size_t size = 0;
    while (true) {
      if (std::tie(ec, size) = co_await async_read_until(chunked_buf_, CRCF);
          ec) {
        break;
      }

      size_t buf_size = chunked_buf_.size();
      size_t additional_size = buf_size - size;
      const char *data_ptr =
          asio::buffer_cast<const char *>(chunked_buf_.data());
      std::string_view size_str(data_ptr, size - CRCF.size());
      auto chunk_size = hex_to_int(size_str);
      chunked_buf_.consume(size);
      if (chunk_size < 0) {
        CINATRA_LOG_DEBUG << "bad chunked size";
        ec = asio::error::make_error_code(
            asio::error::basic_errors::invalid_argument);
        break;
      }

      if (additional_size < size_t(chunk_size + 2)) {
        // not a complete chunk, read left chunk data.
        size_t size_to_read = chunk_size + 2 - additional_size;
        if (std::tie(ec, size) =
                co_await async_read(chunked_buf_, size_to_read);
            ec) {
          break;
        }
      }

      if (chunk_size == 0) {
        // all finished, no more data
        chunked_buf_.consume(chunked_buf_.size());
        data.eof = true;
        break;
      }

      data_ptr = asio::buffer_cast<const char *>(chunked_buf_.data());
      if (chunked_cb_) {
        co_await chunked_cb_(std::string_view(data_ptr, chunk_size));
      }
      else {
        if (ctx.resp_body_stream) {
          std::tie(ec, size) = co_await ctx.resp_body_stream->async_write(
              {data_ptr, (size_t)chunk_size});
        }
        else {
          resp_chunk_str_.append(data_ptr, chunk_size);
        }
      }

      chunked_buf_.consume(chunk_size + CRCF.size());
    }
    co_return ec;
  }

  async_simple::coro::Lazy<resp_data> connect(
      const uri_t &u, std::vector<asio::ip::tcp::endpoint> *eps = nullptr) {
    std::vector<asio::ip::tcp::endpoint> eps_tmp;
    if (eps == nullptr) {
      eps = &eps_tmp;
    }
    if (should_reset_) {
      reset();
    }
    else {
      should_reset_ = true;
    }
    if (socket_->has_closed_) {
      auto time_out_guard =
          timer_guard(this, conn_timeout_duration_, "connect timer");
      socket_->is_timeout_ = false;
      host_ = proxy_host_.empty() ? u.get_host() : proxy_host_;
      port_ = proxy_port_.empty() ? u.get_port() : proxy_port_;
      if (eps->empty()) {
        CINATRA_LOG_TRACE << "start resolve host: " << host_ << ":" << port_;
        auto [ec, iter] = co_await coro_io::async_resolve(
            &executor_wrapper_, socket_->impl_, host_, port_);
        if (ec) {
          co_return resp_data{ec, 404};
        }
        else {
          asio::ip::tcp::resolver::iterator end;
          while (iter != end) {
            eps->push_back(iter->endpoint());
            ++iter;
          }
          if (eps->empty()) [[unlikely]] {
            co_return resp_data{std::make_error_code(std::errc::not_connected),
                                404};
          }
        }
      }
      CINATRA_LOG_TRACE
          << "start connect to endpoint lists. total endpoint count:"
          << eps->size()
          << ", the first endpoint is: " << (*eps)[0].address().to_string()
          << ":" << std::to_string((*eps)[0].port());
      std::error_code ec;
      if (ec = co_await coro_io::async_connect(socket_->impl_, *eps); ec) {
        co_return resp_data{ec, 404};
      }
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
      if (connect_timeout_forever_) {
        socket_->is_timeout_ = true;
      }
#endif
      if (socket_->is_timeout_) {
        ec = make_error_code(http_errc::connect_timeout);
        co_return resp_data{ec, 404};
      }

      if (enable_tcp_no_delay_) {
        socket_->impl_.set_option(asio::ip::tcp::no_delay(true), ec);
        if (ec) {
          co_return resp_data{ec, 404};
        }
      }

      if (u.is_ssl) {
#ifdef CINATRA_ENABLE_SSL
        if (!has_init_ssl_) {
          size_t pos = u.host.find("www.");
          std::string host;
          if (pos != std::string_view::npos) {
            host = std::string{u.host.substr(pos + 4)};
          }
          else {
            host = std::string{u.host};
          }
          bool r = init_ssl(asio::ssl::verify_none, "", host);
          if (!r) {
            co_return resp_data{
                std::make_error_code(std::errc::invalid_argument), 404};
          }
        }
#endif
        if (ec = co_await handle_shake(); ec) {
          co_return resp_data{ec, 404};
        }
      }
      socket_->has_closed_ = false;
      CINATRA_LOG_TRACE
          << "connect to endpoint: "
          << socket_->impl_.remote_endpoint().address().to_string() << ":"
          << socket_->impl_.remote_endpoint().port() << " successfully";
    }
    co_return resp_data{};
  }

  size_t multipart_content_len() {
    size_t content_len = 0;
    for (auto &[key, part] : form_data_) {
      content_len += 75;
      content_len += key.size() + 1;
      if (!part.filename.empty()) {
        content_len += (12 + part.filename.size() + 1);
        auto ext = std::filesystem::path(part.filename).extension().string();
        if (auto it = g_content_type_map.find(ext);
            it != g_content_type_map.end()) {
          content_len += (14 + it->second.size());
        }
      }

      content_len += 4;

      content_len += (part.size + 2);
    }
    content_len += (6 + BOUNDARY.size());
    return content_len;
  }

  async_simple::coro::Lazy<resp_data> send_single_part(
      const std::string &key, const multipart_t &part) {
    std::string part_content_head;
    part_content_head.append("--").append(BOUNDARY).append(CRCF);
    part_content_head.append("Content-Disposition: form-data; name=\"");
    part_content_head.append(key).append("\"");
    bool is_file = !part.filename.empty();
    std::string short_name =
        std::filesystem::path(part.filename).filename().string();
    if (is_file) {
      part_content_head.append("; filename=\"")
          .append(short_name)
          .append("\"")
          .append(CRCF);
      auto ext = std::filesystem::path(short_name).extension().string();
      if (auto it = g_content_type_map.find(ext);
          it != g_content_type_map.end()) {
        part_content_head.append("Content-Type: ")
            .append(it->second)
            .append(CRCF);
      }

      part_content_head.append(CRCF);
    }
    else {
      part_content_head.append(TWO_CRCF);
    }
    if (auto [ec, size] = co_await async_write(asio::buffer(part_content_head));
        ec) {
      co_return resp_data{ec, 404};
    }

    if (is_file) {
      coro_io::coro_file file{};
      file.open(part.filename, std::ios::in);
      assert(file.is_open());
      std::string file_data;
      detail::resize(file_data, max_single_part_size_);
      while (true) {
        auto [rd_ec, rd_size] =
            co_await file.async_read(file_data.data(), file_data.size());
        if (auto [ec, size] =
                co_await async_write(asio::buffer(file_data.data(), rd_size));
            ec) {
          co_return resp_data{ec, 404};
        }
        if (file.eof()) {
          if (auto [ec, size] = co_await async_write(asio::buffer(CRCF)); ec) {
            co_return resp_data{ec, 404};
          }
          break;
        }
      }
    }
    else {
      std::array<asio::const_buffer, 2> arr{asio::buffer(part.content),
                                            asio::buffer(CRCF)};
      if (auto [ec, size] = co_await async_write(arr); ec) {
        co_return resp_data{ec, 404};
      }
    }

    co_return resp_data{{}, 200};
  }

  async_simple::coro::Lazy<resp_data> async_read_ws() {
    resp_data data{};

    head_buf_.consume(head_buf_.size());
    std::shared_ptr sock = socket_;
    asio::streambuf &read_buf = sock->head_buf_;
    bool has_init_ssl = false;
#ifdef CINATRA_ENABLE_SSL
    has_init_ssl = has_init_ssl_;
#endif
    websocket ws{};
    while (true) {
      if (auto [ec, _] = co_await async_read_ws(
              sock, read_buf, ws.left_header_len(), has_init_ssl);
          ec) {
        if (socket_->is_timeout_) {
          co_return resp_data{make_error_code(http_errc::request_timeout), 404};
        }
        data.net_err = ec;
        data.status = 404;

        if (sock->has_closed_) {
          co_return data;
        }

        close_socket(*sock);
        co_return data;
      }

      const char *data_ptr = asio::buffer_cast<const char *>(read_buf.data());
      auto ret = ws.parse_header(data_ptr, read_buf.size(), false);
      if (ret == ws_header_status::incomplete) {
        continue;
      }
      else if (ret == ws_header_status::error) {
        data.net_err = std::make_error_code(std::errc::protocol_error);
        data.status = 404;
        close_socket(*sock);
        co_return data;
      }

      frame_header *header = (frame_header *)data_ptr;
      bool is_close_frame = header->opcode == opcode::close;

      read_buf.consume(read_buf.size());

      size_t payload_len = ws.payload_length();

      if (auto [ec, size] =
              co_await async_read_ws(sock, read_buf, payload_len, has_init_ssl);
          ec) {
        data.net_err = ec;
        data.status = 404;
        close_socket(*sock);
        co_return data;
      }

      data_ptr = asio::buffer_cast<const char *>(read_buf.data());
#ifdef CINATRA_ENABLE_GZIP
      if (is_server_support_ws_deflate_ && enable_ws_deflate_) {
        inflate_str_.clear();
        if (!cinatra::gzip_codec::inflate({data_ptr, payload_len},
                                          inflate_str_)) {
          CINATRA_LOG_ERROR << "uncompuress data error";
          data.status = 404;
          data.net_err = std::make_error_code(std::errc::protocol_error);
          co_return data;
        }
        data_ptr = inflate_str_.data();
        payload_len = inflate_str_.length();
      }
#endif
      if (is_close_frame) {
        if (payload_len >= 2) {
          payload_len -= 2;
          data_ptr += sizeof(uint16_t);
        }
      }
      data.status = 200;
      data.resp_body = {data_ptr, payload_len};

      read_buf.consume(read_buf.size());

      if (is_close_frame) {
        std::string reason = "close";
        auto close_str = ws.format_close_payload(close_code::normal,
                                                 reason.data(), reason.size());
        auto span = std::span<char>(close_str);
        auto encode_header = ws.encode_frame(span, opcode::close, true);
        std::vector<asio::const_buffer> buffers{asio::buffer(encode_header),
                                                asio::buffer(reason)};

        co_await async_write_ws(sock, buffers, has_init_ssl);

        close_socket(*sock);

        data.net_err = asio::error::eof;
        data.status = 404;
        co_return data;
      }
      co_return data;
    }
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read_ws(
      auto sock, AsioBuffer &&buffer, size_t size_to_read,
      bool has_init_ssl = false) noexcept {
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl) {
      return coro_io::async_read(*sock->ssl_stream_, buffer, size_to_read);
    }
    else {
#endif
      return coro_io::async_read(sock->impl_, buffer, size_to_read);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write_ws(
      auto sock, AsioBuffer &&buffer, bool has_init_ssl = false) {
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl) {
      return coro_io::async_write(*sock->ssl_stream_, buffer);
    }
    else {
#endif
      return coro_io::async_write(sock->impl_, buffer);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read(
      AsioBuffer &&buffer, size_t size_to_read) noexcept {
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
    if (read_failed_forever_) {
      return async_read_failed();
    }
#endif
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl_) {
      return coro_io::async_read(*socket_->ssl_stream_, buffer, size_to_read);
    }
    else {
#endif
      return coro_io::async_read(socket_->impl_, buffer, size_to_read);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

#ifdef INJECT_FOR_HTTP_CLIENT_TEST
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
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write(
      AsioBuffer &&buffer) {
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
    if (write_failed_forever_) {
      return async_write_failed();
    }
#endif
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl_) {
      return coro_io::async_write(*socket_->ssl_stream_, buffer);
    }
    else {
#endif
      return coro_io::async_write(socket_->impl_, buffer);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  template <typename AsioBuffer>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read_until(
      AsioBuffer &buffer, asio::string_view delim) noexcept {
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
    if (read_failed_forever_) {
      return async_read_failed();
    }
#endif
#ifdef CINATRA_ENABLE_SSL
    if (has_init_ssl_) {
      return coro_io::async_read_until(*socket_->ssl_stream_, buffer, delim);
    }
    else {
#endif
      return coro_io::async_read_until(socket_->impl_, buffer, delim);
#ifdef CINATRA_ENABLE_SSL
    }
#endif
  }

  static void close_socket(socket_t &socket) {
    std::error_code ec;
    socket.impl_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket.impl_.close(ec);
    socket.has_closed_ = true;
  }

  async_simple::coro::Lazy<bool> timeout(
      auto &timer, std::chrono::steady_clock::duration duration,
      std::string msg) {
    auto watcher = std::weak_ptr(socket_);
    timer.expires_after(duration);
    auto is_timeout = co_await timer.async_await();
    if (!is_timeout) {
      co_return false;
    }
    if (auto socket = watcher.lock(); socket) {
      socket_->is_timeout_ = true;
      CINATRA_LOG_WARNING << msg << " timeout";
      close_socket(*socket_);
    }
    co_return true;
  }

  template <typename S>
  bool has_schema(const S &url) {
    size_t pos_http = url.find("http://");
    size_t pos_https = url.find("https://");
    size_t pos_ws = url.find("ws://");
    size_t pos_wss = url.find("wss://");
    bool has_http_scheme =
        ((pos_http != std::string::npos) && pos_http == 0) ||
        ((pos_https != std::string::npos) && pos_https == 0) ||
        ((pos_ws != std::string::npos) && pos_ws == 0) ||
        ((pos_wss != std::string::npos) && pos_wss == 0);
    return has_http_scheme;
  }

  friend class multipart_reader_t<coro_http_client>;
  http_parser parser_;
  coro_io::ExecutorWrapper<> executor_wrapper_;
  coro_io::period_timer timer_;
  std::shared_ptr<socket_t> socket_;
  asio::streambuf &head_buf_;
  asio::streambuf &chunked_buf_;
  std::string body_;

  std::unordered_map<std::string, std::string> req_headers_;

  std::string proxy_request_uri_ = "";
  std::string proxy_host_;
  std::string proxy_port_;

  std::string proxy_basic_auth_username_;
  std::string proxy_basic_auth_password_;

  std::string proxy_bearer_token_auth_token_;

  std::map<std::string, multipart_t> form_data_;
  size_t max_single_part_size_ = 1024 * 1024;

  std::string ws_sec_key_;
  std::string host_;
  std::string port_;

#ifdef CINATRA_ENABLE_SSL
  std::unique_ptr<asio::ssl::context> ssl_ctx_ = nullptr;
  bool has_init_ssl_ = false;
  bool is_ssl_schema_ = false;
#ifdef YLT_ENABLE_NTLS
  bool use_ntls_ = true;
#endif  // YLT_ENABLE_NTLS
  bool need_set_sni_host_ = true;
#endif
  std::string redirect_uri_;
  bool enable_follow_redirect_ = false;
  std::chrono::steady_clock::duration conn_timeout_duration_ =
      std::chrono::seconds(30);
  std::chrono::steady_clock::duration req_timeout_duration_ =
      std::chrono::seconds(60);
  std::chrono::steady_clock::time_point create_tp_;
  bool enable_tcp_no_delay_ = true;
  std::string resp_chunk_str_;
  std::span<char> out_buf_;
  bool should_reset_ = false;
  config config_;

  bool enable_ws_deflate_ = false;
#ifdef CINATRA_ENABLE_GZIP
  bool is_server_support_ws_deflate_ = false;
  std::string inflate_str_;
#endif
  content_encoding encoding_type_ = content_encoding::none;
  int64_t max_http_body_len_ =
      INT64_MAX;  // in default we don't limit http body len

  std::function<async_simple::coro::Lazy<void>(std::string_view)> chunked_cb_;
#if defined(CINATRA_ENABLE_BROTLI) || defined(CINATRA_ENABLE_GZIP)
  std::string uncompressed_str_;
#endif

#ifdef BENCHMARK_TEST
  bool stop_bench_ = false;
  size_t total_len_ = 0;
#endif
#ifdef INJECT_FOR_HTTP_CLIENT_TEST
 public:
  bool write_failed_forever_ = false;
  bool connect_timeout_forever_ = false;
  bool parse_failed_forever_ = false;
  bool read_failed_forever_ = false;
  bool write_header_timeout_ = false;
  bool write_payload_timeout_ = false;
  bool read_timeout_ = false;
#endif
};

}  // namespace cinatra
