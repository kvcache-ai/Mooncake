#pragma once

#include <memory>

#include "cinatra/coro_http_client.hpp"
#include "cinatra/coro_http_response.hpp"
#include "cinatra/coro_http_router.hpp"
#include "cinatra/define.h"
#include "cinatra/mime_types.hpp"
#include "cinatra_log_wrapper.hpp"
#include "coro_http_connection.hpp"
#include "ylt/coro_io/coro_file.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_io/load_balancer.hpp"

namespace cinatra {
enum class file_resp_format_type {
  chunked,
  range,
};
class coro_http_server {
 public:
  coro_http_server(asio::io_context &ctx, unsigned short port,
                   std::string address = "0.0.0.0")
      : out_ctx_(&ctx), port_(port), acceptor_(ctx), check_timer_(ctx) {
    out_executor_ =
        std::make_unique<coro_io::ExecutorWrapper<>>(out_ctx_->get_executor());
    init_address(std::move(address));
  }

  coro_http_server(asio::io_context &ctx,
                   std::string address /* = "0.0.0.0:9001" */)
      : out_ctx_(&ctx), acceptor_(ctx), check_timer_(ctx) {
    out_executor_ =
        std::make_unique<coro_io::ExecutorWrapper<>>(out_ctx_->get_executor());
    init_address(std::move(address));
  }

  coro_http_server(size_t thread_num, unsigned short port,
                   std::string address = "0.0.0.0", bool cpu_affinity = false)
      : pool_(std::make_unique<coro_io::io_context_pool>(thread_num,
                                                         cpu_affinity)),
        port_(port),
        acceptor_(pool_->get_executor()->get_asio_executor()),
        check_timer_(pool_->get_executor()->get_asio_executor()) {
    init_address(std::move(address));
  }

  coro_http_server(size_t thread_num,
                   std::string address /* = "0.0.0.0:9001" */,
                   bool cpu_affinity = false)
      : pool_(std::make_unique<coro_io::io_context_pool>(thread_num,
                                                         cpu_affinity)),
        acceptor_(pool_->get_executor()->get_asio_executor()),
        check_timer_(pool_->get_executor()->get_asio_executor()) {
    init_address(std::move(address));
  }

  ~coro_http_server() {
    CINATRA_LOG_INFO << "coro_http_server will quit";
    stop();
  }

  void set_no_delay(bool r) { no_delay_ = r; }

  void set_max_http_body_size(int64_t max_size) {
    max_http_body_len_ = max_size;
  }

#ifdef CINATRA_ENABLE_SSL
  void init_ssl(const std::string &cert_file, const std::string &key_file,
                const std::string &passwd) {
    cert_file_ = cert_file;
    key_file_ = key_file;
    passwd_ = passwd;
    use_ssl_ = true;
  }

#ifdef YLT_ENABLE_NTLS
  /*!
   * Initialize NTLS with dual certificates (signing and encryption)
   * @param sign_cert_file SM2 signing certificate file path
   * @param sign_key_file SM2 signing private key file path
   * @param enc_cert_file SM2 encryption certificate file path
   * @param enc_key_file SM2 encryption private key file path
   * @param ca_cert_file CA certificate file path (optional)
   * @param enable_client_verify enable client certificate verification
   */
  void init_ntls(const std::string &sign_cert_file,
                 const std::string &sign_key_file,
                 const std::string &enc_cert_file,
                 const std::string &enc_key_file,
                 const std::string &ca_cert_file = "",
                 bool enable_client_verify = false,
                 const std::string &passwd = "") {
    ntls_config_.sign_cert_file = sign_cert_file;
    ntls_config_.sign_key_file = sign_key_file;
    ntls_config_.enc_cert_file = enc_cert_file;
    ntls_config_.enc_key_file = enc_key_file;
    ntls_config_.ca_cert_file = ca_cert_file;
    ntls_config_.enable_client_verify = enable_client_verify;
    ntls_config_.enable_ntls = true;
    use_ntls_ = true;
    passwd_ = passwd;
  }

  /*!
   * Initialize NTLS with base path and relative file paths
   */
  void init_ntls(
      const std::string &base_path, const std::string &sign_cert_file,
      const std::string &sign_key_file, const std::string &enc_cert_file,
      const std::string &enc_key_file, const std::string &ca_cert_file = "",
      bool enable_client_verify = false, const std::string &cipher_suites = "",
      coro_http_connection::ntls_mode mode =
          coro_http_connection::ntls_mode::tlcp_dual_cert) {
    ntls_config_.base_path = base_path;
    ntls_config_.mode = mode;

    if (mode == coro_http_connection::ntls_mode::tls13_single_cert) {
      // RFC 8998 TLS 1.3 + GM single certificate mode
      ntls_config_.gm_cert_file = sign_cert_file;
      ntls_config_.gm_key_file = sign_key_file;
      ntls_config_.cipher_suites = cipher_suites.empty()
                                       ? "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"
                                       : cipher_suites;
    }
    else {
      // GB/T 38636-2020 TLCP dual certificate mode (default)
      ntls_config_.sign_cert_file = sign_cert_file;
      ntls_config_.sign_key_file = sign_key_file;
      ntls_config_.enc_cert_file = enc_cert_file;
      ntls_config_.enc_key_file = enc_key_file;
      ntls_config_.cipher_suites =
          cipher_suites.empty() ? "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3"
                                : cipher_suites;
    }

    ntls_config_.ca_cert_file = ca_cert_file;
    ntls_config_.enable_client_verify = enable_client_verify;
    ntls_config_.enable_ntls = true;
    use_ntls_ = true;
  }

  /*!
   * Initialize NTLS with RFC 8998 TLS 1.3 + GM single certificate mode
   */
  void init_ntls(const std::string &base_path, const std::string &gm_cert_file,
                 const std::string &gm_key_file,
                 const std::string &ca_cert_file = "",
                 bool enable_client_verify = false,
                 const std::string &cipher_suites = "") {
    ntls_config_.base_path = base_path;
    ntls_config_.mode = coro_http_connection::ntls_mode::tls13_single_cert;
    ntls_config_.gm_cert_file = gm_cert_file;
    ntls_config_.gm_key_file = gm_key_file;
    ntls_config_.ca_cert_file = ca_cert_file;
    ntls_config_.cipher_suites = cipher_suites.empty()
                                     ? "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"
                                     : cipher_suites;
    ntls_config_.enable_client_verify = enable_client_verify;
    ntls_config_.enable_ntls = true;
    use_ntls_ = true;
  }

  /*!
   * Set NTLS cipher suites
   */
  void set_ntls_cipher_suites(const std::string &cipher_suites) {
    ntls_config_.cipher_suites = cipher_suites;
  }
#endif  // YLT_ENABLE_NTLS
#endif

  // only call once, not thread safe.
  std::error_code sync_start() noexcept {
    auto ret = async_start();
    ret.wait();
    return ret.value();
  }

  // only call once, not thread safe.
  async_simple::Future<std::error_code> async_start() {
    errc_ = listen();

    async_simple::Promise<std::error_code> promise;
    auto future = promise.getFuture();

    if (!errc_) {
      if (out_ctx_ == nullptr) {
        std::lock_guard lock(thd_mtx_);
        thd_ = std::thread([this] {
          pool_->run();
        });
      }

      accept().start([p = std::move(promise), this](auto &&res) mutable {
        if (res.hasError()) {
          errc_ = std::make_error_code(std::errc::io_error);
          p.setValue(errc_);
        }
        else {
          p.setValue(res.value());
        }
      });
    }
    else {
      promise.setValue(errc_);
    }

    return future;
  }

  // only call once, not thread safe.
  void stop() {
    {
      std::lock_guard lock(thd_mtx_);
      if (out_ctx_ == nullptr && !thd_.joinable()) {
        return;
      }
    }

    stop_timer_ = true;

    close_acceptor();

    // close current connections.
    {
      std::scoped_lock lock(*conn_mtx_);
      for (auto &conn : connections_) {
        conn.second->close(false);
      }
      connections_.clear();
    }

    if (out_ctx_ == nullptr) {
      CINATRA_LOG_INFO << "wait for server's thread-pool finish all work.";
      pool_->stop();

      CINATRA_LOG_INFO << "server's thread-pool finished.";
      {
        std::lock_guard lock(thd_mtx_);
        thd_.join();
      }

      CINATRA_LOG_INFO << "stop coro_http_server ok";
    }
    else {
      out_ctx_ = nullptr;
    }
  }

  // call it after server async_start or sync_start.
  uint16_t port() const { return port_; }

  template <http_method... method, typename Func, typename... Aspects>
  void set_http_handler(std::string key, Func handler, Aspects &&...asps) {
    static_assert(sizeof...(method) >= 1, "must set http_method");
    if constexpr (sizeof...(method) == 1) {
      (router_.set_http_handler<method>(std::move(key), std::move(handler),
                                        std::forward<Aspects>(asps)...),
       ...);
    }
    else {
      (router_.set_http_handler<method>(key, handler,
                                        std::forward<Aspects>(asps)...),
       ...);
    }
  }

  template <http_method... method, typename Func, typename... Aspects>
  void set_http_handler(std::string key, Func handler,
                        util::class_type_t<Func> &owner, Aspects &&...asps) {
    static_assert(std::is_member_function_pointer_v<Func>,
                  "must be member function");
    using return_type = typename util::function_traits<Func>::return_type;
    if constexpr (coro_io::is_lazy_v<return_type>) {
      std::function<async_simple::coro::Lazy<void>(coro_http_request & req,
                                                   coro_http_response & resp)>
          f = std::bind(handler, &owner, std::placeholders::_1,
                        std::placeholders::_2);
      set_http_handler<method...>(std::move(key), std::move(f),
                                  std::forward<Aspects>(asps)...);
    }
    else {
      std::function<void(coro_http_request & req, coro_http_response & resp)>
          f = std::bind(handler, &owner, std::placeholders::_1,
                        std::placeholders::_2);
      set_http_handler<method...>(std::move(key), std::move(f),
                                  std::forward<Aspects>(asps)...);
    }
  }

  template <http_method... method, typename... Aspects>
  void set_http_proxy_handler(std::string url_path,
                              std::vector<std::string_view> hosts,
                              coro_io::load_balance_algorithm type =
                                  coro_io::load_balance_algorithm::random,
                              std::vector<int> weights = {},
                              Aspects &&...aspects) {
    if (hosts.empty()) {
      throw std::invalid_argument("not config hosts yet!");
    }

    auto load_balancer =
        std::make_shared<coro_io::load_balancer<coro_http_client>>(
            coro_io::load_balancer<coro_http_client>::create(
                hosts, {.lba = type}, weights));
    auto handler =
        [this, load_balancer, type](
            coro_http_request &req,
            coro_http_response &response) -> async_simple::coro::Lazy<void> {
      co_await load_balancer->send_request(
          [this, &req, &response](
              coro_http_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            co_await reply(client, host, req, response);
          });
    };

    if constexpr (sizeof...(method) == 0) {
      set_http_handler<http_method::GET, http_method::POST, http_method::DEL,
                       http_method::HEAD, http_method::PUT, http_method::PATCH,
                       http_method::CONNECT, http_method::TRACE,
                       http_method::OPTIONS>(url_path, std::move(handler),
                                             std::forward<Aspects>(aspects)...);
    }
    else {
      set_http_handler<method...>(url_path, std::move(handler),
                                  std::forward<Aspects>(aspects)...);
    }
  }

  template <http_method... method, typename... Aspects>
  void set_websocket_proxy_handler(std::string url_path,
                                   std::vector<std::string_view> hosts,
                                   coro_io::load_balance_algorithm type =
                                       coro_io::load_balance_algorithm::random,
                                   std::vector<int> weights = {},
                                   Aspects &&...aspects) {
    if (hosts.empty()) {
      throw std::invalid_argument("not config hosts yet!");
    }

    auto load_balancer =
        std::make_shared<coro_io::load_balancer<coro_http_client>>(
            coro_io::load_balancer<coro_http_client>::create(
                hosts, {.lba = type}, weights));

    set_http_handler<cinatra::GET>(
        url_path,
        [load_balancer](coro_http_request &req, coro_http_response &resp)
            -> async_simple::coro::Lazy<void> {
          websocket_result result{};
          while (true) {
            result = co_await req.get_conn()->read_websocket();
            if (result.ec) {
              break;
            }

            if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
              CINATRA_LOG_INFO << "close frame";
              break;
            }

            auto ret = co_await load_balancer->send_request(
                [&req, result](coro_http_client &client, std::string_view host)
                    -> async_simple::coro::Lazy<std::error_code> {
                  auto r =
                      co_await client.write_websocket(std::string(result.data));
                  if (r.net_err) {
                    co_return r.net_err;
                  }
                  auto data = co_await client.read_websocket();
                  if (data.net_err) {
                    co_return data.net_err;
                  }
                  auto ec = co_await req.get_conn()->write_websocket(
                      std::string(result.data));
                  if (ec) {
                    co_return ec;
                  }
                  co_return std::error_code{};
                });
            if (!ret.has_value()) {
              req.get_conn()->close();
              break;
            }
          }
        },
        std::forward<Aspects>(aspects)...);
  }

  void set_max_size_of_cache_files(size_t max_size = 3 * 1024 * 1024) {
    std::error_code ec;
    for (const auto &file :
         std::filesystem::recursive_directory_iterator(static_dir_, ec)) {
      if (ec) {
        continue;
      }

      if (!file.is_directory()) {
        size_t filesize = fs::file_size(file, ec);
        if (ec || filesize > max_size) {
          continue;
        }

        std::ifstream ifs(file.path(), std::ios::binary);
        if (ifs.is_open()) {
          std::string content;
          detail::resize(content, filesize);
          ifs.read(content.data(), content.size());
          static_file_cache_.emplace(file.path().string(), std::move(content));
        }
      }
    }
  }

  const coro_http_router &get_router() const { return router_; }

  void set_file_resp_format_type(file_resp_format_type type) {
    format_type_ = type;
  }

  void set_transfer_chunked_size(size_t size) { chunked_size_ = size; }

#ifdef INJECT_FOR_HTTP_SEVER_TEST
  void set_write_failed_forever(bool r) { write_failed_forever_ = r; }

  void set_read_failed_forever(bool r) { read_failed_forever_ = r; }
#endif

  template <typename... Aspects>
  void set_static_res_dir(std::string_view uri_suffix = "",
                          std::string file_path = "www", Aspects &&...aspects) {
    bool has_double_dot = (file_path.find("..") != std::string::npos) ||
                          (uri_suffix.find("..") != std::string::npos);
    if (std::filesystem::path(file_path).has_root_path() ||
        std::filesystem::path(uri_suffix).has_root_path() || has_double_dot) {
      CINATRA_LOG_ERROR << "invalid file path: " << file_path;
      std::exit(1);
    }

    if (!uri_suffix.empty()) {
      static_dir_router_path_ =
          std::filesystem::path(uri_suffix).make_preferred().string();
    }

    if (!file_path.empty()) {
      file_path = std::filesystem::path(file_path).filename().string();
      if (file_path.empty()) {
        static_dir_ = fs::absolute(fs::current_path().string()).string();
      }
      else {
        static_dir_ =
            std::filesystem::path(file_path).make_preferred().string();
      }
    }
    else {
      static_dir_ = fs::absolute(fs::current_path().string()).string();
    }

    files_.clear();
    std::error_code ec;
    for (const auto &file :
         std::filesystem::recursive_directory_iterator(static_dir_, ec)) {
      if (ec) {
        continue;
      }
      if (!file.is_directory()) {
        files_.push_back(file.path().string());
      }
    }

    std::filesystem::path router_path =
        std::filesystem::path(static_dir_router_path_);

    std::string uri;
    for (auto &file : files_) {
      auto relative_path =
          std::filesystem::path(file.substr(static_dir_.length())).string();
      if (size_t pos = relative_path.find('\\') != std::string::npos) {
        replace_all(relative_path, "\\", "/");
      }

      if (static_dir_router_path_.empty()) {
        uri = relative_path;
      }
      else {
        uri = fs::path("/")
                  .append(static_dir_router_path_)
                  .concat(relative_path)
                  .string();
      }

      set_http_handler<cinatra::GET>(
          uri,
          [this, file_name = file](
              coro_http_request &req,
              coro_http_response &resp) -> async_simple::coro::Lazy<void> {
            std::string_view extension = get_extension(file_name);
            std::string_view mime = get_mime_type(extension);
            auto range_str = req.get_header_value("Range");

            if (auto it = static_file_cache_.find(file_name);
                it != static_file_cache_.end()) {
              auto range_header = build_range_header(
                  mime, file_name, std::to_string(fs::file_size(file_name)));
              resp.set_delay(true);
              std::string &body = it->second;
              std::array<asio::const_buffer, 2> arr{asio::buffer(range_header),
                                                    asio::buffer(body)};
              co_await req.get_conn()->async_write(arr);
              co_return;
            }

            std::string content;
            detail::resize(content, chunked_size_);

            coro_io::coro_file in_file{};
            in_file.open(file_name, std::ios::in);
            if (!in_file.is_open()) {
              resp.set_status_and_content(status_type::not_found,
                                          file_name + "not found");
              co_return;
            }

            size_t file_size = fs::file_size(file_name);

            if (format_type_ == file_resp_format_type::chunked &&
                range_str.empty()) {
              resp.set_format_type(format_type::chunked);
              bool ok;
              if (ok = co_await resp.get_conn()->begin_chunked(); !ok) {
                co_return;
              }

              while (true) {
                auto [ec, size] =
                    co_await in_file.async_read(content.data(), content.size());
                if (ec) {
                  resp.set_status(status_type::no_content);
                  co_await resp.get_conn()->reply();
                  co_return;
                }

                bool r = co_await resp.get_conn()->write_chunked(
                    std::string_view(content.data(), size));
                if (!r) {
                  co_return;
                }

                if (in_file.eof()) {
                  co_await resp.get_conn()->end_chunked();
                  break;
                }
              }
            }
            else {
              auto pos = range_str.find('=');
              if (pos != std::string_view::npos) {
                range_str = range_str.substr(pos + 1);
                bool is_valid = true;
                auto ranges =
                    parse_ranges(range_str, fs::file_size(file_name), is_valid);
                if (!is_valid) {
                  resp.set_status(status_type::range_not_satisfiable);
                  co_return;
                }

                assert(!ranges.empty());

                if (ranges.size() == 1) {
                  // single part
                  auto [start, end] = ranges[0];
                  in_file.seek(start, std::ios::beg);
                  size_t part_size = end + 1 - start;
                  int status = (part_size == file_size) ? 200 : 206;
                  std::string content_range = "Content-Range: bytes ";
                  content_range.append(std::to_string(start))
                      .append("-")
                      .append(std::to_string(end))
                      .append("/")
                      .append(std::to_string(file_size))
                      .append(CRCF);
                  auto range_header = build_range_header(
                      mime, file_name, std::to_string(part_size), status,
                      content_range);
                  resp.set_delay(true);
                  bool r = co_await req.get_conn()->write_data(range_header);
                  if (!r) {
                    co_return;
                  }

                  co_await send_single_part(in_file, content, req, resp,
                                            part_size);
                }
                else {
                  // multiple ranges
                  resp.set_delay(true);
                  std::string file_size_str = std::to_string(file_size);
                  size_t content_len = 0;
                  std::vector<std::string> multi_heads = build_part_heads(
                      ranges, mime, file_size_str, content_len);
                  auto range_header = build_multiple_range_header(content_len);
                  bool r = co_await req.get_conn()->write_data(range_header);
                  if (!r) {
                    co_return;
                  }

                  for (int i = 0; i < ranges.size(); i++) {
                    std::string &part_header = multi_heads[i];
                    r = co_await req.get_conn()->write_data(part_header);
                    if (!r) {
                      co_return;
                    }

                    auto [start, end] = ranges[i];
                    bool ok = in_file.seek(start, std::ios::beg);
                    if (!ok) {
                      resp.set_status_and_content(status_type::bad_request,
                                                  "invalid range");
                      co_await resp.get_conn()->reply();
                      co_return;
                    }
                    size_t part_size = end + 1 - start;

                    std::string_view more = CRCF;
                    if (i == ranges.size() - 1) {
                      more = MULTIPART_END;
                    }
                    r = co_await send_single_part(in_file, content, req, resp,
                                                  part_size, more);
                    if (!r) {
                      co_return;
                    }
                  }
                }
                co_return;
              }

              auto range_header = build_range_header(mime, file_name,
                                                     std::to_string(file_size));
              resp.set_delay(true);
              bool r = co_await req.get_conn()->write_data(range_header);
              if (!r) {
                co_return;
              }

              while (true) {
                auto [ec, size] =
                    co_await in_file.async_read(content.data(), content.size());
                if (ec) {
                  resp.set_status(status_type::no_content);
                  co_await resp.get_conn()->reply();
                  co_return;
                }

                r = co_await req.get_conn()->write_data(
                    std::string_view(content.data(), size));
                if (!r) {
                  co_return;
                }

                if (in_file.eof()) {
                  break;
                }
              }
            }
          },
          std::forward<Aspects>(aspects)...);
    }
  }

  void set_check_duration(auto duration) { check_duration_ = duration; }

  void set_timeout_duration(
      std::chrono::steady_clock::duration timeout_duration) {
    if (timeout_duration > std::chrono::steady_clock::duration::zero()) {
      need_check_ = true;
      timeout_duration_ = timeout_duration;
      start_check_timer();
    }
  }

  void set_shrink_to_fit(bool r) { need_shrink_every_time_ = r; }

  void set_default_handler(std::function<async_simple::coro::Lazy<void>(
                               coro_http_request &, coro_http_response &)>
                               handler) {
    default_handler_ = std::move(handler);
  }

  size_t connection_count() {
    std::scoped_lock lock(*conn_mtx_);
    return connections_.size();
  }

  std::string_view address() { return address_; }
  std::error_code get_errc() { return errc_; }

  /*!
   * Set client filter callback
   * @param filter callback function that takes endpoint and returns bool
   *               true to allow connection, false to reject
   */
  void client_filter(
      std::function<bool(const asio::ip::tcp::endpoint &)> filter) {
    client_filter_ = std::move(filter);
  }

 private:
  std::error_code listen() {
    CINATRA_LOG_INFO << "begin to listen " << port_;
    using asio::ip::tcp;
    asio::error_code ec;

    asio::ip::tcp::resolver::query query(address_, std::to_string(port_));
    asio::ip::tcp::resolver resolver(acceptor_.get_executor());
    asio::ip::tcp::resolver::iterator it = resolver.resolve(query, ec);

    asio::ip::tcp::resolver::iterator it_end;
    if (ec || it == it_end) {
      CINATRA_LOG_ERROR << "bad address: " << address_
                        << " error: " << ec.message();
      if (ec) {
        return ec;
      }
      return std::make_error_code(std::errc::address_not_available);
    }

    auto endpoint = it->endpoint();
    acceptor_.open(endpoint.protocol(), ec);
    if (ec) {
      CINATRA_LOG_ERROR << "acceptor open failed"
                        << " error: " << ec.message();
      return ec;
    }
#ifdef __GNUC__
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
#endif
    acceptor_.bind(endpoint, ec);
    if (ec) {
      CINATRA_LOG_ERROR << "bind port: " << port_ << " error: " << ec.message();
      std::error_code ignore_ec;
      acceptor_.cancel(ignore_ec);
      acceptor_.close(ignore_ec);
      return ec;
    }
#ifdef _MSC_VER
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif
    acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
      CINATRA_LOG_ERROR << "get local endpoint port: " << port_
                        << " listen error: " << ec.message();
      return ec;
    }

    auto end_point = acceptor_.local_endpoint(ec);
    if (ec) {
      CINATRA_LOG_ERROR << "get local endpoint port: " << port_
                        << " error: " << ec.message();
      return ec;
    }
    port_ = end_point.port();

    CINATRA_LOG_INFO << "listen port " << port_ << " successfully";
    return {};
  }

 public:
  void transfer_connection(coro_io::socket_wrapper_t &&soc,
                           std::string_view head_msg) {
    auto conn = accept_impl(std::move(soc), true);
    conn->add_head(head_msg);
    start_one(conn, true).via(conn->get_executor()).detach();
  }

 private:
  std::shared_ptr<coro_http_connection> accept_impl(
      coro_io::socket_wrapper_t &&socket, bool is_transfer_connect = false) {
    uint64_t conn_id = ++conn_id_;
    CINATRA_LOG_DEBUG << "new connection comming, id: " << conn_id;
    auto conn = std::make_shared<coro_http_connection>(
        socket.get_executor(), std::move(socket), router_);
    if (no_delay_) {
      conn->tcp_socket().set_option(asio::ip::tcp::no_delay(true));
    }
    conn->set_max_http_body_size(max_http_body_len_);
    if (need_shrink_every_time_) {
      conn->set_shrink_to_fit(true);
    }
    if (need_check_) {
      conn->set_check_timeout(true);
    }
    if (default_handler_) {
      conn->set_default_handler(default_handler_);
    }

#ifdef INJECT_FOR_HTTP_SEVER_TEST
    if (write_failed_forever_) {
      conn->set_write_failed_forever(write_failed_forever_);
    }
    if (read_failed_forever_) {
      conn->set_read_failed_forever(read_failed_forever_);
    }
#endif

#ifdef CINATRA_ENABLE_SSL
    if (!is_transfer_connect && use_ssl_) {
      conn->init_ssl(cert_file_, key_file_, passwd_);
    }
#ifdef YLT_ENABLE_NTLS
    else if (!is_transfer_connect && use_ntls_) {
      conn->init_ntls(ntls_config_, passwd_);
    }
#endif  // YLT_ENABLE_NTLS
#endif
    std::weak_ptr<std::mutex> weak(conn_mtx_);
    conn->set_quit_callback(
        [this, weak](const uint64_t &id) {
          auto mtx = weak.lock();
          if (mtx) {
            std::scoped_lock lock(*mtx);
            if (!connections_.empty())
              connections_.erase(id);
          }
        },
        conn_id);
    {
      std::scoped_lock lock(*conn_mtx_);
      connections_.emplace(conn_id, conn);
    }
    return std::move(conn);
  }

  async_simple::coro::Lazy<std::error_code> accept() {
    for (;;) {
      coro_io::ExecutorWrapper<> *executor;
      if (out_ctx_ == nullptr) {
        executor = pool_->get_executor();
      }
      else {
        out_executor_ = std::make_unique<coro_io::ExecutorWrapper<>>(
            out_ctx_->get_executor());
        executor = out_executor_.get();
      }
      asio::ip::tcp::socket socket(executor->get_asio_executor());

      auto error = co_await coro_io::async_accept(acceptor_, socket);
      if (error) {
        CINATRA_LOG_INFO << "accept failed, error: " << error.message();
        if (error == asio::error::operation_aborted ||
            error == asio::error::bad_descriptor) {
          acceptor_close_waiter_.set_value();
          co_return error;
        }
        continue;
      }

      // Client filter check
      if (client_filter_) {
        auto remote_endpoint = socket.remote_endpoint(error);
        if (error) {
          CINATRA_LOG_WARNING << "Failed to get remote endpoint: "
                              << error.message();
          continue;
        }

        if (!client_filter_(remote_endpoint)) {
          CINATRA_LOG_WARNING << "HTTP connection rejected from "
                              << remote_endpoint.address().to_string()
                              << " by client filter";
          continue;
        }

        CINATRA_LOG_DEBUG << "HTTP connection accepted from "
                          << remote_endpoint.address().to_string()
                          << " (client filter passed)";
      }

      auto conn =
          accept_impl(coro_io::socket_wrapper_t{std::move(socket), executor});
      start_one(conn).via(conn->get_executor()).detach();
    }
  }
  async_simple::coro::Lazy<void> start_one(
      std::shared_ptr<coro_http_connection> conn) noexcept {
    co_await conn->start();
  }

  async_simple::coro::Lazy<void> start_one(
      std::shared_ptr<coro_http_connection> conn, bool has_shake) noexcept {
    co_await conn->start(has_shake);
  }

  void close_acceptor() {
    asio::dispatch(acceptor_.get_executor(), [this]() {
      asio::error_code ec;
      acceptor_.cancel(ec);
      acceptor_.close(ec);
    });
    acceptor_close_waiter_.get_future().wait();
  }

  void start_check_timer() {
    check_timer_.expires_after(check_duration_);
    check_timer_.async_wait([this](auto ec) {
      if (ec || stop_timer_) {
        return;
      }

      check_timeout();
      start_check_timer();
    });
  }

  void check_timeout() {
    auto cur_time = std::chrono::system_clock::now();

    std::unordered_map<uint64_t, std::shared_ptr<coro_http_connection>> conns;

    {
      std::scoped_lock lock(*conn_mtx_);
      for (auto it = connections_.begin();
           it != connections_.end();)  // no "++"!
      {
        if (cur_time - it->second->get_last_rwtime() > timeout_duration_) {
          it->second->close(false);
          connections_.erase(it++);
        }
        else {
          ++it;
        }
      }
    }
  }

  std::string build_multiple_range_header(size_t content_len) {
    std::string header_str = "HTTP/1.1 206 Partial Content\r\n";
    header_str.append("Content-Length: ");
    header_str.append(std::to_string(content_len)).append(CRCF);
    header_str.append("Content-Type: multipart/byteranges; boundary=");
    header_str.append(BOUNDARY).append(TWO_CRCF);
    return header_str;
  }

  std::vector<std::string> build_part_heads(auto &ranges, std::string_view mime,
                                            std::string_view file_size_str,
                                            size_t &content_len) {
    std::vector<std::string> multi_heads;
    for (auto [start, end] : ranges) {
      std::string part_header = "--";
      part_header.append(BOUNDARY).append(CRCF);
      part_header.append("Content-Type: ").append(mime).append(CRCF);
      part_header.append("Content-Range: ").append("bytes ");
      part_header.append(std::to_string(start))
          .append("-")
          .append(std::to_string(end))
          .append("/")
          .append(file_size_str)
          .append(TWO_CRCF);
      content_len += part_header.size();
      multi_heads.push_back(std::move(part_header));
      size_t part_size = end + 1 - start + CRCF.size();
      content_len += part_size;
    }
    content_len += (BOUNDARY.size() + 4);
    return multi_heads;
  }

  std::string build_range_header(std::string_view mime,
                                 std::string_view filename,
                                 std::string_view file_size_str,
                                 int status = 200,
                                 std::string_view content_range = "") {
    std::string header_str = "HTTP/1.1 ";
    header_str.append(std::to_string(status));
    header_str.append(
        " OK\r\nAccess-Control-Allow-origin: "
        "*\r\nAccept-Ranges: bytes\r\n");
    if (!content_range.empty()) {
      header_str.append(content_range);
    }
    header_str.append("Content-Disposition: attachment;filename=");
    header_str.append(filename).append("\r\n");
    header_str.append("Connection: keep-alive\r\n");
    header_str.append("Content-Type: ").append(mime).append("\r\n");
    header_str.append("Content-Length: ");
    header_str.append(file_size_str).append("\r\n\r\n");
    return header_str;
  }

  async_simple::coro::Lazy<bool> send_single_part(auto &in_file, auto &content,
                                                  auto &req, auto &resp,
                                                  size_t part_size,
                                                  std::string_view more = "") {
    while (true) {
      size_t read_size = (std::min)(part_size, chunked_size_);
      if (read_size == 0) {
        break;
      }
      auto [ec, size] = co_await in_file.async_read(content.data(), read_size);
      if (ec) {
        resp.set_status(status_type::no_content);
        co_await resp.get_conn()->reply();
        co_return false;
      }

      part_size -= read_size;

      bool r = true;
      if (more.empty()) {
        r = co_await req.get_conn()->write_data(
            std::string_view(content.data(), size));
      }
      else {
        std::array<asio::const_buffer, 2> arr{
            asio::buffer(content.data(), size), asio::buffer(more)};
        auto [ec, _] = co_await req.get_conn()->async_write(arr);
        if (ec) {
          r = false;
        }
      }

      if (!r) {
        co_return false;
      }
    }

    co_return true;
  }

  template <class T, class Pred>
  size_t erase_if(std::span<T> &sp, Pred p) {
    auto it = std::remove_if(sp.begin(), sp.end(), p);
    size_t count = sp.end() - it;
    sp = std::span<T>(sp.data(), sp.data() + count);
    return count;
  }

  int remove_result_headers(resp_data &result, std::string_view value) {
    bool r = false;
    return erase_if(result.resp_headers, [&](http_header &header) {
      if (r) {
        return false;
      }

      r = (header.value.find(value) != std::string_view::npos);

      return r;
    });
  }

  void handle_response_header(resp_data &result, std::string &length) {
    int r = remove_result_headers(result, "chunked");
    if (r == 0) {
      r = remove_result_headers(result, "multipart/form-data");
      if (r) {
        length = std::to_string(result.resp_body.size());
        for (auto &[key, val] : result.resp_headers) {
          if (key == "Content-Length") {
            val = length;
            break;
          }
        }
      }
    }
  }

  async_simple::coro::Lazy<void> reply(coro_http_client &client,
                                       std::string_view host,
                                       coro_http_request &req,
                                       coro_http_response &response) {
    uri_t uri;
    std::string proxy_host;

    if (host.find("//") == std::string_view::npos) {
      proxy_host.append("http://").append(host);
      uri.parse_from(proxy_host.data());
    }
    else {
      uri.parse_from(host.data());
    }
    std::unordered_map<std::string, std::string> req_headers;
    for (auto &[k, v] : req.get_headers()) {
      req_headers.emplace(k, v);
    }
    req_headers["Host"] = uri.host;

    auto ctx = req_context<std::string_view>{.content = req.get_body()};
    auto result = co_await client.async_request(
        req.full_url(), method_type(req.get_method()), std::move(ctx),
        std::move(req_headers));

    std::string length;
    handle_response_header(result, length);
    response.add_header_span(result.resp_headers);

    response.set_status_and_content_view(
        static_cast<status_type>(result.status), result.resp_body);
    co_await response.get_conn()->reply();
    response.set_delay(true);
  }

  bool is_ip_v6(std::string_view address) {
    asio::ip::address_v6::bytes_type bytes;
    unsigned long scope_id = 0;

    struct in6_addr addr;
    asio::error_code ec;
    return asio::detail::socket_ops::inet_pton(ASIO_OS_DEF(AF_INET6),
                                               address.data(), &bytes[0],
                                               &scope_id, ec) > 0;
  }

  void init_address(std::string address) {
#if __has_include(<ylt/easylog.hpp>)
    easylog::logger<>::instance();  // init easylog singleton to make sure
                                    // server destruct before easylog.
#endif
    if (size_t pos = address.find(':');
        pos != std::string::npos && !is_ip_v6(address)) {
      auto port_sv = std::string_view(address).substr(pos + 1);

      uint16_t port;
      auto [ptr, ec] = std::from_chars(
          port_sv.data(), port_sv.data() + port_sv.size(), port, 10);
      if (ec != std::errc{}) {
        address_ = std::move(address);
        return;
      }

      port_ = port;
      address = address.substr(0, pos);
    }

    address_ = std::move(address);
  }

 private:
  std::unique_ptr<coro_io::io_context_pool> pool_;
  asio::io_context *out_ctx_ = nullptr;
  std::unique_ptr<coro_io::ExecutorWrapper<>> out_executor_ = nullptr;
  uint16_t port_;
  std::string address_;
  std::error_code errc_ = {};
  asio::ip::tcp::acceptor acceptor_;
  std::thread thd_;
  std::mutex thd_mtx_;
  std::promise<void> acceptor_close_waiter_;
  bool no_delay_ = true;

  uint64_t conn_id_ = 0;
  std::unordered_map<uint64_t, std::shared_ptr<coro_http_connection>>
      connections_;
  std::shared_ptr<std::mutex> conn_mtx_ = std::make_shared<std::mutex>();
  std::chrono::steady_clock::duration check_duration_ =
      std::chrono::seconds(15);
  std::chrono::steady_clock::duration timeout_duration_{};
  asio::steady_timer check_timer_;
  bool need_check_ = false;
  std::atomic<bool> stop_timer_ = false;

  std::string static_dir_router_path_ = "";
  std::string static_dir_ = "";
  std::vector<std::string> files_;
  size_t chunked_size_ = 1024 * 10;

  std::unordered_map<std::string, std::string> static_file_cache_;
  file_resp_format_type format_type_ = file_resp_format_type::chunked;
#ifdef CINATRA_ENABLE_SSL
  std::string cert_file_;
  std::string key_file_;
  std::string passwd_;
  bool use_ssl_ = false;
#ifdef YLT_ENABLE_NTLS
  bool use_ntls_ = false;
  // NTLS configuration
  struct {
    std::string base_path;

    // TLCP dual certificate configuration (GB/T 38636-2020)
    std::string sign_cert_file;
    std::string sign_key_file;
    std::string enc_cert_file;
    std::string enc_key_file;

    // TLS 1.3 + GM single certificate configuration (RFC 8998)
    std::string gm_cert_file;
    std::string gm_key_file;

    // Common configuration
    std::string ca_cert_file;
    std::string cipher_suites = "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3";
    coro_http_connection::ntls_mode mode =
        coro_http_connection::ntls_mode::tlcp_dual_cert;
    bool enable_client_verify = false;
    bool enable_ntls = false;
  } ntls_config_;
#endif  // YLT_ENABLE_NTLS
#endif
  coro_http_router router_;
  bool need_shrink_every_time_ = false;
  std::function<async_simple::coro::Lazy<void>(coro_http_request &,
                                               coro_http_response &)>
      default_handler_ = nullptr;
  int64_t max_http_body_len_ = INT64_MAX;
#ifdef INJECT_FOR_HTTP_SEVER_TEST
  bool write_failed_forever_ = false;
  bool read_failed_forever_ = false;
#endif

  std::function<bool(const asio::ip::tcp::endpoint &)> client_filter_;
};

using http_server = coro_http_server;
using request = coro_http_request;
using response = coro_http_response;
}  // namespace cinatra