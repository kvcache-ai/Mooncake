#include "coro_rpc.h"

#include <asio/ip/host_name.hpp>
#include <system_error>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ylt/coro_io/load_balancer.hpp"

inline char *create_copy_cstr(std::string msg) {
  char *buf = (char *)malloc(msg.size() + 1);
  memcpy(buf, msg.data(), msg.size());
  buf[msg.size()] = '\0';
  return buf;
}

inline void set_rpc_result(rpc_result &ret, coro_rpc::rpc_error ec) {
  ret.ec = ec.val();
  ret.err_msg = create_copy_cstr(ec.msg);
}

// rpc server
inline void ylt_load_service(coro_rpc::context<void> context, uint64_t req_id) {
  auto ctx_ptr = std::make_unique<coro_rpc::context<void>>(std::move(context));
  load_service(ctx_ptr.release(), req_id);
}

void response_error(void *ctx, uint16_t err_code, const char *err_msg) {
  std::unique_ptr<coro_rpc::context<void>> context(
      (coro_rpc::context<void> *)ctx);

  context->response_error(coro_rpc::err_code(err_code), err_msg);
}

void *response_msg(void *ctx, char *msg, uint64_t size) {
  std::unique_ptr<coro_rpc::context<void>> context(
      (coro_rpc::context<void> *)ctx);
  // wait for response finish;
  auto promise = std::make_unique<std::promise<std::error_code>>();
  context->get_context_info()->set_complete_handler(
      [p = promise.get()](const std::error_code &ec, std::size_t) {
        p->set_value(ec);
      });

  context->get_context_info()->set_response_attachment(
      std::string_view(msg, size));
  context->response_msg();
  return promise.release();
}

rpc_result wait_response_finish(void *p) {
  auto promise = (std::promise<std::error_code> *)p;
  auto ec = promise->get_future().get();
  delete promise;
  if (!ec) {
    return {};
  }

  char *buf = create_copy_cstr(ec.message());
  return rpc_result{0, buf, 0};
}

void *start_rpc_server(char *addr, server_config conf) {
  auto server = std::make_unique<coro_rpc::coro_rpc_server>(
      conf.parallel, addr, std::chrono::seconds(600));
  server->register_handler<ylt_load_service>();
#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    if (conf.min_recv_buf_count != 0)
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;

    if (conf.max_recv_buf_count != 0) {
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    }

    ib_conf.cap.max_recv_wr =
        (std::max)(ib_conf.cap.max_recv_wr, ib_conf.recv_buffer_cnt + 1);

    std::string nic_name;
    if (conf.device_name != nullptr) {
      nic_name = conf.device_name;
    }
    auto dev = coro_io::get_global_ib_device({.dev_name = nic_name});
    if (dev == nullptr) {
      return nullptr;
    }
    ib_conf.device = dev;

    server->init_ibv(ib_conf);
  }
#endif

  auto res = server->async_start();
  if (res.hasResult()) {
    ELOG_ERROR << "start server failed";
    return nullptr;
  }

  return server.release();
}

void stop_rpc_server(void *server) {
  if (server != nullptr) {
    auto s = (coro_rpc::coro_rpc_server *)server;
    s->stop();
    delete s;
  }
}

#ifdef YLT_ENABLE_IBV
void config_buffer_pool(pool_config conf) {
  coro_io::ib_buffer_pool_t::config_t pool_conf{};
  if (conf.buffer_size != 0) {
    pool_conf.buffer_size = conf.buffer_size;
  }

  if (conf.max_memory_usage != 0) {
    pool_conf.max_memory_usage = conf.max_memory_usage;
  }

  coro_io::get_global_ib_device({.buffer_pool_config = pool_conf});
}
#endif

// rpc client
void *create_client_pool(char *addr, client_config conf) {
  std::vector<std::string_view> hosts{std::string_view(addr)};
  coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
  pool_conf.max_connection_life_time = std::chrono::seconds(3600);
#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    if (conf.min_recv_buf_count != 0)
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;

    if (conf.max_recv_buf_count != 0) {
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    }

    ib_conf.cap.max_recv_wr =
        (std::max)(ib_conf.cap.max_recv_wr, ib_conf.recv_buffer_cnt + 1);

    std::string device_name;
    if (conf.local_ip != nullptr) {
      device_name = conf.local_ip;
    }
    auto dev = coro_io::get_global_ib_device({.dev_name = device_name});
    if (dev == nullptr) {
      return nullptr;
    }
    ib_conf.device = dev;

    pool_conf.client_config.socket_config = ib_conf;
  }
#endif

  pool_conf.client_config.connect_timeout_duration =
      std::chrono::seconds{conf.connect_timeout_sec};

  pool_conf.client_config.request_timeout_duration =
      std::chrono::seconds{conf.req_timeout_sec};

  if (conf.local_ip != nullptr && !conf.enable_ib) {
    pool_conf.client_config.local_ip = conf.local_ip;
  }

  ELOG_INFO << "client config connect timeout seconds: "
            << conf.connect_timeout_sec
            << ", request timeout seconds: " << conf.req_timeout_sec
            << ", local_ip: " << pool_conf.client_config.local_ip;

  auto ld = coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
      hosts, {pool_conf});
  auto ld_ptr = std::make_unique<decltype(ld)>(std::move(ld));
  return ld_ptr.release();
}

void free_client_pool(void *pool) {
  auto ptr = (coro_io::load_balancer<coro_rpc::coro_rpc_client> *)pool;
  delete ptr;
}

rpc_result load(void *pool, uint64_t req_id, char *dest, uint64_t dest_len) {
  using namespace async_simple::coro;
  rpc_result ret{};
  if (dest == nullptr || dest_len == 0) {
    set_rpc_result(ret,
                   coro_rpc::rpc_error(coro_rpc::errc::invalid_rpc_arguments));
    return ret;
  }

  auto ld = (coro_io::load_balancer<coro_rpc::coro_rpc_client> *)pool;
  auto lazy = [&]() -> Lazy<void> {
    auto result =
        co_await ld->send_request([&](coro_rpc::coro_rpc_client &client,
                                      std::string_view hostname) -> Lazy<void> {
          client.set_resp_attachment_buf(std::span<char>(dest, dest_len));
          auto result = co_await client.call<ylt_load_service>(req_id);
          if (!result) {
            set_rpc_result(ret, result.error());
            co_return;
          }

          if (!client.is_resp_attachment_in_external_buf()) {
            set_rpc_result(
                ret, coro_rpc::rpc_error(coro_rpc::errc::message_too_large));
            co_return;
          }
          ret.len = client.get_resp_attachment().size();
        });
    if (!result) {
      if (result.error() == std::errc::connection_refused) {
        set_rpc_result(ret, coro_rpc::rpc_error(coro_rpc::errc::not_connected));
      }
      else {
        set_rpc_result(ret,
                       coro_rpc::rpc_error(
                           coro_rpc::errc::io_error,
                           std::make_error_code(result.error()).message()));
      }
    }
  };

  syncAwait(lazy());

  return ret;
}

#ifdef YLT_ENABLE_IBV
uint64_t global_memory_usage() {
  return coro_io::ib_buffer_pool_t::global_memory_usage();
}
uint64_t global_history_max_memory_usage() {
  return coro_io::ib_buffer_pool_t::global_history_max_memory_usage();
}
#endif

// log
void init_rpc_log(char *log_filename, int log_level, uint64_t max_file_size,
                  uint64_t max_file_num, bool async) {
  if (log_filename == nullptr) {
    easylog::set_min_severity((easylog::Severity)log_level);
    ELOG_DEBUG << "init log, log_level: " << log_level;
    return;
  }

  std::string filename(log_filename);
  easylog::init_log((easylog::Severity)log_level, filename, async, true,
                    max_file_size, max_file_num, false);
  ELOG_DEBUG << "init log " << log_level << ", " << filename << ","
             << max_file_size << "," << max_file_num;
}

void flush_rpc_log() { easylog::flush(); }

char *get_first_local_ip() {
  std::string local_ip = "localhost";
  using asio::ip::tcp;
  tcp::resolver resolver(coro_io::get_global_executor()->get_asio_executor());
  tcp::resolver::query query(asio::ip::host_name(), "");
  tcp::resolver::iterator iter = resolver.resolve(query);
  tcp::resolver::iterator end;  // End marker.
  while (iter != end) {
    tcp::endpoint ep = *iter++;
    auto addr = ep.address();
    if (addr.is_v4()) {
      local_ip = addr.to_string();
      break;
    }
  }

  return create_copy_cstr(local_ip);
}