#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "async_simple/coro/Lazy.h"
#include "ylt/coro_http/coro_http_server.hpp"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

using namespace coro_http;

struct login_info_t {
  std::string_view type;
  std::string_view content;
  std::vector<std::string> user_list;
};
YLT_REFL(login_info_t, type, content, user_list);

using logout_info_t = login_info_t;

struct user_info_t {
  std::string_view type;
  std::string_view from;
  std::string_view content;
};
YLT_REFL(user_info_t, type, from, content);

struct message_t {
  std::string type;
  std::string_view content;
};
YLT_REFL(message_t, type, content);

struct conn_holder {
  std::weak_ptr<coro_http_connection> conn;
  std::string user_name;
};

async_simple::coro::Lazy<std::vector<uint64_t>> broadcast(
    auto &conn_map, std::string &resp_str) {
  std::vector<uint64_t> dead_conn_ids;
  for (auto &[id, holder] : conn_map) {
    auto conn = holder.conn.lock();
    if (conn == nullptr) {
      dead_conn_ids.push_back(id);
      continue;
    }
    auto ec = co_await conn->write_websocket(resp_str);
    if (ec) {
      std::cout << ec.message() << "\n";
      continue;
    }
  }

  resp_str.clear();
  co_return dead_conn_ids;
}

int main() {
  std::cout << fs::current_path() << "\n";
  coro_http::coro_http_server server(1, 9001);
  server.set_static_res_dir("", "");
  std::mutex mtx;
  std::unordered_map<uint64_t, conn_holder> conn_map;
  server.set_http_handler<cinatra::GET>(
      "/",
      [&](coro_http_request &req,
          coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        websocket_result result{};

        std::unordered_map<uint64_t, conn_holder> map;
        std::string resp_str;
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            {
              std::scoped_lock lock(mtx);
              conn_map.erase(req.get_conn()->conn_id());
            }
            break;
          }

          message_t msg{};
          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            std::cout << "close frame\n";
            msg.type = "logout";
          }
          else {
            struct_json::from_json(msg, result.data);
          }

          if (msg.type == "user") {
            std::string from;
            {
              std::scoped_lock lock(mtx);
              from = conn_map.at(req.get_conn()->conn_id()).user_name;
              map = conn_map;
            }

            user_info_t info{msg.type, from, msg.content};
            struct_json::to_json(info, resp_str);
          }
          else if (msg.type == "login" || msg.type == "logout") {
            std::string user_name;

            {
              std::scoped_lock lock(mtx);
              if (msg.type == "login") {
                user_name = std::string{msg.content};
                conn_map.emplace(
                    req.get_conn()->conn_id(),
                    conn_holder{req.get_conn()->shared_from_this(), user_name});
              }
              else {
                user_name = conn_map.at(req.get_conn()->conn_id()).user_name;
                conn_map.erase(req.get_conn()->conn_id());
              }
              map = conn_map;
            }

            if (!map.empty()) {
              std::vector<std::string> user_list;
              std::transform(map.begin(), map.end(),
                             std::back_inserter(user_list), [](auto &kv) {
                               return kv.second.user_name;
                             });
              logout_info_t info{msg.type, user_name, std::move(user_list)};
              struct_json::to_json(info, resp_str);
            }
          }

          std::cout << result.data << "\n";

          auto ids = co_await broadcast(map, resp_str);
          if (!ids.empty()) {
            // clean dead connection
            std::scoped_lock lock(mtx);
            for (auto id : ids) {
              conn_map.erase(id);
            }
          }

          if (msg.type == "logout") {
            break;
          }
        }
      });
  server.sync_start();
}