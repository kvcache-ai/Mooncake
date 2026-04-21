#include <iostream>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "rpc_service.h"

int main() {
  coro_rpc::coro_rpc_server server(4, 9000);

  server.register_handler<echo, upload, upload_file, download_file>();

  dummy d{};
  server.register_handler<&dummy::echo>(&d);

  [[maybe_unused]] auto ret = server.start();
  assert(!ret);
  return 0;
}
