#include <filesystem>
#include <iostream>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "rpc_service.h"

async_simple::coro::Lazy<void> test(coro_rpc::coro_rpc_client &client,
                                    auto filename) {
  [[maybe_unused]] auto ret = co_await client.connect("127.0.0.1", "9000");
  assert(!ret);

  auto result = co_await client.call<echo>("hello lantinglibs");
  if (result) {
    std::cout << result.value() << "\n";
  }
  else {
    std::cout << result.error().msg << '\n';
    co_return;
  }

  auto r = co_await client.call<&dummy::echo>("hello lantinglibs");
  if (r) {
    std::cout << r.value() << "\n";
  }
  else {
    std::cout << r.error().msg << '\n';
    co_return;
  }

  std::ifstream file(filename, std::ios::binary);
  if (!file.is_open()) {
    std::cout << "open file failed"
              << "\n";
    co_return;
  }

  std::cout << "begin to upload file " << filename << "\n";

  char buf[1024];
  file_part part{.filename = filename, .content = "", .eof = false};

  while (!file.eof()) {
    size_t real_size = file.read(buf, 1024).gcount();
    if (real_size != 1024) {
      std::cout << "real size = " << real_size << "\n";
    }
    part.content = {buf, real_size};

    auto call_result = co_await client.call<upload_file>(part);
    if (!call_result) {
      std::cout << "upload failed, lost size " << real_size << "\n";
      co_return;
    }
  }

  part.content = "";
  part.eof = true;
  co_await client.call<upload_file>(part);

  std::cout << "upload finished\n";

  // download file
  std::string download_filename =
      std::filesystem::path(filename).filename().string();
  std::cout << "begin to download file " << download_filename << "\n";
  std::string saved_filename =
      "temp" + std::filesystem::path(filename).extension().string();

  std::ofstream out(saved_filename, std::ios::binary | std::ios::app);
  while (true) {
    auto dresult = co_await client.call<download_file>(download_filename);
    if (dresult) {
      response_part download_part = dresult.value();
      if (download_part.ec != std::errc{}) {
        std::cout << "download failed\n";
        co_return;
      }

      out.write(download_part.content.data(), download_part.content.size());
      if (download_part.eof) {
        out.close();
        std::cout << "download " << download_filename << " finished\n";
        break;
      }
    }
    else {
      std::cout << "download failed " << dresult.error().msg << "\n";
      co_return;
    }
  }
  co_return;
}

int main() {
  std::cout << "please input filename\n";
  std::string filename;
  std::cin >> filename;

  if (!std::filesystem::exists(filename)) {
    std::cout << filename << " not exist\n";
    return -1;
  }

  if (!std::filesystem::is_regular_file(filename)) {
    std::cout << filename << " is not a file\n";
    return -1;
  }

  coro_rpc::coro_rpc_client client;
  async_simple::coro::syncAwait(test(client, filename));
}
