#pragma once

#include <fstream>
#include <string>
#include <ylt/coro_rpc/coro_rpc_context.hpp>
using namespace coro_rpc;

std::string echo(std::string str);

struct dummy {
  std::string echo(std::string str) { return str; }
};

struct file_part {
  std::string filename;
  std::string content;
  bool eof;
};
// this function is just for case study, only allow one client to upload file.
std::errc upload(file_part part);

// support arbitrary clients to upload file.
void upload_file(coro_rpc::context<std::errc> conn, file_part part);

struct response_part {
  std::errc ec;
  std::string content;
  bool eof;
};

void download_file(coro_rpc::context<response_part> conn, std::string filename);