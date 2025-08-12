#ifndef TRANSFER_CLIENT_H
#define TRANSFER_CLIENT_H

#include <string>
#include <string_view>
#include <ylt/coro_rpc/coro_rpc_context.hpp>

void echo_with_attachment(coro_rpc::context<void> ctx);

void simple_rpc_simulation();
#endif // CORO_RPC_RPC_API_HPP