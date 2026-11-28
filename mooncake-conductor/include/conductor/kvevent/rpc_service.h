#pragma once

// coro_io.hpp uses std::signal/SIGPIPE without including <csignal>; it
// survives standalone builds only via header-only asio's transitive includes.
// The top-level build compiles asio separately (ASIO_SEPARATE_COMPILATION),
// dropping that transitive include, so provide it before any ylt header.
#include <csignal>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake::conductor::kvevent {

class ConductorService;

// Registers every ConductorService method as an RPC handler. Method names on
// the wire are the C++ member names; both endpoints of the channel share
// conductor/client/types.h, so handler and client stub cannot drift.
void RegisterConductorRpcService(coro_rpc::coro_rpc_server& server,
                                 ConductorService& service);

}  // namespace mooncake::conductor::kvevent
