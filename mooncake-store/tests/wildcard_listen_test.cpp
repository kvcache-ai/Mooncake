// Regression test for https://github.com/kvcache-ai/Mooncake/issues/2798
//
// mooncake_master binds its RPC server and its HTTP servers to the numeric
// wildcard address 0.0.0.0. The ylt acceptors used to turn that address into
// an endpoint through getaddrinfo(), which some environments answer with
// EAI_NONAME for numeric literals, so startup died with
//   "bad address: 0.0.0.0 error: Host not found (authoritative)".
//
// This test interposes getaddrinfo() so that it always rejects the wildcard
// literals, reproducing such an environment on any host, and asserts that both
// servers still come up: numeric literals must be parsed directly instead of
// being handed to the system resolver.

// _GNU_SOURCE must be defined before <dlfcn.h> so glibc exposes RTLD_NEXT.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <gtest/gtest.h>
#include <netdb.h>

#include <string_view>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "utils.h"

namespace {

bool IsWildcardAddress(const char *node) {
    if (node == nullptr) {
        return false;
    }
    const std::string_view host(node);
    return host == "0.0.0.0" || host == "::";
}

}  // namespace

// Interposes the libc symbol for every getaddrinfo() call made from this test
// binary, including the ones inside the header-only ylt acceptors.
extern "C" int getaddrinfo(const char *node, const char *service,
                           const struct addrinfo *hints,
                           struct addrinfo **res) {
    using GetAddrInfoFn = int (*)(const char *, const char *,
                                  const struct addrinfo *, struct addrinfo **);
    static auto *real_getaddrinfo =
        reinterpret_cast<GetAddrInfoFn>(dlsym(RTLD_NEXT, "getaddrinfo"));
    if (IsWildcardAddress(node)) {
        return EAI_NONAME;
    }
    // If RTLD_NEXT resolution failed (e.g. static linking), fall back to an
    // error instead of dereferencing a null function pointer. EAI_FAIL is used
    // rather than EAI_SYSTEM because the latter tells callers to inspect errno,
    // which is not set here.
    if (real_getaddrinfo == nullptr) {
        return EAI_FAIL;
    }
    return real_getaddrinfo(node, service, hints, res);
}

TEST(WildcardListenTest, RpcServerListensWhenResolverRejectsWildcard) {
    const int free_port = mooncake::getFreeTcpPort();
    ASSERT_GT(free_port, 0) << "failed to obtain a free TCP port";
    const auto port = static_cast<unsigned short>(free_port);
    coro_rpc::coro_rpc_server server(1, port, "0.0.0.0");

    auto stopped = server.async_start();
    ASSERT_FALSE(stopped.hasResult())
        << "failed to listen on 0.0.0.0: " << server.get_errc().message();
    EXPECT_EQ(server.port(), port);

    server.stop();
}

// 0.0.0.0 is covered by the RPC test above; bind the HTTP server to the IPv6
// wildcard "::" so the numeric-literal path is exercised for both families.
TEST(WildcardListenTest, HttpServerListensWhenResolverRejectsWildcard) {
    const int free_port = mooncake::getFreeTcpPort();
    ASSERT_GT(free_port, 0) << "failed to obtain a free TCP port";
    const auto port = static_cast<unsigned short>(free_port);
    coro_http::coro_http_server server(1, port, "::");

    auto stopped = server.async_start();
    ASSERT_FALSE(stopped.hasResult()) << "failed to listen on ::";

    server.stop();
}
