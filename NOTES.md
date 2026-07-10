# Issue #2798 — `mooncake_master` fails with `bad address: 0.0.0.0 error: Host not found (authoritative)`

## 1. Root cause

`mooncake_master` binds its RPC server (`--rpc_address`, default `0.0.0.0`) and
its HTTP servers (metrics, metadata) to the IPv4 wildcard literal. Both servers
come from yalantinglibs (ylt), which is pinned as the `extern/yalantinglibs`
submodule.

In the pinned revision (`6a0e067`, Apr 2026) both acceptors turn the configured
listen address into an endpoint by handing it to the **system resolver**:

* `include/ylt/coro_io/server_acceptor.hpp` (coro_rpc):

  ```cpp
  asio::ip::tcp::resolver::query query(address_, std::to_string(port_));
  auto it = resolver.resolve(query, ec);
  if (ec || it == it_end) { ELOG_ERROR << "resolve address " << address_ ...; }
  ```

* `include/ylt/standalone/cinatra/coro_http_server.hpp:719` (coro_http) — the
  same `resolver::resolve()` call, logging `bad address: ...` from line 725,
  which is exactly the line number in the reporter's log.

`asio::ip::tcp::resolver::query(host, service)` defaults to
`resolver_query_base::address_configured`, i.e. `AI_ADDRCONFIG`, and asio maps
`EAI_NONAME` to `asio::error::host_not_found`, whose message is
*"Host not found (authoritative)"* — the reporter's error verbatim, for both
servers.

So the wildcard address is never parsed as a numeric IP literal; it is looked up
through `getaddrinfo()`. Whether `getaddrinfo("0.0.0.0", ..., AI_ADDRCONFIG)`
succeeds is environment-dependent (glibc version, and whether the host has a
non-loopback IPv4 address configured — containers, IPv6-only hosts). On glibc
2.35 it happens to succeed, which is why this is not seen everywhere; on the
reporter's host it returns `EAI_NONAME` and the master aborts during startup.
Nothing about `0.0.0.0` requires a name lookup: it is a numeric literal and
should be parsed directly.

Upstream ylt fixed exactly this in `coro_io/listen_endpoint.hpp`
(`resolve_listen_endpoint()`), and its comment names the same cause:

> If `address` is a numeric IP literal (e.g. `"::"`, `"0.0.0.0"`,
> `"127.0.0.1"`), it is parsed directly via `asio::ip::make_address` to avoid
> the ambiguity of `getaddrinfo()` on different glibc versions / container
> environments.

The fix landed in ylt `b2057ca` (#1186) and was hardened in `7801bc9` (#1190);
both coro_rpc's `server_acceptor` and cinatra's `coro_http_server` now use it.
Mooncake's pinned revision predates them.

## 2. The fix and why

Bump `extern/yalantinglibs` from `6a0e067` to `7801bc9` (16 commits, same
`main` line the current pin is on). That revision parses numeric listen
addresses with `asio::ip::make_address()` and only falls back to the resolver
for real hostnames such as `localhost`.

I chose the dependency bump over a Mooncake-side workaround because Mooncake has
no way to avoid the bad code path itself: it only passes an address *string* to
`coro_rpc_server` / `coro_http_server`, and every string — numeric or not —
went through `getaddrinfo()` inside ylt. Rewriting the address at the Mooncake
layer (e.g. substituting a concrete interface IP, or `::`) would be a guess
about which lookups the broken environment accepts, would silently change the
bind semantics, and would corrupt `local_hostname` (`rpc_address + ":" + port`).
There is also no injection point at all for the two HTTP servers, which fail the
same way.

I also added a regression test that reproduces the failing environment on any
host, rather than one that only passes where the resolver already behaves.

## 3. Files changed

| File | Change |
| --- | --- |
| `extern/yalantinglibs` | submodule pin `6a0e067` → `7801bc9` (the actual fix) |
| `mooncake-store/tests/wildcard_listen_test.cpp` | new regression test |
| `mooncake-store/tests/CMakeLists.txt` | register `wildcard_listen_test` (+ `${CMAKE_DL_LIBS}` for `dlsym`) |

The test interposes `getaddrinfo()` in the test binary so that it returns
`EAI_NONAME` for `0.0.0.0` and `::` and delegates everything else to libc. It
then asserts that a `coro_rpc_server` and a `coro_http_server` both still listen
on `0.0.0.0`. Interposition works because ylt is header-only, so its
`getaddrinfo()` call sites are compiled into the test binary.

## 4. Risk / uncertainty

* **Verified:** the dependency bump fixes the bind failure, and the test detects
  the regression. **Not verified:** that Mooncake as a whole builds and its full
  suite passes against ylt `7801bc9`. I could not build Mooncake here (no
  toolchain/deps). The 16 commits are mostly `coro_io`/ibverbs/cinatra fixes;
  the constructors and `init_ibv()` Mooncake uses are unchanged, and I checked
  the `coro_rpc_server` / `coro_http_server` signatures at that revision. This
  still needs a real CI build before merge — that is the main risk.
* One of the bumped commits (#1186/#1190) makes a `::` listen address create a
  dual-stack acceptor on Linux. Mooncake's default `0.0.0.0` is an IPv4
  endpoint and is unaffected, but `ipv6_client_test` / any deployment that binds
  `::` will now also get an IPv4 acceptor. This is upstream's intended behavior,
  but it is a behavior change worth a look.
* I could not reproduce the reporter's `getaddrinfo()` failure natively (glibc
  2.35 here resolves `0.0.0.0` even with no interfaces configured), so the exact
  environmental trigger on their host — old glibc, container without a
  non-loopback IPv4 address, or a resolver shim — is inferred, not observed. The
  failing *code path* is confirmed either way, and the fix removes the
  dependence on it.
* `.gitmodules` still says `branch = v0.5.7` while both the old and new pins are
  `main` commits (the fix is not on the `v0.5.7` branch). I left that
  pre-existing inconsistency alone to keep the change scoped.

## 5. How I verified it

Compiled the exact test body against both ylt revisions, with the same
`getaddrinfo()` interposer the test uses:

```
# pinned revision 6a0e067 (current)
rpc server listened on 0.0.0.0: NO (port=45311, errc=bad_address)
http server listened on 0.0.0.0: NO
FAIL

# bumped revision 7801bc9 (this change)
rpc server listened on 0.0.0.0: YES (port=45311, errc=ok)
http server listened on 0.0.0.0: YES
PASS
```

The `bad_address` error code is the one `coro_rpc_server::async_start()` returns
after logging `resolve address 0.0.0.0 error: Host not found (authoritative)`,
i.e. the failure reported in #2798.

Separately, a standalone C++ program confirmed that (a) symbol interposition of
`getaddrinfo()` from within a binary intercepts calls made by header-only code,
(b) the old path (`getaddrinfo` + `AI_ADDRCONFIG`) fails while the new path
(`inet_pton` + `bind`) succeeds on the wildcard, and (c) non-wildcard lookups
(`127.0.0.1`) still reach the real resolver, so the test does not disturb
anything else.
