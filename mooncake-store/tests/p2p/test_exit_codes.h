#pragma once

namespace mooncake::testing {

// Exit code used to skip cross-process test suites under ASAN builds.
//
// Why skip: the cross-process harness re-executes its own binary via
// fork()/execv() to run the RPC server / P2P master in child processes.
// The ASAN/LSan runtime is not fork-safe in multithreaded processes: the
// forked child inherits sanitizer runtime locks held by threads that no
// longer exist (LSan also runs a stop-the-world tracer thread), so the
// child deadlocks on its first allocation. This reproduced as an
// indefinite hang in the ASAN CI jobs.
//
// This mirrors the existing project convention: redis_chaos_test (the only
// other fork-based test) is likewise kept out of the main ASAN ctest run
// via its "redis_ha" label (ctest -LE redis_ha).
//
// 77 is the conventional skip exit code (ctest / autotools convention) and
// must match SKIP_RETURN_CODE in tests/p2p/CMakeLists.txt.
constexpr int kAsanSkipExitCode = 77;

}  // namespace mooncake::testing
