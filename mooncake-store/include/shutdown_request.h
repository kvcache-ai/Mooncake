#pragma once

#include <signal.h>

#include <chrono>

namespace mooncake {

// Process-wide SIGINT/SIGTERM intake for a process that drains before it
// exits. The first signal sets a sticky flag. A second signal ends the
// process at once with _exit(128 + signo).

// Installs the SIGINT and SIGTERM handlers with sigaction and SA_ONSTACK,
// which the Go c-shared HA backends in the master process require. Returns
// false with errno set if a sigaction call fails. A handler installed before
// the failure stays installed.
bool InstallShutdownSignalHandlers();

// True once SIGINT or SIGTERM was received. Safe from any thread.
bool ShutdownRequested();

// Records a shutdown request that code other than the signal handler
// observed, such as an injected predicate. Sticky and idempotent. Call it
// before any shutdown work: it starts the watchdog deadline.
void LatchShutdownRequest();

// True once LatchShutdownRequest or ExitAfterShutdown ran.
bool ShutdownLatched();

// Starts a detached watchdog thread. Once a signal was received or a request
// was latched, the watchdog ends the process with _exit(EXIT_FAILURE) if it
// is still alive `deadline` later. The deadline starts once and is never
// reset. The thread takes no locks and does no I/O. Call it once, serially,
// before the work it bounds; a repeated serial call returns true without a
// second thread. Concurrent calls are not supported. Returns false if the
// thread cannot be created.
bool StartShutdownWatchdog(std::chrono::milliseconds deadline);

// Ends the process after a completed shutdown. Latches the request, flushes
// glog only when the watchdog thread exists, so a blocked flush still ends
// at the deadline, then calls _exit(status); no destructors run.
[[noreturn]] void ExitAfterShutdown(int status);

namespace detail {

using SigactionFn = int (*)(int, const struct sigaction*, struct sigaction*);

// Test seam: InstallShutdownSignalHandlers() calls this with ::sigaction.
bool InstallShutdownSignalHandlersWith(SigactionFn install);

}  // namespace detail

}  // namespace mooncake
