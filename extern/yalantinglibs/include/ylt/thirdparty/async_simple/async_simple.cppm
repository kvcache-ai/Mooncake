// There unhandled macro uses found in the body:
//	'CPU_SETSIZE' defined in /usr/include/sched.h:82:10
//	'CPU_ISSET' defined in /usr/include/sched.h:85:10
//	'CPU_ZERO' defined in /usr/include/sched.h:87:10
//	'CPU_SET' defined in /usr/include/sched.h:83:10
module;
// WARNING: Detected unhandled non interesting includes.
// It is not suggested mix includes and imports from the compiler's
// perspective. Since it may introduce redeclarations within different
// translation units and the compiler is not able to handle such patterns
// efficiently.
//
// See
// https://clang.llvm.org/docs/StandardCPlusPlusModules.html#performance-tips
#include <stdio.h>
#include <cassert>
#include <climits>
#include <cstdint>
#include <version>

#ifdef __linux__
#include <sched.h>
#endif

#if __has_include(<libaio.h>) && !defined(ASYNC_SIMPLE_HAS_NOT_AIO)
#include <libaio.h>
#endif

export module async_simple;
import std;
#define ASYNC_SIMPLE_USE_MODULES
export extern "C++" {
  #include "util/move_only_function.h"
  #include "coro/Traits.h"
  #include "CommonMacros.h"
  #include "Common.h"
  #include "MoveWrapper.h"
  #include "experimental/coroutine.h"
  #include "async_simple/Signal.h"
  #include "Executor.h"
  #include "async_simple/coro/LazyLocalBase.h"
  #include "Unit.h"
  #include "Try.h"
  #include "FutureState.h"
  #include "LocalState.h"
  #include "Traits.h"
  #include "Future.h"
  #include "Promise.h"
  #include "coro/DetachedCoroutine.h"
  #include "coro/ViaCoroutine.h"
#if defined(__clang_major__)  && __clang_major__ >= 17
  #include "coro/PromiseAllocator.h"
#endif
  #include "coro/Lazy.h"
  #include "uthread/internal/thread_impl.h"
  #include "uthread/Await.h"
  #include "uthread/Latch.h"
  #include "uthread/internal/thread.h"
  #include "uthread/Uthread.h"
  #include "uthread/Async.h"
  #include "coro/ConditionVariable.h"
  #include "coro/SpinLock.h"
  #include "coro/Latch.h"
  #include "coro/CountEvent.h"
  #include "coro/Collect.h"
  #include "IOExecutor.h"
  #include "coro/SharedMutex.h"
  #include "uthread/Collect.h"
  #include "executors/SimpleIOExecutor.h"
  #include "coro/Mutex.h"
  #include "Collect.h"
  #include "util/Condition.h"
  #include "coro/Dispatch.h"
  #include "coro/Sleep.h"
  #include "util/Queue.h"
  #include "util/ThreadPool.h"
  #include "coro/ResumeBySchedule.h"
  #include "coro/FutureAwaiter.h"
  #include "coro/SyncAwait.h"
  #include "executors/SimpleExecutor.h"
  #include "coro/Semaphore.h"
  // There are some bugs in clang lower versions.
#if defined(__clang_major__)  && __clang_major__ >= 17
  #include "coro/Generator.h"
#endif
}
