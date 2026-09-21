// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <glog/logging.h>

// MOONCAKE_GLOG_HAS_IS_INITIALIZED is defined by FindGLOG.cmake via the
// glog::glog INTERFACE compile definitions. It is set to 1 only when the
// detected glog version is >= 0.6.0, where google::IsGoogleLoggingInitialized()
// is publicly exported as a top-level symbol. This defensive fallback keeps
// this header usable even if the macro was not propagated for some reason.
#ifndef MOONCAKE_GLOG_HAS_IS_INITIALIZED
#define MOONCAKE_GLOG_HAS_IS_INITIALIZED 0
#endif

#if !MOONCAKE_GLOG_HAS_IS_INITIALIZED
// glog < 0.6.0 does NOT export google::IsGoogleLoggingInitialized() at the
// top level; the public declaration was only added in 0.6.0. However, the
// same function has always existed in these older versions inside the
// internal namespace google::glog_internal_namespace_ (declared in glog's
// private header src/utilities.h, which is not installed). The symbol is
// exported by libglog, so we forward-declare it here to reuse it without
// depending on any private header.
//
// NOTE: In glog >= 0.6.0 this internal symbol no longer exists (it was moved
// to the top-level google:: namespace), which is exactly why this branch is
// only compiled when MOONCAKE_GLOG_HAS_IS_INITIALIZED == 0.
namespace google {
namespace glog_internal_namespace_ {
bool IsGoogleLoggingInitialized();
}  // namespace glog_internal_namespace_
}  // namespace google
#endif

namespace mooncake {

// Initializes glog unless it has already been initialized — glog >= 0.6.0
// CHECK-fails with "You called InitGoogleLogging() twice!" otherwise. Two
// paths may have initialized glog before a standalone main() gets here:
//   - an embedding host process (EP / inference frameworks), or
//   - our own static initializer: rdma_transport/worker_pool.cpp evaluates
//     globalConfig() before main(), and loadGlobalConfig() initializes glog
//     when MC_LOG_DIR is set — and also sets FLAGS_log_dir, so main()'s
//     !FLAGS_log_dir.empty() check cannot detect this case.
inline void InitGoogleLoggingOnce(const char* name) {
#if MOONCAKE_GLOG_HAS_IS_INITIALIZED
    using google::IsGoogleLoggingInitialized;
#else
    using google::glog_internal_namespace_::IsGoogleLoggingInitialized;
#endif
    if (!IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(name);
    }
}

}  // namespace mooncake
