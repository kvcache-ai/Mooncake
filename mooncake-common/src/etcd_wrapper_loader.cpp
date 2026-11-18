#include "etcd_wrapper_loader.h"

#include <dlfcn.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace mooncake::etcd_wrapper {

namespace {

constexpr const char kLibraryName[] = "libetcd_wrapper.so";
constexpr const char kEnvLibraryPath[] = "MOONCAKE_ETCD_WRAPPER_PATH";

#define MOONCAKE_ETCD_WRAPPER_SYMBOLS(X)                                       \
    X(NewEtcdClient, GoInt (*)(char*, char**))                                 \
    X(EtcdPutWrapper, GoInt (*)(char*, char*, char**))                         \
    X(EtcdGetWrapper, GoInt (*)(char*, char**, char**))                        \
    X(EtcdDeleteWrapper, GoInt (*)(char*, char**))                             \
    X(EtcdCloseWrapper, void (*)())                                            \
    X(SetStoreEtcdMaxSendMessageBytes, GoInt (*)(int32_t, char**))             \
    X(NewStoreEtcdClient, GoInt (*)(char*, char**))                            \
    X(EtcdStoreResetClientWrapper, GoInt (*)(char*, char**))                   \
    X(NewSnapshotEtcdClient, GoInt (*)(char*, char**))                         \
    X(EtcdStoreGetWrapper,                                                     \
      GoInt (*)(char*, int, char**, int*, GoInt64*, char**))                   \
    X(EtcdStoreGrantLeaseWrapper, GoInt (*)(GoInt64, GoInt64*, char**))        \
    X(EtcdStoreRevokeLeaseWrapper, GoInt (*)(GoInt64, char**))                 \
    X(EtcdStoreCreateWithLeaseWrapper,                                         \
      GoInt (*)(char*, int, char*, int, GoInt64, GoInt64*, char**))            \
    X(EtcdStoreAcquireMaintenanceSessionWrapper,                               \
      GoInt (*)(char*, int, GoInt64, GoInt64*, GoInt64*, GoInt64*, char**))    \
    X(EtcdStoreCloseMaintenanceSessionWrapper, GoInt (*)(GoInt64, char**))     \
    X(EtcdStoreMaintenanceSessionAliveWrapper, GoInt (*)(GoInt64, char**))     \
    X(EtcdStoreWatchUntilDeletedWrapper, GoInt (*)(char*, int, char**))        \
    X(EtcdStoreCancelWatchWrapper, GoInt (*)(char*, int, char**))              \
    X(EtcdStoreKeepAliveWrapper, GoInt (*)(GoInt64, char**))                   \
    X(EtcdStoreCancelKeepAliveWrapper, GoInt (*)(GoInt64, char**))             \
    X(EtcdStoreWaitKeepAliveReadyWrapper, GoInt (*)(GoInt64, GoInt, char**))   \
    X(EtcdStorePutWrapper, GoInt (*)(char*, int, char*, int, char**))          \
    X(EtcdStoreCreateWrapper, GoInt (*)(char*, int, char*, int, char**))       \
    X(EtcdStoreBatchCreateWrapper, GoInt (*)(char**, char**, int, char**))     \
    X(EtcdStoreTxnCompareAndPutWrapper,                                        \
      GoInt (*)(char**, int*, int*, char**, int*, GoInt64*, int, char**, int*, \
                char**, int*, int*, int, char**))                              \
    X(EtcdStoreGetWithPrefixWrapper,                                           \
      GoInt (*)(char*, int, char**, int**, char**, int**, int*, char**))       \
    X(EtcdStoreGetRangeAsJsonWrapper,                                          \
      GoInt (*)(char*, int, char*, int, int, char**, int*, GoInt64*, char**))  \
    X(EtcdStoreGetFirstKeyWithPrefixWrapper,                                   \
      GoInt (*)(char*, int, char**, int*, char**))                             \
    X(EtcdStoreGetLastKeyWithPrefixWrapper,                                    \
      GoInt (*)(char*, int, char**, int*, char**))                             \
    X(EtcdStoreDeleteRangeWrapper, GoInt (*)(char*, int, char*, int, char**))  \
    X(EtcdStoreWatchWithPrefixFromRevisionWrapper,                             \
      GoInt (*)(char*, int, GoInt64, void*, void*, char**))                    \
    X(EtcdStoreWaitWatchWithPrefixStoppedWrapper,                              \
      GoInt (*)(char*, int, int, char**))                                      \
    X(EtcdStoreCancelWatchWithPrefixWrapper, GoInt (*)(char*, int, char**))    \
    X(SnapshotStorePutWrapper, GoInt (*)(char*, int, char*, int, char**))      \
    X(SnapshotStoreGetWrapper,                                                 \
      GoInt (*)(char*, int, char**, int*, GoInt64*, char**))                   \
    X(SnapshotStoreDeleteWrapper, GoInt (*)(char*, int, int, char**))

struct SymbolTable {
#define DECLARE_SYMBOL(name, sig) \
    using name##Fn = sig;         \
    name##Fn name = nullptr;
    MOONCAKE_ETCD_WRAPPER_SYMBOLS(DECLARE_SYMBOL)
#undef DECLARE_SYMBOL
};

struct LoaderState {
    std::mutex mu;
    std::atomic<bool> loaded{false};
    void* handle = nullptr;
    SymbolTable syms;
};

LoaderState& GetState() {
    static LoaderState state;
    return state;
}

void SetError(char** err_msg, const std::string& msg) {
    if (err_msg != nullptr) {
        *err_msg = strdup(msg.c_str());
    }
}

std::string Dirname(const char* path) {
    if (path == nullptr) {
        return {};
    }
    std::string str(path);
    const auto slash = str.find_last_of('/');
    if (slash == std::string::npos) {
        return ".";
    }
    if (slash == 0) {
        return "/";
    }
    return str.substr(0, slash);
}

std::vector<std::string> BuildCandidatePaths() {
    std::vector<std::string> paths;
    Dl_info info{};
    if (dladdr(reinterpret_cast<const void*>(&IsEtcdWrapperLoaded), &info) !=
            0 &&
        info.dli_fname != nullptr) {
        const std::string dir = Dirname(info.dli_fname);
        if (!dir.empty()) {
            paths.push_back(dir + "/" + kLibraryName);
            paths.push_back(dir + "/../lib/" + kLibraryName);
            paths.push_back(dir + "/etcd/" + kLibraryName);
            paths.push_back(dir + "/../mooncake-common/etcd/" + kLibraryName);
            paths.push_back(dir + "/../../mooncake-common/etcd/" +
                            kLibraryName);
            paths.push_back(dir + "/../../../mooncake-common/etcd/" +
                            kLibraryName);
        }
    }
#ifdef MOONCAKE_ETCD_WRAPPER_BUILD_PATH
    paths.emplace_back(MOONCAKE_ETCD_WRAPPER_BUILD_PATH);
#endif
    return paths;
}

void* OpenLibrary(std::string* err) {
    const char* env_path = std::getenv(kEnvLibraryPath);
    if (env_path != nullptr && env_path[0] != '\0') {
        void* handle = dlopen(env_path, RTLD_LAZY | RTLD_LOCAL);
        if (handle == nullptr && err != nullptr) {
            const char* dl_err = dlerror();
            *err = std::string("failed to load ") + env_path + " (from " +
                   kEnvLibraryPath +
                   "): " + (dl_err != nullptr ? dl_err : "unknown error");
        }
        return handle;
    }

    if (void* existing = dlopen(kLibraryName, RTLD_LAZY | RTLD_NOLOAD);
        existing != nullptr) {
        return existing;
    }

    std::string last_open_error;
    for (const auto& candidate : BuildCandidatePaths()) {
        if (access(candidate.c_str(), R_OK) != 0) {
            continue;
        }
        void* handle = dlopen(candidate.c_str(), RTLD_LAZY | RTLD_LOCAL);
        if (handle != nullptr) {
            return handle;
        }
        const char* dl_err = dlerror();
        last_open_error =
            candidate + ": " + (dl_err != nullptr ? dl_err : "unknown error");
    }

    void* handle = dlopen(kLibraryName, RTLD_LAZY | RTLD_LOCAL);
    if (handle != nullptr) {
        return handle;
    }

    if (err != nullptr) {
        const char* dl_err = dlerror();
        *err =
            std::string("failed to load ") + kLibraryName + ": " +
            (dl_err != nullptr ? dl_err
                               : (last_open_error.empty() ? "unknown error"
                                                          : last_open_error));
    }
    return nullptr;
}

template <typename FnPtr>
bool ResolveSymbol(void* handle, const char* name, FnPtr* out,
                   std::string* err) {
    dlerror();
    void* sym = dlsym(handle, name);
    const char* dl_err = dlerror();
    if (sym == nullptr || dl_err != nullptr) {
        if (err != nullptr) {
            *err = std::string("failed to resolve symbol ") + name + " from " +
                   kLibraryName + ": " +
                   (dl_err != nullptr ? dl_err : "null symbol");
        }
        return false;
    }
    *out = reinterpret_cast<FnPtr>(sym);
    return true;
}

bool LoadAllSymbols(void* handle, SymbolTable* syms, std::string* err) {
#define LOAD_SYMBOL(name, sig)                               \
    if (!ResolveSymbol(handle, #name, &(syms->name), err)) { \
        return false;                                        \
    }
    MOONCAKE_ETCD_WRAPPER_SYMBOLS(LOAD_SYMBOL)
#undef LOAD_SYMBOL
    return true;
}

bool EnsureLoaded(std::string* err) {
    LoaderState& state = GetState();
    if (state.loaded.load(std::memory_order_acquire)) {
        return true;
    }

    std::lock_guard<std::mutex> lock(state.mu);
    if (state.loaded.load(std::memory_order_relaxed)) {
        return true;
    }

    // Do not dlclose() a Go c-shared DSO on error or shutdown: the Go runtime
    // registers background threads and signal handlers that cannot be safely
    // unloaded. Keep the handle if dlopen succeeded so retries avoid reloading.
    if (state.handle == nullptr) {
        state.handle = OpenLibrary(err);
        if (state.handle == nullptr) {
            return false;
        }
    }

    SymbolTable resolved;
    if (!LoadAllSymbols(state.handle, &resolved, err)) {
        return false;
    }

    state.syms = resolved;
    state.loaded.store(true, std::memory_order_release);
    return true;
}

template <typename MemberPtr, typename... Args>
GoInt Invoke(char** err_msg, MemberPtr member, Args&&... args) {
    std::string err;
    if (!EnsureLoaded(&err)) {
        SetError(err_msg, err);
        return -1;
    }
    auto fn = GetState().syms.*member;
    return fn(std::forward<Args>(args)..., err_msg);
}

}  // namespace

bool IsEtcdWrapperLoaded() {
    return GetState().loaded.load(std::memory_order_acquire);
}

GoInt NewEtcdClient(char* endpoints, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::NewEtcdClient, endpoints);
}

GoInt EtcdPutWrapper(char* key, char* value, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdPutWrapper, key, value);
}

GoInt EtcdGetWrapper(char* key, char** value, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdGetWrapper, key, value);
}

GoInt EtcdDeleteWrapper(char* key, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdDeleteWrapper, key);
}

void EtcdCloseWrapper() {
    if (!IsEtcdWrapperLoaded()) {
        return;
    }
    GetState().syms.EtcdCloseWrapper();
}

GoInt SetStoreEtcdMaxSendMessageBytes(int32_t maxBytes, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::SetStoreEtcdMaxSendMessageBytes,
                  maxBytes);
}

GoInt NewStoreEtcdClient(char* endpoints, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::NewStoreEtcdClient, endpoints);
}

GoInt EtcdStoreResetClientWrapper(char* endpoints, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreResetClientWrapper, endpoints);
}

GoInt NewSnapshotEtcdClient(char* endpoints, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::NewSnapshotEtcdClient, endpoints);
}

GoInt EtcdStoreGetWrapper(char* key, int keySize, char** value, int* valueSize,
                          GoInt64* revisionId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGetWrapper, key, keySize,
                  value, valueSize, revisionId);
}

GoInt EtcdStoreGrantLeaseWrapper(GoInt64 ttl, GoInt64* leaseId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGrantLeaseWrapper, ttl,
                  leaseId);
}

GoInt EtcdStoreRevokeLeaseWrapper(GoInt64 leaseId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreRevokeLeaseWrapper, leaseId);
}

GoInt EtcdStoreCreateWithLeaseWrapper(char* key, int keySize, char* value,
                                      int valueSize, GoInt64 leaseId,
                                      GoInt64* revisionId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCreateWithLeaseWrapper, key,
                  keySize, value, valueSize, leaseId, revisionId);
}

GoInt EtcdStoreAcquireMaintenanceSessionWrapper(
    char* key, int keySize, GoInt64 ttl, GoInt64* sessionHandle,
    GoInt64* leaseId, GoInt64* createRevision, char** errMsg) {
    return Invoke(errMsg,
                  &SymbolTable::EtcdStoreAcquireMaintenanceSessionWrapper, key,
                  keySize, ttl, sessionHandle, leaseId, createRevision);
}

GoInt EtcdStoreCloseMaintenanceSessionWrapper(GoInt64 sessionHandle,
                                              char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCloseMaintenanceSessionWrapper,
                  sessionHandle);
}

GoInt EtcdStoreMaintenanceSessionAliveWrapper(GoInt64 sessionHandle,
                                              char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreMaintenanceSessionAliveWrapper,
                  sessionHandle);
}

GoInt EtcdStoreWatchUntilDeletedWrapper(char* key, int keySize, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreWatchUntilDeletedWrapper, key,
                  keySize);
}

GoInt EtcdStoreCancelWatchWrapper(char* key, int keySize, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCancelWatchWrapper, key,
                  keySize);
}

GoInt EtcdStoreKeepAliveWrapper(GoInt64 leaseId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreKeepAliveWrapper, leaseId);
}

GoInt EtcdStoreCancelKeepAliveWrapper(GoInt64 leaseId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCancelKeepAliveWrapper,
                  leaseId);
}

GoInt EtcdStoreWaitKeepAliveReadyWrapper(GoInt64 leaseId, GoInt timeoutMs,
                                         char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreWaitKeepAliveReadyWrapper,
                  leaseId, timeoutMs);
}

GoInt EtcdStorePutWrapper(char* key, int keySize, char* value, int valueSize,
                          char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStorePutWrapper, key, keySize,
                  value, valueSize);
}

GoInt EtcdStoreCreateWrapper(char* key, int keySize, char* value, int valueSize,
                             char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCreateWrapper, key, keySize,
                  value, valueSize);
}

GoInt EtcdStoreBatchCreateWrapper(char** keys, char** values, int count,
                                  char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreBatchCreateWrapper, keys,
                  values, count);
}

GoInt EtcdStoreTxnCompareAndPutWrapper(
    char** compareKeys, int* compareKeySizes, int* compareKinds,
    char** compareValues, int* compareValueSizes, GoInt64* compareRevisions,
    int compareCount, char** putKeys, int* putKeySizes, char** putValues,
    int* putValueSizes, int* putPreserveLeases, int putCount, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreTxnCompareAndPutWrapper,
                  compareKeys, compareKeySizes, compareKinds, compareValues,
                  compareValueSizes, compareRevisions, compareCount, putKeys,
                  putKeySizes, putValues, putValueSizes, putPreserveLeases,
                  putCount);
}

GoInt EtcdStoreGetWithPrefixWrapper(char* prefix, int prefixSize, char** keys,
                                    int** keySizes, char** values,
                                    int** valueSizes, int* count,
                                    char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGetWithPrefixWrapper, prefix,
                  prefixSize, keys, keySizes, values, valueSizes, count);
}

GoInt EtcdStoreGetRangeAsJsonWrapper(char* startKey, int startKeySize,
                                     char* endKey, int endKeySize, int limit,
                                     char** outJson, int* outJsonSize,
                                     GoInt64* revisionId, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGetRangeAsJsonWrapper,
                  startKey, startKeySize, endKey, endKeySize, limit, outJson,
                  outJsonSize, revisionId);
}

GoInt EtcdStoreGetFirstKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** firstKey, int* firstKeySize,
                                            char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGetFirstKeyWithPrefixWrapper,
                  prefix, prefixSize, firstKey, firstKeySize);
}

GoInt EtcdStoreGetLastKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                           char** lastKey, int* lastKeySize,
                                           char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreGetLastKeyWithPrefixWrapper,
                  prefix, prefixSize, lastKey, lastKeySize);
}

GoInt EtcdStoreDeleteRangeWrapper(char* startKey, int startKeySize,
                                  char* endKey, int endKeySize, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreDeleteRangeWrapper, startKey,
                  startKeySize, endKey, endKeySize);
}

GoInt EtcdStoreWatchWithPrefixFromRevisionWrapper(char* prefix, int prefixSize,
                                                  GoInt64 startRevision,
                                                  void* callbackContext,
                                                  void* callbackFunc,
                                                  char** errMsg) {
    return Invoke(
        errMsg, &SymbolTable::EtcdStoreWatchWithPrefixFromRevisionWrapper,
        prefix, prefixSize, startRevision, callbackContext, callbackFunc);
}

GoInt EtcdStoreWaitWatchWithPrefixStoppedWrapper(char* prefix, int prefixSize,
                                                 int timeoutMs, char** errMsg) {
    return Invoke(errMsg,
                  &SymbolTable::EtcdStoreWaitWatchWithPrefixStoppedWrapper,
                  prefix, prefixSize, timeoutMs);
}

GoInt EtcdStoreCancelWatchWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** errMsg) {
    return Invoke(errMsg, &SymbolTable::EtcdStoreCancelWatchWithPrefixWrapper,
                  prefix, prefixSize);
}

GoInt SnapshotStorePutWrapper(char* key, int keySize, char* value,
                              int valueSize, char** errMsg) {
    return Invoke(errMsg, &SymbolTable::SnapshotStorePutWrapper, key, keySize,
                  value, valueSize);
}

GoInt SnapshotStoreGetWrapper(char* key, int keySize, char** value,
                              int* valueSize, GoInt64* revisionId,
                              char** errMsg) {
    return Invoke(errMsg, &SymbolTable::SnapshotStoreGetWrapper, key, keySize,
                  value, valueSize, revisionId);
}

GoInt SnapshotStoreDeleteWrapper(char* key, int keySize, int usePrefix,
                                 char** errMsg) {
    return Invoke(errMsg, &SymbolTable::SnapshotStoreDeleteWrapper, key,
                  keySize, usePrefix);
}

}  // namespace mooncake::etcd_wrapper
