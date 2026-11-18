#include "etcd_wrapper_loader.h"

#include <cstdlib>
#include <cstring>

using mooncake::etcd_wrapper::GoInt;
using mooncake::etcd_wrapper::GoInt64;

namespace {

int g_close_calls = 0;

}  // namespace

extern "C" {

GoInt NewEtcdClient(char* endpoints, char** errMsg) {
    (void)endpoints;
    if (errMsg != nullptr) {
        *errMsg = nullptr;
    }
    return 0;
}

GoInt EtcdPutWrapper(char* key, char* value, char** errMsg) {
    (void)key;
    (void)value;
    if (errMsg != nullptr) {
        *errMsg = nullptr;
    }
    return 0;
}

GoInt EtcdGetWrapper(char* key, char** value, char** errMsg) {
    (void)key;
    if (value != nullptr) {
        *value = strdup("mock-value");
    }
    if (errMsg != nullptr) {
        *errMsg = nullptr;
    }
    return 0;
}

GoInt EtcdDeleteWrapper(char* key, char** errMsg) {
    (void)key;
    if (errMsg != nullptr) {
        *errMsg = nullptr;
    }
    return 0;
}

void EtcdCloseWrapper() { ++g_close_calls; }

GoInt SetStoreEtcdMaxSendMessageBytes(int32_t maxBytes, char** errMsg) {
    (void)maxBytes;
    (void)errMsg;
    return 0;
}

GoInt NewStoreEtcdClient(char* endpoints, char** errMsg) {
    (void)endpoints;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreResetClientWrapper(char* endpoints, char** errMsg) {
    (void)endpoints;
    (void)errMsg;
    return 0;
}

GoInt NewSnapshotEtcdClient(char* endpoints, char** errMsg) {
    (void)endpoints;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGetWrapper(char* key, int keySize, char** value, int* valueSize,
                          GoInt64* revisionId, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)revisionId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGrantLeaseWrapper(GoInt64 ttl, GoInt64* leaseId, char** errMsg) {
    (void)ttl;
    (void)leaseId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreRevokeLeaseWrapper(GoInt64 leaseId, char** errMsg) {
    (void)leaseId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCreateWithLeaseWrapper(char* key, int keySize, char* value,
                                      int valueSize, GoInt64 leaseId,
                                      GoInt64* revisionId, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)leaseId;
    (void)revisionId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreAcquireMaintenanceSessionWrapper(
    char* key, int keySize, GoInt64 ttl, GoInt64* sessionHandle,
    GoInt64* leaseId, GoInt64* createRevision, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)ttl;
    (void)sessionHandle;
    (void)leaseId;
    (void)createRevision;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCloseMaintenanceSessionWrapper(GoInt64 sessionHandle,
                                              char** errMsg) {
    (void)sessionHandle;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreMaintenanceSessionAliveWrapper(GoInt64 sessionHandle,
                                              char** errMsg) {
    (void)sessionHandle;
    (void)errMsg;
    return 1;
}

GoInt EtcdStoreWatchUntilDeletedWrapper(char* key, int keySize, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCancelWatchWrapper(char* key, int keySize, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreKeepAliveWrapper(GoInt64 leaseId, char** errMsg) {
    (void)leaseId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCancelKeepAliveWrapper(GoInt64 leaseId, char** errMsg) {
    (void)leaseId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreWaitKeepAliveReadyWrapper(GoInt64 leaseId, GoInt timeoutMs,
                                         char** errMsg) {
    (void)errMsg;
    return (leaseId == 42 && timeoutMs == 250) ? 0 : -1;
}

GoInt EtcdStorePutWrapper(char* key, int keySize, char* value, int valueSize,
                          char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCreateWrapper(char* key, int keySize, char* value, int valueSize,
                             char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreBatchCreateWrapper(char** keys, char** values, int count,
                                  char** errMsg) {
    (void)keys;
    (void)values;
    (void)count;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreTxnCompareAndPutWrapper(
    char** compareKeys, int* compareKeySizes, int* compareKinds,
    char** compareValues, int* compareValueSizes, GoInt64* compareRevisions,
    int compareCount, char** putKeys, int* putKeySizes, char** putValues,
    int* putValueSizes, int* putPreserveLeases, int putCount, char** errMsg) {
    (void)compareKeys;
    (void)compareKeySizes;
    (void)compareKinds;
    (void)compareValues;
    (void)compareValueSizes;
    (void)compareRevisions;
    (void)compareCount;
    (void)putKeys;
    (void)putKeySizes;
    (void)putValues;
    (void)putValueSizes;
    (void)putPreserveLeases;
    (void)putCount;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGetWithPrefixWrapper(char* prefix, int prefixSize, char** keys,
                                    int** keySizes, char** values,
                                    int** valueSizes, int* count,
                                    char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)keys;
    (void)keySizes;
    (void)values;
    (void)valueSizes;
    (void)count;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGetRangeAsJsonWrapper(char* startKey, int startKeySize,
                                     char* endKey, int endKeySize, int limit,
                                     char** outJson, int* outJsonSize,
                                     GoInt64* revisionId, char** errMsg) {
    (void)startKey;
    (void)startKeySize;
    (void)endKey;
    (void)endKeySize;
    (void)limit;
    (void)outJson;
    (void)outJsonSize;
    (void)revisionId;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGetFirstKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** firstKey, int* firstKeySize,
                                            char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)firstKey;
    (void)firstKeySize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreGetLastKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                           char** lastKey, int* lastKeySize,
                                           char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)lastKey;
    (void)lastKeySize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreDeleteRangeWrapper(char* startKey, int startKeySize,
                                  char* endKey, int endKeySize, char** errMsg) {
    (void)startKey;
    (void)startKeySize;
    (void)endKey;
    (void)endKeySize;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreWatchWithPrefixFromRevisionWrapper(char* prefix, int prefixSize,
                                                  GoInt64 startRevision,
                                                  void* callbackContext,
                                                  void* callbackFunc,
                                                  char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)startRevision;
    (void)callbackContext;
    (void)callbackFunc;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreWaitWatchWithPrefixStoppedWrapper(char* prefix, int prefixSize,
                                                 int timeoutMs, char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)timeoutMs;
    (void)errMsg;
    return 0;
}

GoInt EtcdStoreCancelWatchWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** errMsg) {
    (void)prefix;
    (void)prefixSize;
    (void)errMsg;
    return 0;
}

GoInt SnapshotStorePutWrapper(char* key, int keySize, char* value,
                              int valueSize, char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)errMsg;
    return 0;
}

GoInt SnapshotStoreGetWrapper(char* key, int keySize, char** value,
                              int* valueSize, GoInt64* revisionId,
                              char** errMsg) {
    (void)key;
    (void)keySize;
    (void)value;
    (void)valueSize;
    (void)revisionId;
    (void)errMsg;
    return 0;
}

GoInt SnapshotStoreDeleteWrapper(char* key, int keySize, int usePrefix,
                                 char** errMsg) {
    (void)key;
    (void)keySize;
    (void)usePrefix;
    (void)errMsg;
    return 0;
}

int MockEtcdCloseCalls() { return g_close_calls; }

}  // extern "C"
