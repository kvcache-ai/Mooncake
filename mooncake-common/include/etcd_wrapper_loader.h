#pragma once

#include <cstdint>
#include <type_traits>

namespace mooncake::etcd_wrapper {

// Match CGO's generated GoInt / GoInt64 types from `go build
// -buildmode=c-shared` without requiring the generated libetcd_wrapper.h at
// compile time.
using GoInt64 = long long;
using GoInt = std::conditional_t<sizeof(void*) == 8, long long, int>;

bool IsEtcdWrapperLoaded();

GoInt NewEtcdClient(char* endpoints, char** errMsg);
GoInt EtcdPutWrapper(char* key, char* value, char** errMsg);
GoInt EtcdGetWrapper(char* key, char** value, char** errMsg);
GoInt EtcdDeleteWrapper(char* key, char** errMsg);
void EtcdCloseWrapper();

GoInt SetStoreEtcdMaxSendMessageBytes(int32_t maxBytes, char** errMsg);
GoInt NewStoreEtcdClient(char* endpoints, char** errMsg);
GoInt EtcdStoreResetClientWrapper(char* endpoints, char** errMsg);
GoInt NewSnapshotEtcdClient(char* endpoints, char** errMsg);

GoInt EtcdStoreGetWrapper(char* key, int keySize, char** value, int* valueSize,
                          GoInt64* revisionId, char** errMsg);
GoInt EtcdStoreGrantLeaseWrapper(GoInt64 ttl, GoInt64* leaseId, char** errMsg);
GoInt EtcdStoreRevokeLeaseWrapper(GoInt64 leaseId, char** errMsg);
GoInt EtcdStoreCreateWithLeaseWrapper(char* key, int keySize, char* value,
                                      int valueSize, GoInt64 leaseId,
                                      GoInt64* revisionId, char** errMsg);
GoInt EtcdStoreAcquireMaintenanceSessionWrapper(
    char* key, int keySize, GoInt64 ttl, GoInt64* sessionHandle,
    GoInt64* leaseId, GoInt64* createRevision, char** errMsg);
GoInt EtcdStoreCloseMaintenanceSessionWrapper(GoInt64 sessionHandle,
                                              char** errMsg);
GoInt EtcdStoreMaintenanceSessionAliveWrapper(GoInt64 sessionHandle,
                                              char** errMsg);
GoInt EtcdStoreWatchUntilDeletedWrapper(char* key, int keySize, char** errMsg);
GoInt EtcdStoreCancelWatchWrapper(char* key, int keySize, char** errMsg);
GoInt EtcdStoreKeepAliveWrapper(GoInt64 leaseId, char** errMsg);
GoInt EtcdStoreCancelKeepAliveWrapper(GoInt64 leaseId, char** errMsg);
GoInt EtcdStoreWaitKeepAliveReadyWrapper(GoInt64 leaseId, GoInt timeoutMs,
                                         char** errMsg);
GoInt EtcdStorePutWrapper(char* key, int keySize, char* value, int valueSize,
                          char** errMsg);
GoInt EtcdStoreCreateWrapper(char* key, int keySize, char* value, int valueSize,
                             char** errMsg);
GoInt EtcdStoreBatchCreateWrapper(char** keys, char** values, int count,
                                  char** errMsg);
GoInt EtcdStoreTxnCompareAndPutWrapper(
    char** compareKeys, int* compareKeySizes, int* compareKinds,
    char** compareValues, int* compareValueSizes, GoInt64* compareRevisions,
    int compareCount, char** putKeys, int* putKeySizes, char** putValues,
    int* putValueSizes, int* putPreserveLeases, int putCount, char** errMsg);
GoInt EtcdStoreGetWithPrefixWrapper(char* prefix, int prefixSize, char** keys,
                                    int** keySizes, char** values,
                                    int** valueSizes, int* count,
                                    char** errMsg);
GoInt EtcdStoreGetRangeAsJsonWrapper(char* startKey, int startKeySize,
                                     char* endKey, int endKeySize, int limit,
                                     char** outJson, int* outJsonSize,
                                     GoInt64* revisionId, char** errMsg);
GoInt EtcdStoreGetFirstKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** firstKey, int* firstKeySize,
                                            char** errMsg);
GoInt EtcdStoreGetLastKeyWithPrefixWrapper(char* prefix, int prefixSize,
                                           char** lastKey, int* lastKeySize,
                                           char** errMsg);
GoInt EtcdStoreDeleteRangeWrapper(char* startKey, int startKeySize,
                                  char* endKey, int endKeySize, char** errMsg);
GoInt EtcdStoreWatchWithPrefixFromRevisionWrapper(char* prefix, int prefixSize,
                                                  GoInt64 startRevision,
                                                  void* callbackContext,
                                                  void* callbackFunc,
                                                  char** errMsg);
GoInt EtcdStoreWaitWatchWithPrefixStoppedWrapper(char* prefix, int prefixSize,
                                                 int timeoutMs, char** errMsg);
GoInt EtcdStoreCancelWatchWithPrefixWrapper(char* prefix, int prefixSize,
                                            char** errMsg);

GoInt SnapshotStorePutWrapper(char* key, int keySize, char* value,
                              int valueSize, char** errMsg);
GoInt SnapshotStoreGetWrapper(char* key, int keySize, char** value,
                              int* valueSize, GoInt64* revisionId,
                              char** errMsg);
GoInt SnapshotStoreDeleteWrapper(char* key, int keySize, int usePrefix,
                                 char** errMsg);

}  // namespace mooncake::etcd_wrapper
