#ifndef MOONCAKE_PG_H_
#define MOONCAKE_PG_H_

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__GNUC__)
#define MOONCAKE_PG_EXPORT __attribute__((visibility("default")))
#else
#define MOONCAKE_PG_EXPORT
#endif

#define MOONCAKE_PG_MAX_RANKS 64
#define MOONCAKE_PG_MAX_ERROR_STRING 256
#define MOONCAKE_PG_CONFIG_UNDEF_INT INT_MIN

#define MOONCAKE_PG_COMM_CONFIG_MAGIC 0x20B6DE6B
#define MOONCAKE_PG_COMM_CONFIG_VERSION 1u

typedef struct mooncakePgContext* mooncakePgContext_t;
typedef struct mooncakePgComm* mooncakePgComm_t;
typedef struct mooncakePgCompletion* mooncakePgCompletion_t;
typedef void* mooncakePgStream_t;

/* Keep non-success entries synchronized with C++ PGErrorCode. */
typedef enum mooncakePgResult {
    mooncakePgSuccess = 0,
    mooncakePgInvalidArgument = 1,
    mooncakePgInvalidState = 2,
    mooncakePgNotSupported = 3,
    mooncakePgTimeout = 4,
    mooncakePgResourceBusy = 5,
    mooncakePgTransferEngineError = 6,
    mooncakePgRpcError = 7,
    mooncakePgSystemError = 8,
    mooncakePgInternalError = 9,
} mooncakePgResult_t;

typedef enum mooncakePgDataType {
    mooncakePgInt8 = 0,
    mooncakePgUint8 = 1,
    mooncakePgInt16 = 2,
    mooncakePgUint16 = 3,
    mooncakePgInt32 = 4,
    mooncakePgUint32 = 5,
    mooncakePgInt64 = 6,
    mooncakePgUint64 = 7,
    mooncakePgFloat16 = 8,
    mooncakePgFloat32 = 9,
    mooncakePgFloat64 = 10,
    mooncakePgBfloat16 = 11,
    mooncakePgBool = 12,
    mooncakePgFloat8e4m3fn = 13,
    mooncakePgFloat8e5m2 = 14,
    mooncakePgFloat8e4m3fnuz = 15,
    mooncakePgFloat8e5m2fnuz = 16,
    mooncakePgFloat8e8m0fnu = 17,
} mooncakePgDataType_t;

typedef enum mooncakePgReduceOp {
    mooncakePgSum = 0,
    mooncakePgAvg = 1,
    mooncakePgProduct = 2,
    mooncakePgMin = 3,
    mooncakePgMax = 4,
} mooncakePgReduceOp_t;

typedef enum mooncakePgDeviceType {
    mooncakePgDeviceCpu = 0,
    mooncakePgDeviceGpu = 1,
} mooncakePgDeviceType_t;

typedef enum mooncakePgIdResolvePolicy {
    mooncakePgIdResolveCreateOrAttach = 0,
    mooncakePgIdResolveAttachOrExtend = 1,
} mooncakePgIdResolvePolicy_t;

typedef struct mooncakePgCommConfig {
    size_t structSize;
    unsigned int magic;
    unsigned int version;
    const char* groupId;
    int rank;
    int size;
    int maxGroupSize;
    /* Required InGroupRank-to-GlobalRank mapping with exactly size entries. */
    const int32_t* globalRanks;
    size_t globalRankCount;
    mooncakePgDeviceType_t deviceType;
    int deviceIndex;
    mooncakePgIdResolvePolicy_t idResolvePolicy;
    int autoDeactivateOnFailure;
    int autoSyncOnFailure;
    /* Optional caller-owned mirror of the communicator's active ranks. */
    int32_t* activeRanksMirror;
    size_t activeRanksMirrorCount;
    int activeRanksMirrorIsDevice;
    int activeRanksMirrorDeviceIndex;
} mooncakePgCommConfig_t;

#define MOONCAKE_PG_COMM_CONFIG_INITIALIZER \
    {sizeof(mooncakePgCommConfig_t),        \
     MOONCAKE_PG_COMM_CONFIG_MAGIC,         \
     MOONCAKE_PG_COMM_CONFIG_VERSION,       \
     NULL,                                  \
     MOONCAKE_PG_CONFIG_UNDEF_INT,          \
     MOONCAKE_PG_CONFIG_UNDEF_INT,          \
     MOONCAKE_PG_CONFIG_UNDEF_INT,          \
     NULL,                                  \
     0,                                     \
     mooncakePgDeviceGpu,                   \
     MOONCAKE_PG_CONFIG_UNDEF_INT,          \
     mooncakePgIdResolveCreateOrAttach,     \
     1,                                     \
     1,                                     \
     NULL,                                  \
     0,                                     \
     0,                                     \
     MOONCAKE_PG_CONFIG_UNDEF_INT}

typedef enum mooncakePgProposalStatus {
    mooncakePgProposalRejected = 0,
    mooncakePgProposalApplied = 1,
    mooncakePgProposalAppliedWithDroppedRanks = 2,
} mooncakePgProposalStatus_t;

typedef struct mooncakePgProposalResponse {
    mooncakePgProposalStatus_t status;
    uint64_t newEpoch;
    size_t droppedRankCount;
    int32_t droppedRanks[MOONCAKE_PG_MAX_RANKS];
    char rejectReason[MOONCAKE_PG_MAX_ERROR_STRING];
} mooncakePgProposalResponse_t;

typedef enum mooncakePgSyncAfterFailureStatus {
    mooncakePgSyncReconciled = 0,
    mooncakePgSyncNoPending = 1,
    mooncakePgSyncRejected = 2,
} mooncakePgSyncAfterFailureStatus_t;

typedef struct mooncakePgSyncAfterFailureResponse {
    mooncakePgSyncAfterFailureStatus_t status;
    char rejectReason[MOONCAKE_PG_MAX_ERROR_STRING];
} mooncakePgSyncAfterFailureResponse_t;

MOONCAKE_PG_EXPORT const char* mooncakePgGetErrorString(
    mooncakePgResult_t result);
MOONCAKE_PG_EXPORT const char* mooncakePgGetLastError(void);

MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgContextCreate(mooncakePgContext_t* context);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextInitialize(
    mooncakePgContext_t context, int globalRank, int maxWorldSize);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextLaunchCoordinator(
    mooncakePgContext_t context, char* coordinatorAddressBuf,
    size_t coordinatorAddressBufSize);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextConnectCoordinator(
    mooncakePgContext_t context, const char* coordinatorAddress);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgContextSetHostIp(mooncakePgContext_t context, const char* hostIp);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextSetTransferEngine(
    mooncakePgContext_t context, void* transferEngine);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextSetDeviceFilter(
    mooncakePgContext_t context, const char* const* filters,
    size_t filterCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgContextSetCollectiveTimeout(
    mooncakePgContext_t context, size_t timeoutUs);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgContextSetP2PTimeout(mooncakePgContext_t context, int64_t timeoutUs);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgContextSetFaultReconciliationWindow(mooncakePgContext_t context,
                                              int64_t timeoutUs);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgContextDestroy(mooncakePgContext_t context);

MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommCreate(
    mooncakePgContext_t context, const mooncakePgCommConfig_t* config,
    mooncakePgComm_t* comm);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommDestroy(mooncakePgComm_t comm);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetRank(mooncakePgComm_t comm, int* rank);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetSize(mooncakePgComm_t comm, int* size);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetMaxGroupSize(mooncakePgComm_t comm, int* maxGroupSize);

MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgBroadcastGpu(const void* sendBuffer, void* recvBuffer, size_t count,
                       mooncakePgDataType_t dataType, int root,
                       mooncakePgComm_t comm, mooncakePgStream_t stream,
                       int32_t* failedRanksHint, size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgAllReduceGpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp,
    mooncakePgComm_t comm, mooncakePgStream_t stream, int32_t* failedRanksHint,
    size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgAllGatherGpu(const void* sendBuffer, void* recvBuffer, size_t count,
                       mooncakePgDataType_t dataType, mooncakePgComm_t comm,
                       mooncakePgStream_t stream, int32_t* failedRanksHint,
                       size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgReduceScatterGpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp,
    mooncakePgComm_t comm, mooncakePgStream_t stream, int32_t* failedRanksHint,
    size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgAllToAllGpu(const void* sendBuffer, void* recvBuffer, size_t count,
                      mooncakePgDataType_t dataType, mooncakePgComm_t comm,
                      mooncakePgStream_t stream, int32_t* failedRanksHint,
                      size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgReduceGpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp, int root,
    mooncakePgComm_t comm, mooncakePgStream_t stream, int32_t* failedRanksHint,
    size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgGatherGpu(const void* sendBuffer, void* recvBuffer, size_t count,
                    mooncakePgDataType_t dataType, int root,
                    mooncakePgComm_t comm, mooncakePgStream_t stream,
                    int32_t* failedRanksHint, size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgScatterGpu(const void* sendBuffer, void* recvBuffer, size_t count,
                     mooncakePgDataType_t dataType, int root,
                     mooncakePgComm_t comm, mooncakePgStream_t stream,
                     int32_t* failedRanksHint, size_t failedRanksHintCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgBarrierGpu(mooncakePgComm_t comm, mooncakePgStream_t stream,
                     int32_t* failedRanksHint, size_t failedRanksHintCount);

MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgBroadcastCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, int root, mooncakePgComm_t comm,
    int32_t* failedRanksHint, size_t failedRanksHintCount,
    mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgAllReduceCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp,
    mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgAllGatherCpu(const void* sendBuffer, void* recvBuffer, size_t count,
                       mooncakePgDataType_t dataType, mooncakePgComm_t comm,
                       int32_t* failedRanksHint, size_t failedRanksHintCount,
                       mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgReduceScatterCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp,
    mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgAllToAllCpu(const void* sendBuffer, void* recvBuffer, size_t count,
                      mooncakePgDataType_t dataType, mooncakePgComm_t comm,
                      int32_t* failedRanksHint, size_t failedRanksHintCount,
                      mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgReduceCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, mooncakePgReduceOp_t reduceOp, int root,
    mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgGatherCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, int root, mooncakePgComm_t comm,
    int32_t* failedRanksHint, size_t failedRanksHintCount,
    mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgScatterCpu(
    const void* sendBuffer, void* recvBuffer, size_t count,
    mooncakePgDataType_t dataType, int root, mooncakePgComm_t comm,
    int32_t* failedRanksHint, size_t failedRanksHintCount,
    mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgBarrierCpu(
    mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);

MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgSendGpu(
    const void* sendBuffer, size_t count, mooncakePgDataType_t dataType,
    int peer, mooncakePgComm_t comm, mooncakePgStream_t stream,
    int32_t* failedRanksHint, size_t failedRanksHintCount,
    mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgRecvGpu(
    void* recvBuffer, size_t count, mooncakePgDataType_t dataType, int peer,
    mooncakePgComm_t comm, mooncakePgStream_t stream, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgSendCpu(
    const void* sendBuffer, size_t count, mooncakePgDataType_t dataType,
    int peer, mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgRecvCpu(
    void* recvBuffer, size_t count, mooncakePgDataType_t dataType, int peer,
    mooncakePgComm_t comm, int32_t* failedRanksHint,
    size_t failedRanksHintCount, mooncakePgCompletion_t* completion);

MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCompletionIsCompleted(
    mooncakePgCompletion_t completion, int* completed);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCompletionWait(mooncakePgCompletion_t completion, int64_t timeoutUs);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCompletionDestroy(mooncakePgCompletion_t completion);

MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommGetActiveRanks(
    mooncakePgComm_t comm, int32_t* activeRanks, size_t rankCount);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetPeerState(mooncakePgComm_t comm, const int32_t* ranks,
                           size_t rankCount, int32_t* peerStates);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommActivateRanks(
    mooncakePgComm_t comm, const int32_t* ranks, size_t rankCount,
    mooncakePgProposalResponse_t* response);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommDeactivateRanks(
    mooncakePgComm_t comm, const int32_t* ranks, size_t rankCount,
    mooncakePgProposalResponse_t* response);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommJoin(mooncakePgComm_t comm);
MOONCAKE_PG_EXPORT mooncakePgResult_t mooncakePgCommSyncAfterFailure(
    mooncakePgComm_t comm, mooncakePgSyncAfterFailureResponse_t* response);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetEpoch(mooncakePgComm_t comm, uint64_t* epoch);
MOONCAKE_PG_EXPORT mooncakePgResult_t
mooncakePgCommGetNumSyncedRanks(mooncakePgComm_t comm, int* numSyncedRanks);

#ifdef __cplusplus
}
#endif

#endif  // MOONCAKE_PG_H_
