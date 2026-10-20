#ifndef MEMFABRIC_TYPES_H_
#define MEMFABRIC_TYPES_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SMEM_TLS_PATH_SIZE 256

typedef enum {
    SM_OK = 0,
    SM_ERROR = -1,
    SM_INVALID_PARAM = -2000,
    SM_MALLOC_FAILED = -2001,
    SM_NEW_OBJECT_FAILED = -2002,
    SM_NOT_STARTED = -2003,
    SM_TIMEOUT = -2004,
    SM_REPEAT_CALL = -2005,
    SM_DUPLICATED_OBJECT = -2006,
    SM_OBJECT_NOT_EXISTS = -2007,
    SM_NOT_INITIALIZED = -2008,
    SM_RESOURCE_IN_USE = -2009,
    SM_RECONNECT = -2010,
    SM_GET_OBJIECT = -2011,
    SM_PARTIAL_FAILED = -2012,
    SM_INNER_BUSY = -2013,
    SM_NOT_CONNECTED = -2014,
    SM_NEED_RETRY = -2015,
} SMErrorCode;

typedef enum {
    SMEMB_DATA_OP_SDMA = 1U << 0,
    SMEMB_DATA_OP_HOST_RDMA = 1U << 1,
    SMEMB_DATA_OP_HOST_TCP = 1U << 2,
    SMEMB_DATA_OP_DEVICE_RDMA = 1U << 3,
    SMEMB_DATA_OP_HOST_URMA = 1U << 4,
    SMEMB_DATA_OP_HOST_SHM = 1U << 5,
    SMEMB_DATA_OP_DEVICE_URMA = 1U << 6,
    SMEMB_DATA_OP_DEVICE_UBOE = 1U << 7,
    SMEMB_DATA_OP_BUTT
} smem_bm_data_op_type;

typedef struct {
    bool tlsEnable;
    char caPath[SMEM_TLS_PATH_SIZE];
    char crlPath[SMEM_TLS_PATH_SIZE];
    char certPath[SMEM_TLS_PATH_SIZE];
    char keyPath[SMEM_TLS_PATH_SIZE];
    char keyPassPath[SMEM_TLS_PATH_SIZE];
    char packagePath[SMEM_TLS_PATH_SIZE];
    char decrypterLibPath[SMEM_TLS_PATH_SIZE];
} smem_tls_config;

typedef void *smem_trans_t;

typedef enum {
    SMEM_TRANS_NONE = 0,
    SMEM_TRANS_SENDER,
    SMEM_TRANS_RECEIVER,
    SMEM_TRANS_BOTH,
    SMEM_TRANS_BUTT
} smem_trans_role_t;

typedef struct {
    smem_trans_role_t role; /* transfer role */
    uint32_t initTimeout;   /* func timeout, default 120 seconds */
    uint32_t deviceId;      /* npu device id */
    uint32_t flags;         /* optional flags */
    smem_bm_data_op_type
        dataOpType; /* data operation type; see smem_bm_data_op_type */
    char nic[64];   /* host NIC addr for host_rdma, format: tcp://ip:port */
    char url[64];   /* unique id of this instance, format: ip:port, port 0 means
                       auto detection */
    smem_tls_config hcomTlsConfig; /* hcom TLS config for host_rdma */
} smem_trans_config_t;

typedef enum {
    SMEM_TRANS_OP_READ = 0,
    SMEM_TRANS_OP_WRITE = 1,
} smem_trans_opcode_t;

typedef struct {
    const char *destUrl;
    void **srcList;
    void **destList;
    size_t *sizeList;
    uint32_t batchSize;
    smem_trans_opcode_t opcode;
    void *stream;
    uint32_t flags;
} smem_trans_batch_params_t;

#ifdef __cplusplus
}
#endif

#endif
