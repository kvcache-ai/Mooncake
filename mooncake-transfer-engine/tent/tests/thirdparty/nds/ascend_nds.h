/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef ASCEND_NDS_H
#define ASCEND_NDS_H

/*
 * Ascend NPU Direct Storage — public C facade (GDS-shaped).
 *
 * This header wraps the statically linked XDS async primitives behind a
 * surface modelled on NVIDIA GPUDirect Storage (cuFile) so that GDS users can
 * migrate with minimal effort. All names are Ascend-flavoured (ndsFile* /
 * NDSfile*_t) to avoid symbol collisions when both libraries coexist.
 *
 * Lifecycle (mirrors cuFile):
 *   ndsFileDriverOpen()          // init driver (parameter-less)
 *   ndsFileRegisterFs(&fs)      // register filesystem topology (user-managed)
 *   ndsFileHandleRegister(&fh, &descr)   // bind an open fd to a handle
 *   ndsFileBufRegister(buf, len, 0)      // pin HBM/CMB VA for P2P
 *   ndsFileBatchIOSetUp(&batch, nr)      // create a batch context
 *   ndsFileBatchIOSubmit(batch, nr, params, NDS_FILE_IO_REGISTERED_MEM)
 *   ndsFileBatchIOGetStatus(batch, min, &got, events, timeout)
 *   ndsFileRead/Write(fh, buf, size, foff, boff)  // sync convenience
 *   ndsFileBatchIODestroy / ndsFileHandleDeregister / ndsFileBufDeregister
 *   ndsFileDeregisterFs(&fs) / ndsFileDriverClose()
 *
 * Conventions (deviate from cuFile only where noted):
 *   - Functions return int: 0 on success, -errno on failure.
 *     ndsFileBatchIOSubmit returns the number of requests accepted.
 *   - ndsFileRead/Write and NDSfileIOEvents_t.ret also return int (0/-errno);
 *     unlike cuFileRead/Write (which return ssize_t bytes transferred), NDS
 *     completions report only success/failure with no byte count, so the number
 *     of bytes transferred is not returned (a 0 implies the full request
 *     completed — NDS per-iov DMA is all-or-nothing).
 *   - I/O is fully async via the batch API; sync read/write are convenience
 *     wrappers (submit + blocking reap). There are NO stream/async APIs.
 *   - Offsets / lengths / buffer addresses must be 512-byte aligned.
 *
 * Error codes use negative Linux errno values.
 *     0              success. For ndsFileRead/Write a 0 means the full
 *                    request completed (no byte count is returned).
 *     -EINVAL  (22)  invalid argument — null pointer, size 0 or > 4 GiB,
 *                    wrong handle type, zero batch capacity, etc.
 *     -ENODEV  (19)  driver not open — call ndsFileDriverOpen() first.
 *     -EBUSY   (16)  driver already open (duplicate ndsFileDriverOpen).
 *                   Driver close also returns this when calls or batches are active.
 *     -EPROTONOSUPPORT  XDS reports an unsupported user-space API version.
 *     -ENOMEM  (12)  out of memory (handle allocation).
 *     -EIO      (5)  I/O failure (including synchronous submission failure).
 *     -ETIMEDOUT (110) synchronous reap timeout — getevents did not return
 *                    the expected count within 30 s (ndsFileRead/Write).
 *     -ENOSYS  (38)  XDS did not return an I/O context on success.
 *     other -errno   passed through from XDS (e.g. -EACCES, -EFAULT,
 *                    -EBADF for permission / bad address / bad fd).
 *
 * Per-completion event: NDSfileIOEvents_t.ret uses the same 0/-errno
 * convention (0 = the request completed, -errno = it failed).
 *
 * Unsupported cuFile APIs (intentionally not declared):
 *   cuFileReadAsync / cuFileWriteAsync, cuFileStreamRegister / Deregister,
 *   cuFileBatchIOCancel, cuFileDriverGetProperties / SetPollMode / SetMax*
 *   (no driver tuning knobs). Define NDS_FILE_NO_ASYNC at build time to detect.
 */

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h> /* off_t */
#include <time.h>      /* struct timespec */

#ifdef __cplusplus
extern "C" {
#endif

/* Compile-time marker: async-stream / tuning APIs are unavailable. */
#define NDS_FILE_NO_ASYNC 1

/* Opaque handle types (mirror cuFile). */
typedef void *NDSfileHandle_t;       /* registered file handle (wraps an fd) */
typedef void *NDSfileBatchHandle_t;  /* batch I/O context */

/* File handle descriptor (mirror CUfileDescr_t). */
enum NDSfileHandleType {
    NDS_FILE_HANDLE_TYPE_OPAQUE_FD = 1, /* Linux fd (the only supported type) */
};

typedef struct NDSfileDescr {
    enum NDSfileHandleType type;
    union {
        int fd; /* Linux open file / block-device fd */
    } handle;
} NDSfileDescr_t;

/*
 * Filesystem topology descriptor for ndsFileRegisterFs(). NDS requires the
 * underlying block-device fds to be registered explicitly (cuFileDriverOpen is
 * parameter-less and auto-detects; NDS exposes topology registration to the
 * user). Each fd must be opened with O_DIRECT by the caller. With the
 * current XDS implementation, the kernel topology remains active until
 * ndsFileDriverClose(); keep the topology fds open until then.
 */
typedef struct NDSfileFsDesc {
    const int *fs_fd;     /* topology fd array (caller-owned, kept open) */
    unsigned int fs_fd_cnt; /* >= 1 */
    unsigned int reserved; /* must be 0 */
} NDSfileFsDesc_t;

/* I/O opcode (mirror CUfileOpcode_t). */
typedef enum {
    NDS_FILE_READ = 0,
    NDS_FILE_WRITE = 1,
} NDSfileOpcode_t;

#define NDS_FILE_MAX_BATCH_IO 1024u
#define NDS_FILE_MAX_IOV 65536u
#define NDS_FILE_MAX_MEM_RANGES 8192u
#define NDS_FILE_IO_REGISTERED_MEM (1u << 0)

/* One segment of a batch request. */
typedef struct NDSfileIOVec {
    void *buf;
    size_t length;       /* transfer length in bytes, <= UINT32_MAX */
} NDSfileIOVec_t;

/* One vector request transfers consecutive file bytes to or from iov_count buffers. */
typedef struct NDSfileIOParams {
    void *bufPtr_bases;   /* device/host memory bases (NDSfileIOVec_t array) */
    off_t file_offset;    /* offset in the target file/device */
    off_t bufPtr_offset;  /* offset into each buffer base */
    size_t iov_count;     /* number of I/O vectors */
    NDSfileHandle_t fh;   /* registered file handle */
    NDSfileOpcode_t opcode;
    void *cookie;        /* echoed in completion events */
} NDSfileIOParams_t;

/* Completion status (mirror CUfileStatus_t). */
typedef enum {
    NDS_FILE_WAITING = 0x000001,
    NDS_FILE_PENDING = 0x000002,
    NDS_FILE_INVALID = 0x000004,
    NDS_FILE_CANCELED = 0x000008,
    NDS_FILE_COMPLETE = 0x000010,
    NDS_FILE_TIMEOUT = 0x000020,
    NDS_FILE_FAILED = 0x000040,
} NDSfileStatus_t;

typedef struct NDSfileIOEvents {
    void *cookie;
    NDSfileStatus_t status; /* NDS_FILE_COMPLETE on success, NDS_FILE_FAILED otherwise */
    int ret;               /* 0 on success (byte count unavailable); -errno on failure */
} NDSfileIOEvents_t;

/* Buffer-register flags (GDS-compatible values; NDS ignores them, masks to 0). */
#define NDS_FILE_RDMA_REGISTER 1
#define NDS_FILE_RDMA_RELAXED_ORDERING (1 << 1)

/*
 * Initialise the NDS driver. Parameter-less (mirrors cuFileDriverOpen): the
 * filesystem topology is registered separately via ndsFileRegisterFs() after
 * the driver is open. Call once, single-threaded. Returns 0/-errno.
 */
int ndsFileDriverOpen(void);

/*
 * Register a filesystem topology after ndsFileDriverOpen(). The descriptor
 * and its fd array need to remain valid for the duration of the call.
 * Returns 0/-errno.
 */
int ndsFileRegisterFs(const NDSfileFsDesc_t *desc);

/*
 * Unregister the topology identified by the supplied fd values. With the
 * current XDS implementation this clears only facade bookkeeping; the kernel
 * topology remains active until ndsFileDriverClose(). Returns 0/-errno.
 */
int ndsFileDeregisterFs(const NDSfileFsDesc_t *desc);

/*
 * Tear down the driver, invalidate file handles and release registered
 * memory and topologies. Finish all NDS operations and destroy all batch
 * contexts first. Returns -EBUSY if a call is active or a batch still
 * exists, -ENODEV if the driver is already closed, or 0 on success.
 */
int ndsFileDriverClose(void);

/* Register an open fd as a file handle. Caller owns the fd (close it yourself
 * after deregister). Returns 0/-errno; *fh set on success. */
int ndsFileHandleRegister(NDSfileHandle_t *fh, const NDSfileDescr_t *descr);

/* Deregister a file handle without closing its fd. Use only a live handle. */
void ndsFileHandleDeregister(NDSfileHandle_t fh);

/* Pin / unpin a VA range for P2P. flags currently ignored (masked to 0).
 * Synchronous I/O auto-detects registered ranges. Batch I/O selects them
 * with NDS_FILE_IO_REGISTERED_MEM. Returns 0/-errno. */
int ndsFileBufRegister(void *buf, size_t length, int flags);

int ndsFileBufDeregister(void *buf);

/* Create / destroy a batch I/O context. nr is a queue-capacity hint.
 * Destroy drains outstanding I/O and consumes the handle even if draining
 * reports an error. Complete concurrent use of this context before destroying
 * it. Returns 0/-errno. */
int ndsFileBatchIOSetUp(NDSfileBatchHandle_t *batch, unsigned nr);

int ndsFileBatchIODestroy(NDSfileBatchHandle_t batch);

/*
 * Submit one I/O control block per vector request. A nonnegative return gives
 * the number of leading entries accepted (possibly less than nr). A negative
 * return gives -errno and no entries were accepted.
 * Reap accepted entries with ndsFileBatchIOGetStatus. The file handle, fd and
 * buffers must remain valid until completion. With NDS_FILE_IO_REGISTERED_MEM,
 * each iov must fit inside one registered range. An IOCB spanning multiple
 * ranges uses one-shot I/O. Pass 0 to use one-shot I/O for all requests.
 */
int ndsFileBatchIOSubmit(NDSfileBatchHandle_t batch, unsigned nr,
                         const NDSfileIOParams_t *params, unsigned flags);

/*
 * Reap completions: wait for at least min_nr, up to *nr (capacity). On return
 * *nr holds the actual event count. timeout NULL = wait forever, {0,0} =
 * non-blocking. Returns 0/-errno (see NDSfileIOEvents_t.ret for per-event).
 */
int ndsFileBatchIOGetStatus(NDSfileBatchHandle_t batch, unsigned min_nr,
                            unsigned *nr, NDSfileIOEvents_t *events,
                            struct timespec *timeout);

/* Synchronous read/write convenience (NDS is async-only internally). Uses an
 * ephemeral context per call. Returns 0 on success, -errno on failure.
 * NDS completion reports only success/failure, not a byte count, so the number
 * of bytes transferred is not returned; a 0 return implies the full request
 * completed (NDS per-iov DMA is all-or-nothing). */
int ndsFileRead(NDSfileHandle_t fh, void *buf, size_t size,
                 off_t file_offset, off_t bufPtr_offset);
int ndsFileWrite(NDSfileHandle_t fh, const void *buf, size_t size,
                 off_t file_offset, off_t bufPtr_offset);

/* Query the NDS API version (NDS_API_VERSION). Usable before driver open. */
int ndsFileGetVersion(int *version);

#ifdef __cplusplus
}
#endif
#endif /* ASCEND_NDS_H */
