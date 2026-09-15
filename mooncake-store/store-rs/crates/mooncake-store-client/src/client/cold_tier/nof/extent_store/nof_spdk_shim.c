#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/nvmf_spec.h"

struct mc_nof_device {
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_ns *ns;
    struct spdk_nvme_qpair *qpair;
    uint32_t sector_size;
    uint64_t sector_count;
    pthread_mutex_t lock;
    pthread_t admin_thread;
    atomic_bool stop_admin_thread;
    atomic_bool transport_failed;
    bool admin_thread_started;
};

struct mc_nof_connect_ctx { struct spdk_nvme_ctrlr *ctrlr; const char *hostnqn; };
struct mc_nof_io_ctx { bool done; int status; };

static pthread_mutex_t g_env_lock = PTHREAD_MUTEX_INITIALIZER;
static bool g_env_ready = false;
static pthread_mutex_t g_probe_lock = PTHREAD_MUTEX_INITIALIZER;

static int mc_nof_copy_string(char *dst, size_t dst_len, const char *src, const char *field, char *err, size_t err_len);

static void *mc_nof_poll_admin(void *arg) {
    struct mc_nof_device *device = (struct mc_nof_device *)arg;
    const struct timespec interval = { .tv_sec = 0, .tv_nsec = 10 * 1000 * 1000 };
    while (!atomic_load_explicit(&device->stop_admin_thread, memory_order_acquire)) {
        if (spdk_nvme_ctrlr_process_admin_completions(device->ctrlr) < 0) {
            atomic_store_explicit(&device->transport_failed, true, memory_order_release);
            break;
        }
        nanosleep(&interval, NULL);
    }
    return NULL;
}

static void mc_nof_set_error(char *err, size_t err_len, const char *message, int code) {
    if (err != NULL && err_len > 0) { snprintf(err, err_len, "%s: %d", message, code); }
}

static int mc_nof_init_env(int no_huge, char *err, size_t err_len) {
    pthread_mutex_lock(&g_env_lock);
    if (g_env_ready) { pthread_mutex_unlock(&g_env_lock); return 0; }
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.opts_size = sizeof(opts);
    opts.name = "mooncake-nof";
    opts.no_pci = true;
    opts.no_huge = no_huge != 0;
    opts.mem_size = no_huge != 0 ? 512 : 0;
    opts.shm_id = -1;
    int rc = spdk_env_init(&opts);
    if (rc != 0) { mc_nof_set_error(err, err_len, "spdk_env_init failed", rc); pthread_mutex_unlock(&g_env_lock); return rc; }
    g_env_ready = true;
    pthread_mutex_unlock(&g_env_lock);
    return 0;
}

static bool mc_nof_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
    (void)trid;
    struct mc_nof_connect_ctx *ctx = (struct mc_nof_connect_ctx *)cb_ctx;
    opts->keep_alive_timeout_ms = 2000;
    opts->admin_timeout_ms = 2000;
    if (ctx->hostnqn != NULL && ctx->hostnqn[0] != '\0') {
        if (mc_nof_copy_string(opts->hostnqn, sizeof(opts->hostnqn), ctx->hostnqn, "invalid hostnqn", NULL, 0) != 0) {
            return false;
        }
    }
    return true;
}

static void mc_nof_attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
    (void)trid; (void)opts; ((struct mc_nof_connect_ctx *)cb_ctx)->ctrlr = ctrlr;
}

static void mc_nof_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    struct mc_nof_io_ctx *ctx = (struct mc_nof_io_ctx *)arg;
    ctx->status = spdk_nvme_cpl_is_error(completion) ? -EIO : 0;
    ctx->done = true;
}

static int mc_nof_wait(struct mc_nof_device *device, struct mc_nof_io_ctx *ctx) {
    while (!ctx->done) {
        int rc = spdk_nvme_qpair_process_completions(device->qpair, 0);
        if (rc < 0) {
            atomic_store_explicit(&device->transport_failed, true, memory_order_release);
            return rc;
        }
    }
    return ctx->status;
}

static int mc_nof_copy_string(char *dst, size_t dst_len, const char *src, const char *field, char *err, size_t err_len) {
    if (src == NULL || src[0] == '\0') { mc_nof_set_error(err, err_len, field, -EINVAL); return -EINVAL; }
    size_t len = strlen(src);
    if (len >= dst_len) { mc_nof_set_error(err, err_len, field, -ENAMETOOLONG); return -ENAMETOOLONG; }
    memcpy(dst, src, len + 1);
    return 0;
}

struct mc_nof_device *mc_nof_connect(const char *transport, const char *traddr, const char *trsvcid, const char *subnqn, const char *hostnqn, uint32_t nsid, int no_huge, uint64_t *capacity_bytes, uint32_t *sector_size, char *err, size_t err_len) {
    if (nsid == 0) { nsid = 1; }
    if (mc_nof_init_env(no_huge, err, err_len) != 0) { return NULL; }
    struct spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    if (transport == NULL || (strcmp(transport, "TCP") != 0 && strcmp(transport, "RDMA") != 0) ||
        spdk_nvme_transport_id_populate_trstring(&trid, transport) != 0) {
        mc_nof_set_error(err, err_len, "SPDK transport must be TCP or RDMA", -EINVAL);
        return NULL;
    }
    trid.trtype = strcmp(transport, "TCP") == 0 ? SPDK_NVME_TRANSPORT_TCP : SPDK_NVME_TRANSPORT_RDMA;
    trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;
    if (mc_nof_copy_string(trid.traddr, sizeof(trid.traddr), traddr, "invalid traddr", err, err_len) != 0 ||
        mc_nof_copy_string(trid.trsvcid, sizeof(trid.trsvcid), trsvcid, "invalid trsvcid", err, err_len) != 0 ||
        mc_nof_copy_string(trid.subnqn, sizeof(trid.subnqn), subnqn, "invalid subnqn", err, err_len) != 0) { return NULL; }

    struct mc_nof_connect_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.hostnqn = hostnqn;
    pthread_mutex_lock(&g_probe_lock);
    int rc = spdk_nvme_probe(&trid, &ctx, mc_nof_probe_cb, mc_nof_attach_cb, NULL);
    pthread_mutex_unlock(&g_probe_lock);
    if (rc != 0 || ctx.ctrlr == NULL) { mc_nof_set_error(err, err_len, "spdk_nvme_probe failed", rc != 0 ? rc : -ENODEV); return NULL; }

    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctx.ctrlr, nsid);
    if (ns == NULL || !spdk_nvme_ns_is_active(ns)) { spdk_nvme_detach(ctx.ctrlr); mc_nof_set_error(err, err_len, "active namespace not found", -ENODEV); return NULL; }
    struct spdk_nvme_qpair *qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctx.ctrlr, NULL, 0);
    if (qpair == NULL) { spdk_nvme_detach(ctx.ctrlr); mc_nof_set_error(err, err_len, "spdk_nvme_ctrlr_alloc_io_qpair failed", -ENOMEM); return NULL; }

    struct mc_nof_device *device = (struct mc_nof_device *)calloc(1, sizeof(*device));
    if (device == NULL) { spdk_nvme_ctrlr_free_io_qpair(qpair); spdk_nvme_detach(ctx.ctrlr); mc_nof_set_error(err, err_len, "allocating device handle failed", -ENOMEM); return NULL; }
    device->ctrlr = ctx.ctrlr;
    device->ns = ns;
    device->qpair = qpair;
    device->sector_size = spdk_nvme_ns_get_sector_size(ns);
    device->sector_count = spdk_nvme_ns_get_num_sectors(ns);
    rc = pthread_mutex_init(&device->lock, NULL);
    if (rc != 0) {
        spdk_nvme_ctrlr_free_io_qpair(qpair);
        spdk_nvme_detach(ctx.ctrlr);
        free(device);
        mc_nof_set_error(err, err_len, "initializing device lock failed", -rc);
        return NULL;
    }
    atomic_init(&device->stop_admin_thread, false);
    atomic_init(&device->transport_failed, false);
    rc = pthread_create(&device->admin_thread, NULL, mc_nof_poll_admin, device);
    if (rc != 0) {
        pthread_mutex_destroy(&device->lock);
        spdk_nvme_ctrlr_free_io_qpair(qpair);
        spdk_nvme_detach(ctx.ctrlr);
        free(device);
        mc_nof_set_error(err, err_len, "starting SPDK admin poller failed", -rc);
        return NULL;
    }
    device->admin_thread_started = true;
    if (capacity_bytes != NULL) { *capacity_bytes = device->sector_count * (uint64_t)device->sector_size; }
    if (sector_size != NULL) { *sector_size = device->sector_size; }
    return device;
}

int mc_nof_health(struct mc_nof_device *device, char *err, size_t err_len) {
    if (device == NULL) {
        mc_nof_set_error(err, err_len, "invalid SPDK device", -EINVAL);
        return -EINVAL;
    }
    if (atomic_load_explicit(&device->transport_failed, memory_order_acquire) ||
        spdk_nvme_ctrlr_is_failed(device->ctrlr) ||
        spdk_nvme_ctrlr_get_admin_qp_failure_reason(device->ctrlr) != SPDK_NVME_QPAIR_FAILURE_NONE ||
        !spdk_nvme_qpair_is_connected(device->qpair)) {
        mc_nof_set_error(err, err_len, "SPDK controller transport failed", -ENODEV);
        return -ENODEV;
    }
    return 0;
}

void mc_nof_close(struct mc_nof_device *device) {
    if (device == NULL) { return; }
    if (device->admin_thread_started) {
        atomic_store_explicit(&device->stop_admin_thread, true, memory_order_release);
        pthread_join(device->admin_thread, NULL);
    }
    pthread_mutex_lock(&g_probe_lock);
    pthread_mutex_lock(&device->lock);
    if (device->qpair != NULL) { spdk_nvme_ctrlr_free_io_qpair(device->qpair); device->qpair = NULL; }
    if (device->ctrlr != NULL) { spdk_nvme_detach(device->ctrlr); device->ctrlr = NULL; }
    pthread_mutex_unlock(&device->lock);
    pthread_mutex_unlock(&g_probe_lock);
    pthread_mutex_destroy(&device->lock);
    free(device);
}

int mc_nof_write(struct mc_nof_device *device, uint64_t offset, const void *src, uint64_t len, char *err, size_t err_len) {
    if (device == NULL || src == NULL || len == 0 || offset % device->sector_size != 0 || len % device->sector_size != 0) { mc_nof_set_error(err, err_len, "unaligned SPDK write", -EINVAL); return -EINVAL; }
    uint64_t lba = offset / device->sector_size;
    uint64_t lba_count64 = len / device->sector_size;
    if (lba + lba_count64 > device->sector_count || lba_count64 > UINT32_MAX) { mc_nof_set_error(err, err_len, "SPDK write out of range", -ERANGE); return -ERANGE; }
    void *buf = spdk_dma_zmalloc((size_t)len, 4096, NULL);
    if (buf == NULL) { mc_nof_set_error(err, err_len, "SPDK write DMA allocation failed", -ENOMEM); return -ENOMEM; }
    memcpy(buf, src, (size_t)len);
    struct mc_nof_io_ctx io = { .done = false, .status = 0 };
    pthread_mutex_lock(&device->lock);
    int rc = spdk_nvme_ns_cmd_write(device->ns, device->qpair, buf, lba, (uint32_t)lba_count64, mc_nof_complete, &io, 0);
    if (rc == 0) { rc = mc_nof_wait(device, &io); }
    pthread_mutex_unlock(&device->lock);
    spdk_dma_free(buf);
    if (rc != 0) { mc_nof_set_error(err, err_len, "SPDK write failed", rc); }
    return rc;
}

int mc_nof_read(struct mc_nof_device *device, uint64_t offset, void *dst, uint64_t len, char *err, size_t err_len) {
    if (device == NULL || dst == NULL || len == 0 || offset % device->sector_size != 0 || len % device->sector_size != 0) { mc_nof_set_error(err, err_len, "unaligned SPDK read", -EINVAL); return -EINVAL; }
    uint64_t lba = offset / device->sector_size;
    uint64_t lba_count64 = len / device->sector_size;
    if (lba + lba_count64 > device->sector_count || lba_count64 > UINT32_MAX) { mc_nof_set_error(err, err_len, "SPDK read out of range", -ERANGE); return -ERANGE; }
    void *buf = spdk_dma_zmalloc((size_t)len, 4096, NULL);
    if (buf == NULL) { mc_nof_set_error(err, err_len, "SPDK read DMA allocation failed", -ENOMEM); return -ENOMEM; }
    struct mc_nof_io_ctx io = { .done = false, .status = 0 };
    pthread_mutex_lock(&device->lock);
    int rc = spdk_nvme_ns_cmd_read(device->ns, device->qpair, buf, lba, (uint32_t)lba_count64, mc_nof_complete, &io, 0);
    if (rc == 0) { rc = mc_nof_wait(device, &io); }
    pthread_mutex_unlock(&device->lock);
    if (rc == 0) { memcpy(dst, buf, (size_t)len); } else { mc_nof_set_error(err, err_len, "SPDK read failed", rc); }
    spdk_dma_free(buf);
    return rc;
}

int mc_nof_flush(struct mc_nof_device *device, char *err, size_t err_len) {
    if (device == NULL) { mc_nof_set_error(err, err_len, "invalid SPDK device", -EINVAL); return -EINVAL; }
    struct mc_nof_io_ctx io = { .done = false, .status = 0 };
    pthread_mutex_lock(&device->lock);
    int rc = spdk_nvme_ns_cmd_flush(device->ns, device->qpair, mc_nof_complete, &io);
    if (rc == 0) { rc = mc_nof_wait(device, &io); }
    pthread_mutex_unlock(&device->lock);
    if (rc != 0) { mc_nof_set_error(err, err_len, "SPDK flush failed", rc); }
    return rc;
}
