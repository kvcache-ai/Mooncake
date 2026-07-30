/* SPDX-License-Identifier: MIT */
/*
 * Description: uring_cmd based ublk
 *
 * Covers cancellable uring_cmd feature.
 */
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>
#include <poll.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/inotify.h>
#include <sys/wait.h>

#include "liburing.h"
#include "helpers.h"
#ifdef CONFIG_HAVE_UBLK_HEADER
#include <linux/ublk_cmd.h>

/****************** part 1: libublk ********************/

#define CTRL_DEV		"/dev/ublk-control"
#define UBLKC_DEV		"/dev/ublkc"
#define UBLKB_DEV		"/dev/ublkb"
#define UBLK_CTRL_RING_DEPTH            32

/* queue idle timeout */
#define UBLKSRV_IO_IDLE_SECS		20

#define UBLK_IO_MAX_BYTES               65536
#define UBLK_MAX_QUEUES                 4
#define UBLK_QUEUE_DEPTH                128

#define UBLK_DBG_DEV            (1U << 0)
#define UBLK_DBG_QUEUE          (1U << 1)
#define UBLK_DBG_IO_CMD         (1U << 2)
#define UBLK_DBG_IO             (1U << 3)
#define UBLK_DBG_CTRL_CMD       (1U << 4)
#define UBLK_LOG                (1U << 5)

struct ublk_dev;
struct ublk_queue;

struct ublk_ctrl_cmd_data {
	__u32 cmd_op;
#define CTRL_CMD_HAS_DATA	1
#define CTRL_CMD_HAS_BUF	2
	__u32 flags;

	__u64 data[2];
	__u64 addr;
	__u32 len;
};

struct ublk_io {
	char *buf_addr;

#define UBLKSRV_NEED_FETCH_RQ		(1UL << 0)
#define UBLKSRV_NEED_COMMIT_RQ_COMP	(1UL << 1)
#define UBLKSRV_IO_FREE			(1UL << 2)
	unsigned int flags;

	unsigned int result;
};

struct ublk_tgt_ops {
	const char *name;
	int (*init_tgt)(struct ublk_dev *);
	void (*deinit_tgt)(struct ublk_dev *);

	int (*queue_io)(struct ublk_queue *, int tag);
	void (*tgt_io_done)(struct ublk_queue *,
			int tag, const struct io_uring_cqe *);
};

struct ublk_tgt {
	unsigned long dev_size;
	const struct ublk_tgt_ops *ops;
	struct ublk_params params;
};

struct ublk_queue {
	int q_id;
	int q_depth;
	unsigned int cmd_inflight;
	unsigned int io_inflight;
	struct ublk_dev *dev;
	const struct ublk_tgt_ops *tgt_ops;
	char *io_cmd_buf;
	struct io_uring ring;
	struct ublk_io ios[UBLK_QUEUE_DEPTH];
#define UBLKSRV_QUEUE_STOPPING	(1U << 0)
#define UBLKSRV_QUEUE_IDLE	(1U << 1)
	unsigned state;
	pid_t tid;
	pthread_t thread;
};

struct ublk_dev {
	struct ublk_tgt tgt;
	struct ublksrv_ctrl_dev_info  dev_info;
	struct ublk_queue q[UBLK_MAX_QUEUES];

	int fds[2];	/* fds[0] points to /dev/ublkcN */
	int nr_fds;
	int ctrl_fd;
	struct io_uring ring;
};

#ifndef offsetof
#define offsetof(TYPE, MEMBER)  ((size_t)&((TYPE *)0)->MEMBER)
#endif

#ifndef container_of
#define container_of(ptr, type, member) ({                              \
	unsigned long __mptr = (unsigned long)(ptr);                    \
	((type *)(__mptr - offsetof(type, member))); })
#endif

#define round_up(val, rnd) \
	(((val) + ((rnd) - 1)) & ~((rnd) - 1))

static unsigned int ublk_dbg_mask = 0;

static const struct ublk_tgt_ops *ublk_find_tgt(const char *name);

static inline int is_target_io(__u64 user_data)
{
	return (user_data & (1ULL << 63)) != 0;
}

static inline __u64 build_user_data(unsigned tag, unsigned op,
		unsigned tgt_data, unsigned is_target_io)
{
	assert(!(tag >> 16) && !(op >> 8) && !(tgt_data >> 16));

	return tag | (op << 16) | (tgt_data << 24) | (__u64)is_target_io << 63;
}

static inline unsigned int user_data_to_tag(__u64 user_data)
{
	return user_data & 0xffff;
}

static inline unsigned int user_data_to_op(__u64 user_data)
{
	return (user_data >> 16) & 0xff;
}

static void ublk_err(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
}

static void ublk_dbg(int level, const char *fmt, ...)
{
	if (level & ublk_dbg_mask) {
		va_list ap;
		va_start(ap, fmt);
		vfprintf(stdout, fmt, ap);
        }
}

static inline void *ublk_get_sqe_cmd(const struct io_uring_sqe *sqe)
{
	return (void *)&sqe->cmd;
}

static inline void ublk_mark_io_done(struct ublk_io *io, int res)
{
	io->flags |= (UBLKSRV_NEED_COMMIT_RQ_COMP | UBLKSRV_IO_FREE);
	io->result = res;
}

static inline const struct ublksrv_io_desc *ublk_get_iod(
                const struct ublk_queue *q, int tag)
{
        return (struct ublksrv_io_desc *)
                &(q->io_cmd_buf[tag * sizeof(struct ublksrv_io_desc)]);
}

static inline void ublk_set_sqe_cmd_op(struct io_uring_sqe *sqe,
		__u32 cmd_op)
{
        __u32 *addr = (__u32 *)&sqe->off;

        addr[0] = cmd_op;
        addr[1] = 0;
}

static inline int ublk_setup_ring(struct io_uring *r, int depth,
		int cq_depth, unsigned flags)
{
	struct io_uring_params p;

	memset(&p, 0, sizeof(p));
	p.flags = flags | IORING_SETUP_CQSIZE;
	p.cq_entries = cq_depth;

	return io_uring_queue_init_params(depth, r, &p);
}

static void ublk_ctrl_init_cmd(struct ublk_dev *dev,
		struct io_uring_sqe *sqe,
		struct ublk_ctrl_cmd_data *data)
{
	struct ublksrv_ctrl_dev_info *info = &dev->dev_info;
	struct ublksrv_ctrl_cmd *cmd = (struct ublksrv_ctrl_cmd *)ublk_get_sqe_cmd(sqe);

	sqe->fd = dev->ctrl_fd;
	sqe->opcode = IORING_OP_URING_CMD;
	sqe->ioprio = 0;

	if (data->flags & CTRL_CMD_HAS_BUF) {
		cmd->addr = data->addr;
		cmd->len = data->len;
	}

	if (data->flags & CTRL_CMD_HAS_DATA)
		cmd->data[0] = data->data[0];

	cmd->dev_id = info->dev_id;
	cmd->queue_id = -1;

	ublk_set_sqe_cmd_op(sqe, data->cmd_op);

	io_uring_sqe_set_data(sqe, cmd);
}

static int __ublk_ctrl_cmd(struct ublk_dev *dev,
		struct ublk_ctrl_cmd_data *data)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret = -EINVAL;

	sqe = io_uring_get_sqe(&dev->ring);
	if (!sqe) {
		ublk_err("%s: can't get sqe ret %d\n", __func__, ret);
		return ret;
	}

	ublk_ctrl_init_cmd(dev, sqe, data);

	ret = io_uring_submit(&dev->ring);
	if (ret < 0) {
		ublk_err("uring submit ret %d\n", ret);
		return ret;
	}

	ret = io_uring_wait_cqe(&dev->ring, &cqe);
	if (ret < 0) {
		ublk_err("wait cqe: %s\n", strerror(-ret));
		return ret;
	}
	io_uring_cqe_seen(&dev->ring, cqe);

	return cqe->res;
}

static int ublk_ctrl_start_dev(struct ublk_dev *dev,
		int daemon_pid)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op	= UBLK_U_CMD_START_DEV,
		.flags	= CTRL_CMD_HAS_DATA,
	};

	dev->dev_info.ublksrv_pid = data.data[0] = daemon_pid;

	return __ublk_ctrl_cmd(dev, &data);
}

static int ublk_ctrl_add_dev(struct ublk_dev *dev)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op	= UBLK_U_CMD_ADD_DEV,
		.flags	= CTRL_CMD_HAS_BUF,
		.addr = (__u64) (uintptr_t) &dev->dev_info,
		.len = sizeof(struct ublksrv_ctrl_dev_info),
	};

	return __ublk_ctrl_cmd(dev, &data);
}

static int ublk_ctrl_del_dev(struct ublk_dev *dev)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op = UBLK_U_CMD_DEL_DEV,
		.flags = 0,
	};

	return __ublk_ctrl_cmd(dev, &data);
}

static int ublk_ctrl_get_info(struct ublk_dev *dev)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op	= UBLK_U_CMD_GET_DEV_INFO,
		.flags	= CTRL_CMD_HAS_BUF,
		.addr = (__u64) (uintptr_t) &dev->dev_info,
		.len = sizeof(struct ublksrv_ctrl_dev_info),
	};

	return __ublk_ctrl_cmd(dev, &data);
}

static int ublk_ctrl_set_params(struct ublk_dev *dev,
		struct ublk_params *params)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op	= UBLK_U_CMD_SET_PARAMS,
		.flags	= CTRL_CMD_HAS_BUF,
		.addr = (__u64) (uintptr_t) params,
		.len = sizeof(*params),
	};
	params->len = sizeof(*params);
	return __ublk_ctrl_cmd(dev, &data);
}

static int ublk_ctrl_get_features(struct ublk_dev *dev,
		__u64 *features)
{
	struct ublk_ctrl_cmd_data data = {
		.cmd_op	= UBLK_U_CMD_GET_FEATURES,
		.flags	= CTRL_CMD_HAS_BUF,
		.addr = (__u64) (uintptr_t) features,
		.len = sizeof(*features),
	};

	return __ublk_ctrl_cmd(dev, &data);
}

static void ublk_ctrl_deinit(struct ublk_dev *dev)
{
	close(dev->ctrl_fd);
	free(dev);
}

static struct ublk_dev *ublk_ctrl_init(void)
{
	struct ublk_dev *dev = (struct ublk_dev *)calloc(1, sizeof(*dev));
	struct ublksrv_ctrl_dev_info *info = &dev->dev_info;
	int ret;

	dev->ctrl_fd = open(CTRL_DEV, O_RDWR);
	if (dev->ctrl_fd < 0) {
		free(dev);
		return NULL;
	}

	info->max_io_buf_bytes = UBLK_IO_MAX_BYTES;

	ret = ublk_setup_ring(&dev->ring, UBLK_CTRL_RING_DEPTH,
			UBLK_CTRL_RING_DEPTH, IORING_SETUP_SQE128);
	if (ret < 0) {
		ublk_err("queue_init: %s\n", strerror(-ret));
		free(dev);
		return NULL;
	}
	dev->nr_fds = 1;

	return dev;
}

static int ublk_queue_cmd_buf_sz(struct ublk_queue *q)
{
	int size =  q->q_depth * sizeof(struct ublksrv_io_desc);
	unsigned int page_sz = getpagesize();

	return round_up(size, page_sz);
}

static void ublk_queue_deinit(struct ublk_queue *q)
{
	int i;
	int nr_ios = q->q_depth;

	io_uring_unregister_ring_fd(&q->ring);

	if (q->ring.ring_fd > 0) {
		io_uring_unregister_files(&q->ring);
		close(q->ring.ring_fd);
		q->ring.ring_fd = -1;
	}

	if (q->io_cmd_buf)
		munmap(q->io_cmd_buf, ublk_queue_cmd_buf_sz(q));

	for (i = 0; i < nr_ios; i++)
		free(q->ios[i].buf_addr);
}

static int ublk_queue_init(struct ublk_queue *q)
{
	struct ublk_dev *dev = q->dev;
	int depth = dev->dev_info.queue_depth;
	int i, ret = -1;
	int cmd_buf_size, io_buf_size;
	unsigned long off;
	int ring_depth = depth, cq_depth = depth;

	q->tgt_ops = dev->tgt.ops;
	q->state = 0;
	q->q_depth = depth;
	q->cmd_inflight = 0;
	q->tid = gettid();

	cmd_buf_size = ublk_queue_cmd_buf_sz(q);
	off = UBLKSRV_CMD_BUF_OFFSET +
		q->q_id * (UBLK_MAX_QUEUE_DEPTH * sizeof(struct ublksrv_io_desc));
	q->io_cmd_buf = (char *)mmap(0, cmd_buf_size, PROT_READ,
			MAP_SHARED | MAP_POPULATE, dev->fds[0], off);
	if (q->io_cmd_buf == MAP_FAILED) {
		ublk_err("ublk dev %d queue %d map io_cmd_buf failed %m\n",
				q->dev->dev_info.dev_id, q->q_id);
		goto fail;
	}

	io_buf_size = dev->dev_info.max_io_buf_bytes;
	for (i = 0; i < q->q_depth; i++) {
		q->ios[i].buf_addr = NULL;

		if (posix_memalign((void **)&q->ios[i].buf_addr,
					getpagesize(), io_buf_size)) {
			ublk_err("ublk dev %d queue %d io %d posix_memalign failed %m\n",
					dev->dev_info.dev_id, q->q_id, i);
			goto fail;
		}
		q->ios[i].flags = UBLKSRV_NEED_FETCH_RQ | UBLKSRV_IO_FREE;
	}

	ret = ublk_setup_ring(&q->ring, ring_depth, cq_depth,
			IORING_SETUP_COOP_TASKRUN);
	if (ret < 0) {
		ublk_err("ublk dev %d queue %d setup io_uring failed %d\n",
				q->dev->dev_info.dev_id, q->q_id, ret);
		goto fail;
	}

	io_uring_register_ring_fd(&q->ring);

	ret = io_uring_register_files(&q->ring, dev->fds, dev->nr_fds);
	if (ret) {
		ublk_err("ublk dev %d queue %d register files failed %d\n",
				q->dev->dev_info.dev_id, q->q_id, ret);
		goto fail;
	}

	return 0;
 fail:
	ublk_queue_deinit(q);
	ublk_err("ublk dev %d queue %d failed\n",
			dev->dev_info.dev_id, q->q_id);
	return -ENOMEM;
}

static int ublk_dev_prep(struct ublk_dev *dev)
{
	int dev_id = dev->dev_info.dev_id;
	char buf[64];
	int ret = 0;

	snprintf(buf, 64, "%s%d", UBLKC_DEV, dev_id);
	dev->fds[0] = open(buf, O_RDWR);
	if (dev->fds[0] < 0) {
		ret = -EBADF;
		ublk_err("can't open %s, ret %d\n", buf, dev->fds[0]);
		goto fail;
	}

	if (dev->tgt.ops->init_tgt)
		ret = dev->tgt.ops->init_tgt(dev);

	return ret;
fail:
	close(dev->fds[0]);
	return ret;
}

static void ublk_dev_unprep(struct ublk_dev *dev)
{
	if (dev->tgt.ops->deinit_tgt)
		dev->tgt.ops->deinit_tgt(dev);
	close(dev->fds[0]);
}

static int ublk_queue_io_cmd(struct ublk_queue *q,
		struct ublk_io *io, unsigned tag)
{
	struct ublksrv_io_cmd *cmd;
	struct io_uring_sqe *sqe;
	unsigned int cmd_op = 0;
	__u64 user_data;

	/* only freed io can be issued */
	if (!(io->flags & UBLKSRV_IO_FREE))
		return 0;

	/* we issue because we need either fetching or committing */
	if (!(io->flags &
		(UBLKSRV_NEED_FETCH_RQ | UBLKSRV_NEED_COMMIT_RQ_COMP)))
		return 0;

	if (io->flags & UBLKSRV_NEED_COMMIT_RQ_COMP)
		cmd_op = UBLK_U_IO_COMMIT_AND_FETCH_REQ;
	else if (io->flags & UBLKSRV_NEED_FETCH_RQ)
		cmd_op = UBLK_U_IO_FETCH_REQ;

	sqe = io_uring_get_sqe(&q->ring);
	if (!sqe) {
		ublk_err("%s: run out of sqe %d, tag %d\n",
				__func__, q->q_id, tag);
		return -1;
	}

	cmd = (struct ublksrv_io_cmd *)ublk_get_sqe_cmd(sqe);

	if (cmd_op == UBLK_U_IO_COMMIT_AND_FETCH_REQ)
		cmd->result = io->result;

	/* These fields should be written once, never change */
	ublk_set_sqe_cmd_op(sqe, cmd_op);
	sqe->fd		= 0;	/* dev->fds[0] */
	sqe->opcode	= IORING_OP_URING_CMD;
	sqe->flags	= IOSQE_FIXED_FILE;
	sqe->rw_flags	= 0;
	cmd->tag	= tag;
	cmd->addr	= (__u64) (uintptr_t) io->buf_addr;
	cmd->q_id	= q->q_id;

	user_data = build_user_data(tag, _IOC_NR(cmd_op), 0, 0);
	io_uring_sqe_set_data64(sqe, user_data);

	io->flags = 0;

	q->cmd_inflight += 1;

	ublk_dbg(UBLK_DBG_IO_CMD, "%s: (qid %d tag %u cmd_op %u) iof %x stopping %d\n",
			__func__, q->q_id, tag, cmd_op,
			io->flags, !!(q->state & UBLKSRV_QUEUE_STOPPING));
	return 1;
}

static int ublk_complete_io(struct ublk_queue *q,
		unsigned tag, int res)
{
	struct ublk_io *io = &q->ios[tag];

	ublk_mark_io_done(io, res);

	return ublk_queue_io_cmd(q, io, tag);
}

static void ublk_submit_fetch_commands(struct ublk_queue *q)
{
	int i = 0;

	for (i = 0; i < q->q_depth; i++)
		ublk_queue_io_cmd(q, &q->ios[i], i);
}

static int ublk_queue_is_idle(struct ublk_queue *q)
{
	return !io_uring_sq_ready(&q->ring) && !q->io_inflight;
}

static int ublk_queue_is_done(struct ublk_queue *q)
{
	return (q->state & UBLKSRV_QUEUE_STOPPING) && ublk_queue_is_idle(q);
}

static inline void ublksrv_handle_tgt_cqe(struct ublk_queue *q,
		struct io_uring_cqe *cqe)
{
	unsigned tag = user_data_to_tag(cqe->user_data);

	if (cqe->res < 0 && cqe->res != -EAGAIN)
		ublk_err("%s: failed tgt io: res %d qid %u tag %u, cmd_op %u\n",
			__func__, cqe->res, q->q_id,
			user_data_to_tag(cqe->user_data),
			user_data_to_op(cqe->user_data));

	if (q->tgt_ops->tgt_io_done)
		q->tgt_ops->tgt_io_done(q, tag, cqe);
}

static void ublk_handle_cqe(struct io_uring *r,
		struct io_uring_cqe *cqe, void *data)
{
	struct ublk_queue *q = container_of(r, struct ublk_queue, ring);
	unsigned tag = user_data_to_tag(cqe->user_data);
	unsigned cmd_op = user_data_to_op(cqe->user_data);
	int fetch = (cqe->res != UBLK_IO_RES_ABORT) &&
		!(q->state & UBLKSRV_QUEUE_STOPPING);
	struct ublk_io *io;

	ublk_dbg(UBLK_DBG_IO_CMD, "%s: res %d (qid %d tag %u cmd_op %u target %d) stopping %d\n",
			__func__, cqe->res, q->q_id, tag, cmd_op,
			is_target_io(cqe->user_data),
			(q->state & UBLKSRV_QUEUE_STOPPING));

	/* Don't retrieve io in case of target io */
	if (is_target_io(cqe->user_data)) {
		ublksrv_handle_tgt_cqe(q, cqe);
		return;
	}

	io = &q->ios[tag];
	q->cmd_inflight--;

	if (!fetch) {
		q->state |= UBLKSRV_QUEUE_STOPPING;
		io->flags &= ~UBLKSRV_NEED_FETCH_RQ;
	}

	if (cqe->res == UBLK_IO_RES_OK) {
		assert(tag < q->q_depth);
		q->tgt_ops->queue_io(q, tag);
	} else {
		/*
		 * COMMIT_REQ will be completed immediately since no fetching
		 * piggyback is required.
		 *
		 * Marking IO_FREE only, then this io won't be issued since
		 * we only issue io with (UBLKSRV_IO_FREE | UBLKSRV_NEED_*)
		 *
		 * */
		io->flags = UBLKSRV_IO_FREE;
	}
}

static int ublk_reap_events_uring(struct io_uring *r)
{
	struct io_uring_cqe *cqe;
	unsigned head;
	int count = 0;

	io_uring_for_each_cqe(r, head, cqe) {
		ublk_handle_cqe(r, cqe, NULL);
		count += 1;
	}
	io_uring_cq_advance(r, count);

	return count;
}

static int ublk_process_io(struct ublk_queue *q)
{
	int ret, reapped;

	ublk_dbg(UBLK_DBG_QUEUE, "dev%d-q%d: to_submit %d inflight cmd %u stopping %d\n",
				q->dev->dev_info.dev_id,
				q->q_id, io_uring_sq_ready(&q->ring),
				q->cmd_inflight,
				(q->state & UBLKSRV_QUEUE_STOPPING));

	if (ublk_queue_is_done(q))
		return -ENODEV;

	ret = io_uring_submit_and_wait(&q->ring, 1);
	reapped = ublk_reap_events_uring(&q->ring);

	ublk_dbg(UBLK_DBG_QUEUE, "submit result %d, reapped %d stop %d idle %d\n",
			ret, reapped, (q->state & UBLKSRV_QUEUE_STOPPING),
			(q->state & UBLKSRV_QUEUE_IDLE));

	return reapped;
}

static void *ublk_io_handler_fn(void *data)
{
	struct ublk_queue *q = data;
	int dev_id = q->dev->dev_info.dev_id;
	int ret;

	ret = ublk_queue_init(q);
	if (ret) {
		ublk_err("ublk dev %d queue %d init queue failed\n",
				dev_id, q->q_id);
		return NULL;
	}
	ublk_dbg(UBLK_DBG_QUEUE, "tid %d: ublk dev %d queue %d started\n",
			q->tid, dev_id, q->q_id);

	/* submit all io commands to ublk driver */
	ublk_submit_fetch_commands(q);
	do {
		if (ublk_process_io(q) < 0)
			break;
	} while (1);

	ublk_dbg(UBLK_DBG_QUEUE, "ublk dev %d queue %d exited\n", dev_id, q->q_id);
	ublk_queue_deinit(q);
	return NULL;
}

static void ublk_set_parameters(struct ublk_dev *dev)
{
	int ret;

	ret = ublk_ctrl_set_params(dev, &dev->tgt.params);
	if (ret)
		ublk_err("dev %d set basic parameter failed %d\n",
				dev->dev_info.dev_id, ret);
}

static int ublk_start_daemon(struct ublk_dev *dev)
{
	int ret, i;
	void *thread_ret;
	const struct ublksrv_ctrl_dev_info *dinfo = &dev->dev_info;

	if (daemon(1, 1) < 0)
		return -errno;

	ublk_dbg(UBLK_DBG_DEV, "%s enter\n", __func__);

	ret = ublk_dev_prep(dev);
	if (ret)
		return ret;

	for (i = 0; i < dinfo->nr_hw_queues; i++) {
		dev->q[i].dev = dev;
		dev->q[i].q_id = i;
		pthread_create(&dev->q[i].thread, NULL,
				ublk_io_handler_fn,
				&dev->q[i]);
	}

	/* everything is fine now, start us */
	ublk_set_parameters(dev);
	ret = ublk_ctrl_start_dev(dev, getpid());
	if (ret < 0) {
		ublk_err("%s: ublk_ctrl_start_dev failed: %d\n", __func__, ret);
		goto fail;
	}

	/* wait until we are terminated */
	for (i = 0; i < dinfo->nr_hw_queues; i++)
		pthread_join(dev->q[i].thread, &thread_ret);
 fail:
	ublk_dev_unprep(dev);
	ublk_dbg(UBLK_DBG_DEV, "%s exit\n", __func__);

	return ret;
}

static int wait_ublk_dev(char *dev_name, int evt_mask, unsigned timeout)
{
#define EV_SIZE (sizeof(struct inotify_event))
#define EV_BUF_LEN (128 * (EV_SIZE + 16))
	struct pollfd pfd;
	int fd, wd;
	int ret = -EINVAL;

	fd = inotify_init();
	if (fd < 0) {
		ublk_dbg(UBLK_DBG_DEV, "%s: inotify init failed\n", __func__);
		return fd;
	}

	wd = inotify_add_watch(fd, "/dev", evt_mask);
	if (wd == -1) {
		ublk_dbg(UBLK_DBG_DEV, "%s: add watch for /dev failed\n", __func__);
		goto fail;
	}

	pfd.fd = fd;
	pfd.events = POLL_IN;
	while (1) {
		int i = 0;
		char buffer[EV_BUF_LEN];
		ret = poll(&pfd, 1, 1000 * timeout);

		if (ret == -1) {
			ublk_err("%s: poll inotify failed: %d\n", __func__, ret);
			goto rm_watch;
		} else if (ret == 0) {
			ublk_err("%s: poll inotify timeout\n", __func__);
			ret = -ENOENT;
			goto rm_watch;
		}

		ret = read(fd, buffer, EV_BUF_LEN);
		if (ret < 0) {
			ublk_err("%s: read inotify fd failed\n", __func__);
			goto rm_watch;
		}

		while (i < ret) {
			struct inotify_event *event = (struct inotify_event *)&buffer[i];

			ublk_dbg(UBLK_DBG_DEV, "%s: inotify event %x %s\n",
					__func__, event->mask, event->name);
			if (event->mask & evt_mask) {
				if (!strcmp(event->name, dev_name)) {
					ret = 0;
					goto rm_watch;
				}
			}
			i += EV_SIZE + event->len;
		}
	}
rm_watch:
	inotify_rm_watch(fd, wd);
fail:
	close(fd);
	return ret;
}

static int ublk_stop_io_daemon(const struct ublk_dev *dev)
{
	int daemon_pid = dev->dev_info.ublksrv_pid;
	int dev_id = dev->dev_info.dev_id;
	char ublkc[64];
	int ret;

	/*
	 * Wait until ublk char device is closed, when our daemon is shutdown
	 */
	snprintf(ublkc, sizeof(ublkc), "%s%d", "ublkc", dev_id);
	ret = wait_ublk_dev(ublkc, IN_CLOSE_WRITE, 10);
	waitpid(dev->dev_info.ublksrv_pid, NULL, 0);
	ublk_dbg(UBLK_DBG_DEV, "%s: pid %d dev_id %d ret %d\n",
			__func__, daemon_pid, dev_id, ret);

	return ret;
}

static int cmd_dev_add(char *tgt_type, int *exp_id, unsigned nr_queues,
		       unsigned depth)
{
	const struct ublk_tgt_ops *ops;
	struct ublksrv_ctrl_dev_info *info;
	struct ublk_dev *dev;
	int dev_id = *exp_id;
	char ublkb[64];
	int ret;

	ops = ublk_find_tgt(tgt_type);
	if (!ops) {
		ublk_err("%s: no such tgt type, type %s\n",
				__func__, tgt_type);
		return -ENODEV;
	}

	if (nr_queues > UBLK_MAX_QUEUES || depth > UBLK_QUEUE_DEPTH) {
		ublk_err("%s: invalid nr_queues or depth queues %u depth %u\n",
				__func__, nr_queues, depth);
		return -EINVAL;
	}

	dev = ublk_ctrl_init();
	if (!dev) {
		ublk_err("%s: can't alloc dev id %d, type %s\n",
				__func__, dev_id, tgt_type);
		return -ENOMEM;
	}

	info = &dev->dev_info;
	info->dev_id = dev_id;
        info->nr_hw_queues = nr_queues;
        info->queue_depth = depth;
	dev->tgt.ops = ops;

	ret = ublk_ctrl_add_dev(dev);
	if (ret < 0) {
		ublk_err("%s: can't add dev id %d, type %s ret %d\n",
				__func__, dev_id, tgt_type, ret);
		goto fail;
	}

	switch (fork()) {
	case -1:
		goto fail;
	case 0:
		ublk_start_daemon(dev);
		return 0;
	}

	/*
	 * Wait until ublk disk is added, when our daemon is started
	 * successfully
	 */
	snprintf(ublkb, sizeof(ublkb), "%s%u", "ublkb", dev->dev_info.dev_id);
	ret = wait_ublk_dev(ublkb, IN_CREATE, 3);
	if (ret < 0) {
		ublk_err("%s: can't start daemon id %d, type %s\n",
				__func__, dev_id, tgt_type);
		ublk_ctrl_del_dev(dev);
	} else {
		*exp_id = dev->dev_info.dev_id;
	}
fail:
	ublk_ctrl_deinit(dev);
	return ret;
}

static int cmd_dev_del_by_kill(int number)
{
	struct ublk_dev *dev;
	int ret;

	dev = ublk_ctrl_init();
	dev->dev_info.dev_id = number;

	ret = ublk_ctrl_get_info(dev);
	if (ret < 0)
		goto fail;

	/* simulate one ublk daemon panic */
	kill(dev->dev_info.ublksrv_pid, 9);

	ret = ublk_stop_io_daemon(dev);
	if (ret < 0)
		ublk_err("%s: can't stop daemon id %d\n", __func__, number);
	ublk_ctrl_del_dev(dev);
fail:
	if (ret >= 0)
		ret = ublk_ctrl_get_info(dev);
	ublk_ctrl_deinit(dev);

	return (ret != 0) ? 0 : -EIO;
}

/****************** part 2: target implementation ********************/

static int ublk_null_tgt_init(struct ublk_dev *dev)
{
	const struct ublksrv_ctrl_dev_info *info = &dev->dev_info;
	unsigned long dev_size = 250UL << 30;

	dev->tgt.dev_size = dev_size;
	dev->tgt.params = (struct ublk_params) {
		.types = UBLK_PARAM_TYPE_BASIC,
		.basic = {
			.logical_bs_shift	= 9,
			.physical_bs_shift	= 12,
			.io_opt_shift		= 12,
			.io_min_shift		= 9,
			.max_sectors		= info->max_io_buf_bytes >> 9,
			.dev_sectors		= dev_size >> 9,
		},
	};

	return 0;
}

static int ublk_null_queue_io(struct ublk_queue *q, int tag)
{
	const struct ublksrv_io_desc *iod = ublk_get_iod(q, tag);

	ublk_complete_io(q, tag, iod->nr_sectors << 9);

	return 0;
}

static const struct ublk_tgt_ops tgt_ops_list[] = {
	{
		.name = "null",
		.init_tgt = ublk_null_tgt_init,
		.queue_io = ublk_null_queue_io,
	},
};

static const struct ublk_tgt_ops *ublk_find_tgt(const char *name)
{
	const struct ublk_tgt_ops *ops;
	int i;

	if (name == NULL)
		return NULL;

	for (i = 0; sizeof(tgt_ops_list) / sizeof(*ops); i++)
		if (strcmp(tgt_ops_list[i].name, name) == 0)
			return &tgt_ops_list[i];
	return NULL;
}


/****************** part 3: IO test over ublk disk ********************/

#include "helpers.h"
#include "liburing.h"
#define BS		4096
#define BUFFERS		128

struct io_ctx {
	int dev_id;
	int write;
	int seq;

	/* output */
	int res;
	pthread_t handle;
};

static int __test_io(struct io_uring *ring, int fd, int write,
		int seq, struct iovec *vecs, int exp_len, off_t start)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;
	off_t offset;

	offset = start;
	for (i = 0; i < BUFFERS; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "sqe get failed\n");
			goto err;
		}
		if (!seq)
			offset = start + BS * (rand() % BUFFERS);
		if (write) {
			io_uring_prep_write_fixed(sqe, fd, vecs[i].iov_base,
						  vecs[i].iov_len,
						  offset, i);
		} else {
			io_uring_prep_read_fixed(sqe, fd, vecs[i].iov_base,
						 vecs[i].iov_len,
						 offset, i);
		}
		sqe->user_data = i;
		if (seq)
			offset += BS;
	}

	ret = io_uring_submit(ring);
	if (ret != BUFFERS) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret, BUFFERS);
		goto err;
	}

	for (i = 0; i < BUFFERS; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			goto err;
		}
		if (exp_len == -1) {
			int iov_len = vecs[cqe->user_data].iov_len;

			if (cqe->res != iov_len) {
				fprintf(stderr, "cqe res %d, wanted %d\n",
					cqe->res, iov_len);
				goto err;
			}
		} else if (cqe->res != exp_len) {
			fprintf(stderr, "cqe res %d, wanted %d\n", cqe->res, exp_len);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

/* Run IO over ublk block device  */
static int test_io(struct io_ctx *ctx)
{
	struct io_uring ring;
	int ret, ring_flags = 0;
	char buf[256];
	int fd = -1;
	off_t offset = 0;
	unsigned long long bytes;
	int open_flags = O_DIRECT;
	struct iovec *vecs = t_create_buffers(BUFFERS, BS);

	ret = t_create_ring(BUFFERS, &ring, ring_flags);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	snprintf(buf, sizeof(buf), "%s%d", UBLKB_DEV, ctx->dev_id);

	if (ctx->write)
		open_flags |= O_WRONLY;
	else
		open_flags |= O_RDONLY;
	fd = open(buf, open_flags);
	if (fd < 0) {
		if (errno == EINVAL)
			return 0;
		return 1;
	}

	if (ioctl(fd, BLKGETSIZE64, &bytes) != 0)
		return 1;

	ret = t_register_buffers(&ring, vecs, BUFFERS);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "buffer reg failed: %d\n", ret);
		return 1;
	}

	for (offset = 0; offset < bytes; offset += BS * BUFFERS) {
		ret = __test_io(&ring, fd, ctx->write, ctx->seq, vecs, BS,
				offset);
		if (ret != T_SETUP_OK) {
			fprintf(stderr, "/dev/ublkb%d read failed: offset %lu ret %d\n",
					ctx->dev_id, (unsigned long) offset, ret);
			break;
		}
	}

	close(fd);
	io_uring_unregister_buffers(&ring);
	io_uring_queue_exit(&ring);

	return ret;
}

static void *test_io_fn(void *data)
{
	struct io_ctx *ctx = data;

	ctx->res = test_io(ctx);

	return data;
}

static void ignore_stderr(void)
{
	int devnull = open("/dev/null", O_WRONLY);

	if (devnull >= 0) {
		dup2(devnull, fileno(stderr));
		close(devnull);
	}
}

static int test_io_worker(int dev_id)
{
	const int nr_jobs = 4;
	struct io_ctx ctx[nr_jobs];
	int i, ret = 0;

	for (i = 0; i < nr_jobs; i++) {
		ctx[i].dev_id = dev_id;
		ctx[i].write = (i & 0x1) ? 0 : 1;
		ctx[i].seq = 1;

		pthread_create(&ctx[i].handle, NULL, test_io_fn, &ctx[i]);
	}

	for (i = 0; i < nr_jobs; i++) {
		pthread_join(ctx[i].handle, NULL);

		if (!ret && ctx[i].res)
			ret = ctx[i].res;
	}

	return ret;
}

/*
 * Run IO over created ublk device, meantime delete this ublk device
 *
 * Cover cancellable uring_cmd
 * */
static int test_del_ublk_with_io(void)
{
	const unsigned wait_ms = 200;
	char *tgt_type = "null";
	int dev_id = -1;
	int ret, pid;

	ret = cmd_dev_add(tgt_type, &dev_id, 2, BUFFERS);
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "buffer reg failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	switch ((pid = fork())) {
	case -1:
		fprintf(stderr, "fork failed\n");
		return T_EXIT_FAIL;
	case 0:
		/* io error is expected since the parent is killing ublk */
		ignore_stderr();
		test_io_worker(dev_id);
		return 0;
	default:
		/*
		 * Wait a little while until ublk IO pipeline is warm up,
		 * then try to shutdown ublk device by `kill -9 $ublk_daemon_pid`.
		 *
		 * cancellable uring_cmd code path can be covered in this way.
		 */
		usleep(wait_ms * 1000);
		ret = cmd_dev_del_by_kill(dev_id);
		waitpid(pid, NULL, 0);
		return ret;
	}
}

int main(int argc, char *argv[])
{
	const int nr_loop = 4;
	struct ublk_dev *dev;
	__u64 features;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	dev = ublk_ctrl_init();
	/* ublk isn't supported or the module isn't loaded */
	if (!dev)
		return T_EXIT_SKIP;

	/* kernel doesn't support get_features */
	ret = ublk_ctrl_get_features(dev, &features);
	if (ret < 0)
		return T_EXIT_SKIP;

	if (!(features & UBLK_F_CMD_IOCTL_ENCODE))
		return T_EXIT_SKIP;

	for (i = 0; i < nr_loop; i++) {
		if (test_del_ublk_with_io())
			return T_EXIT_FAIL;
	}
	ublk_ctrl_deinit(dev);
	return T_EXIT_PASS;
}
#else
int main(int argc, char *argv[])
{
	return T_EXIT_SKIP;
}
#endif
