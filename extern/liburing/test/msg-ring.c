/* SPDX-License-Identifier: MIT */
/*
 * Description: test ring messaging command
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

static int no_msg;
static int no_sync_msg;

static int test_own_sync(struct io_uring *ring)
{
	struct io_uring_sqe sqe = { };
	struct io_uring_cqe *cqe;
	int ret;

	if (no_sync_msg)
		return 0;

	io_uring_prep_msg_ring(&sqe, ring->ring_fd, 0x10, 0x1234, 0);
	sqe.user_data = 1;

	ret = io_uring_register_sync_msg(&sqe);
	if (ret == -EINVAL) {
		no_sync_msg = 1;
		return 0;
	} else if (ret != 0) {
		fprintf(stderr, "register_sync_msg: %d\n", ret);
		return 1;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		return 1;
	}
	switch (cqe->user_data) {
	case 0x1234:
		if (cqe->res != 0x10) {
			fprintf(stderr, "invalid len %x\n", cqe->res);
			return -1;
		}
		break;
	default:
		fprintf(stderr, "Invalid user_data\n");
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	return 0;
}

static int test_own(struct io_uring *ring, int do_sync)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i;

	if (do_sync)
		return test_own_sync(ring);

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_msg_ring(sqe, ring->ring_fd, 0x10, 0x1234, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "wait completion %d\n", ret);
			goto err;
		}
		switch (cqe->user_data) {
		case 1:
			if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP) {
				no_msg = 1;
				return 0;
			}
			if (cqe->res != 0) {
				fprintf(stderr, "cqe res %d\n", cqe->res);
				return -1;
			}
			break;
		case 0x1234:
			if (cqe->res != 0x10) {
				fprintf(stderr, "invalid len %x\n", cqe->res);
				return -1;
			}
			break;
		default:
			fprintf(stderr, "Invalid user_data\n");
			return -1;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

struct data {
	struct io_uring *ring;
	unsigned int flags;
	pthread_barrier_t startup;
	pthread_barrier_t barrier;
};

static void *wait_cqe_fn(void *__data)
{
	struct data *d = __data;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	io_uring_queue_init(4, &ring, d->flags);
	d->ring = &ring;
	pthread_barrier_wait(&d->startup);

	pthread_barrier_wait(&d->barrier);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		goto err;
	}

	if (cqe->user_data != 0x5aa5) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		goto err;
	}
	if (cqe->res != 0x20) {
		fprintf(stderr, "len %x\n", cqe->res);
		goto err;
	}

	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return NULL;
err:
	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return (void *) (unsigned long) 1;
}

static int test_remote_sync(unsigned int ring_flags)
{
	struct io_uring *target;
	pthread_t thread;
	void *tret;
	struct io_uring_sqe sqe = { };
	struct data d;
	int ret;

	if (no_sync_msg)
		return 0;

	d.flags = ring_flags;
	pthread_barrier_init(&d.barrier, NULL, 2);
	pthread_barrier_init(&d.startup, NULL, 2);
	pthread_create(&thread, NULL, wait_cqe_fn, &d);

	pthread_barrier_wait(&d.startup);
	target = d.ring;

	io_uring_prep_msg_ring(&sqe, target->ring_fd, 0x20, 0x5aa5, 0);
	sqe.user_data = 1;

	pthread_barrier_wait(&d.barrier);

	ret = io_uring_register_sync_msg(&sqe);
	if (ret == -EINVAL) {
		no_sync_msg = 1;
		return 0;
	} else if (ret != 0) {
		fprintf(stderr, "sync_msg: %d\n", ret);
		goto err;
	}
	pthread_join(thread, &tret);
	return 0;
err:
	return 1;
}

static int test_remote(struct io_uring *ring, unsigned int ring_flags,
		       int do_sync)
{
	struct io_uring *target;
	pthread_t thread;
	void *tret;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct data d;
	int ret;

	if (do_sync)
		return test_remote_sync(ring_flags);

	d.flags = ring_flags;
	pthread_barrier_init(&d.barrier, NULL, 2);
	pthread_barrier_init(&d.startup, NULL, 2);
	pthread_create(&thread, NULL, wait_cqe_fn, &d);

	pthread_barrier_wait(&d.startup);
	target = d.ring;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_msg_ring(sqe, target->ring_fd, 0x20, 0x5aa5, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	pthread_barrier_wait(&d.barrier);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != 0) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		return -1;
	}
	if (cqe->user_data != 1) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		io_uring_cqe_seen(ring, cqe);
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	pthread_join(thread, &tret);
	return 0;
err:
	return 1;
}

static void *remote_submit_fn(void *data)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring *target = data;
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "thread ring setup failed: %d\n", ret);
		goto err;
	}
	sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_msg_ring(sqe, target->ring_fd, 0x20, 0x5aa5, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != 0 || cqe->user_data != 1) {
		fprintf(stderr, "invalid cqe\n");
		goto err;
	}
	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return NULL;
err:
	return (void *) (unsigned long) 1;
}

static int test_remote_submit(struct io_uring *target)
{
	struct io_uring_cqe *cqe;
	pthread_t thread;
	void *tret;
	int ret;

	pthread_create(&thread, NULL, remote_submit_fn, target);

	ret = io_uring_wait_cqe(target, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != 0x20) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return -1;
	}
	if (cqe->user_data != 0x5aa5) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		return -1;
	}
	io_uring_cqe_seen(target, cqe);
	pthread_join(thread, &tret);
	return 0;
err:
	return 1;
}

static int test_invalid(struct io_uring *ring, bool fixed)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, fd = 1;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return 1;
	}

	if (fixed) {
		ret = io_uring_register_files(ring, &fd, 1);
		if (ret) {
			fprintf(stderr, "file register %d\n", ret);
			return 1;
		}
		io_uring_prep_msg_ring(sqe, 0, 0, 0x8989, 0);
		sqe->flags |= IOSQE_FIXED_FILE;
	} else {
		io_uring_prep_msg_ring(sqe, 1, 0, 0x8989, 0);
	}

	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != -EBADFD) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	if (fixed)
		io_uring_unregister_files(ring);
	return 0;
err:
	if (fixed)
		io_uring_unregister_files(ring);
	return 1;
}

static int test_disabled_ring(struct io_uring *ring, int flags)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct io_uring disabled_ring;
	int ret;

	flags |= IORING_SETUP_R_DISABLED;
	ret = io_uring_queue_init(8, &disabled_ring, flags);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_msg_ring(sqe, disabled_ring.ring_fd, 0x10, 0x1234, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return 1;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		return 1;
	}
	if (cqe->res != 0 && cqe->res != -EBADFD) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return 1;
	}
	if (cqe->user_data != 1) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		return 1;
	}

	io_uring_cqe_seen(ring, cqe);
	io_uring_queue_exit(&disabled_ring);
	return 0;
}

static int test(int ring_flags)
{
	struct io_uring ring, ring2, pring;
	int ret, i;

	ret = io_uring_queue_init(8, &ring, ring_flags);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &ring2, ring_flags);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &pring, ring_flags | IORING_SETUP_IOPOLL);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_own(&ring, 0);
	if (ret) {
		fprintf(stderr, "test_own sync failed\n");
		return T_EXIT_FAIL;
	}
	if (no_msg)
		return T_EXIT_SKIP;

	ret = test_own(&ring, 1);
	if (ret) {
		fprintf(stderr, "test_own async failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_own(&pring, 0);
	if (ret) {
		fprintf(stderr, "test_own async iopoll failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_own(&pring, 1);
	if (ret) {
		fprintf(stderr, "test_own sync iopoll failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_invalid(&ring, 0);
	if (ret) {
		fprintf(stderr, "test_invalid failed\n");
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 2; i++) {
		ret = test_invalid(&ring, 1);
		if (ret) {
			fprintf(stderr, "test_invalid fixed failed\n");
			return T_EXIT_FAIL;
		}
	}

	ret = test_remote(&ring, ring_flags, 0);
	if (ret) {
		fprintf(stderr, "test_remote failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_remote(&ring, ring_flags, 1);
	if (ret) {
		fprintf(stderr, "test_remote sync failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	io_uring_queue_exit(&pring);

	if (t_probe_defer_taskrun()) {
		ret = io_uring_queue_init(8, &ring, IORING_SETUP_SINGLE_ISSUER |
						    IORING_SETUP_DEFER_TASKRUN);
		if (ret) {
			fprintf(stderr, "deferred ring setup failed: %d\n", ret);
			return T_EXIT_FAIL;
		}

		ret = test_own(&ring, 0);
		if (ret) {
			fprintf(stderr, "test_own async deferred failed\n");
			return T_EXIT_FAIL;
		}

		ret = test_own(&ring, 1);
		if (ret) {
			fprintf(stderr, "test_own sync deferred failed\n");
			return T_EXIT_FAIL;
		}

		for (i = 0; i < 2; i++) {
			ret = test_invalid(&ring, i);
			if (ret) {
				fprintf(stderr, "test_invalid(0) deferred failed\n");
				return T_EXIT_FAIL;
			}
		}

		ret = test_remote_submit(&ring);
		if (ret) {
			fprintf(stderr, "test_remote_submit failed\n");
			return T_EXIT_FAIL;
		}
		io_uring_queue_exit(&ring);

		if (test_disabled_ring(&ring2, 0)) {
			fprintf(stderr, "test_disabled_ring failed\n");
			return T_EXIT_FAIL;
		}

		if (test_disabled_ring(&ring2, IORING_SETUP_SINGLE_ISSUER |
						IORING_SETUP_DEFER_TASKRUN)) {
			fprintf(stderr, "test_disabled_ring defer failed\n");
			return T_EXIT_FAIL;
		}
	}

	io_uring_queue_exit(&ring2);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "ring flags 0 failed\n");
		return ret;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test(IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "ring flags defer failed\n");
		return ret;
	}

	return ret;
}
