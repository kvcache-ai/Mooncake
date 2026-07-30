/* SPDX-License-Identifier: MIT */
/*
 * Description: exercise futex wait/wake/waitv
 *
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <linux/futex.h>

#include "liburing.h"
#include "helpers.h"

#define LOOPS	500
#define NFUTEX	8

#ifndef FUTEX2_SIZE_U8
#define FUTEX2_SIZE_U8		0x00
#define FUTEX2_SIZE_U16		0x01
#define FUTEX2_SIZE_U32		0x02
#define FUTEX2_SIZE_U64		0x03
#define FUTEX2_NUMA		0x04
			/*	0x08 */
			/*	0x10 */
			/*	0x20 */
			/*	0x40 */
#define FUTEX2_PRIVATE		FUTEX_PRIVATE_FLAG

#define FUTEX2_SIZE_MASK	0x03
#endif

static int no_futex;

static void *fwake(void *data)
{
	unsigned int *futex = data;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init: %d\n", ret);
		return NULL;
	}

	*futex = 1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_futex_wake(sqe, futex, 1, FUTEX_BITSET_MATCH_ANY,
				 FUTEX2_SIZE_U32, 0);
	sqe->user_data = 3;

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return NULL;
	}
	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return NULL;
}

static int __test(struct io_uring *ring, int vectored, int async,
		  int async_cancel)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct futex_waitv fw[NFUTEX];
	unsigned int *futex;
	pthread_t threads[NFUTEX];
	void *tret;
	int ret, i, nfutex;

	nfutex = NFUTEX;
	if (!vectored)
		nfutex = 1;

	futex = calloc(nfutex, sizeof(*futex));
	for (i = 0; i < nfutex; i++) {
		fw[i].val = 0;
		fw[i].uaddr = (unsigned long) &futex[i];
		fw[i].flags = FUTEX2_SIZE_U32;
		fw[i].__reserved = 0;
	}

	sqe = io_uring_get_sqe(ring);
	if (vectored)
		io_uring_prep_futex_waitv(sqe, fw, nfutex, 0);
	else
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					 FUTEX2_SIZE_U32, 0);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 1;

	io_uring_submit(ring);

	for (i = 0; i < nfutex; i++)
		pthread_create(&threads[i], NULL, fwake, &futex[i]);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel64(sqe, 1, 0);
	if (async_cancel)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 2;

	io_uring_submit(ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "parent wait %d\n", ret);
			return 1;
		}

		if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP) {
			no_futex = 1;
			free(futex);
			return 0;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	ret = io_uring_peek_cqe(ring, &cqe);
	if (!ret) {
		fprintf(stderr, "peek found cqe!\n");
		return 1;
	}

	for (i = 0; i < nfutex; i++)
		pthread_join(threads[i], &tret);

	free(futex);
	return 0;
}

static int test(int flags, int vectored)
{
	struct io_uring ring;
	int ret, i;

	ret = io_uring_queue_init(8, &ring, flags);
	if (ret)
		return ret;

	for (i = 0; i < LOOPS; i++) {
		int async_cancel = !(i % 2);
		int async_wait = !(i % 3);
		ret = __test(&ring, vectored, async_wait, async_cancel);
		if (ret) {
			fprintf(stderr, "flags=%x, failed=%d\n", flags, i);
			break;
		}
		if (no_futex)
			break;
	}

	io_uring_queue_exit(&ring);
	return ret;
}

static int test_order(int vectored, int async)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct futex_waitv fw = { };
	struct io_uring_sync_cancel_reg reg = { };
	struct io_uring ring;
	unsigned int *futex;
	int ret, i;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret)
		return ret;

	futex = malloc(sizeof(*futex));
	*futex = 0;

	fw.uaddr = (unsigned long) futex;
	fw.flags = FUTEX2_SIZE_U32;

	/*
	 * Submit two futex waits
	 */
	sqe = io_uring_get_sqe(&ring);
	if (!vectored)
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					 FUTEX2_SIZE_U32, 0);
	else
		io_uring_prep_futex_waitv(sqe, &fw, 1, 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(&ring);
	if (!vectored)
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					 FUTEX2_SIZE_U32, 0);
	else
		io_uring_prep_futex_waitv(sqe, &fw, 1, 0);
	sqe->user_data = 2;

	io_uring_submit(&ring);

	/*
	 * Now submit wake for just one futex
	 */
	*futex = 1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_futex_wake(sqe, futex, 1, FUTEX_BITSET_MATCH_ANY,
				 FUTEX2_SIZE_U32, 0);
	sqe->user_data = 100;
	if (async)
		sqe->flags |= IOSQE_ASYNC;

	io_uring_submit(&ring);

	/*
	 * We expect to find completions for the first futex wait, and
	 * the futex wake. We should not see the last futex wait.
	 */
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait %d\n", ret);
			return 1;
		}
		if (cqe->user_data == 1 || cqe->user_data == 100) {
			io_uring_cqe_seen(&ring, cqe);
			continue;
		}
		fprintf(stderr, "unexpected cqe %lu, res %d\n", (unsigned long) cqe->user_data, cqe->res);
		return 1;
	}

	ret = io_uring_peek_cqe(&ring, &cqe);
	if (ret != -EAGAIN) {
		fprintf(stderr, "Unexpected cqe available: %d\n", cqe->res);
		return 1;
	}

	reg.addr = 2;
	ret = io_uring_register_sync_cancel(&ring, &reg);
	if (ret != 1) {
		fprintf(stderr, "Failed to cancel pending futex wait: %d\n", ret);
		return 1;
	}

	io_uring_queue_exit(&ring);
	free(futex);
	return 0;
}

static int test_multi_wake(int vectored)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct futex_waitv fw;
	struct io_uring ring;
	unsigned int *futex;
	int ret, i;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret)
		return ret;

	futex = malloc(sizeof(*futex));
	*futex = 0;

	fw.val = 0;
	fw.uaddr = (unsigned long) futex;
	fw.flags = FUTEX2_SIZE_U32;
	fw.__reserved = 0;

	/*
	 * Submit two futex waits
	 */
	sqe = io_uring_get_sqe(&ring);
	if (!vectored)
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					 FUTEX2_SIZE_U32, 0);
	else
		io_uring_prep_futex_waitv(sqe, &fw, 1, 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(&ring);
	if (!vectored)
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					 FUTEX2_SIZE_U32, 0);
	else
		io_uring_prep_futex_waitv(sqe, &fw, 1, 0);
	sqe->user_data = 2;

	io_uring_submit(&ring);

	/*
	 * Now submit wake for both futexes
	 */
	*futex = 1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_futex_wake(sqe, futex, 2, FUTEX_BITSET_MATCH_ANY,
				 FUTEX2_SIZE_U32, 0);
	sqe->user_data = 100;

	io_uring_submit(&ring);

	/*
	 * We expect to find completions for the both futex waits, and
	 * the futex wake.
	 */
	for (i = 0; i < 3; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait %d\n", ret);
			return 1;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "cqe error %d\n", cqe->res);
			return 1;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	ret = io_uring_peek_cqe(&ring, &cqe);
	if (!ret) {
		fprintf(stderr, "peek found cqe!\n");
		return 1;
	}

	io_uring_queue_exit(&ring);
	free(futex);
	return 0;
}

/*
 * Test that waking 0 futexes returns 0
 */
static int test_wake_zero(void)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	unsigned int *futex;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret)
		return ret;

	futex = malloc(sizeof(*futex));
	*futex = 0;

	sqe = io_uring_get_sqe(&ring);
	sqe->user_data = 1;
	io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
				 FUTEX2_SIZE_U32, 0);

	io_uring_submit(&ring);

	sqe = io_uring_get_sqe(&ring);
	sqe->user_data = 2;
	io_uring_prep_futex_wake(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
				 FUTEX2_SIZE_U32, 0);

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);

	/*
	 * Should get zero res and it should be the wake
	 */
	if (cqe->res || cqe->user_data != 2) {
		fprintf(stderr, "cqe res %d, data %ld\n", cqe->res, (long) cqe->user_data);
		return 1;
	}
	io_uring_cqe_seen(&ring, cqe);

	/*
	 * Should not have the wait complete
	 */
	ret = io_uring_peek_cqe(&ring, &cqe);
	if (!ret) {
		fprintf(stderr, "peek found cqe!\n");
		return 1;
	}

	io_uring_queue_exit(&ring);
	free(futex);
	return 0;
}

/*
 * Test invalid wait/wake/waitv flags
 */
static int test_invalid(void)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct futex_waitv fw;
	struct io_uring ring;
	unsigned int *futex;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret)
		return ret;

	futex = malloc(sizeof(*futex));
	*futex = 0;

	sqe = io_uring_get_sqe(&ring);
	sqe->user_data = 1;
	io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY, 0x1000,
				 0);

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);

	/*
	 * Should get zero res and it should be the wake
	 */
	if (cqe->res != -EINVAL) {
		fprintf(stderr, "wait cqe res %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(&ring, cqe);

	sqe = io_uring_get_sqe(&ring);
	sqe->user_data = 1;
	io_uring_prep_futex_wake(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY, 0x1000,
				 0);

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);

	/*
	 * Should get zero res and it should be the wake
	 */
	if (cqe->res != -EINVAL) {
		fprintf(stderr, "wake cqe res %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(&ring, cqe);

	fw.val = 0;
	fw.uaddr = (unsigned long) futex;
	fw.flags = FUTEX2_SIZE_U32 | 0x1000;
	fw.__reserved = 0;

	sqe = io_uring_get_sqe(&ring);
	sqe->user_data = 1;
	io_uring_prep_futex_waitv(sqe, &fw, 1, 0);

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);

	/*
	 * Should get zero res and it should be the wake
	 */
	if (cqe->res != -EINVAL) {
		fprintf(stderr, "waitv cqe res %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(&ring, cqe);

	io_uring_queue_exit(&ring);
	free(futex);
	return 0;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, 0);
	if (ret) {
		fprintf(stderr, "test 0 0 failed\n");
		return T_EXIT_FAIL;
	}
	if (no_futex)
		return T_EXIT_SKIP;

	ret = test(0, 1);
	if (ret) {
		fprintf(stderr, "test 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_wake_zero();
	if (ret) {
		fprintf(stderr, "wake 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_invalid();
	if (ret) {
		fprintf(stderr, "test invalid failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SQPOLL, 0);
	if (ret) {
		fprintf(stderr, "test sqpoll 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SQPOLL, 1);
	if (ret) {
		fprintf(stderr, "test sqpoll 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 0);
	if (ret) {
		fprintf(stderr, "test single coop 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1);
	if (ret) {
		fprintf(stderr, "test single coop 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_COOP_TASKRUN, 0);
	if (ret) {
		fprintf(stderr, "test taskrun 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_COOP_TASKRUN, 1);
	if (ret) {
		fprintf(stderr, "test taskrun 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_order(0, 0);
	if (ret) {
		fprintf(stderr, "test_order 0 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_order(1, 0);
	if (ret) {
		fprintf(stderr, "test_order 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_order(0, 1);
	if (ret) {
		fprintf(stderr, "test_order 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_order(1, 1);
	if (ret) {
		fprintf(stderr, "test_order 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_multi_wake(0);
	if (ret) {
		fprintf(stderr, "multi_wake 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_multi_wake(1);
	if (ret) {
		fprintf(stderr, "multi_wake 1 failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
