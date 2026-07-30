/* SPDX-License-Identifier: MIT */
/*
 * Description: run various min_timeout tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

struct data {
	pthread_barrier_t startup;
	unsigned long usec_sleep;
	int out_fds[8];
	int nr_fds;
};

static int time_pass(struct timeval *start, unsigned long min_t,
		     unsigned long max_t, const char *name)
{
	unsigned long elapsed;

	elapsed = mtime_since_now(start);
	if (elapsed < min_t || elapsed > max_t) {
		fprintf(stderr, "%s fails time check\n", name);
		fprintf(stderr, " elapsed=%lu, min=%lu, max=%lu\n", elapsed,
				min_t, max_t);
		return T_EXIT_FAIL;
	}
	return T_EXIT_PASS;
}

static void *pipe_write(void *data)
{
	struct data *d = data;
	char buf[32];
	int i;

	memset(buf, 0x55, sizeof(buf));

	pthread_barrier_wait(&d->startup);

	if (d->usec_sleep)
		usleep(d->usec_sleep);

	for (i = 0; i < d->nr_fds; i++) {
		int ret;

		ret = write(d->out_fds[i], buf, sizeof(buf));
		if (ret < 0) {
			perror("write");
			return NULL;
		}
	}

	return NULL;
}

static int __test_writes(struct io_uring *ring, int npipes, int usec_sleep,
			 int usec_wait, int min_t, int max_t, const char *name)
{
	struct __kernel_timespec ts;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct timeval tv;
	int ret, i, fds[4][2];
	pthread_t thread;
	struct data d;
	char buf[32];
	void *tret;

	for (i = 0; i < npipes; i++) {
		if (pipe(fds[i]) < 0) {
			perror("pipe");
			return T_EXIT_FAIL;
		}
		d.out_fds[i] = fds[i][1];
	}
	d.nr_fds = npipes;

	pthread_barrier_init(&d.startup, NULL, 2);
	d.usec_sleep = usec_sleep;

	pthread_create(&thread, NULL, pipe_write, &d);
	pthread_barrier_wait(&d.startup);

	for (i = 0; i < npipes; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_read(sqe, fds[i][0], buf, sizeof(buf), 0);
	}

	io_uring_submit(ring);

	ts.tv_sec = 1;
	ts.tv_nsec = 0;
	gettimeofday(&tv, NULL);
	ret = io_uring_wait_cqes_min_timeout(ring, &cqe, 4, &ts, usec_wait, NULL);
	if (ret) {
		fprintf(stderr, "wait_cqes: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = time_pass(&tv, min_t, max_t, name);

	io_uring_cq_advance(ring, npipes);

	pthread_join(thread, &tret);
	for (i = 0; i < npipes; i++) {
		close(fds[i][0]);
		close(fds[i][1]);
	}
	return ret;
}
/*
 * Test doing min_wait for N events, where 0 events are already available
 * on wait enter but N/2 are posted within the min_wait window. We'll expect to
 * return when the min_wait window expires.
 */
static int test_some_wait(struct io_uring *ring)
{
	return __test_writes(ring, 2, 1000, 100000, 95, 120, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, where 0 events are already available
 * on wait enter but N are posted within the min_wait window. We'll expect to
 * return upon arrival of the N events, not the full min_wait window.
 */
static int test_post_wait(struct io_uring *ring)
{
	return __test_writes(ring, 4, 10000, 200000, 9, 12, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, where 0 events are already available
 * on wait enter and one is posted after the min_wait timeout has expired.
 * That first event should cause wait to abort, even if the task has asked
 * for more to wait on.
 */
static int test_late(struct io_uring *ring)
{
	return __test_writes(ring, 1, 100000, 10000, 95, 120, __FUNCTION__);
}

static int __test_nop(struct io_uring *ring, int nr_nops, int min_t, int max_t,
		      unsigned long long_wait, const char *name)
{
	struct __kernel_timespec ts;
	struct io_uring_cqe *cqe;
	struct timeval tv;
	int i, ret;

	for (i = 0; i < nr_nops; i++) {
		struct io_uring_sqe *sqe;

		sqe = io_uring_get_sqe(ring);
		io_uring_prep_nop(sqe);
	}

	if (nr_nops)
		io_uring_submit(ring);

	ts.tv_sec = 0;
	ts.tv_nsec = long_wait * 1000;
	gettimeofday(&tv, NULL);
	ret = io_uring_wait_cqes_min_timeout(ring, &cqe, 4, &ts, 50000, NULL);
	io_uring_cq_advance(ring, nr_nops);
	if (nr_nops) {
		if (ret) {
			fprintf(stderr, "wait_cqes: %d\n", ret);
			return T_EXIT_FAIL;
		}
	} else {
		if (ret != -ETIME) {
			fprintf(stderr, "wait_cqes: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}

	return time_pass(&tv, min_t, max_t, name);
}

/*
 * Test doing min_wait for N events, where N/2 events are already available
 * on wait enter. This should abort waiting after min_wait, not do the full
 * wait.
 */
static int test_some(struct io_uring *ring)
{
	return __test_nop(ring, 2, 45, 55, 100000, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, where N events are already available
 * on wait enter.
 */
static int test_already(struct io_uring *ring)
{
	return __test_nop(ring, 4, 0, 1, 100000, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, and nothing ever gets posted. We'd
 * expect the time to be the normal wait time, not the min_wait time.
 */
static int test_nothing(struct io_uring *ring)
{
	return __test_nop(ring, 0, 95, 110, 100000, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, and nothing ever gets posted, and use
 * a min_wait time that's bigger than the total wait. We only expect the
 * min_wait to elapse.
 */
static int test_min_wait_biggest(struct io_uring *ring)
{
	return __test_nop(ring, 0, 45, 55, 20000, __FUNCTION__);
}

/*
 * Test doing min_wait for N events, and nothing ever gets posted, and use
 * a min_wait time that's roughly equal to the total wait. We only expect the
 * min_wait to elapse.
 */
static int test_min_wait_equal(struct io_uring *ring)
{
	return __test_nop(ring, 0, 45, 55, 50001, __FUNCTION__);
}

int main(int argc, char *argv[])
{
	struct io_uring ring1, ring2, ring3;
	struct io_uring_params p = { };
	int ret, no_sq_poll = 0;

	if (argc > 1)
		return 0;

	ret = t_create_ring_params(8, &ring1, &p);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return ret;
	if (!(p.features & IORING_FEAT_MIN_TIMEOUT))
		return T_EXIT_SKIP;

	p.flags = IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN;
	ret = t_create_ring_params(8, &ring2, &p);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return ret;

	p.flags = IORING_SETUP_SQPOLL;
	ret = t_create_ring_params(8, &ring3, &p);
	if (ret != T_SETUP_OK)
		no_sq_poll = 1;

	ret = test_already(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_already(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_already(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_some(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_some(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_some(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_late(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_late(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_late(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_post_wait(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_post_wait(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_post_wait(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_some_wait(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_some_wait(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_some_wait(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_nothing(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_nothing(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_nothing(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_min_wait_biggest(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_min_wait_biggest(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_min_wait_biggest(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	ret = test_min_wait_equal(&ring1);
	if (ret == T_EXIT_FAIL || ret == T_EXIT_SKIP)
		return ret;

	ret = test_min_wait_equal(&ring2);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	if (!no_sq_poll) {
		ret = test_min_wait_equal(&ring3);
		if (ret == T_EXIT_FAIL)
			return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring1);
	io_uring_queue_exit(&ring2);
	if (!no_sq_poll)
		io_uring_queue_exit(&ring3);
	return T_EXIT_PASS;
}
