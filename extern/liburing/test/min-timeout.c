/* SPDX-License-Identifier: MIT */
/*
 * Description: test min timeout handling
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define NPIPES	8
#define NWRITES	6

#define WAIT_USEC	(250000)

static int no_min_timeout;

struct d {
	int fd[NPIPES];
	long delay;
};

static void *thread_fn(void *data)
{
	struct d *d = data;
	char buf[32];
	int i;

	memset(buf, 0x55, sizeof(buf));

	for (i = 0; i < NWRITES; i++) {
		int ret;

		usleep(d->delay);
		ret = write(d->fd[i], buf, sizeof(buf));
		if (ret != sizeof(buf)) {
			fprintf(stderr, "bad write %d\n", ret);
			break;
		}
	}
	return NULL;
}

/*
 * Allow 25% tolerance
 */
static int within_range(unsigned int target, unsigned int msec)
{
	unsigned int high, low;

	low = (target * 3) / 4;
	high = (target * 5) / 4;
	return (msec >= low && msec <= high);
}

static int test(int flags, int expected_ctx, int min_wait, int write_delay,
		int nr_cqes, int msec_target)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	struct __kernel_timespec ts;
	struct rusage s, e;
	pthread_t thread;
	struct d d;
	struct io_uring_params p = { .flags = flags, };
	int ret, fds[NPIPES][2], i;
	struct timeval start_time;
	char buf[32];
	void *tret;
	long ttime;

	ret = io_uring_queue_init_params(NPIPES, &ring, &p);
	if (ret == -EINVAL)
		return T_EXIT_SKIP;
	if (!(p.features & IORING_FEAT_MIN_TIMEOUT)) {
		no_min_timeout = 1;
		return T_EXIT_SKIP;
	}

	for (i = 0; i < NPIPES; i++) {
		if (pipe(fds[i]) < 0) {
			perror("pipe");
			return 1;
		}
		d.fd[i] = fds[i][1];
	}

	d.delay = write_delay;
	pthread_create(&thread, NULL, thread_fn, &d);

	for (i = 0; i < NPIPES; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, fds[i][0], buf, sizeof(buf), 0);
	}

	ts.tv_sec = 0;
	ts.tv_nsec = WAIT_USEC * 1000LL;

	gettimeofday(&start_time, NULL);
	getrusage(RUSAGE_THREAD, &s);
	ret = io_uring_submit_and_wait_min_timeout(&ring, &cqe, 8, &ts, min_wait, NULL);
	if (ret != NPIPES)
		fprintf(stderr, "submit_and_wait=%d\n", ret);

	getrusage(RUSAGE_THREAD, &e);
	e.ru_nvcsw -= s.ru_nvcsw;
	ttime = mtime_since_now(&start_time);
	if (!within_range(msec_target, ttime)) {
		fprintf(stderr, "Expected %d msec, got %ld msec\n", msec_target,
								ttime);
		fprintf(stderr, "flags=%x, min_wait=%d, write_delay=%d\n",
				flags, min_wait, write_delay);
	}
	/* will usually be accurate, but allow for offset of 1 */
	if (e.ru_nvcsw != expected_ctx &&
	    (e.ru_nvcsw - expected_ctx > 1))
		fprintf(stderr, "%ld ctx switches, expected %d\n", e.ru_nvcsw,
								expected_ctx);
	
	for (i = 0; i < NPIPES; i++) {
		ret = io_uring_peek_cqe(&ring, &cqe);
		if (ret)
			break;
		io_uring_cqe_seen(&ring, cqe);
	}

	if (i != nr_cqes) {
		fprintf(stderr, "Got %d CQEs, expected %d\n", i, nr_cqes);
		return T_EXIT_FAIL;
	}

	pthread_join(thread, &tret);

	for (i = 0; i < NPIPES; i++) {
		close(fds[i][0]);
		close(fds[i][1]);
	}

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, NWRITES + 1, 0, 2000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;
	if (no_min_timeout)
		return T_EXIT_SKIP;

	ret = test(0, NWRITES + 1, 50000, 2000, NWRITES, 50);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(0, NWRITES + 1, 500000, 2000, NWRITES, 500);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* no writes within min timeout, but it's given. expect 1 cqe */
	ret = test(0, 1, 10000, 20000, 1, 20);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* same as above, but no min timeout. should time out and we get 6 */
	ret = test(0, NWRITES + 1, 0, 20000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1,
			0, 2000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1,
			50000, 2000, NWRITES, 50);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1,
			500000, 2000, NWRITES, 500);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* no writes within min timeout, but it's given. expect 1 cqe */
	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1,
			10000, 20000, 1, 20);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* same as above, but no min timeout. should time out and we get 6 */
	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, 1,
			0, 20000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SQPOLL, 1, 0, 2000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SQPOLL, 1, 50000, 2000, NWRITES, 50);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SQPOLL, 1, 500000, 2000, NWRITES, 500);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* no writes within min timeout, but it's given. expect 1 cqe */
	ret = test(IORING_SETUP_SQPOLL, 1, 10000, 20000, 1, 20);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	/* same as above, but no min timeout. should time out and we get 6 */
	ret = test(IORING_SETUP_SQPOLL, 1, 0, 20000, NWRITES, WAIT_USEC / 1000);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	return ret;
}
