/* SPDX-License-Identifier: MIT */
/*
 * Description: run various timeout tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>

#include "helpers.h"
#include "liburing.h"
#include "../src/syscall.h"

#define IO_NSEC_PER_SEC			1000000000ULL

static bool support_abs = false;
static bool support_clock = false;

static unsigned long long timespec_to_ns(struct timespec *ts)
{
	return ts->tv_nsec + ts->tv_sec * IO_NSEC_PER_SEC;
}
static struct timespec ns_to_timespec(unsigned long long t)
{
	struct timespec ts;

	ts.tv_sec = t / IO_NSEC_PER_SEC;
	ts.tv_nsec = t - ts.tv_sec * IO_NSEC_PER_SEC;
	return ts;
}

static long long ns_since(struct timespec *ts)
{
	struct timespec now;
	int ret;

	ret = clock_gettime(CLOCK_MONOTONIC, &now);
	if (ret) {
		fprintf(stderr, "clock_gettime failed\n");
		exit(T_EXIT_FAIL);
	}

	return timespec_to_ns(&now) - timespec_to_ns(ts);

}

static int t_io_uring_wait(struct io_uring *ring, int nr, unsigned enter_flags,
			   struct timespec *ts)
{
	struct __kernel_timespec kts = {
		.tv_sec = ts->tv_sec,
		.tv_nsec = ts->tv_nsec
	};
	struct io_uring_getevents_arg arg = {
		.sigmask	= 0,
		.sigmask_sz	= _NSIG / 8,
		.ts		= (unsigned long) &kts
	};
	int ret;

	enter_flags |= IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG;
	ret = io_uring_enter2(ring->ring_fd, 0, nr, enter_flags,
			      &arg, sizeof(arg));
	return ret;
}

static int probe_timers(void)
{
	struct io_uring_clock_register cr = { .clockid = CLOCK_MONOTONIC, };
	struct io_uring ring;
	struct timespec ts;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "probe ring setup failed: %d\n", ret);
		return ret;
	}

	ret = clock_gettime(CLOCK_MONOTONIC, &ts);
	if (ret) {
		fprintf(stderr, "clock_gettime failed\n");
		return ret;
	}

	ret = t_io_uring_wait(&ring, 0, IORING_ENTER_ABS_TIMER, &ts);
	if (!ret) {
		support_abs = true;
	} else if (ret != -EINVAL) {
		fprintf(stderr, "wait failed %i\n", ret);
		return ret;
	}

	ret = io_uring_register_clock(&ring, &cr);
	if (!ret) {
		support_clock = true;
	} else if (ret != -EINVAL) {
		fprintf(stderr, "io_uring_register_clock %i\n", ret);
		return ret;
	}

	io_uring_queue_exit(&ring);
	return 0;
}

static int test_timeout(bool abs, bool set_clock)
{
	unsigned enter_flags = abs ? IORING_ENTER_ABS_TIMER : 0;
	struct io_uring ring;
	struct timespec start, end, ts;
	long long dt;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (set_clock) {
		struct io_uring_clock_register cr = {};

		cr.clockid = CLOCK_BOOTTIME;
		ret = io_uring_register_clock(&ring, &cr);
		if (ret) {
			fprintf(stderr, "io_uring_register_clock failed\n");
			return 1;
		}
	}

	/* pass current time */
	ret = clock_gettime(CLOCK_MONOTONIC, &start);
	assert(ret == 0);

	ts = abs ? start : ns_to_timespec(0);
	ret = t_io_uring_wait(&ring, 1, enter_flags, &ts);
	if (ret != -ETIME) {
		fprintf(stderr, "wait current time failed, %i\n", ret);
		return 1;
	}

	if (ns_since(&start) >= IO_NSEC_PER_SEC) {
		fprintf(stderr, "current time test failed\n");
		return 1;
	}

	if (abs) {
		/* expired time */
		ret = clock_gettime(CLOCK_MONOTONIC, &start);
		assert(ret == 0);
		ts = ns_to_timespec(timespec_to_ns(&start) - IO_NSEC_PER_SEC);

		ret = t_io_uring_wait(&ring, 1, enter_flags, &ts);
		if (ret != -ETIME) {
			fprintf(stderr, "expired timeout wait failed, %i\n", ret);
			return 1;
		}

		ret = clock_gettime(CLOCK_MONOTONIC, &end);
		assert(ret == 0);

		if (ns_since(&start) >= IO_NSEC_PER_SEC) {
			fprintf(stderr, "expired timer test failed\n");
			return 1;
		}
	}

	/* 1s wait */
	ret = clock_gettime(CLOCK_MONOTONIC, &start);
	assert(ret == 0);

	dt = 2 * IO_NSEC_PER_SEC + (abs ? timespec_to_ns(&start) : 0);
	ts = ns_to_timespec(dt);
	ret = t_io_uring_wait(&ring, 1, enter_flags, &ts);
	if (ret != -ETIME) {
		fprintf(stderr, "wait timeout failed, %i\n", ret);
		return 1;
	}

	dt = ns_since(&start);
	if (dt < IO_NSEC_PER_SEC || dt > 3 * IO_NSEC_PER_SEC) {
		fprintf(stderr, "early wake up, %lld\n", dt);
		return 1;
	}
	return 0;
}

static int test_clock_setup(void)
{
	struct io_uring ring;
	struct io_uring_clock_register cr = {};
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = __sys_io_uring_register(ring.ring_fd, IORING_REGISTER_CLOCK, NULL, 0);
	if (!ret) {
		fprintf(stderr, "invalid null clock registration %i\n", ret);
		return T_EXIT_FAIL;
	}

	cr.clockid = -1;
	ret = __sys_io_uring_register(ring.ring_fd, IORING_REGISTER_CLOCK, &cr, 0);
	if (ret != -EINVAL) {
		fprintf(stderr, "invalid clockid registration %i\n", ret);
		return T_EXIT_FAIL;
	}

	cr.clockid = CLOCK_MONOTONIC;
	ret = __sys_io_uring_register(ring.ring_fd, IORING_REGISTER_CLOCK, &cr, 0);
	if (ret) {
		fprintf(stderr, "clock monotonic registration failed %i\n", ret);
		return T_EXIT_FAIL;
	}

	cr.clockid = CLOCK_BOOTTIME;
	ret = __sys_io_uring_register(ring.ring_fd, IORING_REGISTER_CLOCK, &cr, 0);
	if (ret) {
		fprintf(stderr, "clock boottime registration failed %i\n", ret);
		return T_EXIT_FAIL;
	}

	cr.clockid = CLOCK_MONOTONIC;
	ret = __sys_io_uring_register(ring.ring_fd, IORING_REGISTER_CLOCK, &cr, 0);
	if (ret) {
		fprintf(stderr, "2nd clock monotonic registration failed %i\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	return 0;
}

int main(int argc, char *argv[])
{
	int ret, i;

	if (argc > 1)
		return 0;

	ret = probe_timers();
	if (ret) {
		fprintf(stderr, "probe failed\n");
		return T_EXIT_FAIL;
	}
	if (!support_abs && !support_clock)
		return T_EXIT_SKIP;

	if (support_clock) {
		ret = test_clock_setup();
		if (ret) {
			fprintf(stderr, "test_clock_setup failed\n");
			return T_EXIT_FAIL;
		}
	}

	for (i = 0; i < 4; i++) {
		bool abs = i & 1;
		bool clock = i & 2;

		if (abs && !support_abs)
			continue;
		if (clock && !support_clock)
			continue;

		ret = test_timeout(abs, clock);
		if (ret) {
			fprintf(stderr, "test_timeout failed %i %i\n",
					abs, clock);
			return ret;
		}
	}

	return 0;
}
