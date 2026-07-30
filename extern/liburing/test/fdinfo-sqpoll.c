/* SPDX-License-Identifier: MIT */
/*
 * Description: Race fdinfo reading with SQPOLL thread exiting
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <pthread.h>

#include "helpers.h"
#include "liburing.h"

struct data {
	struct io_uring ring;
	pthread_t thread;
	pthread_barrier_t barrier;
	volatile int done;
};

static int rand_between(int a, int b)
{
	return a + rand() % (b - a + 1);
}

static void *fdinfo_read(void *__data)
{
	struct data *d = __data;
	char fd_name[128];
	char *buf;
	int fd;

	buf = malloc(4096);

	sprintf(fd_name, "/proc/self/fdinfo/%d", d->ring.ring_fd);
	fd = open(fd_name, O_RDONLY);
	if (fd < 0) {
		perror("open");
		return NULL;
	}

	pthread_barrier_wait(&d->barrier);

	do {
		int ret = read(fd, buf, 4096);

		if (ret < 0) {
			perror("fdinfo read");
			break;
		}
	} while (!d->done);

	close(fd);
	free(buf);
	return NULL;
}

static int __test(void)
{
	struct data d = { };
	void *tret;
	int ret;

	ret = t_create_ring(8, &d.ring, IORING_SETUP_SQPOLL);
	if (ret == T_SETUP_SKIP)
		exit(T_EXIT_SKIP);
	else if (ret < 0)
		exit(T_EXIT_FAIL);
	pthread_barrier_init(&d.barrier, NULL, 2);
	pthread_create(&d.thread, NULL, fdinfo_read, &d);
	pthread_barrier_wait(&d.barrier);
	usleep(rand_between(1000, 100000));
	d.done = 1;
	pthread_join(d.thread, &tret);
	exit(T_EXIT_PASS);
}

static int test(void)
{
	pid_t pid;

	pid = fork();
	if (pid) {
		int wstatus;

		usleep(rand_between(10, 2000));
		kill(pid, SIGINT);
		waitpid(pid, &wstatus, 0);
	} else {
		return __test();
	}

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int i, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	for (i = 0; i < 1000; i++) {
		ret = test();
		if (ret == T_EXIT_SKIP) {
			return T_EXIT_SKIP;
		} else if (ret) {
			fprintf(stderr, "test failed\n");
			return T_EXIT_FAIL;
		}
	}

	return T_EXIT_PASS;
}
