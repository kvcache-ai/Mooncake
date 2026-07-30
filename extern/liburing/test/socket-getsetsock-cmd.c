/* SPDX-License-Identifier: MIT */
/*
 * Description: Check that {g,s}etsockopt CMD operations on sockets are
 * consistent.
 *
 * The tests basically do the same socket operation using regular system calls
 * and io_uring commands, and then compare the results.
 */

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <linux/tcp.h>

#include "liburing.h"
#include "helpers.h"

#define USERDATA 0xff42ff
#define MSG "foobarbaz"

static int no_sock_opt;

struct fds {
	int tx;
	int rx;
};

static struct fds create_sockets(void)
{
	struct fds retval;
	int fd[2];

	t_create_socket_pair(fd, true);

	retval.tx = fd[0];
	retval.rx = fd[1];

	return retval;
}

static struct io_uring create_ring(void)
{
	struct io_uring ring;
	int ring_flags = 0;
	int err;

	err = io_uring_queue_init(32, &ring, ring_flags);
	assert(err == 0);

	return ring;
}

static int submit_cmd_sqe(struct io_uring *ring, int32_t fd,
			  int op, int level, int optname,
			  void *optval, int optlen,
			  bool async)
{
	struct io_uring_sqe *sqe;
	int err;

	assert(fd > 0);

	sqe = io_uring_get_sqe(ring);
	assert(sqe != NULL);

	io_uring_prep_cmd_sock(sqe, op, fd, level, optname, optval, optlen);
	sqe->user_data = USERDATA;
	if (async)
		sqe->flags |= IOSQE_ASYNC;

	/* Submitting SQE */
	err = io_uring_submit_and_wait(ring, 1);
	if (err != 1)
		fprintf(stderr, "Failure: io_uring_submit_and_wait returned %d\n", err);

	return err;
}

static int receive_cqe(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	int err;

	err = io_uring_wait_cqe(ring, &cqe);
	assert(err ==  0);
	assert(cqe->user_data == USERDATA);
	io_uring_cqe_seen(ring, cqe);

	/* Return the result of the operation */
	return cqe->res;
}

/*
 * Run getsock operation using SO_RCVBUF using io_uring cmd operation and
 * getsockopt(2) and compare the results.
 */
static int run_get_rcvbuf(struct io_uring *ring, struct fds *sockfds, bool async)
{
	int sval, uval, ulen, err;
	unsigned int slen;

	/* System call values */
	slen = sizeof(sval);
	/* io_uring values */
	ulen = sizeof(uval);

	/* get through io_uring cmd */
	err = submit_cmd_sqe(ring, sockfds->rx, SOCKET_URING_OP_GETSOCKOPT,
			     SOL_SOCKET, SO_RCVBUF, &uval, ulen, async);
	assert(err == 1);

	/* Wait for the CQE */
	err = receive_cqe(ring);
	if (err == -EOPNOTSUPP)
		return T_EXIT_SKIP;
	if (err < 0) {
		fprintf(stderr, "Error received. %d\n", err);
		return T_EXIT_FAIL;
	}
	/* The output of CQE->res contains the length */
	ulen = err;

	/* Executes the same operation using system call */
	err = getsockopt(sockfds->rx, SOL_SOCKET, SO_RCVBUF, &sval, &slen);
	assert(err == 0);

	/* Make sure that io_uring operation returns the same value as the systemcall */
	assert(ulen == slen);
	assert(uval == sval);

	return T_EXIT_PASS;
}

/*
 * Run getsock operation using SO_PEERNAME using io_uring cmd operation
 * and getsockopt(2) and compare the results.
 */
static int run_get_peername(struct io_uring *ring, struct fds *sockfds, bool async)
{
	struct sockaddr sval, uval = {};
	socklen_t slen = sizeof(sval);
	socklen_t ulen = sizeof(uval);
	int err;

	/* Get values from the systemcall */
	err = getsockopt(sockfds->tx, SOL_SOCKET, SO_PEERNAME, &sval, &slen);
	assert(err == 0);

	/* Getting SO_PEERNAME */
	err = submit_cmd_sqe(ring, sockfds->rx, SOCKET_URING_OP_GETSOCKOPT,
			     SOL_SOCKET, SO_PEERNAME, &uval, ulen, async);
	assert(err == 1);

	/* Wait for the CQE */
	err = receive_cqe(ring);
	if (err == -EOPNOTSUPP || err == -EINVAL) {
		no_sock_opt = 1;
		return T_EXIT_SKIP;
	}

	if (err < 0) {
		fprintf(stderr, "%s: Error in the CQE: %d\n", __func__, err);
		return T_EXIT_FAIL;
	}

	/* The length comes from cqe->res, which is returned from receive_cqe() */
	ulen = err;

	/* Make sure that io_uring operation returns the same values as the systemcall */
	assert(sval.sa_family == uval.sa_family);
	assert(slen == ulen);

	return T_EXIT_PASS;
}

/*
 * Run getsockopt tests. Basically comparing io_uring output and systemcall results
 */
static int run_getsockopt_test(struct io_uring *ring, struct fds *sockfds)
{
	int err;

	err = run_get_peername(ring, sockfds, false);
	if (err)
		return err;

	err = run_get_peername(ring, sockfds, true);
	if (err)
		return err;

	err = run_get_rcvbuf(ring, sockfds, false);
	if (err)
		return err;

	return run_get_rcvbuf(ring, sockfds, true);
}

/*
 * Given a `val` value, set it in SO_REUSEPORT using io_uring cmd, and read using
 * getsockopt(2), and make sure they match.
 */
static int run_setsockopt_reuseport(struct io_uring *ring, struct fds *sockfds,
				    int val, bool async)
{
	unsigned int slen, ulen;
	int sval, uval = val;
	int err;

	slen = sizeof(sval);
	ulen = sizeof(uval);

	/* Setting SO_REUSEPORT */
	err = submit_cmd_sqe(ring, sockfds->rx, SOCKET_URING_OP_SETSOCKOPT,
			     SOL_SOCKET, SO_REUSEPORT, &uval, ulen, async);
	assert(err == 1);

	err = receive_cqe(ring);
	if (err == -EOPNOTSUPP)
		return T_EXIT_SKIP;

	/* Get values from the systemcall */
	err = getsockopt(sockfds->rx, SOL_SOCKET, SO_REUSEPORT, &sval, &slen);
	assert(err == 0);

	/* Make sure the set using io_uring cmd matches what systemcall returns */
	assert(uval == sval);
	assert(ulen == slen);

	return T_EXIT_PASS;
}

/*
 * Given a `val` value, set the TCP_USER_TIMEOUT using io_uring and read using
 * getsockopt(2). Make sure they match
 */
static int run_setsockopt_usertimeout(struct io_uring *ring, struct fds *sockfds,
				      int val, bool async)
{
	int optname = TCP_USER_TIMEOUT;
	int level = IPPROTO_TCP;
	unsigned int slen, ulen;
	int sval, uval, err;

	slen = sizeof(uval);
	ulen = sizeof(uval);

	uval = val;

	/* Setting timeout */
	err = submit_cmd_sqe(ring, sockfds->rx, SOCKET_URING_OP_SETSOCKOPT,
			     level, optname, &uval, ulen, async);
	assert(err == 1);

	err = receive_cqe(ring);
	if (err == -EOPNOTSUPP)
		return T_EXIT_SKIP;
	if (err < 0) {
		fprintf(stderr, "%s: Got an error: %d\n", __func__, err);
		return T_EXIT_FAIL;
	}

	/* Get the value from the systemcall, to make sure it was set */
	err = getsockopt(sockfds->rx, level, optname, &sval, &slen);
	assert(err == 0);
	assert(uval == sval);

	return T_EXIT_PASS;
}

/* Test setsockopt() for SOL_SOCKET */
static int run_setsockopt_test(struct io_uring *ring, struct fds *sockfds)
{
	int err, i;
	int j;

	for (j = 0; j < 2; j++) {
		bool async = j & 1;

		for (i = 0; i <= 1; i++) {
			err = run_setsockopt_reuseport(ring, sockfds, i, async);
			if (err)
				return err;
		}

		for (i = 1; i <= 10; i++) {
			err = run_setsockopt_usertimeout(ring, sockfds, i, async);
			if (err)
				return err;
		}
	}

	return err;
}

/* Send data through the sockets */
static void send_data(struct fds *s)
{
	int written_bytes;
	/* Send data sing the sockstruct->send */
	written_bytes = write(s->tx, MSG, strlen(MSG));
	assert(written_bytes == strlen(MSG));
}

int main(int argc, char *argv[])
{
	struct fds sockfds;
	struct io_uring ring;
	int err;

	if (argc > 1)
		return T_EXIT_SKIP;

	/* Simply io_uring ring creation */
	ring = create_ring();

	/* Create sockets */
	sockfds = create_sockets();

	send_data(&sockfds);

	err = run_getsockopt_test(&ring, &sockfds);
	if (err) {
		if (err == T_EXIT_SKIP) {
			fprintf(stderr, "Skipping tests.\n");
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "Failed to run test: %d\n", err);
		return err;
	}
	if (no_sock_opt)
		return T_EXIT_SKIP;

	err = run_setsockopt_test(&ring, &sockfds);
	if (err) {
		if (err == T_EXIT_SKIP) {
			fprintf(stderr, "Skipping tests.\n");
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "Failed to run test: %d\n", err);
		return err;
	}

	io_uring_queue_exit(&ring);
	return err;
}
