/* SPDX-License-Identifier: MIT */
/*
 * Configure and operate a TCP socket solely with io_uring.
 */
#include <stdio.h>
#include <string.h>
#include <liburing.h>
#include <err.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <netinet/ip.h>
#include "liburing.h"
#include "helpers.h"

static void msec_to_ts(struct __kernel_timespec *ts, unsigned int msec)
{
        ts->tv_sec = msec / 1000;
        ts->tv_nsec = (msec % 1000) * 1000000;
}

static const char *magic = "Hello World!";
static bool no_getsockname = false;

enum {
	SRV_INDEX = 0,
	CLI_INDEX,
	CONN_INDEX,
};

static int connect_client(struct io_uring *ring, unsigned short peer_port)
{
	struct __kernel_timespec ts;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int head, ret, submitted = 0;
	struct sockaddr_in peer_addr;
 	socklen_t addr_len = sizeof(peer_addr);

	peer_addr.sin_family = AF_INET;
	peer_addr.sin_port = peer_port;
	peer_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_socket_direct(sqe, AF_INET, SOCK_STREAM, 0,
				    CLI_INDEX, 0);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_connect(sqe, CLI_INDEX, (struct sockaddr*) &peer_addr, addr_len);
	sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_send(sqe, CLI_INDEX, magic, strlen(magic), 0);
	sqe->flags |= IOSQE_FIXED_FILE;

	submitted = ret = io_uring_submit(ring);
	if (ret < 0)
		return T_SETUP_SKIP;

	msec_to_ts(&ts, 300);
	ret = io_uring_wait_cqes(ring, &cqe, submitted, &ts, NULL);
	if (ret < 0)
		return T_SETUP_SKIP;

	io_uring_for_each_cqe(ring, head, cqe) {
		ret = cqe->res;
		if (ret < 0)
			return T_SETUP_SKIP;
	} io_uring_cq_advance(ring, submitted);

	return T_SETUP_OK;
}

/*
 * getsockname was added to the kernel a few releases after bind/listen.
 * In order to provide a backward-compatible test, fallback to
 * non-io-uring if we are on an older kernel, allowing the test to
 * continue.
 */
static int do_getsockname(struct io_uring *ring, int direct_socket,
			  int peer, struct sockaddr *saddr,
			  socklen_t *saddr_len)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int res = 0, fd;

	if (!no_getsockname) {
		/* attempt io_uring. Command might not exist */
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_cmd_getsockname(sqe, direct_socket,
					      saddr, saddr_len, peer);
		sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
		io_uring_submit(ring);
		io_uring_wait_cqe(ring, &cqe);
		res = cqe->res;
		io_uring_cqe_seen(ring, cqe);
	}

	if (no_getsockname || res == -ENOTSUP) {
		/*
		 * Older kernel.  install the fd and use the getsockname
		 * syscall.
		 */
		no_getsockname = true;

		sqe = io_uring_get_sqe(ring);
		io_uring_prep_fixed_fd_install(sqe, direct_socket, 0);
		io_uring_submit(ring);
		io_uring_wait_cqe(ring, &cqe);
		fd = cqe->res;
		io_uring_cqe_seen(ring, cqe);

		if (fd < 0) {
			fprintf(stderr, "installing direct fd failed. %d\n",
				cqe->res);
			return T_EXIT_FAIL;
		}
		if (peer)
			res = getpeername(fd, saddr, saddr_len);
		else
			res = getsockname(fd, saddr, saddr_len);

		if (res) {
			fprintf(stderr, "get%sname syscall failed. %d\n",
				peer? "peer":"sock", errno);
			return T_EXIT_FAIL;
		}
		close(fd);
	} else if (res < 0) {
		fprintf(stderr, "getsockname server failed. %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	return 0;
}

static int setup_srv(struct io_uring *ring)
{
	struct sockaddr_in server_addr;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct __kernel_timespec ts;
	int ret, val, submitted;
	unsigned head;

	memset(&server_addr, 0, sizeof(struct sockaddr_in));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(0);
	server_addr.sin_addr.s_addr = htons(INADDR_ANY);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_socket_direct(sqe, AF_INET, SOCK_STREAM, 0, SRV_INDEX, 0);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	val = 1;
	io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_SETSOCKOPT, 0, SOL_SOCKET,
			       SO_REUSEADDR, &val, sizeof(val));
	sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_bind(sqe, SRV_INDEX, (struct sockaddr *) &server_addr,
			   sizeof(struct sockaddr_in));
	sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_listen(sqe, SRV_INDEX, 1);
	sqe->flags |= IOSQE_FIXED_FILE;

	submitted = ret = io_uring_submit(ring);
	if (ret < 0) {
		fprintf(stderr, "submission failed. %d\n", ret);
		return T_EXIT_FAIL;
	}

	msec_to_ts(&ts, 300);
	ret = io_uring_wait_cqes(ring, &cqe, ret, &ts, NULL);
	if (ret < 0) {
		fprintf(stderr, "submission failed. %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_for_each_cqe(ring, head, cqe) {
		ret = cqe->res;
		if (ret < 0) {
			fprintf(stderr, "Server startup failed. step %d got %d \n", head, ret);
			return T_EXIT_FAIL;
		}
	} io_uring_cq_advance(ring, submitted);

	return T_SETUP_OK;
}

static int test_good_server(unsigned int ring_flags)
{
	struct sockaddr_in saddr = {};
	socklen_t saddr_len = sizeof(saddr);
	struct __kernel_timespec ts;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, port;
	int fds[3];
	char buf[1024];

	memset(fds, -1, sizeof(fds));

	ret = t_create_ring(10, &ring, ring_flags | IORING_SETUP_SUBMIT_ALL);
	if (ret < 0) {
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return T_SETUP_SKIP;
	}

	ret = io_uring_register_files(&ring, fds, 3);
	if (ret) {
		fprintf(stderr, "server file register %d\n", ret);
		return T_SETUP_SKIP;
	}

	ret = setup_srv(&ring);
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "srv startup failed.\n");
		return T_EXIT_FAIL;
	}

	if (do_getsockname(&ring, SRV_INDEX, 0, (struct sockaddr*) &saddr,
			   &saddr_len))
		return T_EXIT_FAIL;

	if (connect_client(&ring, saddr.sin_port) != T_SETUP_OK) {
		fprintf(stderr, "cli startup failed.\n");
		return T_SETUP_SKIP;
	}

	/* Wait for a connection */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_accept_direct(sqe, SRV_INDEX, NULL, NULL, 0, CONN_INDEX);
	sqe->flags |= IOSQE_FIXED_FILE;

	io_uring_submit(&ring);
	io_uring_wait_cqe(&ring, &cqe);
	if (cqe->res < 0) {
		fprintf(stderr, "accept failed. %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	/* Test that getsockname on the peer (getpeername) yields a
	 * sane result.
	 */
	port = saddr.sin_port;
	saddr.sin_port = 0;
	if (do_getsockname(&ring, CLI_INDEX, 1,
			   (struct sockaddr*)&saddr, &saddr_len))
		return T_EXIT_FAIL;

	if (saddr.sin_addr.s_addr != htonl(INADDR_LOOPBACK) ||
	    saddr.sin_port != port) {
		fprintf(stderr, "getsockname peer got wrong address: %s:%d\n",
			inet_ntoa(saddr.sin_addr), saddr.sin_port);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recv(sqe, CONN_INDEX, buf, sizeof(buf), 0);
	sqe->flags |= IOSQE_FIXED_FILE;

	io_uring_submit(&ring);
	io_uring_wait_cqe_timeout(&ring, &cqe, &ts);

	if (cqe->res < 0) {
		fprintf(stderr, "bad receive cqe. %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	ret = cqe->res;
	io_uring_cqe_seen(&ring, cqe);

	io_uring_queue_exit(&ring);

	if (ret != strlen(magic) || strncmp(buf, magic, ret)) {
		fprintf(stderr, "didn't receive expected string. Got %d '%s'\n", ret, buf);
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

static int test_bad_bind(void)
{
	struct sockaddr_in server_addr;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int sock = -1, err;
	int ret = T_EXIT_FAIL;

	memset(&server_addr, 0, sizeof(struct sockaddr_in));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(9001);
	server_addr.sin_addr.s_addr = htons(INADDR_ANY);

	err = t_create_ring(1, &ring, 0);
	if (err < 0) {
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return T_SETUP_SKIP;
	}

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		perror("socket");
		goto fail;
	}

	/* Bind with size 0 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_bind(sqe, sock, (struct sockaddr *) &server_addr, 0);
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;

	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;

	if (cqe->res != -EINVAL)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	/* Bind with bad fd */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_bind(sqe, 0, (struct sockaddr *) &server_addr,  sizeof(struct sockaddr_in));
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;

	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;
	if (cqe->res != -ENOTSOCK)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	ret = T_EXIT_PASS;

	/* bind with weird value */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_bind(sqe, sock, (struct sockaddr *) &server_addr,  sizeof(struct sockaddr_in));
	sqe->rw_flags = 1;
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;

	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;
	if (cqe->res != -EINVAL)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	ret = T_EXIT_PASS;

fail:
	io_uring_queue_exit(&ring);
	if (sock != -1)
		close(sock);
	return ret;
}

static int test_bad_listen(void)
{
	struct sockaddr_in server_addr;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int sock = -1, err;
	int ret = T_EXIT_FAIL;

	memset(&server_addr, 0, sizeof(struct sockaddr_in));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(8001);
	server_addr.sin_addr.s_addr = htons(INADDR_ANY);

	err = t_create_ring(1, &ring, 0);
	if (err < 0) {
		fprintf(stderr, "queue_init: %d\n", err);
		return T_SETUP_SKIP;
	}

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		perror("socket");
		goto fail;
	}

	err = t_bind_ephemeral_port(sock, &server_addr);
	if (err) {
		fprintf(stderr, "bind: %s\n", strerror(-err));
		goto fail;
	}

	/* listen on bad sock */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_listen(sqe, 0, 1);
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;

	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;

	if (cqe->res != -ENOTSOCK)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	/* listen with weird parameters */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_listen(sqe, sock, 1);
	sqe->addr2 = 0xffffff;
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;

	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;

	if (cqe->res != -EINVAL)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	ret = T_EXIT_PASS;
fail:
	io_uring_queue_exit(&ring);
	if (sock != -1)
		close(sock);
	return ret;
}

static int test_bad_sockname(void)
{
	struct sockaddr_in saddr;
	socklen_t saddr_len;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int sock = -1, err;
	int ret = T_EXIT_FAIL;

	memset(&saddr, 0, sizeof(struct sockaddr_in));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(8001);
	saddr.sin_addr.s_addr = htons(INADDR_ANY);

	err = t_create_ring(1, &ring, 0);
	if (err < 0) {
		fprintf(stderr, "queue_init: %d\n", err);
		return T_SETUP_SKIP;
	}

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		perror("socket");
		goto fail;
	}

	err = t_bind_ephemeral_port(sock, &saddr);
	if (err) {
		fprintf(stderr, "bind: %s\n", strerror(-err));
		goto fail;
	}

	/* getsockname on a !socket fd.  with getsockname(2), this would
	 * return -ENOTSOCK, but we can't do it in an io_uring_cmd.
	 */
	sqe = io_uring_get_sqe(&ring);
	saddr_len = sizeof(saddr);
	io_uring_prep_cmd_getsockname(sqe, 1, (struct sockaddr*)&saddr, &saddr_len, 0);
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;
	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;
	if (cqe->res != -ENOTSUP)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	/* getsockname with weird parameters */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_cmd_getsockname(sqe, sock, (struct sockaddr*)&saddr,
				      &saddr_len, 3);
	err = io_uring_submit(&ring);
	if (err < 0)
		goto fail;
	err = io_uring_wait_cqe(&ring, &cqe);
	if (err)
		goto fail;
	if (cqe->res != -EINVAL)
		goto fail;
	io_uring_cqe_seen(&ring, cqe);

	ret = T_EXIT_PASS;
fail:
	io_uring_queue_exit(&ring);
	if (sock != -1)
		close(sock);
	return ret;
}

int main(int argc, char *argv[])
{
	struct io_uring_probe *probe;
	int ret;

	if (argc > 1)
		return 0;

	/*
	 * This test is not supported on older kernels. Check for
	 * OP_LISTEN, since that is the last feature required to support
	 * it.
	 */
	probe = io_uring_get_probe();
	if (!probe)
		return T_EXIT_SKIP;
	if (!io_uring_opcode_supported(probe, IORING_OP_LISTEN))
		return T_EXIT_SKIP;

	ret = test_good_server(0);
	if (ret) {
		fprintf(stderr, "good 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_good_server(IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN);
	if (ret) {
		fprintf(stderr, "good defer failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_good_server(IORING_SETUP_SQPOLL);
	if (ret) {
		fprintf(stderr, "good sqpoll failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_bad_bind();
	if (ret) {
		fprintf(stderr, "bad bind failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_bad_listen();
	if (ret) {
		fprintf(stderr, "bad listen failed\n");
		return T_EXIT_FAIL;
	}
	if (!no_getsockname) {
		ret = test_bad_sockname();
		if (ret) {
			fprintf(stderr, "bad sockname failed\n");
			return T_EXIT_FAIL;
		}
	}
	return T_EXIT_PASS;
}
