/* SPDX-License-Identifier: MIT */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>

#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <linux/ipv6.h>
#include <linux/socket.h>
#include <linux/sockios.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <netinet/in.h>
#include <netinet/ip6.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <linux/mman.h>

#include "liburing.h"
#include "helpers.h"

#define MAX_MSG	128

#define HOST	"127.0.0.1"
#define HOSTV6	"::1"

#define MAX_IOV 32
#define CORK_REQS 5
#define RX_TAG 10000
#define BUFFER_OFFSET 41

#ifndef ARRAY_SIZE
	#define ARRAY_SIZE(a) (sizeof(a)/sizeof((a)[0]))
#endif

enum {
	BUF_T_NORMAL,
	BUF_T_SMALL,
	BUF_T_NONALIGNED,
	BUF_T_LARGE,
	BUF_T_HUGETLB,

	__BUF_NR,
};

/* 32MB, should be enough to trigger a short send */
#define LARGE_BUF_SIZE		(1U << 25)

static size_t page_sz;
static char *tx_buffer, *rx_buffer;
static struct iovec buffers_iov[__BUF_NR];

static bool has_regvec;
static bool has_sendzc;
static bool has_sendmsg;
static bool hit_enomem;
static bool try_hugepages = 1;
static bool no_send_vec;

static int probe_zc_support(void)
{
	struct io_uring ring;
	struct io_uring_probe *p;
	int ret;

	has_sendzc = has_sendmsg = false;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret)
		return -1;

	p = t_calloc(1, sizeof(*p) + 256 * sizeof(struct io_uring_probe_op));
	if (!p)
		return -1;

	ret = io_uring_register_probe(&ring, p, 256);
	if (ret)
		return -1;

	has_sendzc = p->ops_len > IORING_OP_SEND_ZC;
	has_sendmsg = p->ops_len > IORING_OP_SENDMSG_ZC;
	has_regvec = p->ops_len > IORING_OP_READV_FIXED;
	io_uring_queue_exit(&ring);
	free(p);
	return 0;
}

static bool check_cq_empty(struct io_uring *ring)
{
	struct io_uring_cqe *cqe = NULL;
	int ret;

	ret = io_uring_peek_cqe(ring, &cqe); /* nothing should be there */
	return ret == -EAGAIN;
}

static int test_basic_send(struct io_uring *ring, int sock_tx, int sock_rx,
			   int vec)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct iovec vecs[2];
	int msg_flags = 0;
	unsigned zc_flags = 0;
	int payload_size = 100;
	int ret;

	if (vec && no_send_vec)
		return 0;

	sqe = io_uring_get_sqe(ring);
	if (vec) {
		size_t split = payload_size / 2;

		vecs[0].iov_base = tx_buffer;
		vecs[0].iov_len = split;
		vecs[1].iov_base = tx_buffer + split;
		vecs[1].iov_len = split;
		io_uring_prep_send_zc(sqe, sock_tx, vecs, 2, msg_flags, zc_flags);
		sqe->ioprio |= (1U << 5);
	} else {
		io_uring_prep_send_zc(sqe, sock_tx, tx_buffer, payload_size,
				      msg_flags, zc_flags);
	}
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	assert(ret == 1);

	ret = io_uring_wait_cqe(ring, &cqe);
	assert(!ret && cqe->user_data == 1);
	if (cqe->res != payload_size) {
		if (cqe->res == -EINVAL && vec) {
			int has_more = cqe->flags & IORING_CQE_F_MORE;

			no_send_vec = 1;
			io_uring_cqe_seen(ring, cqe);
			if (has_more) {
				ret = io_uring_peek_cqe(ring, &cqe);
				if (ret) {
					fprintf(stderr, "peek failed\n");
					return T_EXIT_FAIL;
				}
				io_uring_cqe_seen(ring, cqe);
			}
			return T_EXIT_PASS;
		}
		fprintf(stderr, "send failed %i\n", cqe->res);
		return T_EXIT_FAIL;
	}

	assert(cqe->flags & IORING_CQE_F_MORE);
	io_uring_cqe_seen(ring, cqe);

	ret = io_uring_wait_cqe(ring, &cqe);
	assert(!ret);
	assert(cqe->user_data == 1);
	assert(cqe->flags & IORING_CQE_F_NOTIF);
	assert(!(cqe->flags & IORING_CQE_F_MORE));
	io_uring_cqe_seen(ring, cqe);
	assert(check_cq_empty(ring));

	ret = recv(sock_rx, rx_buffer, payload_size, MSG_TRUNC);
	assert(ret == payload_size);
	return T_EXIT_PASS;
}

static int test_send_faults_check(struct io_uring *ring, int expected)
{
	struct io_uring_cqe *cqe;
	int ret, nr_cqes = 0;
	bool more = true;

	while (more) {
		nr_cqes++;
		ret = io_uring_wait_cqe(ring, &cqe);
		assert(!ret);
		assert(cqe->user_data == 1);

		if (nr_cqes == 1 && (cqe->flags & IORING_CQE_F_NOTIF)) {
			fprintf(stderr, "test_send_faults_check notif came first\n");
			return -1;
		}

		if (!(cqe->flags & IORING_CQE_F_NOTIF)) {
			if (cqe->res != expected) {
				fprintf(stderr, "invalid cqe res %i vs expected %i, "
					"user_data %i\n",
					cqe->res, expected, (int)cqe->user_data);
				return -1;
			}
		} else {
			if (cqe->res != 0 || cqe->flags != IORING_CQE_F_NOTIF) {
				fprintf(stderr, "invalid notif cqe %i %i\n",
					cqe->res, cqe->flags);
				return -1;
			}
		}

		more = cqe->flags & IORING_CQE_F_MORE;
		io_uring_cqe_seen(ring, cqe);
	}

	if (nr_cqes > 2) {
		fprintf(stderr, "test_send_faults_check() too many CQEs %i\n",
				nr_cqes);
		return -1;
	}
	assert(check_cq_empty(ring));
	return 0;
}

static int test_send_faults(int sock_tx, int sock_rx)
{
	struct io_uring_sqe *sqe;
	int msg_flags = 0;
	unsigned zc_flags = 0;
	int ret, payload_size = 100;
	struct io_uring ring;

	ret = io_uring_queue_init(32, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		return -1;
	}

	/* invalid buffer */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_send_zc(sqe, sock_tx, (void *)1UL, payload_size,
			      msg_flags, zc_flags);
	sqe->user_data = 1;
	ret = io_uring_submit(&ring);
	assert(ret == 1);

	ret = test_send_faults_check(&ring, -EFAULT);
	if (ret) {
		fprintf(stderr, "test_send_faults with invalid buf failed\n");
		return -1;
	}

	/* invalid address */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_send_zc(sqe, sock_tx, tx_buffer, payload_size,
			      msg_flags, zc_flags);
	io_uring_prep_send_set_addr(sqe, (const struct sockaddr *)1UL,
				    sizeof(struct sockaddr_in6));
	sqe->user_data = 1;
	ret = io_uring_submit(&ring);
	assert(ret == 1);

	ret = test_send_faults_check(&ring, -EFAULT);
	if (ret) {
		fprintf(stderr, "test_send_faults with invalid addr failed\n");
		return -1;
	}

	/* invalid send/recv flags */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_send_zc(sqe, sock_tx, tx_buffer, payload_size,
			      msg_flags, ~0U);
	sqe->user_data = 1;
	ret = io_uring_submit(&ring);
	assert(ret == 1);

	ret = test_send_faults_check(&ring, -EINVAL);
	if (ret) {
		fprintf(stderr, "test_send_faults with invalid flags failed\n");
		return -1;
	}

	return T_EXIT_PASS;
}

struct send_conf {
	bool fixed_buf;
	bool mix_register;
	bool cork;
	bool force_async;
	bool use_sendmsg;
	bool tcp;
	bool zc;
	bool iovec;
	bool long_iovec;
	bool poll_first;
	int buf_index;
	struct sockaddr_storage *addr;
};

static int do_test_inet_send(struct io_uring *ring, int sock_client, int sock_server,
			     struct send_conf *conf)
{
	struct iovec iov[MAX_IOV];
	struct msghdr msghdr[CORK_REQS];
	const unsigned zc_flags = 0;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int nr_reqs = conf->cork ? CORK_REQS : 1;
	int i, ret, nr_cqes, addr_len = 0;
	size_t send_size = buffers_iov[conf->buf_index].iov_len;
	size_t chunk_size = send_size / nr_reqs;
	size_t chunk_size_last = send_size - chunk_size * (nr_reqs - 1);
	char *buf = buffers_iov[conf->buf_index].iov_base;

	assert(MAX_IOV >= CORK_REQS);

	if (conf->addr) {
		sa_family_t fam = ((struct sockaddr_in *)conf->addr)->sin_family;

		addr_len = (fam == AF_INET) ? sizeof(struct sockaddr_in) :
					      sizeof(struct sockaddr_in6);
	}

	memset(rx_buffer, 0, send_size);

	for (i = 0; i < nr_reqs; i++) {
		bool real_fixed_buf = conf->fixed_buf;
		size_t cur_size = chunk_size;
		int msg_flags = MSG_WAITALL;

		if (conf->mix_register)
			real_fixed_buf = rand() & 1;

		if (i != nr_reqs - 1)
			msg_flags |= MSG_MORE;
		else
			cur_size = chunk_size_last;

		sqe = io_uring_get_sqe(ring);

		if (!conf->use_sendmsg) {
			if (conf->zc) {
				io_uring_prep_send_zc(sqe, sock_client, buf + i * chunk_size,
						      cur_size, msg_flags, zc_flags);
			} else {
				io_uring_prep_send(sqe, sock_client, buf + i * chunk_size,
						      cur_size, msg_flags);
			}

			if (real_fixed_buf) {
				sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
				sqe->buf_index = conf->buf_index;
			}
			if (conf->addr)
				io_uring_prep_send_set_addr(sqe, (const struct sockaddr *)conf->addr,
							    addr_len);
		} else {
			struct iovec *io;
			int iov_len;

			if (conf->zc)
				io_uring_prep_sendmsg_zc(sqe, sock_client, &msghdr[i], msg_flags);
			else
				io_uring_prep_sendmsg(sqe, sock_client, &msghdr[i], msg_flags);

			if (real_fixed_buf) {
				sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
				sqe->buf_index = conf->buf_index;
			}

			if (!conf->iovec) {
				io = &iov[i];
				iov_len = 1;
				iov[i].iov_len = cur_size;
				iov[i].iov_base = buf + i * chunk_size;
			} else {
				char *it = buf;
				int j;

				assert(nr_reqs == 1);
				iov_len = conf->long_iovec ? MAX_IOV : 4;
				io = iov;

				for (j = 0; j < iov_len; j++)
					io[j].iov_len = 1;
				/* first want to be easily advanced */
				io[0].iov_base = it;
				it += io[0].iov_len;
				/* this should cause retry */
				io[1].iov_len = chunk_size - iov_len + 1;
				io[1].iov_base = it;
				it += io[1].iov_len;
				/* fill the rest */
				for (j = 2; j < iov_len; j++) {
					io[j].iov_base = it;
					it += io[j].iov_len;
				}
			}

			memset(&msghdr[i], 0, sizeof(msghdr[i]));
			msghdr[i].msg_iov = io;
			msghdr[i].msg_iovlen = iov_len;
			if (conf->addr) {
				msghdr[i].msg_name = conf->addr;
				msghdr[i].msg_namelen = addr_len;
			}
		}
		sqe->user_data = i;
		if (conf->force_async)
			sqe->flags |= IOSQE_ASYNC;
		if (conf->poll_first)
			sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
		if (i != nr_reqs - 1)
			sqe->flags |= IOSQE_IO_LINK;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv(sqe, sock_server, rx_buffer, send_size, MSG_WAITALL);
	sqe->user_data = RX_TAG;

	ret = io_uring_submit(ring);
	if (ret != nr_reqs + 1) {
		fprintf(stderr, "submit failed, got %i expected %i\n", ret, nr_reqs);
		return 1;
	}

	nr_cqes = nr_reqs + 1;
	for (i = 0; i < nr_cqes; i++) {
		int expected = chunk_size;

		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
			return 1;
		}
		if (cqe->user_data == RX_TAG) {
			if (cqe->res != send_size) {
				fprintf(stderr, "rx failed res: %i, expected %i\n",
						cqe->res, (int)send_size);
				return 1;
			}
			io_uring_cqe_seen(ring, cqe);
			continue;
		}
		if ((cqe->flags & IORING_CQE_F_MORE) && (cqe->flags & IORING_CQE_F_NOTIF)) {
			fprintf(stderr, "unexpected cflags %i res %i\n",
					cqe->flags, cqe->res);
			return 1;
		}
		if (cqe->user_data >= nr_reqs) {
			fprintf(stderr, "invalid user_data %lu\n",
					(unsigned long)cqe->user_data);
			return 1;
		}
		if (!(cqe->flags & IORING_CQE_F_NOTIF)) {
			if (cqe->flags & IORING_CQE_F_MORE)
				nr_cqes++;
			if (cqe->user_data == nr_reqs - 1)
				expected = chunk_size_last;
			if (cqe->res != expected) {
				if (cqe->res == -ENOMEM) {
					if (!hit_enomem) {
						fprintf(stderr, "Hit -ENOMEM. "
							"Increase ulimit -l "
							"limit for a complete "
							"test run. Skipping "
							"parts.\n");
						hit_enomem = 1;
					}
					return 0;
				}
				fprintf(stderr, "invalid cqe->res %d expected %d\n",
						 cqe->res, expected);
				return 1;
			}
		}
		io_uring_cqe_seen(ring, cqe);
	}

	for (i = 0; i < send_size; i++) {
		if (buf[i] != rx_buffer[i]) {
			fprintf(stderr, "botched data, first mismated byte %i, "
				"%u vs %u\n", i, buf[i], rx_buffer[i]);
			return 1;
		}
	}
	return 0;
}

static int create_socketpair_ip(struct sockaddr_storage *addr,
				int *sock_client, int *sock_server,
				bool ipv6, bool client_connect,
				bool msg_zc, bool tcp)
{
	return t_create_socketpair_ip(addr, sock_client, sock_server, ipv6,
					client_connect, msg_zc, tcp,
					ipv6 ? HOSTV6 : HOST);
}

static int test_inet_send(struct io_uring *ring)
{
	struct send_conf conf;
	struct sockaddr_storage addr;
	int sock_client = -1, sock_server = -1;
	int ret, j, i;
	int buf_index;

	for (j = 0; j < 32; j++) {
		bool ipv6 = j & 1;
		bool client_connect = j & 2;
		bool msg_zc_set = j & 4;
		bool tcp = j & 8;
		bool swap_sockets = j & 16;

		if (tcp && !client_connect)
			continue;
		if (swap_sockets && !tcp)
			continue;
#ifndef SO_ZEROCOPY
		if (msg_zc_set)
			continue;
#endif
		ret = create_socketpair_ip(&addr, &sock_client, &sock_server, ipv6,
					client_connect, msg_zc_set, tcp);
		if (ret) {
			fprintf(stderr, "sock prep failed %d\n", ret);
			return 1;
		}
		if (swap_sockets) {
			int tmp_sock = sock_client;

			sock_client = sock_server;
			sock_server = tmp_sock;
		}

		for (i = 0; i < 1024; i++) {
			bool regbuf;

			conf.use_sendmsg = i & 1;
			conf.poll_first = i & 2;
			conf.fixed_buf = i & 4;
			conf.addr = (i & 8) ? &addr : NULL;
			conf.cork = i & 16;
			conf.mix_register = i & 32;
			conf.force_async = i & 64;
			conf.zc = i & 128;
			conf.iovec = i & 256;
			conf.long_iovec = i & 512;
			conf.tcp = tcp;
			regbuf = conf.mix_register || conf.fixed_buf;

			if (!tcp && conf.long_iovec)
				continue;
			if (conf.use_sendmsg && regbuf && !has_regvec)
				continue;
			if (conf.iovec && (!conf.use_sendmsg || conf.cork))
				continue;
			if (!conf.zc) {
				if (regbuf)
					continue;
				/*
				* Non zerocopy send w/ addr was added together with sendmsg_zc,
				* skip if we the kernel doesn't support it.
				*/
				if (conf.addr && !has_sendmsg)
					continue;
			}
			if (tcp && (conf.cork || conf.addr))
				continue;
			if (conf.mix_register && (!conf.cork || conf.fixed_buf))
				continue;
			if (!client_connect && conf.addr == NULL)
				continue;
			if (conf.use_sendmsg && !has_sendmsg)
				continue;
			if (msg_zc_set && !conf.zc)
				continue;

			for (buf_index = 0; buf_index < ARRAY_SIZE(buffers_iov); buf_index++) {
				size_t len = buffers_iov[buf_index].iov_len;

				if (!buffers_iov[buf_index].iov_base)
					continue;
				/* UDP IPv4 max datagram size is under 64K */
				if (!tcp && len > (1U << 15))
					continue;

				conf.buf_index = buf_index;
				ret = do_test_inet_send(ring, sock_client, sock_server, &conf);
				if (ret) {
					fprintf(stderr, "send failed fixed buf %i, "
							"conn %i, addr %i, cork %i\n",
						conf.fixed_buf, client_connect,
						!!conf.addr, conf.cork);
					return 1;
				}
			}
		}

		close(sock_client);
		close(sock_server);
	}
	return 0;
}

static int test_async_addr(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct sockaddr_storage addr;
	int sock_tx = -1, sock_rx = -1;
	struct __kernel_timespec ts;
	int ret;

	ts.tv_sec = 1;
	ts.tv_nsec = 0;
	ret = create_socketpair_ip(&addr, &sock_tx, &sock_rx, true, false, false, false);
	if (ret) {
		fprintf(stderr, "sock prep failed %d\n", ret);
		return 1;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_timeout(sqe, &ts, 0, IORING_TIMEOUT_ETIME_SUCCESS);
	sqe->user_data = 1;
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_send_zc(sqe, sock_tx, tx_buffer, 1, 0, 0);
	sqe->user_data = 2;
	io_uring_prep_send_set_addr(sqe, (const struct sockaddr *)&addr,
				    sizeof(struct sockaddr_in6));

	ret = io_uring_submit(ring);
	assert(ret == 2);
	memset(&addr, 0, sizeof(addr));

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
		return 1;
	}
	if (cqe->user_data != 1 || cqe->res != -ETIME) {
		fprintf(stderr, "invalid timeout res %i %i\n",
			(int)cqe->user_data, cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
		return 1;
	}
	if (cqe->user_data != 2 || cqe->res != 1) {
		fprintf(stderr, "invalid send %i %i\n",
			(int)cqe->user_data, cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);
	ret = recv(sock_rx, rx_buffer, 1, MSG_TRUNC);
	assert(ret == 1);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
		return 1;
	}
	assert(cqe->flags & IORING_CQE_F_NOTIF);
	io_uring_cqe_seen(ring, cqe);

	close(sock_tx);
	close(sock_rx);
	return 0;
}

static int test_sendzc_report(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct sockaddr_storage addr;
	int sock_tx, sock_rx;
	int ret;

	ret = create_socketpair_ip(&addr, &sock_tx, &sock_rx, true, true, false, true);
	if (ret) {
		fprintf(stderr, "sock prep failed %d\n", ret);
		return 1;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_send_zc(sqe, sock_tx, tx_buffer, 1, 0,
				IORING_SEND_ZC_REPORT_USAGE);
	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "io_uring_submit failed %i\n", ret);
		return 1;
	}
	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
		return 1;
	}
	if (cqe->res != 1 && cqe->res != -EINVAL) {
		fprintf(stderr, "sendzc report failed %u\n", cqe->res);
		return 1;
	}
	if (!(cqe->flags & IORING_CQE_F_MORE)) {
		fprintf(stderr, "expected notification %i\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "io_uring_wait_cqe failed %i\n", ret);
		return 1;
	}
	if (cqe->flags & IORING_CQE_F_MORE) {
		fprintf(stderr, "F_MORE after notification\n");
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	close(sock_tx);
	close(sock_rx);
	return 0;
}

/* see also send_recv.c:test_invalid */
static int test_invalid_zc(int fds[2])
{
	struct io_uring ring;
	int ret;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	bool notif = false;

	if (!has_sendmsg)
		return 0;

	ret = t_create_ring(8, &ring, 0);
	if (ret)
		return ret;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_sendmsg(sqe, fds[0], NULL, MSG_WAITALL);
	sqe->opcode = IORING_OP_SENDMSG_ZC;
	sqe->flags |= IOSQE_ASYNC;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed %i\n", ret);
		return ret;
	}
	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret)
		return 1;
	if (cqe->flags & IORING_CQE_F_MORE)
		notif = true;
	io_uring_cqe_seen(&ring, cqe);

	if (notif) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret)
			return 1;
		io_uring_cqe_seen(&ring, cqe);
	}
	io_uring_queue_exit(&ring);
	return 0;
}

static int run_basic_tests(void)
{
	struct sockaddr_storage addr;
	int ret, i, sp[2];

	/* create TCP IPv6 pair */
	ret = create_socketpair_ip(&addr, &sp[0], &sp[1], true, true, false, true);
	if (ret) {
		fprintf(stderr, "sock prep failed %d\n", ret);
		return -1;
	}

	for (i = 0; i < 2; i++) {
		struct io_uring ring;
		unsigned ring_flags = 0;

		if (i & 1)
			ring_flags |= IORING_SETUP_DEFER_TASKRUN;

		ret = io_uring_queue_init(32, &ring, ring_flags);
		if (ret) {
			if (ret == -EINVAL)
				continue;
			fprintf(stderr, "queue init failed: %d\n", ret);
			return -1;
		}

		ret = test_basic_send(&ring, sp[0], sp[1], 0);
		if (ret) {
			fprintf(stderr, "test_basic_send() nonvec failed\n");
			return -1;
		}

		ret = test_basic_send(&ring, sp[0], sp[1], 1);
		if (ret) {
			fprintf(stderr, "test_basic_send() vec failed\n");
			return -1;
		}

		ret = test_send_faults(sp[0], sp[1]);
		if (ret) {
			fprintf(stderr, "test_send_faults() failed\n");
			return -1;
		}

		ret = test_invalid_zc(sp);
		if (ret) {
			fprintf(stderr, "test_invalid_zc() failed\n");
			return -1;
		}

		ret = test_async_addr(&ring);
		if (ret) {
			fprintf(stderr, "test_async_addr() failed\n");
			return T_EXIT_FAIL;
		}

		ret = test_sendzc_report(&ring);
		if (ret) {
			fprintf(stderr, "test_sendzc_report() failed\n");
			return T_EXIT_FAIL;
		}

		io_uring_queue_exit(&ring);
	}

	close(sp[0]);
	close(sp[1]);
	return 0;
}

static void free_buffers(void)
{
	if (tx_buffer)
		free(tx_buffer);
	if (rx_buffer)
		free(rx_buffer);
	tx_buffer = rx_buffer = NULL;
}

int main(int argc, char *argv[])
{
	size_t len;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = probe_zc_support();
	if (ret) {
		printf("probe failed\n");
		return T_EXIT_FAIL;
	}
	if (!has_sendzc) {
		printf("no IORING_OP_SEND_ZC support, skip\n");
		return T_EXIT_SKIP;
	}

	page_sz = sysconf(_SC_PAGESIZE);

	if (try_hugepages) {
		len = LARGE_BUF_SIZE;
		tx_buffer = t_aligned_alloc(page_sz, len);
		rx_buffer = t_aligned_alloc(page_sz, len);

		if (tx_buffer && rx_buffer) {
			buffers_iov[BUF_T_LARGE].iov_base = tx_buffer;
			buffers_iov[BUF_T_LARGE].iov_len = len;
		} else {
			printf("skip large buffer tests, can't alloc\n");
			free_buffers();
		}
	}

	if (!tx_buffer) {
		len = 2 * page_sz;
		tx_buffer = t_aligned_alloc(page_sz, len);
		rx_buffer = t_aligned_alloc(page_sz, len);

		if (!tx_buffer || !rx_buffer) {
			fprintf(stderr, "can't allocate buffers\n");
			return T_EXIT_FAIL;
		}
	}

	srand((unsigned)time(NULL));
	for (i = 0; i < len; i++)
		tx_buffer[i] = i;
	memset(rx_buffer, 0, len);

	buffers_iov[BUF_T_NORMAL].iov_base = tx_buffer + page_sz;
	buffers_iov[BUF_T_NORMAL].iov_len = page_sz;
	buffers_iov[BUF_T_SMALL].iov_base = tx_buffer;
	buffers_iov[BUF_T_SMALL].iov_len = 137;
	buffers_iov[BUF_T_NONALIGNED].iov_base = tx_buffer + BUFFER_OFFSET;
	buffers_iov[BUF_T_NONALIGNED].iov_len = 2 * page_sz - BUFFER_OFFSET - 13;

	if (len == LARGE_BUF_SIZE) {
		void *huge_page;
		int off = page_sz + 27;

		len = 1U << 22;
		huge_page = mmap(NULL, len, PROT_READ|PROT_WRITE,
				 MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB | MAP_ANONYMOUS,
				 -1, 0);
		if (huge_page != MAP_FAILED) {
			buffers_iov[BUF_T_HUGETLB].iov_base = huge_page + off;
			buffers_iov[BUF_T_HUGETLB].iov_len = len - off;
		}
	}

	ret = run_basic_tests();
	if (ret)
		return T_EXIT_FAIL;

	for (i = 0; i < 2; i++) {
		struct io_uring ring;
		unsigned ring_flags = 0;

		if (i & 1)
			ring_flags |= IORING_SETUP_SINGLE_ISSUER |
				      IORING_SETUP_DEFER_TASKRUN;

		ret = io_uring_queue_init(32, &ring, ring_flags);
		if (ret) {
			if (ret == -EINVAL)
				continue;
			fprintf(stderr, "queue init failed: %d\n", ret);
			return -1;
		}

		ret = t_register_buffers(&ring, buffers_iov, ARRAY_SIZE(buffers_iov));
		if (ret == T_SETUP_SKIP) {
			fprintf(stderr, "can't register bufs, skip\n");
			goto out;
		} else if (ret != T_SETUP_OK) {
			fprintf(stderr, "buffer registration failed %i\n", ret);
			return T_EXIT_FAIL;
		}

		if (buffers_iov[BUF_T_HUGETLB].iov_base) {
			buffers_iov[BUF_T_HUGETLB].iov_base += 13;
			buffers_iov[BUF_T_HUGETLB].iov_len -= 26;
		}
		if (buffers_iov[BUF_T_LARGE].iov_base) {
			buffers_iov[BUF_T_LARGE].iov_base += 13;
			buffers_iov[BUF_T_LARGE].iov_len -= 26;
		}

		ret = test_inet_send(&ring);
		if (ret) {
			fprintf(stderr, "test_inet_send() failed (defer_taskrun %i)\n",
					 ring_flags & IORING_SETUP_DEFER_TASKRUN);
			return T_EXIT_FAIL;
		}

		if (buffers_iov[BUF_T_HUGETLB].iov_base) {
			buffers_iov[BUF_T_HUGETLB].iov_base -= 13;
			buffers_iov[BUF_T_HUGETLB].iov_len += 26;
		}
		if (buffers_iov[BUF_T_LARGE].iov_base) {
			buffers_iov[BUF_T_LARGE].iov_base -= 13;
			buffers_iov[BUF_T_LARGE].iov_len += 26;
		}
out:
		io_uring_queue_exit(&ring);
	}

	return T_EXIT_PASS;
}
