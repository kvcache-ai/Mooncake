/* SPDX-License-Identifier: MIT */
/*
 * Description: test timestamp retrieving
 *
 */
#include <time.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <linux/errqueue.h>
#include <linux/ipv6.h>
#include <linux/net_tstamp.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>

#include "liburing.h"
#include "helpers.h"

/*
 * Only sparc/hppa should have "non-standard" values for SCM_TS_OPT_ID
 */
#ifndef SCM_TS_OPT_ID
#if defined(__sparc__) || defined(__sparc64__)
#define SCM_TS_OPT_ID	0x005a
#elif defined(__hppa__)
#define SCM_TS_OPT_ID	0x404C
#else
#define SCM_TS_OPT_ID	81
#endif
#endif

static const int cfg_payload_len = 10;
static uint16_t dest_port = 9000;
static uint32_t ts_opt_id = 81;
static bool cfg_use_cmsg_opt_id = false;
static char buffer[128];
static const bool verbose = false;

static struct sockaddr_in6 daddr6;

static int saved_tskey = -1;
static int saved_tskey_type = -1;

static int listen_fd;

struct ctx {
	int family;
	int proto;
	int report_opt;
	int num_pkts;
	int cqe_mixed;
};

static int validate_key(int tskey, int tstype, struct ctx *ctx)
{
	int stepsize;

	/* compare key for each subsequent request
	 * must only test for one type, the first one requested
	 */
	if (saved_tskey == -1 || cfg_use_cmsg_opt_id)
		saved_tskey_type = tstype;
	else if (saved_tskey_type != tstype)
		return 0;

	stepsize = ctx->proto == SOCK_STREAM ? cfg_payload_len : 1;
	stepsize = cfg_use_cmsg_opt_id ? 0 : stepsize;
	if (tskey != saved_tskey + stepsize) {
		fprintf(stderr, "ERROR: key %d, expected %d\n",
				tskey, saved_tskey + stepsize);
		return -EINVAL;
	}

	saved_tskey = tskey;
	return 0;
}

static int test_prep_sock(int family, int proto, unsigned report_opt)
{
	int ipproto = proto == SOCK_STREAM ? IPPROTO_TCP : IPPROTO_UDP;
	unsigned int sock_opt;
	int fd, val = 1;

	fd = socket(family, proto, ipproto);
	if (fd < 0)
		t_error(1, errno, "socket");

	if (proto == SOCK_STREAM) {
		if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
			       (char*) &val, sizeof(val)))
			t_error(1, 0, "setsockopt no nagle");

		if (connect(fd, (void *) &daddr6, sizeof(daddr6)))
			t_error(1, errno, "connect ipv6");
	}

	sock_opt = SOF_TIMESTAMPING_SOFTWARE |
		   SOF_TIMESTAMPING_OPT_CMSG |
		   SOF_TIMESTAMPING_OPT_ID;
	sock_opt |= report_opt;
	sock_opt |= SOF_TIMESTAMPING_OPT_TSONLY;

	if (setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPING,
		       (char *) &sock_opt, sizeof(sock_opt)))
		t_error(1, 0, "setsockopt timestamping");

	return fd;
}

#define MAX_PACKETS 32

struct send_req {
	struct msghdr msg;
	struct iovec iov;
	char control[CMSG_SPACE(sizeof(uint32_t))];
};

static void queue_ts_cmd(struct io_uring *ring, int fd)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_rw(IORING_OP_URING_CMD, sqe, fd, NULL, 0, 0);
	sqe->cmd_op = SOCKET_URING_OP_TX_TIMESTAMP;
	sqe->user_data = 43;
}

static void queue_send(struct io_uring *ring, int fd, void *buf, struct send_req *r,
			int proto)
{
	struct io_uring_sqe *sqe;

	r->iov.iov_base = buf;
	r->iov.iov_len = cfg_payload_len;

	memset(&r->msg, 0, sizeof(r->msg));
	r->msg.msg_iov = &r->iov;
	r->msg.msg_iovlen = 1;
	if (proto == SOCK_STREAM) {
		r->msg.msg_name = (void *)&daddr6;
		r->msg.msg_namelen = sizeof(daddr6);
	}

	if (cfg_use_cmsg_opt_id) {
		struct cmsghdr *cmsg;

		memset(r->control, 0, sizeof(r->control));
		r->msg.msg_control = r->control;
		r->msg.msg_controllen = CMSG_SPACE(sizeof(uint32_t));

		cmsg = CMSG_FIRSTHDR(&r->msg);
		cmsg->cmsg_level = SOL_SOCKET;
		cmsg->cmsg_type = SCM_TS_OPT_ID;
		cmsg->cmsg_len = CMSG_LEN(sizeof(uint32_t));

		*((uint32_t *)CMSG_DATA(cmsg)) = ts_opt_id;
		saved_tskey = ts_opt_id;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_sendmsg(sqe, fd, &r->msg, 0);
	sqe->user_data = 0;
}

static const char *get_tstype_name(int tstype)
{
	if (tstype == SCM_TSTAMP_SCHED)
		return "ENQ";
	if (tstype == SCM_TSTAMP_SND)
		return "SND";
	if (tstype == SCM_TSTAMP_ACK)
		return "ACK";
	return "unknown";
}

static int do_test(struct ctx *ctx)
{
	struct send_req reqs[MAX_PACKETS];
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	unsigned long head;
	int cqes_seen = 0;
	int i, fd, ret;
	int ts_expected = 0, ts_got = 0;
	unsigned ring_flags;

	ts_expected += !!(ctx->report_opt & SOF_TIMESTAMPING_TX_SCHED);
	ts_expected += !!(ctx->report_opt & SOF_TIMESTAMPING_TX_SOFTWARE);
	ts_expected += !!(ctx->report_opt & SOF_TIMESTAMPING_TX_ACK);

	if (ctx->cqe_mixed)
		ring_flags = IORING_SETUP_CQE_MIXED;
	else
		ring_flags = IORING_SETUP_CQE32;

	ret = t_create_ring(32, &ring, ring_flags);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret)
		t_error(1, ret, "queue init\n");

	assert(ctx->num_pkts <= MAX_PACKETS);

	fd = test_prep_sock(ctx->family, ctx->proto, ctx->report_opt);
	if (fd < 0)
		t_error(1, fd, "can't create socket\n");

	memset(buffer, 'a', cfg_payload_len);
	saved_tskey = -1;

	if (cfg_use_cmsg_opt_id)
		saved_tskey = ts_opt_id;

	for (i =  0; i < ctx->num_pkts; i++) {
		queue_send(&ring, fd, buffer, &reqs[i], ctx->proto);
		ret = io_uring_submit(&ring);
		if (ret != 1)
			t_error(1, ret, "submit failed");

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret || cqe->res != cfg_payload_len) {
			fprintf(stderr, "wait send cqe, %d %d, expected %d\n",
				ret, cqe->res, cfg_payload_len);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	usleep(200000);

	queue_ts_cmd(&ring, fd);
	ret = io_uring_submit(&ring);
	if (ret != 1)
		t_error(1, ret, "submit failed");

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_for_each_cqe(&ring, head, cqe) {
		struct io_timespec *ts;
		int tskey, tstype;
		bool hwts;

		cqes_seen++;
		if (cqe->flags & IORING_CQE_F_SKIP) {
			if (!ctx->cqe_mixed) {
				t_error(1, 0, "SKIP set for non-mixed");
				break;
			}
			continue;
		}

		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP)
				return T_EXIT_SKIP;
			if (cqe->res)
				t_error(1, 0, "failed cqe %i", cqe->res);
			break;
		}
		if (ctx->cqe_mixed && !(cqe->flags & IORING_CQE_F_32)) {
			t_error(1, 0, "CQE_F_32 not set");
			break;
		}
		cqes_seen++;

		ts = (void *)(cqe + 1);
		tstype = cqe->flags >> IORING_TIMESTAMP_TYPE_SHIFT;
		tskey = cqe->res;
		hwts = cqe->flags & IORING_CQE_F_TSTAMP_HW;

		ts_got++;
		if (verbose)
			fprintf(stderr, "ts: key %x, type %i (%s), is hw %i, sec %lu, nsec %lu\n",
				tskey, tstype, get_tstype_name(tstype), hwts,
				(unsigned long)ts->tv_sec,
				(unsigned long)ts->tv_nsec);

		ret = validate_key(tskey, tstype, ctx);
		if (ret)
			return T_EXIT_FAIL;
	}

	if (ts_got != ts_expected) {
		fprintf(stderr, "expected %i timestamps, got %i\n",
			ts_expected, ts_got);
		return -EINVAL;
	}

	close(fd);
	io_uring_cq_advance(&ring, cqes_seen);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static void resolve_hostname(const char *name, int port)
{
	memset(&daddr6, 0, sizeof(daddr6));
	daddr6.sin6_family = AF_INET6;
	daddr6.sin6_port = htons(port);
	if (inet_pton(AF_INET6, name, &daddr6.sin6_addr) != 1)
		t_error(1, 0, "ipv6 parse error: %s", name);
}

static void do_listen(int family, int type, void *addr, int alen)
{
	listen_fd = socket(family, type, 0);
	if (listen_fd == -1)
		t_error(1, errno, "socket rx");

	if (bind(listen_fd, addr, alen))
		t_error(1, errno, "bind rx");

	if (type == SOCK_STREAM && listen(listen_fd, 10))
		t_error(1, errno, "listen rx");

	/* leave fd open, will be closed on process exit.
	 * this enables connect() to succeed and avoids icmp replies
	 */
}

static int do_main(int family, int proto, int cqe_mixed)
{
	struct ctx ctx;
	int ret;

	ctx.num_pkts = 1;
	ctx.family = family;
	ctx.proto = proto;
	ctx.cqe_mixed = cqe_mixed;

	if (verbose)
		fprintf(stderr, "test SND\n");
	ctx.report_opt = SOF_TIMESTAMPING_TX_SOFTWARE;
	ret = do_test(&ctx);
	if (ret) {
		if (ret == T_EXIT_SKIP)
			fprintf(stderr, "no timestamp cmd, skip\n");
		return ret;
	}

	if (verbose)
		fprintf(stderr, "test ENQ\n");
	ctx.report_opt = SOF_TIMESTAMPING_TX_SCHED;
	ret = do_test(&ctx);
	if (ret)
		return T_EXIT_FAIL;

	if (verbose)
		fprintf(stderr, "test ENQ + SND\n");
	ctx.report_opt = SOF_TIMESTAMPING_TX_SCHED | SOF_TIMESTAMPING_TX_SOFTWARE;
	ret = do_test(&ctx);
	if (ret)
		return T_EXIT_FAIL;

	if (proto == SOCK_STREAM) {
		if (verbose)
			fprintf(stderr, "test ACK\n");
		ctx.report_opt = SOF_TIMESTAMPING_TX_ACK;
		ret = do_test(&ctx);
		if (ret)
			return T_EXIT_FAIL;

		if (verbose)
			fprintf(stderr, "test SND + ACK\n");
		ctx.report_opt = SOF_TIMESTAMPING_TX_SOFTWARE |
				  SOF_TIMESTAMPING_TX_ACK;
		ret = do_test(&ctx);
		if (ret)
			return T_EXIT_FAIL;

		if (verbose)
			fprintf(stderr, "test ENQ + SND + ACK\n");
		ctx.report_opt = SOF_TIMESTAMPING_TX_SCHED |
				  SOF_TIMESTAMPING_TX_SOFTWARE |
				  SOF_TIMESTAMPING_TX_ACK;
		ret = do_test(&ctx);
		if (ret)
			return T_EXIT_FAIL;
	}
	return 0;
}

int main(int argc, char **argv)
{
	const char *hostname = "::1";
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	resolve_hostname(hostname, dest_port);
	do_listen(PF_INET6, SOCK_STREAM, &daddr6, sizeof(daddr6));
	ret = do_main(PF_INET6, SOCK_STREAM, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "CQE32 test failed\n");
		return ret;
	}

	close(listen_fd);
	do_listen(PF_INET6, SOCK_STREAM, &daddr6, sizeof(daddr6));
	ret = do_main(PF_INET6, SOCK_STREAM, 1);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_PASS;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "CQE32 test failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}
