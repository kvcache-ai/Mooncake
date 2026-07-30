/* SPDX-License-Identifier: MIT */
/*
 * Test that fairness exists between multiple streams, and that setting
 * optlen is honored in terms of terminating a multishot request when
 * a certain byte limit is reached.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define RECV_BIDS	2048
#define RECV_BID_MASK	(RECV_BIDS - 1)
#define RECV_BGID	8
#define RECV_BID_SZ	32

#define PORT	10210
#define HOST	"127.0.0.1"

#define NR_RDS	4

static bool no_iter_support;
static bool no_limit_support;

static int use_port = PORT;

struct recv_data {
	pthread_barrier_t connect;
	pthread_barrier_t startup;
	pthread_barrier_t barrier;

	struct io_uring_buf_ring *br;

	int recv_bytes;
	int accept_fd;
	unsigned int max_sends;
	unsigned int queue_flags;

	int bytes_since_arm;
	int total_bytes;

	int recv_bundle;
	int mshot_limit;

	int use_port;
	int id;

	int mshot_too_big;
	int unfair;
};

#define PER_ITER_LIMIT		1024
#define PER_MSHOT_LIMIT		4096

static void arm_recv(struct io_uring *ring, struct recv_data *rd)
{
	struct io_uring_sqe *sqe;
	int len = PER_ITER_LIMIT;

	rd->bytes_since_arm = 0;
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv_multishot(sqe, rd->accept_fd, NULL, len, 0);
	if (rd->mshot_limit)
		sqe->optlen = PER_MSHOT_LIMIT;
	if (rd->recv_bundle)
		sqe->ioprio |= IORING_RECVSEND_BUNDLE;
	sqe->buf_group = RECV_BGID;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	io_uring_sqe_set_data(sqe, rd);
}

static int recv_prep(struct io_uring *ring, struct recv_data *rd, int *sock)
{
	struct sockaddr_in saddr;
	int sockfd, ret, val, use_fd;
	socklen_t socklen;

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(rd->use_port);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("socket");
		return 1;
	}

	val = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	ret = bind(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("bind");
		goto err;
	}

	ret = listen(sockfd, 1);
	if (ret < 0) {
		perror("listen");
		goto err;
	}

	pthread_barrier_wait(&rd->connect);

	socklen = sizeof(saddr);
	use_fd = accept(sockfd, (struct sockaddr *)&saddr, &socklen);
	if (use_fd < 0) {
		perror("accept");
		goto err;
	}

	rd->accept_fd = use_fd;
	pthread_barrier_wait(&rd->startup);
	pthread_barrier_wait(&rd->barrier);

	rd->recv_bytes = rd->max_sends * 4096;
	arm_recv(ring, rd);
	*sock = sockfd;
	return 0;
err:
	close(sockfd);
	return 1;
}

struct iter {
	int last_res;
	int last_id;
	int count;
};

static void iter_done(struct recv_data *rd, struct iter *iter)
{
	if (!iter->count)
		return;
	iter->count = 0;
}

static struct recv_data *last_rd;
static int bytes_since_last;

static int recv_get_cqe(struct io_uring *ring,
			struct io_uring_cqe **cqe, struct iter *iter)
{
	struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 100000000LL };
	int ret;

	do {
		ret = io_uring_wait_cqe_timeout(ring, cqe, &ts);
		if (!ret) {
			struct recv_data *rd = io_uring_cqe_get_data(*cqe);

			if (last_rd != rd) {
				if (last_rd) {
					int diff = bytes_since_last - PER_ITER_LIMIT;

					if (bytes_since_last > PER_ITER_LIMIT &&
					    diff > RECV_BID_SZ)
						rd->unfair++;
				}
				last_rd = rd;
				bytes_since_last = 0;
			}
			if ((*cqe)->res == iter->last_res && rd->id == iter->last_id) {
				iter->count++;
			} else {
				iter_done(rd, iter);
			}
			iter->last_res = (*cqe)->res;
			bytes_since_last += (*cqe)->res;
			iter->last_id = rd->id;
			return 0;
		}
		if (ret == -ETIME)
			continue;
		fprintf(stderr, "wait recv: %d\n", ret);
		break;
	} while (1);

	return 1;
}

static int do_recv(struct io_uring *ring)
{
	struct iter iter = { .last_res = -4096, };
	struct io_uring_cqe *cqe;
	struct recv_data *rd;
	int ret = 1;
	int done = 0;
	int pending_submit = 0;

	last_rd = NULL;
	bytes_since_last = 0;
	do {
		if (recv_get_cqe(ring, &cqe, &iter))
			break;
		rd = io_uring_cqe_get_data(cqe);
		if (cqe->res == -EINVAL) {
			fprintf(stdout, "recv not supported, skipping\n");
			return 0;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "failed recv cqe: %d\n", cqe->res);
			goto err;
		}
		if (cqe->res && !(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "no buffer set in recv\n");
			goto err;
		}
		rd->recv_bytes -= cqe->res;
		rd->bytes_since_arm += cqe->res;
		rd->total_bytes += cqe->res;
		if (!cqe->res || !rd->recv_bytes) {
			io_uring_cqe_seen(ring, cqe);
			done++;
			continue;
		}
		if (rd->mshot_limit && rd->bytes_since_arm > PER_MSHOT_LIMIT)
			rd->mshot_too_big++;
		io_uring_cqe_seen(ring, cqe);
		if (!(cqe->flags & IORING_CQE_F_MORE) && rd->recv_bytes) {
			arm_recv(ring, rd);
			pending_submit++;
		}
		if (pending_submit == NR_RDS)
			io_uring_submit(ring);
	} while (done < NR_RDS);

	ret = 0;
	iter_done(rd, &iter);
err:
	return ret;
}

static void *recv_fn(void *data)
{
	struct recv_data *rds = data;
	struct io_uring_params p = { };
	struct io_uring_buf_ring *br = NULL;
	struct io_uring ring;
	unsigned int buf_len;
	void *buf = NULL, *ptr;
	int ret, sock[NR_RDS], i;
	int brflags, ring_setup = 0;

	for (i = 0; i < NR_RDS; i++) {
		sock[i] = -1;
		rds[i].accept_fd = -1;
	}

	p.cq_entries = 4096;
	p.flags = rds[0].queue_flags | IORING_SETUP_CQSIZE;
	ret = io_uring_queue_init_params(16, &ring, &p);
	if (ret) {
		if (ret == -EINVAL) {
			no_iter_support = true;
			goto skip;
		}
		fprintf(stderr, "ring init: %d\n", ret);
		goto err;
	}

	ring_setup = 1;
	if (posix_memalign(&buf, 4096, NR_RDS * rds[0].max_sends * 4096))
		goto err;

	brflags = 0;
	br = io_uring_setup_buf_ring(&ring, RECV_BIDS, RECV_BGID, brflags, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_iter_support = true;
			goto skip;
		}
		fprintf(stderr, "failed setting up recv ring %d\n", ret);
		goto err;
	}

	ptr = buf;
	buf_len = RECV_BID_SZ;
	for (i = 0; i < RECV_BIDS; i++) {
		io_uring_buf_ring_add(br, ptr, buf_len, i, RECV_BID_MASK, i);
		ptr += buf_len;
	}
	io_uring_buf_ring_advance(br, RECV_BIDS);

	for (i = 0; i < NR_RDS; i++) {
		ret = recv_prep(&ring, &rds[i], &sock[i]);
		if (ret) {
			fprintf(stderr, "recv_prep failed: %d\n", ret);
			goto err;
		}
	}

	ret = io_uring_submit(&ring);
	if (ret != NR_RDS) {
		struct io_uring_cqe *cqe;

		if (!io_uring_peek_cqe(&ring, &cqe)) {
			if (cqe->res == -EINVAL) {
				if (rds[0].mshot_limit)
					no_limit_support = true;
				else
					no_iter_support = true;
				goto skip;
			}
		}
		fprintf(stderr, "submit failed: %d\n", ret);
		goto err;
	}

	ret = do_recv(&ring);
skip:
	for (i = 0; i < NR_RDS; i++) {
		if (sock[i] != -1)
			close(sock[i]);
		if (rds[i].accept_fd != -1)
			close(rds[i].accept_fd);
	}
	if (br)
		io_uring_free_buf_ring(&ring, br, RECV_BIDS, RECV_BGID);
	if (ring_setup)
		io_uring_queue_exit(&ring);
	free(buf);
err:
	return (void *)(intptr_t)ret;
}

static int do_send(struct recv_data *rd)
{
	struct sockaddr_in saddr = { };
	int sockfd, ret, i;
	void *buf;

	buf = malloc(4096);
	memset(buf, 0xa5, 4096);

	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(rd->use_port);
	inet_pton(AF_INET, HOST, &saddr.sin_addr);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("socket");
		goto err2;
	}

	pthread_barrier_wait(&rd->connect);

	ret = connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("connect");
		goto err;
	}

	pthread_barrier_wait(&rd->startup);

	for (i = 0; i < rd->max_sends; i++) {
		ret = send(sockfd, buf, 4096, 0);
		if (ret < 0) {
			perror("send");
			return 1;
		} else if (ret != 4096) {
			printf("short send\n");
			goto err;
		}
	}

	pthread_barrier_wait(&rd->barrier);
	close(sockfd);
	free(buf);
	return 0;
err:
	close(sockfd);
err2:
	free(buf);
	return 1;
}

struct res {
	int mshot_too_big;
	int unfair;
};

static int test(int nr_send, int bundle, int mshot_limit,
		unsigned int queue_flags, struct res *r)
{
	struct recv_data rds[NR_RDS] = { };
	pthread_t recv_thread;
	int ret, i;
	void *retval;

	memset(r, 0, sizeof(*r));

	for (i = 0; i < NR_RDS; i++) {
		struct recv_data *rd = &rds[i];

		rd->use_port = ++use_port;

		pthread_barrier_init(&rd->connect, NULL, 2);
		pthread_barrier_init(&rd->startup, NULL, 2);
		pthread_barrier_init(&rd->barrier, NULL, 2);

		rd->queue_flags = queue_flags;
		rd->max_sends = nr_send;
		rd->recv_bundle = bundle;
		rd->recv_bytes = nr_send * 4096;
		rd->mshot_limit = mshot_limit;
		rd->id = i + 1;
	}

	ret = pthread_create(&recv_thread, NULL, recv_fn, rds);
	if (ret) {
		fprintf(stderr, "Thread create failed: %d\n", ret);
		return 1;
	}

	for (i = 0; i < NR_RDS; i++) {
		struct recv_data *rd = &rds[i];

		ret = do_send(rd);
		if (ret)
			return ret;
	}

	pthread_join(recv_thread, &retval);

	for (i = 0; i < NR_RDS; i++) {
		struct recv_data *rd = &rds[i];

		r->mshot_too_big += rd->mshot_too_big;
		r->unfair += rd->unfair;
	}

	return (intptr_t) retval;
}

static int run_tests(void)
{
	struct res r;
	int ret;

	ret = test(2, 1, 0, IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER, &r);
	if (ret) {
		if (no_iter_support)
			return T_EXIT_SKIP;
		fprintf(stderr, "test DEFER bundle failed\n");
		return T_EXIT_FAIL;
	}

	/* DEFER_TASKRUN should be fully fair and not have overshoots */
	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "DEFER bundle unfair=%d, too_big=%d\n", r.unfair, r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 0, 0, IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER, &r);
	if (ret) {
		fprintf(stderr, "test DEFER failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "DEFER unfair=%d, too_big=%d\n", r.unfair, r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 1, 0, IORING_SETUP_COOP_TASKRUN, &r);
	if (ret) {
		fprintf(stderr, "test COOP bundle failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "COOP bundle too_big=%d\n", r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 0, 0, IORING_SETUP_COOP_TASKRUN, &r);
	if (ret) {
		fprintf(stderr, "test COOP failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "!DEFER too_big=%d\n", r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 1, 1, IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER, &r);
	if (ret) {
		if (no_limit_support)
			return T_EXIT_PASS;
		fprintf(stderr, "test DEFER bundle cap failed\n");
		return T_EXIT_FAIL;
	}

	/* DEFER_TASKRUN should be fully fair and not have overshoots */
	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "DEFER bundle cap unfair=%d, too_big=%d\n", r.unfair, r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 0, 1, IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER, &r);
	if (ret) {
		fprintf(stderr, "test DEFER cap failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "DEFER cap unfair=%d, too_big=%d\n", r.unfair, r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 1, 1, IORING_SETUP_COOP_TASKRUN, &r);
	if (ret) {
		fprintf(stderr, "test COOP bundle cap failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "COOP bundle cap too_big=%d\n", r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	ret = test(2, 0, 1, IORING_SETUP_COOP_TASKRUN, &r);
	if (ret) {
		fprintf(stderr, "test COOP cap failed\n");
		return T_EXIT_FAIL;
	}

	if (r.unfair || r.mshot_too_big) {
		fprintf(stderr, "COOP cap too_big=%d\n", r.mshot_too_big);
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = run_tests();
	if (ret != T_EXIT_PASS)
		return ret;

	return T_EXIT_PASS;
}
