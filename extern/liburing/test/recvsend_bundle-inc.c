/* SPDX-License-Identifier: MIT */
/*
 * Simple test case showing using send and recv bundles with incremental
 * buffer ring usage
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#define MSG_SIZE 128
#define NR_MIN_MSGS	4
#define NR_MAX_MSGS	32
#define SEQ_SIZE	(MSG_SIZE / sizeof(unsigned long))

static int nr_msgs;

#define RECV_BIDS	8192
#define RECV_BID_MASK	(RECV_BIDS - 1)

#include <liburing.h>

enum t_test_result {
	T_EXIT_PASS   = 0,
	T_EXIT_FAIL   = 1,
	T_EXIT_SKIP   = 77,
};

#define PORT	10202
#define HOST	"127.0.0.1"

static int use_port = PORT;

#define SEND_BGID	7
#define RECV_BGID	8

static int no_send_mshot;

struct recv_data {
	pthread_barrier_t connect;
	pthread_barrier_t startup;
	pthread_barrier_t barrier;
	pthread_barrier_t finish;
	unsigned long seq;
	int recv_bytes;
	int accept_fd;
	int abort;
	unsigned int max_sends;
	int to_eagain;
	void *recv_buf;

	int send_bundle;
	int recv_bundle;
};

static int arm_recv(struct io_uring *ring, struct recv_data *rd)
{
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv_multishot(sqe, rd->accept_fd, NULL, 0, 0);
	if (rd->recv_bundle)
		sqe->ioprio |= IORING_RECVSEND_BUNDLE;
	sqe->buf_group = RECV_BGID;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed: %d\n", ret);
		return 1;
	}

	return 0;
}

static int recv_prep(struct io_uring *ring, struct recv_data *rd, int *sock)
{
	struct sockaddr_in saddr;
	int sockfd, ret, val, use_fd;
	socklen_t socklen;

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(use_port);

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

	if (arm_recv(ring, rd))
		goto err;

	*sock = sockfd;
	return 0;
err:
	close(sockfd);
	return 1;
}

static int verify_seq(struct recv_data *rd, void *verify_ptr, int verify_sz,
		      int start_bid)
{
	unsigned long *seqp;
	int seq_size = verify_sz / sizeof(unsigned long);
	int i;

	seqp = verify_ptr;
	for (i = 0; i < seq_size; i++) {
		if (rd->seq != *seqp) {
			fprintf(stderr, "bid=%d, got seq %lu, wanted %lu, offset %d\n", start_bid, *seqp, rd->seq, i);
			return 0;
		}
		seqp++;
		rd->seq++;
	}

	return 1;
}

static int recv_get_cqe(struct io_uring *ring, struct recv_data *rd,
			struct io_uring_cqe **cqe)
{
	struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 100000000LL };
	int ret;

	do {
		ret = io_uring_wait_cqe_timeout(ring, cqe, &ts);
		if (!ret)
			return 0;
		if (ret == -ETIME) {
			if (rd->abort)
				break;
			continue;
		}
		fprintf(stderr, "wait recv: %d\n", ret);
		break;
	} while (1);

	return 1;
}

static int do_recv(struct io_uring *ring, struct recv_data *rd)
{
	struct io_uring_cqe *cqe;
	void *verify_ptr;
	int verify_sz = 0;
	int verify_bid = 0;
	int bid;

	verify_ptr = malloc(rd->recv_bytes);

	do {
		if (recv_get_cqe(ring, rd, &cqe))
			break;
		if (cqe->res == -EINVAL) {
			fprintf(stdout, "recv not supported, skipping\n");
			return 0;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "failed recv cqe: %d\n", cqe->res);
			goto err;
		}
		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "no buffer set in recv\n");
			goto err;
		}
		if (!(cqe->flags & IORING_CQE_F_BUF_MORE)) {
			fprintf(stderr, "CQE_F_BUF_MORE not set\n");
			goto err;
		}
		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
		if (bid != 0) {
			fprintf(stderr, "got bid %d\n", bid);
			goto err;
		}
		if (!(verify_sz % MSG_SIZE)) {
			if (!verify_seq(rd, verify_ptr, verify_sz, verify_bid))
				goto err;
			verify_bid += verify_sz / MSG_SIZE;
			verify_bid &= RECV_BID_MASK;
			verify_sz = 0;
		} else {
			memcpy(verify_ptr + verify_sz, rd->recv_buf + (bid * MSG_SIZE), cqe->res);
			verify_sz += cqe->res;
		}
		rd->recv_bytes -= cqe->res;
		io_uring_cqe_seen(ring, cqe);
		if (!(cqe->flags & IORING_CQE_F_MORE) && rd->recv_bytes) {
			if (arm_recv(ring, rd))
				goto err;
		}
	} while (rd->recv_bytes);

	if (verify_sz && !(verify_sz % MSG_SIZE) &&
	    !verify_seq(rd, verify_ptr, verify_sz, verify_bid))
		goto err;

	pthread_barrier_wait(&rd->finish);
	return 0;
err:
	pthread_barrier_wait(&rd->finish);
	return 1;
}

static void *recv_fn(void *data)
{
	struct recv_data *rd = data;
	struct io_uring_params p = { };
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	void *buf, *ptr;
	int ret, sock;

	p.cq_entries = 4096;
	p.flags = IORING_SETUP_CQSIZE;
	io_uring_queue_init_params(16, &ring, &p);

	ret = 0;
	if (posix_memalign(&buf, 4096, MSG_SIZE * RECV_BIDS))
		goto err;

	br = io_uring_setup_buf_ring(&ring, RECV_BIDS, RECV_BGID, IOU_PBUF_RING_INC, &ret);
	if (!br) {
		fprintf(stderr, "failed setting up recv ring %d\n", ret);
		goto err;
	}

	ptr = buf;
	io_uring_buf_ring_add(br, ptr, MSG_SIZE * RECV_BIDS, 0, RECV_BID_MASK, 0);
	io_uring_buf_ring_advance(br, 1);
	rd->recv_buf = buf;

	ret = recv_prep(&ring, rd, &sock);
	if (ret) {
		fprintf(stderr, "recv_prep failed: %d\n", ret);
		goto err;
	}

	ret = do_recv(&ring, rd);

	close(sock);
	close(rd->accept_fd);
	free(buf);
	io_uring_queue_exit(&ring);
err:
	return (void *)(intptr_t)ret;
}

static int __do_send_bundle(struct recv_data *rd, struct io_uring *ring, int sockfd)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int bytes_needed = MSG_SIZE * nr_msgs;
	int i, ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_send_bundle(sqe, sockfd, 0, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = SEND_BGID;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1)
		return 1;

	pthread_barrier_wait(&rd->barrier);

	for (i = 0; i < nr_msgs; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait send: %d\n", ret);
			return 1;
		}
		if (!i && cqe->res == -EINVAL) {
			rd->abort = 1;
			no_send_mshot = 1;
			break;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "bad send cqe res: %d\n", cqe->res);
			return 1;
		}
		bytes_needed -= cqe->res;
		if (!bytes_needed) {
			io_uring_cqe_seen(ring, cqe);
			break;
		}
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			fprintf(stderr, "expected more, but MORE not set\n");
			return 1;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
}

static int __do_send(struct recv_data *rd, struct io_uring *ring, int sockfd)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int bytes_needed = MSG_SIZE * nr_msgs;
	int i, ret;

	for (i = 0; i < nr_msgs; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_send(sqe, sockfd, NULL, 0, 0);
		sqe->user_data = 10 + i;
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = SEND_BGID;

		ret = io_uring_submit(ring);
		if (ret != 1)
			return 1;

		if (!i)
			pthread_barrier_wait(&rd->barrier);
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "send wait cqe %d\n", ret);
			return 1;
		}

		if (!i && cqe->res == -EINVAL) {
			rd->abort = 1;
			no_send_mshot = 1;
			break;
		}
		if (cqe->res != MSG_SIZE) {
			fprintf(stderr, "send failed cqe: %d\n", cqe->res);
			return 1;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "bad send cqe res: %d\n", cqe->res);
			return 1;
		}
		bytes_needed -= cqe->res;
		io_uring_cqe_seen(ring, cqe);
		if (!bytes_needed)
			break;
	}

	return 0;
}

static int do_send(struct recv_data *rd)
{
	struct sockaddr_in saddr;
	struct io_uring ring;
	unsigned long seq_buf[SEQ_SIZE], send_seq;
	struct io_uring_params p = { };
	struct io_uring_buf_ring *br;
	int sockfd, ret, len, i;
	socklen_t optlen;
	void *buf, *ptr;

	ret = io_uring_queue_init_params(16, &ring, &p);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		return 1;
	}
	if (!(p.features & IORING_FEAT_RECVSEND_BUNDLE)) {
		no_send_mshot = 1;
		return 0;
	}

	if (posix_memalign(&buf, 4096, MSG_SIZE * nr_msgs))
		return 1;

	br = io_uring_setup_buf_ring(&ring, nr_msgs, SEND_BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			fprintf(stderr, "einval on br setup\n");
			return 0;
		}
		fprintf(stderr, "failed setting up send ring %d\n", ret);
		return 1;
	}

	ptr = buf;
	for (i = 0; i < nr_msgs; i++) {
		io_uring_buf_ring_add(br, ptr, MSG_SIZE, i, nr_msgs - 1, i);
		ptr += MSG_SIZE;
	}
	io_uring_buf_ring_advance(br, nr_msgs);

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(use_port);
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

	optlen = sizeof(len);
	len = 1024 * MSG_SIZE;
	setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &len, optlen);

	/* almost fill queue, leave room for one message */
	send_seq = 0;
	rd->to_eagain = 0;
	while (rd->max_sends && rd->max_sends--) {
		for (i = 0; i < SEQ_SIZE; i++)
			seq_buf[i] = send_seq++;

		ret = send(sockfd, seq_buf, sizeof(seq_buf), MSG_DONTWAIT);
		if (ret < 0) {
			if (errno == EAGAIN) {
				send_seq -= SEQ_SIZE;
				break;
			}
			perror("send");
			return 1;
		} else if (ret != sizeof(seq_buf)) {
			fprintf(stderr, "short %d send\n", ret);
			return 1;
		}

		rd->to_eagain++;
		rd->recv_bytes += sizeof(seq_buf);
	}

	ptr = buf;
	for (i = 0; i < nr_msgs; i++) {
		unsigned long *pseq = ptr;
		int j;

		for (j = 0; j < SEQ_SIZE; j++)
			pseq[j] = send_seq++;
		ptr += MSG_SIZE;
	}

	/* prepare more messages, sending with bundle */
	rd->recv_bytes += (nr_msgs * MSG_SIZE);
	if (rd->send_bundle)
		ret = __do_send_bundle(rd, &ring, sockfd);
	else
		ret = __do_send(rd, &ring, sockfd);
	if (ret)
		goto err;

	pthread_barrier_wait(&rd->finish);

	close(sockfd);
	free(buf);
	io_uring_queue_exit(&ring);
	return 0;

err:
	close(sockfd);
err2:
	io_uring_queue_exit(&ring);
	pthread_barrier_wait(&rd->finish);
	return 1;
}

static int test(int backlog, unsigned int max_sends, int *to_eagain,
		int send_bundle, int recv_bundle)
{
	pthread_t recv_thread;
	struct recv_data rd;
	int ret;
	void *retval;

	memset(&rd, 0, sizeof(rd));
	pthread_barrier_init(&rd.connect, NULL, 2);
	pthread_barrier_init(&rd.startup, NULL, 2);
	pthread_barrier_init(&rd.barrier, NULL, 2);
	pthread_barrier_init(&rd.finish, NULL, 2);
	rd.max_sends = max_sends;
	if (to_eagain)
		*to_eagain = 0;

	rd.send_bundle = send_bundle;
	rd.recv_bundle = recv_bundle;

	ret = pthread_create(&recv_thread, NULL, recv_fn, &rd);
	if (ret) {
		fprintf(stderr, "Thread create failed: %d\n", ret);
		return 1;
	}

	ret = do_send(&rd);
	if (no_send_mshot)
		return 0;

	if (ret)
		return ret;

	pthread_join(recv_thread, &retval);
	if (to_eagain)
		*to_eagain = rd.to_eagain;
	return (intptr_t)retval;
}

static int run_tests(void)
{
	int ret, eagain_hit;

	nr_msgs = NR_MIN_MSGS;

	/* test basic send bundle first */
	ret = test(0, 0, NULL, 0, 0);
	if (ret) {
		fprintf(stderr, "test a failed\n");
		return T_EXIT_FAIL;
	}
	if (no_send_mshot)
		return T_EXIT_SKIP;

	/* test recv bundle */
	ret = test(0, 0, NULL, 0, 1);
	if (ret) {
		fprintf(stderr, "test b failed\n");
		return T_EXIT_FAIL;
	}

	/* test bundling recv and send */
	ret = test(0, 0, NULL, 1, 1);
	if (ret) {
		fprintf(stderr, "test c failed\n");
		return T_EXIT_FAIL;
	}

	/* test bundling with full socket */
	ret = test(1, 1000000, &eagain_hit, 1, 1);
	if (ret) {
		fprintf(stderr, "test d failed\n");
		return T_EXIT_FAIL;
	}

	/* test bundling with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), NULL, 1, 1);
	if (ret) {
		fprintf(stderr, "test e failed\n");
		return T_EXIT_FAIL;
	}

	/* test recv bundle with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), NULL, 0, 1);
	if (ret) {
		fprintf(stderr, "test f failed\n");
		return T_EXIT_FAIL;
	}

	/* test send bundle with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), &eagain_hit, 1, 0);
	if (ret) {
		fprintf(stderr, "test g failed\n");
		return T_EXIT_FAIL;
	}

	/* now repeat the last three tests, but with > FAST_UIOV segments */
	nr_msgs = NR_MAX_MSGS;

	/* test bundling with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), NULL, 1, 1);
	if (ret) {
		fprintf(stderr, "test h failed\n");
		return T_EXIT_FAIL;
	}

	/* test recv bundle with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), NULL, 0, 1);
	if (ret) {
		fprintf(stderr, "test i failed\n");
		return T_EXIT_FAIL;
	}

	/* test send bundle with almost full socket */
	ret = test(1, eagain_hit - (nr_msgs / 2), &eagain_hit, 1, 0);
	if (ret) {
		fprintf(stderr, "test j failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

static int test_tcp(void)
{
	int ret;

	ret = run_tests();
	if (ret == T_EXIT_FAIL)
		fprintf(stderr, "TCP test case failed\n");
	return ret;
}

static bool has_pbuf_ring_inc(void)
{
	struct io_uring_buf_ring *br;
	bool has_pbuf_inc = false;
	struct io_uring ring;
	void *buf;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret)
		return false;

	if (posix_memalign(&buf, 4096, MSG_SIZE * RECV_BIDS))
		return false;

	br = io_uring_setup_buf_ring(&ring, RECV_BIDS, RECV_BGID, IOU_PBUF_RING_INC, &ret);
	if (br) {
		has_pbuf_inc = true;
		io_uring_unregister_buf_ring(&ring, RECV_BGID);
	}
	io_uring_queue_exit(&ring);
	free(buf);
	return has_pbuf_inc;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;
	if (!has_pbuf_ring_inc())
		return T_EXIT_SKIP;

	ret = test_tcp();
	if (ret != T_EXIT_PASS)
		return ret;

	return T_EXIT_PASS;
}
