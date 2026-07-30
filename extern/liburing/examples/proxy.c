/* SPDX-License-Identifier: MIT */
/*
 * Sample program that can act either as a packet sink, where it just receives
 * packets and doesn't do anything with them, or it can act as a proxy where it
 * receives packets and then sends them to a new destination. The proxy can
 * be unidirectional (-B0), or bi-direction (-B1).
 * 
 * Examples:
 *
 * Act as a proxy, listening on port 4444, and send data to 192.168.2.6 on port
 * 4445. Use multishot receive, DEFER_TASKRUN, and fixed files
 *
 * 	./proxy -m1 -r4444 -H 192.168.2.6 -p4445
 *
 * Same as above, but utilize send bundles (-C1, requires -u1 send_ring) as well
 * with ring provided send buffers, and recv bundles (-c1).
 *
 * 	./proxy -m1 -c1 -u1 -C1 -r4444 -H 192.168.2.6 -p4445
 *
 * Act as a bi-directional proxy, listening on port 8888, and send data back
 * and forth between host and 192.168.2.6 on port 22. Use multishot receive,
 * DEFER_TASKRUN, fixed files, and buffers of size 1500.
 *
 * 	./proxy -m1 -B1 -b1500 -r8888 -H 192.168.2.6 -p22
 *
 * Act a sink, listening on port 4445, using multishot receive, DEFER_TASKRUN,
 * and fixed files:
 *
 * 	./proxy -m1 -s1 -r4445
 *
 * Run with -h to see a list of options, and their defaults.
 *
 * (C) 2024 Jens Axboe <axboe@kernel.dk>
 *
 */
#include <fcntl.h>
#include <stdint.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <linux/mman.h>
#include <locale.h>
#include <assert.h>
#include <pthread.h>
#include <liburing.h>

#include "proxy.h"
#include "helpers.h"

/*
 * Will go away once/if bundles are upstreamed and we put the generic
 * definitions in the kernel header.
 */
#ifndef IORING_RECVSEND_BUNDLE
#define IORING_RECVSEND_BUNDLE		(1U << 4)
#endif
#ifndef IORING_FEAT_SEND_BUF_SELECT
#define IORING_FEAT_SEND_BUF_SELECT	(1U << 14)
#endif

static int cur_bgid = 1;
static int nr_conns;
static int open_conns;
static long page_size;

static unsigned long event_loops;
static unsigned long events;

static int recv_mshot = 1;
static int sqpoll;
static int defer_tw = 1;
static int is_sink;
static int fixed_files = 1;
static char *host = "192.168.3.2";
static int send_port = 4445;
static int receive_port = 4444;
static int buf_size = 32;
static int buf_ring_inc;
static int bidi;
static int ipv6;
static int napi;
static int napi_timeout;
static int wait_batch = 1;
static int wait_usec = 1000000;
static int rcv_msg;
static int snd_msg;
static int snd_zc;
static int send_ring = -1;
static int snd_bundle;
static int rcv_bundle;
static int use_huge;
static int ext_stat;
static int verbose;

static int nr_bufs = 256;
static int br_mask;

static int ring_size = 128;

static pthread_mutex_t thread_lock;
static struct timeval last_housekeeping;

/*
 * For sendmsg/recvmsg. recvmsg just has a single vec, sendmsg will have
 * two vecs - one that is currently submitted and being sent, and one that
 * is being prepared. When a new sendmsg is issued, we'll swap which one we
 * use. For send, even though we don't pass in the iovec itself, we use the
 * vec to serialize the sends to avoid reordering.
 */
struct msg_vec {
	struct iovec *iov;
	/* length of allocated vec */
	int vec_size;
	/* length currently being used */
	int iov_len;
	/* only for send, current index we're processing */
	int cur_iov;
};

struct io_msg {
	struct msghdr msg;
	struct msg_vec vecs[2];
	/* current msg_vec being prepared */
	int vec_index;
};

/*
 * Per socket stats per connection. For bi-directional, we'll have both
 * sends and receives on each socket, this helps track them separately.
 * For sink or one directional, each of the two stats will be only sends
 * or receives, not both.
 */
struct conn_dir {
	int index;

	int pending_shutdown;
	int pending_send;
	int pending_recv;

	int snd_notif;

	int out_buffers;

	int rcv, rcv_shrt, rcv_enobufs, rcv_mshot;
	int snd, snd_shrt, snd_enobufs, snd_busy, snd_mshot;

	int snd_next_bid;
	int rcv_next_bid;

	int *rcv_bucket;
	int *snd_bucket;

	unsigned long in_bytes, out_bytes;

	/* only ever have a single recv pending */
	struct io_msg io_rcv_msg;

	/* one send that is inflight, and one being prepared for the next one */
	struct io_msg io_snd_msg;
};

enum {
	CONN_F_STARTED		= 1,
	CONN_F_DISCONNECTING	= 2,
	CONN_F_DISCONNECTED	= 4,
	CONN_F_PENDING_SHUTDOWN	= 8,
	CONN_F_STATS_SHOWN	= 16,
	CONN_F_END_TIME		= 32,
	CONN_F_REAPED		= 64,
};

/*
 * buffer ring belonging to a connection
 */
struct conn_buf_ring {
	struct io_uring_buf_ring *br;
	void *buf;
	int bgid;
};

struct conn {
	struct io_uring ring;

	/* receive side buffer ring, new data arrives here */
	struct conn_buf_ring in_br;
	/* if send_ring is used, outgoing data to send */
	struct conn_buf_ring out_br;

	int tid;
	int in_fd, out_fd;
	int pending_cancels;
	int flags;

	struct conn_dir cd[2];

	struct timeval start_time, end_time;

	union {
		struct sockaddr_in addr;
		struct sockaddr_in6 addr6;
	};

	pthread_t thread;
	pthread_barrier_t startup_barrier;
};

#define MAX_CONNS	1024
static struct conn conns[MAX_CONNS];

#define vlog(str, ...) do {						\
	if (verbose)							\
		printf(str, ##__VA_ARGS__);				\
} while (0)

static int prep_next_send(struct io_uring *ring, struct conn *c,
			  struct conn_dir *cd, int fd);
static void *thread_main(void *data);

static struct conn *cqe_to_conn(struct io_uring_cqe *cqe)
{
	struct userdata ud = { .val = cqe->user_data };

	return &conns[ud.op_tid & TID_MASK];
}

static struct conn_dir *cqe_to_conn_dir(struct conn *c,
					struct io_uring_cqe *cqe)
{
	int fd = cqe_to_fd(cqe);

	return &c->cd[fd != c->in_fd];
}

static int other_dir_fd(struct conn *c, int fd)
{
	if (c->in_fd == fd)
		return c->out_fd;
	return c->in_fd;
}

/* currently active msg_vec */
static struct msg_vec *msg_vec(struct io_msg *imsg)
{
	return &imsg->vecs[imsg->vec_index];
}

static struct msg_vec *snd_msg_vec(struct conn_dir *cd)
{
	return msg_vec(&cd->io_snd_msg);
}

/*
 * Goes from accept new connection -> create socket, connect to end
 * point, prepare recv, on receive do send (unless sink). If either ends
 * disconnects, we transition to shutdown and then close.
 */
enum {
	__ACCEPT	= 1,
	__SOCK		= 2,
	__CONNECT	= 3,
	__RECV		= 4,
	__RECVMSG	= 5,
	__SEND		= 6,
	__SENDMSG	= 7,
	__SHUTDOWN	= 8,
	__CANCEL	= 9,
	__CLOSE		= 10,
	__FD_PASS	= 11,
	__NOP		= 12,
	__STOP		= 13,
};

struct error_handler {
	const char *name;
	int (*error_fn)(struct error_handler *, struct io_uring *, struct io_uring_cqe *);
};

static int recv_error(struct error_handler *err, struct io_uring *ring,
		      struct io_uring_cqe *cqe);
static int send_error(struct error_handler *err, struct io_uring *ring,
		      struct io_uring_cqe *cqe);

static int default_error(struct error_handler *err,
			 struct io_uring __attribute__((__unused__)) *ring,
			 struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);

	fprintf(stderr, "%d: %s error %s\n", c->tid, err->name, strerror(-cqe->res));
	fprintf(stderr, "fd=%d, bid=%d\n", cqe_to_fd(cqe), cqe_to_bid(cqe));
	return 1;
}

/*
 * Move error handling out of the normal handling path, cleanly separating
 * them. If an opcode doesn't need any error handling, set it to NULL. If
 * it wants to stop the connection at that point and not do anything else,
 * then the default handler can be used. Only receive has proper error
 * handling, as we can get -ENOBUFS which is not a fatal condition. It just
 * means we need to wait on buffer replenishing before re-arming the receive.
 */
static struct error_handler error_handlers[] = {
	{ .name = "NULL",	.error_fn = NULL, },
	{ .name = "ACCEPT",	.error_fn = default_error, },
	{ .name = "SOCK",	.error_fn = default_error, },
	{ .name = "CONNECT",	.error_fn = default_error, },
	{ .name = "RECV",	.error_fn = recv_error, },
	{ .name = "RECVMSG",	.error_fn = recv_error, },
	{ .name = "SEND",	.error_fn = send_error, },
	{ .name = "SENDMSG",	.error_fn = send_error, },
	{ .name = "SHUTDOWN",	.error_fn = NULL, },
	{ .name = "CANCEL",	.error_fn = NULL, },
	{ .name = "CLOSE",	.error_fn = NULL, },
	{ .name = "FD_PASS",	.error_fn = default_error, },
	{ .name = "NOP",	.error_fn = NULL, },
	{ .name = "STOP",	.error_fn = default_error, },
};

static void free_buffer_ring(struct io_uring *ring, struct conn_buf_ring *cbr)
{
	if (!cbr->br)
		return;

	io_uring_free_buf_ring(ring, cbr->br, nr_bufs, cbr->bgid);
	cbr->br = NULL;
	if (use_huge)
		munmap(cbr->buf, buf_size * nr_bufs);
	else
		free(cbr->buf);
}

static void free_buffer_rings(struct io_uring *ring, struct conn *c)
{
	free_buffer_ring(ring, &c->in_br);
	free_buffer_ring(ring, &c->out_br);
}

/*
 * Setup a ring provided buffer ring for each connection. If we get -ENOBUFS
 * on receive, for multishot receive we'll wait for half the provided buffers
 * to be returned by pending sends, then re-arm the multishot receive. If
 * this happens too frequently (see enobufs= stat), then the ring size is
 * likely too small. Use -nXX to make it bigger. See recv_enobufs().
 *
 * The alternative here would be to use the older style provided buffers,
 * where you simply setup a buffer group and use SQEs with
 * io_urign_prep_provide_buffers() to add to the pool. But that approach is
 * slower and has been deprecated by using the faster ring provided buffers.
 */
static int setup_recv_ring(struct io_uring *ring, struct conn *c)
{
	struct conn_buf_ring *cbr = &c->in_br;
	int br_flags = 0;
	int ret, i;
	size_t len;
	void *ptr;

	len = buf_size * nr_bufs;
	if (use_huge) {
		cbr->buf = mmap(NULL, len, PROT_READ|PROT_WRITE,
				MAP_PRIVATE|MAP_HUGETLB|MAP_HUGE_2MB|MAP_ANONYMOUS,
				-1, 0);
		if (cbr->buf == MAP_FAILED) {
			perror("mmap");
			return 1;
		}
	} else {
		if (posix_memalign(&cbr->buf, page_size, len)) {
			perror("posix memalign");
			return 1;
		}
	}
	if (buf_ring_inc)
		br_flags = IOU_PBUF_RING_INC;
	cbr->br = io_uring_setup_buf_ring(ring, nr_bufs, cbr->bgid, br_flags, &ret);
	if (!cbr->br) {
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	ptr = cbr->buf;
	for (i = 0; i < nr_bufs; i++) {
		vlog("%d: add bid %d, data %p\n", c->tid, i, ptr);
		io_uring_buf_ring_add(cbr->br, ptr, buf_size, i, br_mask, i);
		ptr += buf_size;
	}
	io_uring_buf_ring_advance(cbr->br, nr_bufs);
	printf("%d: recv buffer ring bgid %d, bufs %d\n", c->tid, cbr->bgid, nr_bufs);
	return 0;
}

/*
 * If 'send_ring' is used and the kernel supports it, we can skip serializing
 * sends as the data will be ordered regardless. This reduces the send handling
 * complexity, as buffers can always be added to the outgoing ring and will be
 * processed in the order in which they were added.
 */
static int setup_send_ring(struct io_uring *ring, struct conn *c)
{
	struct conn_buf_ring *cbr = &c->out_br;
	int br_flags = 0;
	int ret;

	if (buf_ring_inc)
		br_flags = IOU_PBUF_RING_INC;
	cbr->br = io_uring_setup_buf_ring(ring, nr_bufs, cbr->bgid, br_flags, &ret);
	if (!cbr->br) {
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	printf("%d: send buffer ring bgid %d, bufs %d\n", c->tid, cbr->bgid, nr_bufs);
	return 0;
}

static int setup_send_zc(struct io_uring *ring, struct conn *c)
{
	struct iovec *iovs;
	void *buf;
	int i, ret;

	if (snd_msg)
		return 0;

	buf = c->in_br.buf;
	iovs = calloc(nr_bufs, sizeof(struct iovec));
	for (i = 0; i < nr_bufs; i++) {
		iovs[i].iov_base = buf;
		iovs[i].iov_len = buf_size;
		buf += buf_size;
	}

	ret = io_uring_register_buffers(ring, iovs, nr_bufs);
	if (ret) {
		fprintf(stderr, "failed registering buffers: %d\n", ret);
		free(iovs);
		return ret;
	}
	free(iovs);
	return 0;
}

/*
 * Setup an input and output buffer ring.
 */
static int setup_buffer_rings(struct io_uring *ring, struct conn *c)
{
	int ret;

	/* no locking needed on cur_bgid, parent serializes setup */
	c->in_br.bgid = cur_bgid++;
	c->out_br.bgid = cur_bgid++;
	c->out_br.br = NULL;

	ret = setup_recv_ring(ring, c);
	if (ret)
		return ret;
	if (is_sink)
		return 0;
	if (snd_zc) {
		ret = setup_send_zc(ring, c);
		if (ret)
			return ret;
	}
	if (send_ring) {
		ret = setup_send_ring(ring, c);
		if (ret) {
			free_buffer_ring(ring, &c->in_br);
			return ret;
		}
	}

	return 0;
}

struct bucket_stat {
	int nr_packets;
	int count;
};

static int stat_cmp(const void *p1, const void *p2)
{
	const struct bucket_stat *b1 = p1;
	const struct bucket_stat *b2 = p2;

	if (b1->count < b2->count)
		return 1;
	else if (b1->count > b2->count)
		return -1;
	return 0;
}

static void show_buckets(struct conn_dir *cd)
{
	unsigned long snd_total, rcv_total;
	struct bucket_stat *rstat, *sstat;
	int i;

	if (!cd->rcv_bucket || !cd->snd_bucket)
		return;

	rstat = calloc(nr_bufs + 1, sizeof(struct bucket_stat));
	sstat = calloc(nr_bufs + 1, sizeof(struct bucket_stat));

	snd_total = rcv_total = 0;
	for (i = 0; i <= nr_bufs; i++) {
		snd_total += cd->snd_bucket[i];
		sstat[i].nr_packets = i;
		sstat[i].count = cd->snd_bucket[i];
		rcv_total += cd->rcv_bucket[i];
		rstat[i].nr_packets = i;
		rstat[i].count = cd->rcv_bucket[i];
	}

	if (!snd_total && !rcv_total) {
		free(sstat);
		free(rstat);
	}
	if (snd_total)
		qsort(sstat, nr_bufs, sizeof(struct bucket_stat), stat_cmp);
	if (rcv_total)
		qsort(rstat, nr_bufs, sizeof(struct bucket_stat), stat_cmp);

	printf("\t Packets per recv/send:\n");
	for (i = 0; i <= nr_bufs; i++) {
		double snd_prc = 0.0, rcv_prc = 0.0;
		if (!rstat[i].count && !sstat[i].count)
			continue;
		if (rstat[i].count)
			rcv_prc = 100.0 * (rstat[i].count / (double) rcv_total);
		if (sstat[i].count)
			snd_prc = 100.0 * (sstat[i].count / (double) snd_total);
		printf("\t bucket(%3d/%3d): rcv=%u (%.2f%%) snd=%u (%.2f%%)\n",
				rstat[i].nr_packets, sstat[i].nr_packets,
				rstat[i].count, rcv_prc,
				sstat[i].count, snd_prc);
	}

	free(sstat);
	free(rstat);
}

static void __show_stats(struct conn *c)
{
	unsigned long msec, qps;
	unsigned long bytes, bw;
	struct conn_dir *cd;
	int i;

	if (c->flags & (CONN_F_STATS_SHOWN | CONN_F_REAPED))
		return;
	if (!(c->flags & CONN_F_STARTED))
		return;

	if (!(c->flags & CONN_F_END_TIME))
		gettimeofday(&c->end_time, NULL);

	msec = (c->end_time.tv_sec - c->start_time.tv_sec) * 1000;
	msec += (c->end_time.tv_usec - c->start_time.tv_usec) / 1000;

	qps = 0;
	for (i = 0; i < 2; i++)
		qps += c->cd[i].rcv + c->cd[i].snd;

	if (!qps)
		return;

	if (msec)
		qps = (qps * 1000) / msec;

	printf("Conn %d/(in_fd=%d, out_fd=%d): qps=%lu, msec=%lu\n", c->tid,
					c->in_fd, c->out_fd, qps, msec);

	bytes = 0;
	for (i = 0; i < 2; i++) {
		cd = &c->cd[i];

		if (!cd->in_bytes && !cd->out_bytes && !cd->snd && !cd->rcv)
			continue;

		bytes += cd->in_bytes;
		bytes += cd->out_bytes;

		printf("\t%3d: rcv=%u (short=%u, enobufs=%d), snd=%u (short=%u,"
			" busy=%u, enobufs=%d)\n", i, cd->rcv, cd->rcv_shrt,
			cd->rcv_enobufs, cd->snd, cd->snd_shrt, cd->snd_busy,
			cd->snd_enobufs);
		printf("\t   : in_bytes=%lu (Kb %lu), out_bytes=%lu (Kb %lu)\n",
			cd->in_bytes, cd->in_bytes >> 10,
			cd->out_bytes, cd->out_bytes >> 10);
		printf("\t   : mshot_rcv=%d, mshot_snd=%d\n", cd->rcv_mshot,
			cd->snd_mshot);
		show_buckets(cd);

	}
	if (msec) {
		bytes *= 8UL;
		bw = bytes / 1000;
		bw /= msec;
		printf("\tBW=%'luMbit\n", bw);
	}

	c->flags |= CONN_F_STATS_SHOWN;
}

static void show_stats(void)
{
	float events_per_loop = 0.0;
	static int stats_shown;
	int i;

	if (stats_shown)
		return;

	if (events)
		events_per_loop = (float) events / (float) event_loops;

	printf("Event loops: %lu, events %lu, events per loop %.2f\n", event_loops,
							events, events_per_loop);

	for (i = 0; i < MAX_CONNS; i++) {
		struct conn *c = &conns[i];

		__show_stats(c);
	}
	stats_shown = 1;
}

static void sig_int(int __attribute__((__unused__)) sig)
{
	printf("\n");
	show_stats();
	exit(1);
}

/*
 * Special cased for SQPOLL only, as we don't control when SQEs are consumed if
 * that is used. Hence we may need to wait for the SQPOLL thread to keep up
 * until we can get a new SQE. All other cases will break immediately, with a
 * fresh SQE.
 *
 * If we grossly undersized our SQ ring, getting a NULL sqe can happen even
 * for the !SQPOLL case if we're handling a lot of CQEs in our event loop
 * and multishot isn't used. We can do io_uring_submit() to flush what we
 * have here. Only caveat here is that if linked requests are used, SQEs
 * would need to be allocated upfront as a link chain is only valid within
 * a single submission cycle.
 */
static struct io_uring_sqe *get_sqe(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;

	do {
		sqe = io_uring_get_sqe(ring);
		if (sqe)
			break;
		if (!sqpoll)
			io_uring_submit(ring);
		else
			io_uring_sqring_wait(ring);
	} while (1);

	return sqe;
}

/*
 * See __encode_userdata() for how we encode sqe->user_data, which is passed
 * back as cqe->user_data at completion time.
 */
static void encode_userdata(struct io_uring_sqe *sqe, struct conn *c, int op,
			    int bid, int fd)
{
	__encode_userdata(sqe, c->tid, op, bid, fd);
}

static void __submit_receive(struct io_uring *ring, struct conn *c,
			     struct conn_dir *cd, int fd)
{
	struct conn_buf_ring *cbr = &c->in_br;
	struct io_uring_sqe *sqe;

	vlog("%d: submit receive fd=%d\n", c->tid, fd);

	assert(!cd->pending_recv);
	cd->pending_recv = 1;

	/*
	 * For both recv and multishot receive, we use the ring provided
	 * buffers. These are handed to the application ahead of time, and
	 * are consumed when a receive triggers. Note that the address and
	 * length of the receive are set to NULL/0, and we assign the
	 * sqe->buf_group to tell the kernel which buffer group ID to pick
	 * a buffer from. Finally, IOSQE_BUFFER_SELECT is set to tell the
	 * kernel that we want a buffer picked for this request, we are not
	 * passing one in with the request.
	 */
	sqe = get_sqe(ring);
	if (rcv_msg) {
		struct io_msg *imsg = &cd->io_rcv_msg;
		struct msghdr *msg = &imsg->msg;

		memset(msg, 0, sizeof(*msg));
		msg->msg_iov = msg_vec(imsg)->iov;
		msg->msg_iovlen = msg_vec(imsg)->iov_len;

		if (recv_mshot) {
			cd->rcv_mshot++;
			io_uring_prep_recvmsg_multishot(sqe, fd, &imsg->msg, 0);
		} else {
			io_uring_prep_recvmsg(sqe, fd, &imsg->msg, 0);
		}
	} else {
		if (recv_mshot) {
			cd->rcv_mshot++;
			io_uring_prep_recv_multishot(sqe, fd, NULL, 0, 0);
		} else {
			io_uring_prep_recv(sqe, fd, NULL, 0, 0);
		}
	}
	encode_userdata(sqe, c, __RECV, 0, fd);
	sqe->buf_group = cbr->bgid;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	if (fixed_files)
		sqe->flags |= IOSQE_FIXED_FILE;
	if (rcv_bundle)
		sqe->ioprio |= IORING_RECVSEND_BUNDLE;
}

/*
 * One directional just arms receive on our in_fd
 */
static void submit_receive(struct io_uring *ring, struct conn *c)
{
	__submit_receive(ring, c, &c->cd[0], c->in_fd);
}

/*
 * Bi-directional arms receive on both in and out fd
 */
static void submit_bidi_receive(struct io_uring *ring, struct conn *c)
{
	__submit_receive(ring, c, &c->cd[0], c->in_fd);
	__submit_receive(ring, c, &c->cd[1], c->out_fd);
}

/*
 * We hit -ENOBUFS, which means that we ran out of buffers in our current
 * provided buffer group. This can happen if there's an imbalance between the
 * receives coming in and the sends being processed, particularly with multishot
 * receive as they can trigger very quickly. If this happens, defer arming a
 * new receive until we've replenished half of the buffer pool by processing
 * pending sends.
 */
static void recv_enobufs(struct io_uring *ring, struct conn *c,
			 struct conn_dir *cd, int fd)
{
	vlog("%d: enobufs hit\n", c->tid);

	cd->rcv_enobufs++;

	/*
	 * If we're a sink, mark rcv as rearm. If we're not, then mark us as
	 * needing a rearm for receive and send. The completing send will
	 * kick the recv rearm.
	 */
	if (!is_sink) {
		int do_recv_arm = 1;

		if (!cd->pending_send)
			do_recv_arm = !prep_next_send(ring, c, cd, fd);
		if (do_recv_arm)
			__submit_receive(ring, c, &c->cd[0], c->in_fd);
	} else {
		__submit_receive(ring, c, &c->cd[0], c->in_fd);
	}
}

/*
 * Kill this socket - submit a shutdown and link a close to it. We don't
 * care about shutdown status, so mark it as not needing to post a CQE unless
 * it fails.
 */
static void queue_shutdown_close(struct io_uring *ring, struct conn *c, int fd)
{
	struct io_uring_sqe *sqe1, *sqe2;

	/*
	 * On the off chance that we run out of SQEs after the first one,
	 * grab two upfront. This it to prevent our link not working if
	 * get_sqe() ends up doing submissions to free up an SQE, as links
	 * are not valid across separate submissions.
	 */
	sqe1 = get_sqe(ring);
	sqe2 = get_sqe(ring);

	io_uring_prep_shutdown(sqe1, fd, SHUT_RDWR);
	if (fixed_files)
		sqe1->flags |= IOSQE_FIXED_FILE;
	sqe1->flags |= IOSQE_IO_LINK | IOSQE_CQE_SKIP_SUCCESS;
	encode_userdata(sqe1, c, __SHUTDOWN, 0, fd);

	if (fixed_files)
		io_uring_prep_close_direct(sqe2, fd);
	else
		io_uring_prep_close(sqe2, fd);
	encode_userdata(sqe2, c, __CLOSE, 0, fd);
}

/*
 * This connection is going away, queue a cancel for any pending recv, for
 * example, we have pending for this ring. For completeness, we issue a cancel
 * for any request we have pending for both in_fd and out_fd.
 */
static void queue_cancel(struct io_uring *ring, struct conn *c)
{
	struct io_uring_sqe *sqe;
	int flags = 0;

	if (fixed_files)
		flags |= IORING_ASYNC_CANCEL_FD_FIXED;

	sqe = get_sqe(ring);
	io_uring_prep_cancel_fd(sqe, c->in_fd, flags);
	encode_userdata(sqe, c, __CANCEL, 0, c->in_fd);
	c->pending_cancels++;

	if (c->out_fd != -1) {
		sqe = get_sqe(ring);
		io_uring_prep_cancel_fd(sqe, c->out_fd, flags);
		encode_userdata(sqe, c, __CANCEL, 0, c->out_fd);
		c->pending_cancels++;
	}

	io_uring_submit(ring);
}

static int pending_shutdown(struct conn *c)
{
	return c->cd[0].pending_shutdown + c->cd[1].pending_shutdown;
}

static bool should_shutdown(struct conn *c)
{
	int i;

	if (!pending_shutdown(c))
		return false;
	if (is_sink)
		return true;
	if (!bidi)
		return c->cd[0].in_bytes == c->cd[1].out_bytes;

	for (i = 0; i < 2; i++) {
		if (c->cd[0].rcv != c->cd[1].snd)
			return false;
		if (c->cd[1].rcv != c->cd[0].snd)
			return false;
	}

	return true;
}

/*
 * Close this connection - send a ring message to the connection with intent
 * to stop. When the client gets the message, it will initiate the stop.
 */
static void __close_conn(struct io_uring *ring, struct conn *c)
{
	struct io_uring_sqe *sqe;
	uint64_t user_data;

	printf("Client %d: queueing stop\n", c->tid);

	user_data = __raw_encode(c->tid, __STOP, 0, 0);
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_msg_ring(sqe, c->ring.ring_fd, 0, user_data, 0);
	encode_userdata(sqe, c, __NOP, 0, 0);
	io_uring_submit(ring);
}

static void close_cd(struct conn *c, struct conn_dir *cd)
{
	cd->pending_shutdown = 1;

	if (cd->pending_send)
		return;

	if (!(c->flags & CONN_F_PENDING_SHUTDOWN)) {
		gettimeofday(&c->end_time, NULL);
		c->flags |= CONN_F_PENDING_SHUTDOWN | CONN_F_END_TIME;
	}
}

/*
 * We're done with this buffer, add it back to our pool so the kernel is
 * free to use it again.
 */
static int replenish_buffer(struct conn_buf_ring *cbr, int bid, int offset)
{
	void *this_buf = cbr->buf + bid * buf_size;

	assert(bid < nr_bufs);

	io_uring_buf_ring_add(cbr->br, this_buf, buf_size, bid, br_mask, offset);
	return buf_size;
}

/*
 * Iterate buffers from '*bid' and with a total size of 'bytes' and add them
 * back to our receive ring so they can be reused for new receives.
 */
static int replenish_buffers(struct conn *c, int *bid, int bytes)
{
	struct conn_buf_ring *cbr = &c->in_br;
	int nr_packets = 0;

	while (bytes) {
		int this_len = replenish_buffer(cbr, *bid, nr_packets);

		if (this_len > bytes)
			this_len = bytes;
		bytes -= this_len;

		*bid = (*bid + 1) & (nr_bufs - 1);
		nr_packets++;
	}

	io_uring_buf_ring_advance(cbr->br, nr_packets);
	return nr_packets;
}

static void free_mvec(struct msg_vec *mvec)
{
	free(mvec->iov);
	mvec->iov = NULL;
}

static void init_mvec(struct msg_vec *mvec)
{
	memset(mvec, 0, sizeof(*mvec));
	mvec->iov = malloc(sizeof(struct iovec));
	mvec->vec_size = 1;
}

static void init_msgs(struct conn_dir *cd)
{
	memset(&cd->io_snd_msg, 0, sizeof(cd->io_snd_msg));
	memset(&cd->io_rcv_msg, 0, sizeof(cd->io_rcv_msg));
	init_mvec(&cd->io_snd_msg.vecs[0]);
	init_mvec(&cd->io_snd_msg.vecs[1]);
	init_mvec(&cd->io_rcv_msg.vecs[0]);
}

static void free_msgs(struct conn_dir *cd)
{
	free_mvec(&cd->io_snd_msg.vecs[0]);
	free_mvec(&cd->io_snd_msg.vecs[1]);
	free_mvec(&cd->io_rcv_msg.vecs[0]);
}

/*
 * Multishot accept completion triggered. If we're acting as a sink, we're
 * good to go. Just issue a receive for that case. If we're acting as a proxy,
 * then start opening a socket that we can use to connect to the other end.
 */
static int handle_accept(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c;
	int i;

	if (nr_conns == MAX_CONNS) {
		fprintf(stderr, "max clients reached %d\n", nr_conns);
		return 1;
	}

	/* main thread handles this, which is obviously serialized */
	c = &conns[nr_conns];
	c->tid = nr_conns++;
	c->in_fd = -1;
	c->out_fd = -1;

	for (i = 0; i < 2; i++) {
		struct conn_dir *cd = &c->cd[i];

		cd->index = i;
		cd->snd_next_bid = -1;
		cd->rcv_next_bid = -1;
		if (ext_stat) {
			cd->rcv_bucket = calloc(nr_bufs + 1, sizeof(int));
			cd->snd_bucket = calloc(nr_bufs + 1, sizeof(int));
		}
		init_msgs(cd);
	}

	printf("New client: id=%d, in=%d\n", c->tid, c->in_fd);
	gettimeofday(&c->start_time, NULL);

	pthread_barrier_init(&c->startup_barrier, NULL, 2);
	pthread_create(&c->thread, NULL, thread_main, c);

	/*
	 * Wait for thread to have its ring setup, then either assign the fd
	 * if it's non-fixed, or pass the fixed one
	 */
	pthread_barrier_wait(&c->startup_barrier);
	if (!fixed_files) {
		c->in_fd = cqe->res;
	} else {
		struct io_uring_sqe *sqe;
		uint64_t user_data;

		/*
		 * Ring has just been setup, we'll use index 0 as the descriptor
		 * value.
		 */
		user_data = __raw_encode(c->tid, __FD_PASS, 0, 0);
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_msg_ring_fd(sqe, c->ring.ring_fd, cqe->res, 0,
						user_data, 0);
		encode_userdata(sqe, c, __NOP, 0, cqe->res);
	}

	return 0;
}

/*
 * Our socket request completed, issue a connect request to the other end.
 */
static int handle_sock(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct io_uring_sqe *sqe;
	int ret;

	vlog("%d: sock: res=%d\n", c->tid, cqe->res);

	c->out_fd = cqe->res;

	if (ipv6) {
		memset(&c->addr6, 0, sizeof(c->addr6));
		c->addr6.sin6_family = AF_INET6;
		c->addr6.sin6_port = htons(send_port);
		ret = inet_pton(AF_INET6, host, &c->addr6.sin6_addr);
	} else {
		memset(&c->addr, 0, sizeof(c->addr));
		c->addr.sin_family = AF_INET;
		c->addr.sin_port = htons(send_port);
		ret = inet_pton(AF_INET, host, &c->addr.sin_addr);
	}
	if (ret <= 0) {
		if (!ret)
			fprintf(stderr, "host not in right format\n");
		else
			perror("inet_pton");
		return 1;
	}

	sqe = get_sqe(ring);
	if (ipv6) {
		io_uring_prep_connect(sqe, c->out_fd,
					(struct sockaddr *) &c->addr6,
					sizeof(c->addr6));
	} else {
		io_uring_prep_connect(sqe, c->out_fd,
					(struct sockaddr *) &c->addr,
					sizeof(c->addr));
	}
	encode_userdata(sqe, c, __CONNECT, 0, c->out_fd);
	if (fixed_files)
		sqe->flags |= IOSQE_FIXED_FILE;
	return 0;
}

/*
 * Connection to the other end is done, submit a receive to start receiving
 * data. If we're a bidirectional proxy, issue a receive on both ends. If not,
 * then just a single recv will do.
 */
static int handle_connect(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);

	pthread_mutex_lock(&thread_lock);
	open_conns++;
	pthread_mutex_unlock(&thread_lock);

	if (bidi)
		submit_bidi_receive(ring, c);
	else
		submit_receive(ring, c);

	return 0;
}

/*
 * Append new segment to our currently active msg_vec. This will be submitted
 * as a sendmsg (with all of it), or as separate sends, later. If we're using
 * send_ring, then we won't hit this path. Instead, outgoing buffers are
 * added directly to our outgoing send buffer ring.
 */
static void send_append_vec(struct conn_dir *cd, void *data, int len)
{
	struct msg_vec *mvec = snd_msg_vec(cd);

	if (mvec->iov_len == mvec->vec_size) {
		mvec->vec_size <<= 1;
		mvec->iov = realloc(mvec->iov, mvec->vec_size * sizeof(struct iovec));
	}

	mvec->iov[mvec->iov_len].iov_base = data;
	mvec->iov[mvec->iov_len].iov_len = len;
	mvec->iov_len++;
}

/*
 * Queue a send based on the data received in this cqe, which came from
 * a completed receive operation.
 */
static void send_append(struct conn *c, struct conn_dir *cd, void *data,
			int bid, int len)
{
	vlog("%d: send %d (%p, bid %d)\n", c->tid, len, data, bid);

	assert(bid < nr_bufs);

	/* if using provided buffers for send, add it upfront */
	if (send_ring) {
		struct conn_buf_ring *cbr = &c->out_br;

		io_uring_buf_ring_add(cbr->br, data, len, bid, br_mask, 0);
		io_uring_buf_ring_advance(cbr->br, 1);
	} else {
		send_append_vec(cd, data, len);
	}
}

/*
 * For non recvmsg && multishot, a zero receive marks the end. For recvmsg
 * with multishot, we always get the header regardless. Hence a "zero receive"
 * is the size of the header.
 */
static int recv_done_res(int res)
{
	if (!res)
		return 1;
	if (rcv_msg && recv_mshot && res == sizeof(struct io_uring_recvmsg_out))
		return 1;
	return 0;
}

static int recv_inc(struct conn *c, struct conn_dir *cd, int *bid,
		    struct io_uring_cqe *cqe)
{
	struct conn_buf_ring *cbr = &c->out_br;
	struct conn_buf_ring *in_cbr = &c->in_br;
	void *data;

	if (!cqe->res)
		return 0;
	if (cqe->flags & IORING_CQE_F_BUF_MORE)
		return 0;

	data = in_cbr->buf + *bid * buf_size;
	if (is_sink) {
		io_uring_buf_ring_add(in_cbr->br, data, buf_size, *bid, br_mask, 0);
		io_uring_buf_ring_advance(in_cbr->br, 1);
	} else if (send_ring) {
		io_uring_buf_ring_add(cbr->br, data, buf_size, *bid, br_mask, 0);
		io_uring_buf_ring_advance(cbr->br, 1);
	} else {
		send_append(c, cd, data, *bid, buf_size);
	}
	*bid = (*bid + 1) & (nr_bufs - 1);
	return 1;
}

/*
 * Any receive that isn't recvmsg with multishot can be handled the same way.
 * Iterate from '*bid' and 'in_bytes' in total, and append the data to the
 * outgoing queue.
 */
static int recv_bids(struct conn *c, struct conn_dir *cd, int *bid, int in_bytes)
{
	struct conn_buf_ring *cbr = &c->out_br;
	struct conn_buf_ring *in_cbr = &c->in_br;
	struct io_uring_buf *buf;
	int nr_packets = 0;

	while (in_bytes) {
		int this_bytes;
		void *data;

		buf = &in_cbr->br->bufs[*bid];
		data = (void *) (unsigned long) buf->addr;
		this_bytes = buf->len;
		if (this_bytes > in_bytes)
			this_bytes = in_bytes;

		in_bytes -= this_bytes;

		if (send_ring)
			io_uring_buf_ring_add(cbr->br, data, this_bytes, *bid,
						br_mask, nr_packets);
		else
			send_append(c, cd, data, *bid, this_bytes);

		*bid = (*bid + 1) & (nr_bufs - 1);
		nr_packets++;
	}

	if (send_ring)
		io_uring_buf_ring_advance(cbr->br, nr_packets);

	return nr_packets;
}

/*
 * Special handling of recvmsg with multishot
 */
static int recv_mshot_msg(struct conn *c, struct conn_dir *cd, int *bid,
			  int in_bytes)
{
	struct conn_buf_ring *cbr = &c->out_br;
	struct conn_buf_ring *in_cbr = &c->in_br;
	struct io_uring_buf *buf;
	int nr_packets = 0;

	while (in_bytes) {
		struct io_uring_recvmsg_out *pdu;
		int this_bytes;
		void *data;

		buf = &in_cbr->br->bufs[*bid];

		/*
		 * multishot recvmsg puts a header in front of the data - we
		 * have to take that into account for the send setup, and
		 * adjust the actual data read to not take this metadata into
		 * account. For this use case, namelen and controllen will not
		 * be set. If they were, they would need to be factored in too.
		 */
		buf->len -= sizeof(struct io_uring_recvmsg_out);
		in_bytes -= sizeof(struct io_uring_recvmsg_out);

		pdu = (void *) (unsigned long) buf->addr;
		vlog("pdu namelen %d, controllen %d, payload %d flags %x\n",
				pdu->namelen, pdu->controllen, pdu->payloadlen,
				pdu->flags);
		data = (void *) (pdu + 1);

		this_bytes = pdu->payloadlen;
		if (this_bytes > in_bytes)
			this_bytes = in_bytes;

		in_bytes -= this_bytes;

		if (send_ring)
			io_uring_buf_ring_add(cbr->br, data, this_bytes, *bid,
						br_mask, nr_packets);
		else
			send_append(c, cd, data, *bid, this_bytes);

		*bid = (*bid + 1) & (nr_bufs - 1);
		nr_packets++;
	}

	if (send_ring)
		io_uring_buf_ring_advance(cbr->br, nr_packets);

	return nr_packets;
}

static int __handle_recv(struct io_uring *ring, struct conn *c,
			 struct conn_dir *cd, struct io_uring_cqe *cqe)
{
	struct conn_dir *ocd = &c->cd[!cd->index];
	int bid, nr_packets;

	/*
	 * Not having a buffer attached should only happen if we get a zero
	 * sized receive, because the other end closed the connection. It
	 * cannot happen otherwise, as all our receives are using provided
	 * buffers and hence it's not possible to return a CQE with a non-zero
	 * result and not have a buffer attached.
	 */
	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		cd->pending_recv = 0;

		if (!recv_done_res(cqe->res)) {
			fprintf(stderr, "no buffer assigned, res=%d\n", cqe->res);
			return 1;
		}
start_close:
		prep_next_send(ring, c, ocd, other_dir_fd(c, cqe_to_fd(cqe)));
		close_cd(c, cd);
		return 0;
	}

	if (cqe->res && cqe->res < buf_size)
		cd->rcv_shrt++;

	bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;

	/*
	 * BIDI will use the same buffer pool and do receive on both CDs,
	 * so can't reliably check. TODO.
	 */
	if (!bidi && cd->rcv_next_bid != -1 && bid != cd->rcv_next_bid) {
		fprintf(stderr, "recv bid %d, wanted %d\n", bid, cd->rcv_next_bid);
		goto start_close;
	}

	vlog("%d: recv: bid=%d, res=%d, cflags=%x\n", c->tid, bid, cqe->res, cqe->flags);
	/*
	 * If we're a sink, we're done here. Just replenish the buffer back
	 * to the pool. For proxy mode, we will send the data to the other
	 * end and the buffer will be replenished once the send is done with
	 * it.
	 */
	if (buf_ring_inc)
		nr_packets = recv_inc(c, ocd, &bid, cqe);
	else if (is_sink)
		nr_packets = replenish_buffers(c, &bid, cqe->res);
	else if (rcv_msg && recv_mshot)
		nr_packets = recv_mshot_msg(c, ocd, &bid, cqe->res);
	else
		nr_packets = recv_bids(c, ocd, &bid, cqe->res);

	if (cd->rcv_bucket)
		cd->rcv_bucket[nr_packets]++;

	if (!is_sink) {
		ocd->out_buffers += nr_packets;
		assert(ocd->out_buffers <= nr_bufs);
	}

	cd->rcv++;
	cd->rcv_next_bid = bid;

	/*
	 * If IORING_CQE_F_MORE isn't set, then this is either a normal recv
	 * that needs rearming, or it's a multishot that won't post any further
	 * completions. Setup a new one for these cases.
	 */
	if (!(cqe->flags & IORING_CQE_F_MORE)) {
		cd->pending_recv = 0;
		if (recv_done_res(cqe->res))
			goto start_close;
		if (is_sink || !ocd->pending_send)
			__submit_receive(ring, c, cd, cqe_to_fd(cqe));
	}

	/*
	 * Submit a send if we won't get anymore notifications from this
	 * recv, or if we have nr_bufs / 2 queued up. If BIDI mode, send
	 * every buffer. We assume this is interactive mode, and hence don't
	 * delay anything.
	 */
	if (((!ocd->pending_send && (bidi || (ocd->out_buffers >= nr_bufs / 2))) ||
	    !(cqe->flags & IORING_CQE_F_MORE)) && !is_sink)
		prep_next_send(ring, c, ocd, other_dir_fd(c, cqe_to_fd(cqe)));

	if (!recv_done_res(cqe->res))
		cd->in_bytes += cqe->res;
	return 0;
}

static int handle_recv(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct conn_dir *cd = cqe_to_conn_dir(c, cqe);

	return __handle_recv(ring, c, cd, cqe);
}

static int recv_error(struct error_handler *err, struct io_uring *ring,
		      struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct conn_dir *cd = cqe_to_conn_dir(c, cqe);

	cd->pending_recv = 0;

	if (cqe->res != -ENOBUFS)
		return default_error(err, ring, cqe);

	recv_enobufs(ring, c, cd, other_dir_fd(c, cqe_to_fd(cqe)));
	return 0;
}

static void submit_send(struct io_uring *ring, struct conn *c,
			struct conn_dir *cd, int fd, void *data, int len,
			int bid, int flags)
{
	struct io_uring_sqe *sqe;
	int bgid = c->out_br.bgid;

	if (cd->pending_send)
		return;
	cd->pending_send = 1;

	flags |= MSG_WAITALL | MSG_NOSIGNAL;

	sqe = get_sqe(ring);
	if (snd_msg) {
		struct io_msg *imsg = &cd->io_snd_msg;

		if (snd_zc) {
			io_uring_prep_sendmsg_zc(sqe, fd, &imsg->msg, flags);
			cd->snd_notif++;
		} else {
			io_uring_prep_sendmsg(sqe, fd, &imsg->msg, flags);
		}
	} else if (send_ring) {
		io_uring_prep_send(sqe, fd, NULL, 0, flags);
	} else if (!snd_zc) {
		io_uring_prep_send(sqe, fd, data, len, flags);
	} else {
		io_uring_prep_send_zc(sqe, fd, data, len, flags, 0);
		sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
		sqe->buf_index = bid;
		cd->snd_notif++;
	}
	encode_userdata(sqe, c, __SEND, bid, fd);
	if (fixed_files)
		sqe->flags |= IOSQE_FIXED_FILE;
	if (send_ring) {
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = bgid;
	}
	if (snd_bundle) {
		sqe->ioprio |= IORING_RECVSEND_BUNDLE;
		cd->snd_mshot++;
	} else if (send_ring)
		cd->snd_mshot++;
}

/*
 * Prepare the next send request, if we need to. If one is already pending,
 * or if we're a sink and we don't need to do sends, then there's nothing
 * to do.
 *
 * Return 1 if another send completion is expected, 0 if not.
 */
static int prep_next_send(struct io_uring *ring, struct conn *c,
			   struct conn_dir *cd, int fd)
{
	int bid;

	if (cd->pending_send || is_sink)
		return 0;
	if (!cd->out_buffers)
		return 0;

	bid = cd->snd_next_bid;
	if (bid == -1)
		bid = 0;

	if (send_ring) {
		/*
		 * send_ring mode is easy, there's nothing to do but submit
		 * our next send request. That will empty the entire outgoing
		 * queue.
		 */
		submit_send(ring, c, cd, fd, NULL, 0, bid, 0);
		return 1;
	} else if (snd_msg) {
		/*
		 * For sendmsg mode, submit our currently prepared iovec, if
		 * we have one, and swap our iovecs so that any further
		 * receives will start preparing that one.
		 */
		struct io_msg *imsg = &cd->io_snd_msg;

		if (!msg_vec(imsg)->iov_len)
			return 0;
		imsg->msg.msg_iov = msg_vec(imsg)->iov;
		imsg->msg.msg_iovlen = msg_vec(imsg)->iov_len;
		msg_vec(imsg)->iov_len = 0;
		imsg->vec_index = !imsg->vec_index;
		submit_send(ring, c, cd, fd, NULL, 0, bid, 0);
		return 1;
	} else {
		/*
		 * send without send_ring - submit the next available vec,
		 * if any. If this vec is the last one in the current series,
		 * then swap to the next vec. We flag each send with MSG_MORE,
		 * unless this is the last part of the current vec.
		 */
		struct io_msg *imsg = &cd->io_snd_msg;
		struct msg_vec *mvec = msg_vec(imsg);
		int flags = !snd_zc ? MSG_MORE : 0;
		struct iovec *iov;

		if (mvec->iov_len == mvec->cur_iov)
			return 0;
		imsg->msg.msg_iov = msg_vec(imsg)->iov;
		iov = &mvec->iov[mvec->cur_iov];
		mvec->cur_iov++;
		if (mvec->cur_iov == mvec->iov_len) {
			mvec->iov_len = 0;
			mvec->cur_iov = 0;
			imsg->vec_index = !imsg->vec_index;
			flags = 0;
		}
		submit_send(ring, c, cd, fd, iov->iov_base, iov->iov_len, bid, flags);
		return 1;
	}
}

static int handle_send_inc(struct conn *c, struct conn_dir *cd, int bid,
			   struct io_uring_cqe *cqe)
{
	struct conn_buf_ring *in_cbr = &c->in_br;
	int ret = 0;
	void *data;

	if (!cqe->res)
		goto out;
	if (cqe->flags & IORING_CQE_F_BUF_MORE)
		return 0;

	assert(cqe->res <= buf_size);
	cd->out_bytes += cqe->res;

	data = in_cbr->buf + bid * buf_size;
	io_uring_buf_ring_add(in_cbr->br, data, buf_size, bid, br_mask, 0);
	io_uring_buf_ring_advance(in_cbr->br, 1);
	bid = (bid + 1) & (nr_bufs - 1);
	ret = 1;
out:
	if (pending_shutdown(c))
		close_cd(c, cd);

	return ret;
}

/*
 * Handling a send with an outgoing send ring. Get the buffers from the
 * receive side, and add them to the ingoing buffer ring again.
 */
static int handle_send_ring(struct conn *c, struct conn_dir *cd, int bid,
			    int bytes)
{
	struct conn_buf_ring *in_cbr = &c->in_br;
	struct conn_buf_ring *out_cbr = &c->out_br;
	int i = 0;

	while (bytes) {
		struct io_uring_buf *buf = &out_cbr->br->bufs[bid];
		int this_bytes;
		void *this_buf;

		this_bytes = buf->len;
		if (this_bytes > bytes)
			this_bytes = bytes;

		cd->out_bytes += this_bytes;

		vlog("%d: send: bid=%d, len=%d\n", c->tid, bid, this_bytes);

		this_buf = in_cbr->buf + bid * buf_size;
		io_uring_buf_ring_add(in_cbr->br, this_buf, buf_size, bid, br_mask, i);
		/*
		 * Find the provided buffer that the receive consumed, and
		 * which we then used for the send, and add it back to the
		 * pool so it can get picked by another receive. Once the send
		 * is done, we're done with it.
		 */
		bid = (bid + 1) & (nr_bufs - 1);
		bytes -= this_bytes;
		i++;
	}
	cd->snd_next_bid = bid;
	io_uring_buf_ring_advance(in_cbr->br, i);

	if (pending_shutdown(c))
		close_cd(c, cd);

	return i;
}

/*
 * sendmsg, or send without a ring. Just add buffers back to the ingoing
 * ring for receives.
 */
static int handle_send_buf(struct conn *c, struct conn_dir *cd, int bid,
			   int bytes)
{
	struct conn_buf_ring *in_cbr = &c->in_br;
	int i = 0;

	while (bytes) {
		struct io_uring_buf *buf = &in_cbr->br->bufs[bid];
		int this_bytes;

		this_bytes = bytes;
		if (this_bytes > buf->len)
			this_bytes = buf->len;

		vlog("%d: send: bid=%d, len=%d\n", c->tid, bid, this_bytes);

		cd->out_bytes += this_bytes;
		/* each recvmsg mshot package has this overhead */
		if (rcv_msg && recv_mshot)
			cd->out_bytes += sizeof(struct io_uring_recvmsg_out);
		replenish_buffer(in_cbr, bid, i);
		bid = (bid + 1) & (nr_bufs - 1);
		bytes -= this_bytes;
		i++;
	}
	io_uring_buf_ring_advance(in_cbr->br, i);
	cd->snd_next_bid = bid;
	return i;
}

static int __handle_send(struct io_uring *ring, struct conn *c,
			 struct conn_dir *cd, struct io_uring_cqe *cqe)
{
	struct conn_dir *ocd;
	int bid, nr_packets;

	if (send_ring) {
		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "no buffer in send?! %d\n", cqe->res);
			return 1;
		}
		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
	} else {
		bid = cqe_to_bid(cqe);
	}

	/*
	 * CQE notifications only happen with send/sendmsg zerocopy. They
	 * tell us that the data has been acked, and that hence the buffer
	 * is now free to reuse. Waiting on an ACK for each packet will slow
	 * us down tremendously, so do all of our sends and then wait for
	 * the ACKs to come in. They tend to come in bundles anyway. Once
	 * all acks are done (cd->snd_notif == 0), then fire off the next
	 * receive.
	 */
	if (cqe->flags & IORING_CQE_F_NOTIF) {
		cd->snd_notif--;
	} else {
		if (cqe->res && cqe->res < buf_size)
			cd->snd_shrt++;

		/*
		 * BIDI will use the same buffer pool and do sends on both CDs,
		 * so can't reliably check. TODO.
		 */
		if (!bidi && send_ring && cd->snd_next_bid != -1 &&
		    bid != cd->snd_next_bid) {
			fprintf(stderr, "send bid %d, wanted %d at %lu\n", bid,
					cd->snd_next_bid, cd->out_bytes);
			goto out_close;
		}

		assert(bid <= nr_bufs);

		vlog("send: got %d, %lu\n", cqe->res, cd->out_bytes);

		if (buf_ring_inc)
			nr_packets = handle_send_inc(c, cd, bid, cqe);
		else if (send_ring)
			nr_packets = handle_send_ring(c, cd, bid, cqe->res);
		else
			nr_packets = handle_send_buf(c, cd, bid, cqe->res);

		if (cd->snd_bucket)
			cd->snd_bucket[nr_packets]++;

		cd->out_buffers -= nr_packets;
		assert(cd->out_buffers >= 0);

		cd->snd++;
	}

	if (!(cqe->flags & IORING_CQE_F_MORE)) {
		int do_recv_arm;

		cd->pending_send = 0;

		/*
		 * send done - see if the current vec has data to submit, and
		 * do so if it does. if it doesn't have data yet, nothing to
		 * do.
		 */
		do_recv_arm = !prep_next_send(ring, c, cd, cqe_to_fd(cqe));

		ocd = &c->cd[!cd->index];
		if (!cd->snd_notif && do_recv_arm && !ocd->pending_recv) {
			int fd = other_dir_fd(c, cqe_to_fd(cqe));

			__submit_receive(ring, c, ocd, fd);
		}
out_close:
		if (pending_shutdown(c))
			close_cd(c, cd);
	}

	vlog("%d: pending sends %d\n", c->tid, cd->pending_send);
	return 0;
}

static int handle_send(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct conn_dir *cd = cqe_to_conn_dir(c, cqe);

	return __handle_send(ring, c, cd, cqe);
}

static int send_error(struct error_handler *err, struct io_uring *ring,
		      struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct conn_dir *cd = cqe_to_conn_dir(c, cqe);

	cd->pending_send = 0;

	/* res can have high bit set */
	if (cqe->flags & IORING_CQE_F_NOTIF)
		return handle_send(ring, cqe);
	if (cqe->res != -ENOBUFS)
		return default_error(err, ring, cqe);

	cd->snd_enobufs++;
	return 0;
}

/*
 * We don't expect to get here, as we marked it with skipping posting a
 * CQE if it was successful. If it does trigger, than means it fails and
 * that our close has not been done. Log the shutdown error and issue a new
 * separate close.
 */
static int handle_shutdown(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	struct io_uring_sqe *sqe;
	int fd = cqe_to_fd(cqe);

	fprintf(stderr, "Got shutdown notification on fd %d\n", fd);

	if (!cqe->res)
		fprintf(stderr, "Unexpected success shutdown CQE\n");
	else if (cqe->res < 0)
		fprintf(stderr, "Shutdown got %s\n", strerror(-cqe->res));

	sqe = get_sqe(ring);
	if (fixed_files)
		io_uring_prep_close_direct(sqe, fd);
	else
		io_uring_prep_close(sqe, fd);
	encode_userdata(sqe, c, __CLOSE, 0, fd);
	return 0;
}

/*
 * Final stage of a connection, the shutdown and close has finished. Mark
 * it as disconnected and let the main loop reap it.
 */
static int handle_close(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	int fd = cqe_to_fd(cqe);

	printf("Closed client: id=%d, in_fd=%d, out_fd=%d\n", c->tid, c->in_fd, c->out_fd);
	if (fd == c->in_fd)
		c->in_fd = -1;
	else if (fd == c->out_fd)
		c->out_fd = -1;

	if (c->in_fd == -1 && c->out_fd == -1) {
		c->flags |= CONN_F_DISCONNECTED;

		pthread_mutex_lock(&thread_lock);
		__show_stats(c);
		open_conns--;
		pthread_mutex_unlock(&thread_lock);
		free_buffer_rings(ring, c);
		free_msgs(&c->cd[0]);
		free_msgs(&c->cd[1]);
		free(c->cd[0].rcv_bucket);
		free(c->cd[0].snd_bucket);
	}

	return 0;
}

static int handle_cancel(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	int fd = cqe_to_fd(cqe);

	c->pending_cancels--;

	vlog("%d: got cancel fd %d, refs %d\n", c->tid, fd, c->pending_cancels);

	if (!c->pending_cancels) {
		queue_shutdown_close(ring, c, c->in_fd);
		if (c->out_fd != -1)
			queue_shutdown_close(ring, c, c->out_fd);
		io_uring_submit(ring);
	}

	return 0;
}

static void open_socket(struct conn *c)
{
	if (is_sink) {
		pthread_mutex_lock(&thread_lock);
		open_conns++;
		pthread_mutex_unlock(&thread_lock);

		submit_receive(&c->ring, c);
	} else {
		struct io_uring_sqe *sqe;
		int domain;

		if (ipv6)
			domain = AF_INET6;
		else
			domain = AF_INET;

		/*
		 * If fixed_files is set, proxy will use fixed files for any new
		 * file descriptors it instantiates. Fixd files, or fixed
		 * descriptors, are io_uring private file descriptors. They
		 * cannot be accessed outside of io_uring. io_uring holds a
		 * fixed reference to them, which means that we do not need to
		 * grab per-request references to them. Particularly for
		 * threaded applications, grabbing and dropping file references
		 * for each operation can be costly as the file table is shared.
		 * This generally shows up as fget/fput related overhead in any
		 * workload profiles.
		 *
		 * Fixed descriptors are passed in via the 'fd' field just like
		 * regular descriptors, and then marked as such by setting the
		 * IOSQE_FIXED_FILE flag in the sqe->flags field. Some helpers
		 * do that automatically, like the below, others will need it
		 * set manually if they don't have a *direct*() helper.
		 *
		 * For operations that instantiate them, like the opening of a
		 * direct socket, the application may either ask the kernel to
		 * find a free one (as is done below), or the application may
		 * manage the space itself and pass in an index for a currently
		 * free slot in the table. If the kernel is asked to allocate a
		 * free direct descriptor, note that io_uring does not abide by
		 * the POSIX mandated "lowest free must be returned". It may
		 * return any free descriptor of its choosing.
		 */
		sqe = get_sqe(&c->ring);
		if (fixed_files)
			io_uring_prep_socket_direct_alloc(sqe, domain, SOCK_STREAM, 0, 0);
		else
			io_uring_prep_socket(sqe, domain, SOCK_STREAM, 0, 0);
		encode_userdata(sqe, c, __SOCK, 0, 0);
	}
}

/*
 * Start of connection, we got our in descriptor.
 */
static int handle_fd_pass(struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);
	int fd = cqe_to_fd(cqe);

	vlog("%d: got fd pass %d\n", c->tid, fd);
	c->in_fd = fd;
	open_socket(c);
	return 0;
}

static int handle_stop(struct io_uring_cqe *cqe)
{
	struct conn *c = cqe_to_conn(cqe);

	printf("Client %d: queueing shutdown\n", c->tid);
	queue_cancel(&c->ring, c);
	return 0;
}

/*
 * Called for each CQE that we receive. Decode the request type that it
 * came from, and call the appropriate handler.
 */
static int handle_cqe(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	int ret;

	/*
	 * Unlikely, but there's an error in this CQE. If an error handler
	 * is defined, call it, and that will deal with it. If no error
	 * handler is defined, the opcode handler either doesn't care or will
	 * handle it on its own.
	 */
	if (cqe->res < 0) {
		struct error_handler *err = &error_handlers[cqe_to_op(cqe)];

		if (err->error_fn)
			return err->error_fn(err, ring, cqe);
	}

	switch (cqe_to_op(cqe)) {
	case __ACCEPT:
		ret = handle_accept(ring, cqe);
		break;
	case __SOCK:
		ret = handle_sock(ring, cqe);
		break;
	case __CONNECT:
		ret = handle_connect(ring, cqe);
		break;
	case __RECV:
	case __RECVMSG:
		ret = handle_recv(ring, cqe);
		break;
	case __SEND:
	case __SENDMSG:
		ret = handle_send(ring, cqe);
		break;
	case __CANCEL:
		ret = handle_cancel(ring, cqe);
		break;
	case __SHUTDOWN:
		ret = handle_shutdown(ring, cqe);
		break;
	case __CLOSE:
		ret = handle_close(ring, cqe);
		break;
	case __FD_PASS:
		ret = handle_fd_pass(cqe);
		break;
	case __STOP:
		ret = handle_stop(cqe);
		break;
	case __NOP:
		ret = 0;
		break;
	default:
		fprintf(stderr, "bad user data %lx\n", (long) cqe->user_data);
		return 1;
	}

	return ret;
}

static void house_keeping(struct io_uring *ring)
{
	static unsigned long last_bytes;
	unsigned long bytes, elapsed;
	struct conn *c;
	int i, j;

	vlog("House keeping entered\n");

	bytes = 0;
	for (i = 0; i < nr_conns; i++) {
		c = &conns[i];

		for (j = 0; j < 2; j++) {
			struct conn_dir *cd = &c->cd[j];

			bytes += cd->in_bytes + cd->out_bytes;
		}
		if (c->flags & CONN_F_DISCONNECTED) {
			vlog("%d: disconnected\n", i);

			if (!(c->flags & CONN_F_REAPED)) {
				void *ret;

				pthread_join(c->thread, &ret);
				c->flags |= CONN_F_REAPED;
			}
			continue;
		}
		if (c->flags & CONN_F_DISCONNECTING)
			continue;

		if (should_shutdown(c)) {
			__close_conn(ring, c);
			c->flags |= CONN_F_DISCONNECTING;
		}
	}

	elapsed = mtime_since_now(&last_housekeeping);
	if (bytes && elapsed >= 900) {
		unsigned long bw;

		bw = (8 * (bytes - last_bytes) / 1000UL) / elapsed;
		if (bw) {
			if (open_conns)
				printf("Bandwidth (threads=%d): %'luMbit\n", open_conns, bw);
			gettimeofday(&last_housekeeping, NULL);
			last_bytes = bytes;
		}
	}
}

/*
 * Event loop shared between the parent, and the connections. Could be
 * split in two, as they don't handle the same types of events. For the per
 * connection loop, 'c' is valid. For the main loop, it's NULL.
 */
static int __event_loop(struct io_uring *ring, struct conn *c)
{
	struct __kernel_timespec active_ts, idle_ts;
	int flags;

	idle_ts.tv_sec = 0;
	idle_ts.tv_nsec = 100000000LL;
	active_ts = idle_ts;
	if (wait_usec > 1000000) {
		active_ts.tv_sec = wait_usec / 1000000;
		wait_usec -= active_ts.tv_sec * 1000000;
	}
	active_ts.tv_nsec = wait_usec * 1000;

	gettimeofday(&last_housekeeping, NULL);

	flags = 0;
	while (1) {
		struct __kernel_timespec *ts = &idle_ts;
		struct io_uring_cqe *cqe;
		unsigned int head;
		int ret, i, to_wait;

		/*
		 * If wait_batch is set higher than 1, then we'll wait on
		 * that amount of CQEs to be posted each loop. If used with
		 * DEFER_TASKRUN, this can provide a substantial reduction
		 * in context switch rate as the task isn't woken until the
		 * requested number of events can be returned.
		 *
		 * Can be used with -t to set a wait_usec timeout as well.
		 * For example, if an application can deal with 250 usec
		 * of wait latencies, it can set -w8 -t250 which will cause
		 * io_uring to return when either 8 events have been received,
		 * or if 250 usec of waiting has passed.
		 *
		 * If we don't have any open connections, wait on just 1
		 * always.
		 */
		to_wait = 1;
		if (open_conns && !flags) {
			ts = &active_ts;
			to_wait = wait_batch;
		}

		vlog("Submit and wait for %d\n", to_wait);
		ret = io_uring_submit_and_wait_timeout(ring, &cqe, to_wait, ts, NULL);

		if (*ring->cq.koverflow)
			printf("overflow %u\n", *ring->cq.koverflow);
		if (*ring->sq.kflags &  IORING_SQ_CQ_OVERFLOW)
			printf("saw overflow\n");

		vlog("Submit and wait: %d\n", ret);

		i = flags = 0;
		io_uring_for_each_cqe(ring, head, cqe) {
			if (handle_cqe(ring, cqe))
				return 1;
			flags |= cqe_to_conn(cqe)->flags;
			++i;
		}

		vlog("Handled %d events\n", i);

		/*
		 * Advance the CQ ring for seen events when we've processed
		 * all of them in this loop. This can also be done with
		 * io_uring_cqe_seen() in each handler above, which just marks
		 * that single CQE as seen. However, it's more efficient to
		 * mark a batch as seen when we're done with that batch.
		 */
		if (i) {
			io_uring_cq_advance(ring, i);
			events += i;
		}

		event_loops++;
		if (c) {
			if (c->flags & CONN_F_DISCONNECTED)
				break;
		} else {
			house_keeping(ring);
		}
	}

	return 0;
}

/*
 * Main event loop, Submit our multishot accept request, and then just loop
 * around handling incoming connections.
 */
static int parent_loop(struct io_uring *ring, int fd)
{
	struct io_uring_sqe *sqe;

	/*
	 * proxy provides a way to use either multishot receive or not, but
	 * for accept, we always use multishot. A multishot accept request
	 * needs only be armed once, and then it'll trigger a completion and
	 * post a CQE whenever a new connection is accepted. No need to do
	 * anything else, unless the multishot accept terminates. This happens
	 * if it encounters an error. Applications should check for
	 * IORING_CQE_F_MORE in cqe->flags - this tells you if more completions
	 * are expected from this request or not. Non-multishot never have
	 * this set, where multishot will always have this set unless an error
	 * occurs.
	 */
	sqe = get_sqe(ring);
	if (fixed_files)
		io_uring_prep_multishot_accept_direct(sqe, fd, NULL, NULL, 0);
	else
		io_uring_prep_multishot_accept(sqe, fd, NULL, NULL, 0);
	__encode_userdata(sqe, 0, __ACCEPT, 0, fd);

	return __event_loop(ring, NULL);
}

static int init_ring(struct io_uring *ring, int nr_files)
{
	struct io_uring_params params;
	int ret;

	/*
	 * By default, set us up with a big CQ ring. Not strictly needed
	 * here, but it's very important to never overflow the CQ ring.
	 * Events will not be dropped if this happens, but it does slow
	 * the application down in dealing with overflown events.
	 *
	 * Set SINGLE_ISSUER, which tells the kernel that only one thread
	 * is doing IO submissions. This enables certain optimizations in
	 * the kernel.
	 */
	memset(&params, 0, sizeof(params));
	params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
	params.flags |= IORING_SETUP_CQSIZE;
	params.cq_entries = 1024;

	/*
	 * If use_huge is set, setup the ring with IORING_SETUP_NO_MMAP. This
	 * means that the application allocates the memory for the ring, and
	 * the kernel maps it. The alternative is having the kernel allocate
	 * the memory, and then liburing will mmap it. But we can't really
	 * support huge pages that way. If this fails, then ensure that the
	 * system has huge pages set aside upfront.
	 */
	if (use_huge)
		params.flags |= IORING_SETUP_NO_MMAP;

	/*
	 * DEFER_TASKRUN decouples async event reaping and retrying from
	 * regular system calls. If this isn't set, then io_uring uses
	 * normal task_work for this. task_work is always being run on any
	 * exit to userspace. Real applications do more than just call IO
	 * related system calls, and hence we can be running this work way
	 * too often. Using DEFER_TASKRUN defers any task_work running to
	 * when the application enters the kernel anyway to wait on new
	 * events. It's generally the preferred and recommended way to setup
	 * a ring.
	 */
	if (defer_tw) {
		params.flags |= IORING_SETUP_DEFER_TASKRUN;
		sqpoll = 0;
	}

	/*
	 * SQPOLL offloads any request submission and retry operations to a
	 * dedicated thread. This enables an application to do IO without
	 * ever having to enter the kernel itself. The SQPOLL thread will
	 * stay busy as long as there's work to do, and go to sleep if
	 * sq_thread_idle msecs have passed. If it's running, submitting new
	 * IO just needs to make them visible to the SQPOLL thread, it needs
	 * not enter the kernel. For submission, the application will only
	 * enter the kernel if the SQPOLL has been idle long enough that it
	 * has gone to sleep.
	 *
	 * Waiting on events still need to enter the kernel, if none are
	 * available. The application may also use io_uring_peek_cqe() to
	 * check for new events without entering the kernel, as completions
	 * will be continually produced to the CQ ring by the SQPOLL thread
	 * as they occur.
	 */
	if (sqpoll) {
		params.flags |= IORING_SETUP_SQPOLL;
		params.sq_thread_idle = 1000;
		defer_tw = 0;
	}

	/*
	 * If neither DEFER_TASKRUN or SQPOLL is used, set COOP_TASKRUN. This
	 * avoids heavy signal based notifications, which can force an
	 * application to enter the kernel and process it as soon as they
	 * occur.
	 */
	if (!sqpoll && !defer_tw)
		params.flags |= IORING_SETUP_COOP_TASKRUN;

	/*
	 * The SQ ring size need not be larger than any batch of requests
	 * that need to be prepared before submit. Normally in a loop we'd
	 * only need a few, if any, particularly if multishot is used.
	 */
	ret = io_uring_queue_init_params(ring_size, ring, &params);
	if (ret) {
		fprintf(stderr, "%s\n", strerror(-ret));
		return 1;
	}

	/*
	 * If send serialization is available and no option was given to use
	 * it or not, default it to on. If it was turned on and the kernel
	 * doesn't support it, turn it off.
	 */
	if (params.features & IORING_FEAT_SEND_BUF_SELECT) {
		if (send_ring == -1)
			send_ring = 1;
	} else {
		if (send_ring == 1) {
			fprintf(stderr, "Kernel doesn't support ring provided "
				"buffers for sends, disabled\n");
		}
		send_ring = 0;
	}

	if (!send_ring && snd_bundle) {
		fprintf(stderr, "Can't use send bundle without send_ring\n");
		snd_bundle = 0;
	}

	if (fixed_files) {
		/*
		 * If fixed files are used, we need to allocate a fixed file
		 * table upfront where new direct descriptors can be managed.
		 */
		ret = io_uring_register_files_sparse(ring, nr_files);
		if (ret) {
			fprintf(stderr, "file register: %d\n", ret);
			return 1;
		}

		/*
		 * If fixed files are used, we also register the ring fd. See
		 * comment near io_uring_prep_socket_direct_alloc() further
		 * down. This avoids the fget/fput overhead associated with
		 * the io_uring_enter(2) system call itself, which is used to
		 * submit and wait on events.
		 */
		ret = io_uring_register_ring_fd(ring);
		if (ret != 1) {
			fprintf(stderr, "ring register: %d\n", ret);
			return 1;
		}
	}

	if (napi) {
		struct io_uring_napi n = {
			.prefer_busy_poll = napi > 1 ? 1 : 0,
			.busy_poll_to = napi_timeout,
		};

		ret = io_uring_register_napi(ring, &n);
		if (ret) {
			fprintf(stderr, "io_uring_register_napi: %d\n", ret);
			if (ret != -EINVAL)
				return 1;
			fprintf(stderr, "NAPI not available, turned off\n");
		}
	}

	return 0;
}

static void *thread_main(void *data)
{
	struct conn *c = data;
	int ret;

	c->flags |= CONN_F_STARTED;

	/* we need a max of 4 descriptors for each client */
	ret = init_ring(&c->ring, 4);
	if (ret)
		goto done;

	if (setup_buffer_rings(&c->ring, c))
		goto done;

	/*
	 * If we're using fixed files, then we need to wait for the parent
	 * to install the c->in_fd into our direct descriptor table. When
	 * that happens, we'll set things up. If we're not using fixed files,
	 * we can set up the receive or connect now.
	 */
	if (!fixed_files)
		open_socket(c);

	/* we're ready */
	pthread_barrier_wait(&c->startup_barrier);

	__event_loop(&c->ring, c);
done:
	return NULL;
}

static void usage(const char *name)
{
	printf("%s:\n", name);
	printf("\t-m:\t\tUse multishot receive (%d)\n", recv_mshot);
	printf("\t-d:\t\tUse DEFER_TASKRUN (%d)\n", defer_tw);
	printf("\t-S:\t\tUse SQPOLL (%d)\n", sqpoll);
	printf("\t-f:\t\tUse only fixed files (%d)\n", fixed_files);
	printf("\t-a:\t\tUse huge pages for the ring (%d)\n", use_huge);
	printf("\t-t:\t\tTimeout for waiting on CQEs (usec) (%d)\n", wait_usec);
	printf("\t-w:\t\tNumber of CQEs to wait for each loop (%d)\n", wait_batch);
	printf("\t-B:\t\tUse bi-directional mode (%d)\n", bidi);
	printf("\t-s:\t\tAct only as a sink (%d)\n", is_sink);
	printf("\t-q:\t\tRing size to use (%d)\n", ring_size);
	printf("\t-H:\t\tHost to connect to (%s)\n", host);
	printf("\t-r:\t\tPort to receive on (%d)\n", receive_port);
	printf("\t-p:\t\tPort to connect to (%d)\n", send_port);
	printf("\t-6:\t\tUse IPv6 (%d)\n", ipv6);
	printf("\t-N:\t\tUse NAPI polling (%d)\n", napi);
	printf("\t-T:\t\tNAPI timeout (usec) (%d)\n", napi_timeout);
	printf("\t-b:\t\tSend/receive buf size (%d)\n", buf_size);
	printf("\t-n:\t\tNumber of provided buffers (pow2) (%d)\n", nr_bufs);
	printf("\t-u:\t\tUse provided buffers for send (%d)\n", send_ring);
	printf("\t-C:\t\tUse bundles for send (%d)\n", snd_bundle);
	printf("\t-z:\t\tUse zerocopy send (%d)\n", snd_zc);
	printf("\t-c:\t\tUse bundles for recv (%d)\n", snd_bundle);
	printf("\t-M:\t\tUse sendmsg (%d)\n", snd_msg);
	printf("\t-M:\t\tUse recvmsg (%d)\n", rcv_msg);
	printf("\t-x:\t\tShow extended stats (%d)\n", ext_stat);
	printf("\t-V:\t\tIncrease verbosity (%d)\n", verbose);
}

/*
 * Options parsing the ring / net setup
 */
int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct sigaction sa = { };
	const char *optstring;
	int opt, ret, fd;

	setlocale(LC_NUMERIC, "en_US");

	page_size = sysconf(_SC_PAGESIZE);
	if (page_size < 0) {
		perror("sysconf(_SC_PAGESIZE)");
		return 1;
	}

	pthread_mutex_init(&thread_lock, NULL);

	optstring = "m:d:S:s:b:f:H:r:p:n:B:N:T:w:t:M:R:u:c:C:q:a:x:z:i:6Vh?";
	while ((opt = getopt(argc, argv, optstring)) != -1) {
		switch (opt) {
		case 'm':
			recv_mshot = !!atoi(optarg);
			break;
		case 'S':
			sqpoll = !!atoi(optarg);
			break;
		case 'd':
			defer_tw = !!atoi(optarg);
			break;
		case 'b':
			buf_size = atoi(optarg);
			break;
		case 'n':
			nr_bufs = atoi(optarg);
			break;
		case 'u':
			send_ring = !!atoi(optarg);
			break;
		case 'c':
			rcv_bundle = !!atoi(optarg);
			break;
		case 'C':
			snd_bundle = !!atoi(optarg);
			break;
		case 'w':
			wait_batch = atoi(optarg);
			break;
		case 't':
			wait_usec = atoi(optarg);
			break;
		case 's':
			is_sink = !!atoi(optarg);
			break;
		case 'f':
			fixed_files = !!atoi(optarg);
			break;
		case 'H':
			host = strdup(optarg);
			break;
		case 'r':
			receive_port = atoi(optarg);
			break;
		case 'p':
			send_port = atoi(optarg);
			break;
		case 'B':
			bidi = !!atoi(optarg);
			break;
		case 'N':
			napi = !!atoi(optarg);
			break;
		case 'T':
			napi_timeout = atoi(optarg);
			break;
		case '6':
			ipv6 = true;
			break;
		case 'M':
			snd_msg = !!atoi(optarg);
			break;
		case 'z':
			snd_zc = !!atoi(optarg);
			break;
		case 'R':
			rcv_msg = !!atoi(optarg);
			break;
		case 'q':
			ring_size = atoi(optarg);
			break;
		case 'i':
			buf_ring_inc = !!atoi(optarg);
			break;
		case 'a':
			use_huge = !!atoi(optarg);
			break;
		case 'x':
			ext_stat = !!atoi(optarg);
			break;
		case 'V':
			verbose++;
			break;
		case 'h':
		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (bidi && is_sink) {
		fprintf(stderr, "Can't be both bidi proxy and sink\n");
		return 1;
	}
	if (snd_msg && sqpoll) {
		fprintf(stderr, "SQPOLL with msg variants disabled\n");
		snd_msg = 0;
	}
	if (rcv_msg && rcv_bundle) {
		fprintf(stderr, "Can't use bundles with recvmsg\n");
		rcv_msg = 0;
	}
	if (snd_msg && snd_bundle) {
		fprintf(stderr, "Can't use bundles with sendmsg\n");
		snd_msg = 0;
	}
	if (snd_msg && send_ring) {
		fprintf(stderr, "Can't use send ring sendmsg\n");
		snd_msg = 0;
	}
	if (snd_zc && (send_ring || snd_bundle)) {
		fprintf(stderr, "Can't use send zc with bundles or ring\n");
		send_ring = snd_bundle = 0;
	}
	/*
	 * For recvmsg w/multishot, we waste some data at the head of the
	 * packet every time. Adjust the buffer size to account for that,
	 * so we're still handing 'buf_size' actual payload of data.
	 */
	if (rcv_msg && recv_mshot) {
		fprintf(stderr, "Adjusted buf size for recvmsg w/multishot\n");
		buf_size += sizeof(struct io_uring_recvmsg_out);
	}

	br_mask = nr_bufs - 1;

	fd = setup_listening_socket(receive_port, ipv6);
	if (is_sink)
		send_port = -1;

	if (fd == -1)
		return 1;

	atexit(show_stats);
	sa.sa_handler = sig_int;
	sa.sa_flags = SA_RESTART;
	sigaction(SIGINT, &sa, NULL);

	ret = init_ring(&ring, MAX_CONNS * 3);
	if (ret)
		return ret;

	printf("Backend: sqpoll=%d, defer_tw=%d, fixed_files=%d, "
		"is_sink=%d, buf_size=%d, nr_bufs=%d, host=%s, send_port=%d, "
		"receive_port=%d, napi=%d, napi_timeout=%d, huge_page=%d\n",
			sqpoll, defer_tw, fixed_files, is_sink,
			buf_size, nr_bufs, host, send_port, receive_port,
			napi, napi_timeout, use_huge);
	printf(" recv options: recvmsg=%d, recv_mshot=%d, recv_bundle=%d\n",
			rcv_msg, recv_mshot, rcv_bundle);
	printf(" send options: sendmsg=%d, send_ring=%d, send_bundle=%d, "
		"send_zerocopy=%d\n", snd_msg, send_ring, snd_bundle,
			snd_zc);

	return parent_loop(&ring, fd);
}
