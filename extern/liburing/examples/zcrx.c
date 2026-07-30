// SPDX-License-Identifier: GPL-2.0
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <time.h>

#include <arpa/inet.h>
#include <linux/errqueue.h>
#include <linux/if_packet.h>
#include <linux/ipv6.h>
#include <linux/socket.h>
#include <linux/sockios.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <linux/mman.h>

#include <linux/memfd.h>
#include <linux/dma-buf.h>
#include <linux/udmabuf.h>

#include "liburing.h"
#include "helpers.h"

enum {
	AFFINITY_MODE_NONE,
	AFFINITY_MODE_SAME,
	AFFINITY_MODE_DIFFERENT,

	__AFFINITY_MODE_MAX,
};

enum {
	RQ_ALLOC_USER,
	RQ_ALLOC_KERNEL,

	__RQ_ALLOC_MAX,
};

#define REQ_TYPE_SHIFT	3
#define REQ_TYPE_MASK	((1UL << REQ_TYPE_SHIFT) - 1)

enum {
	AREA_TYPE_NORMAL,
	AREA_TYPE_HUGE_PAGES,
	AREA_TYPE_DMABUF,
	__AREA_TYPE_MAX,
};

enum {
	REQ_TYPE_ACCEPT		= 1,
	REQ_TYPE_RX		= 2,
};

struct zc_conn {
	int sockfd;
	unsigned long received;
	unsigned stat_nr_reqs;
	unsigned stat_nr_cqes;
};

static unsigned cfg_rq_entries = 8192;
static unsigned cfg_cq_entries = 8192;
static long cfg_area_size = 256 * 1024 * 1024;
static int cfg_port = 8000;
static const char *cfg_ifname;
static int cfg_queue_id = -1;
static bool cfg_verify_data = false;
static size_t cfg_size = 0;
static unsigned cfg_affinity_mode = AFFINITY_MODE_NONE;
static unsigned cfg_rq_alloc_mode = RQ_ALLOC_USER;
static unsigned cfg_area_type = AREA_TYPE_NORMAL;
static struct sockaddr_in6 cfg_addr;

static long page_size;

static void *area_ptr;
static void *ring_ptr;
static size_t ring_size;
static struct io_uring_zcrx_rq rq_ring;
static unsigned long area_token;
static bool stop;
static __u32 zcrx_id;

static int dmabuf_fd;
static int memfd;

static int listen_fd;
static int target_cpu = -1;

static int get_sock_cpu(int sockfd)
{
	int cpu;
	socklen_t len = sizeof(cpu);

	if (getsockopt(sockfd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len))
		t_error(1, errno, "getsockopt failed\n");
	return cpu;
}

static void set_affinity(int sockfd)
{
	int new_cpu = -1;
	int sock_cpu;
	cpu_set_t mask;

	if (cfg_affinity_mode == AFFINITY_MODE_NONE)
		return;

	sock_cpu = get_sock_cpu(sockfd);
	if (sock_cpu == -1)
		t_error(1, 0, "Can't socket's CPU");

	if (cfg_affinity_mode == AFFINITY_MODE_SAME) {
		new_cpu = sock_cpu;
	} else if (cfg_affinity_mode == AFFINITY_MODE_DIFFERENT) {
		if (target_cpu != -1 && target_cpu != sock_cpu)
			new_cpu = target_cpu;
		else
			new_cpu = sock_cpu ^ 1;
	}

	if (target_cpu != -1 && new_cpu != target_cpu) {
		printf("Couldn't set affinity for multi socket setup\n");
		return;
	}

	CPU_ZERO(&mask);
	CPU_SET(new_cpu, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask))
		t_error(1, errno, "sched_setaffinity() failed\n");
	target_cpu = new_cpu;
}

static struct zc_conn *get_connection(__u64 user_data)
{
	user_data &= ~REQ_TYPE_MASK;
	return (struct zc_conn *)(unsigned long)user_data;
}

static inline size_t get_refill_ring_size(unsigned int rq_entries)
{
	ring_size = rq_entries * sizeof(struct io_uring_zcrx_rqe);
	/* add space for the header (head/tail/etc.) */
	ring_size += page_size;
	return T_ALIGN_UP(ring_size, page_size);
}

static void zcrx_populate_area_udmabuf(struct io_uring_zcrx_area_reg *area_reg)
{
	struct udmabuf_create create;
	int ret, devfd;

	devfd = open("/dev/udmabuf", O_RDWR);
	if (devfd < 0)
		t_error(1, errno, "Failed to open udmabuf dev");

	memfd = memfd_create("udmabuf-test", MFD_ALLOW_SEALING);
	if (memfd < 0)
		t_error(1, errno, "Failed to open udmabuf dev");

	ret = fcntl(memfd, F_ADD_SEALS, F_SEAL_SHRINK);
	if (ret < 0)
		t_error(1, errno, "Failed to set seals");

	ret = ftruncate(memfd, cfg_area_size);
	if (ret == -1)
		t_error(1, errno, "Failed to resize udmabuf");

	memset(&create, 0, sizeof(create));
	create.memfd = memfd;
	create.offset = 0;
	create.size = cfg_area_size;
	dmabuf_fd = ioctl(devfd, UDMABUF_CREATE, &create);
	if (dmabuf_fd < 0)
		t_error(1, errno, "Failed to create udmabuf");

	area_ptr = mmap(NULL, cfg_area_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			dmabuf_fd, 0);
	if (area_ptr == MAP_FAILED)
		t_error(1, errno, "Failed to mmap udmabuf");

	memset(area_reg, 0, sizeof(*area_reg));
	area_reg->addr = 0; /* offset into dmabuf */
	area_reg->len = cfg_area_size;
	area_reg->flags |= IORING_ZCRX_AREA_DMABUF;
	area_reg->dmabuf_fd = dmabuf_fd;

	close(devfd);
}

static void zcrx_populate_area(struct io_uring_zcrx_area_reg *area_reg)
{
	unsigned flags = MAP_PRIVATE | MAP_ANONYMOUS;
	unsigned prot = PROT_READ | PROT_WRITE;

	if (cfg_area_type == AREA_TYPE_DMABUF) {
		zcrx_populate_area_udmabuf(area_reg);
		return;
	}
	if (cfg_area_type == AREA_TYPE_NORMAL) {
		area_ptr = mmap(NULL, cfg_area_size, prot,
				flags, 0, 0);
	} else if (cfg_area_type == AREA_TYPE_HUGE_PAGES) {
		area_ptr = mmap(NULL, cfg_area_size, prot,
				flags | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
	}

	if (area_ptr == MAP_FAILED)
		t_error(1, 0, "mmap(): area allocation failed");

	memset(area_reg, 0, sizeof(*area_reg));
	area_reg->addr = uring_ptr_to_u64(area_ptr);
	area_reg->len = cfg_area_size;
	area_reg->flags = 0;
}

static void setup_zcrx(struct io_uring *ring)
{
	struct io_uring_zcrx_area_reg area_reg;
	unsigned int ifindex;
	unsigned int rq_entries = cfg_rq_entries;
	unsigned rq_flags = 0;
	int ret;

	ifindex = if_nametoindex(cfg_ifname);
	if (!ifindex)
		t_error(1, 0, "bad interface name: %s", cfg_ifname);

	ring_size = get_refill_ring_size(rq_entries);
	ring_ptr = NULL;
	if (cfg_rq_alloc_mode == RQ_ALLOC_USER) {
		ring_ptr = mmap(NULL, ring_size,
				PROT_READ | PROT_WRITE,
				MAP_ANONYMOUS | MAP_PRIVATE,
				0, 0);
		if (ring_ptr == MAP_FAILED)
			t_error(1, 0, "mmap(): refill ring");
		rq_flags |= IORING_MEM_REGION_TYPE_USER;
	}

	struct io_uring_region_desc region_reg = {
		.size = ring_size,
		.user_addr = uring_ptr_to_u64(ring_ptr),
		.flags = rq_flags,
	};

	zcrx_populate_area(&area_reg);

	struct io_uring_zcrx_ifq_reg reg = {
		.if_idx = ifindex,
		.if_rxq = cfg_queue_id,
		.rq_entries = rq_entries,
		.area_ptr = uring_ptr_to_u64(&area_reg),
		.region_ptr = uring_ptr_to_u64(&region_reg),
	};

	ret = io_uring_register_ifq(ring, &reg);
	if (ret)
		t_error(1, 0, "io_uring_register_ifq(): %d", ret);

	if (cfg_rq_alloc_mode == RQ_ALLOC_KERNEL) {
		ring_ptr = mmap(NULL, ring_size,
				PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE,
				ring->ring_fd, region_reg.mmap_offset);
		if (ring_ptr == MAP_FAILED)
			t_error(1, 0, "mmap(): refill ring");
	}

	rq_ring.khead = (unsigned int *)((char *)ring_ptr + reg.offsets.head);
	rq_ring.ktail = (unsigned int *)((char *)ring_ptr + reg.offsets.tail);
	rq_ring.rqes = (struct io_uring_zcrx_rqe *)((char *)ring_ptr + reg.offsets.rqes);
	rq_ring.rq_tail = 0;
	rq_ring.ring_entries = reg.rq_entries;

	zcrx_id = reg.zcrx_id;
	area_token = area_reg.rq_area_token;
}

static void add_accept(struct io_uring *ring, int sockfd)
{
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	io_uring_prep_accept(sqe, sockfd, NULL, NULL, 0);
	sqe->user_data = REQ_TYPE_ACCEPT;
}

static void add_recvzc(struct io_uring *ring, struct zc_conn *conn, size_t len)
{
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	__u64 token;

	token = (__u64)(unsigned long)conn;
	token |= REQ_TYPE_RX;

	conn->stat_nr_reqs++;
	io_uring_prep_rw(IORING_OP_RECV_ZC, sqe, conn->sockfd, NULL, len, 0);
	sqe->ioprio |= IORING_RECV_MULTISHOT;
	sqe->zcrx_ifq_idx = zcrx_id;
	sqe->user_data = token;
}

static void print_socket_info(int sockfd)
{
	struct sockaddr_in6 peer_addr;
	socklen_t addr_len = sizeof(peer_addr);
	char ip_str[INET6_ADDRSTRLEN];
	int port;

	if (getpeername(sockfd, (struct sockaddr *)&peer_addr, &addr_len) < 0) {
		t_error(1, errno, "getpeername failed");
		return;
	}
	if (!inet_ntop(AF_INET6, &peer_addr.sin6_addr, ip_str, sizeof(ip_str))) {
		t_error(1, errno, "inet_ntop failed");
		return;
	}
	port = ntohs(peer_addr.sin6_port);

	printf("socket accepted: fd %i, Peer IP %s, Peer port %d\n",
		sockfd, ip_str, port);
}

static void process_accept(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	struct zc_conn *conn;

	if (cqe->res < 0) {
		printf("Accept failed %i, terminate\n", cqe->res);
		stop = false;
		return;
	}

	conn = aligned_alloc(64, sizeof(*conn));
	if (!conn)
		t_error(1, 0, "can't allocate conn structure");
	if (conn->sockfd)
		t_error(1, 0, "Unexpected second connection");

	memset(conn, 0, sizeof(*conn));
	conn->sockfd = cqe->res;
	print_socket_info(conn->sockfd);
	set_affinity(conn->sockfd);
	add_recvzc(ring, conn, cfg_size);

	add_accept(ring, listen_fd);
}

static void verify_data(__u8 *data, size_t size, unsigned long seq)
{
	size_t i;

	if (!cfg_verify_data)
		return;

	for (i = 0; i < size; i++) {
		__u8 expected = (__u8)'a' + (seq + i) % 26;
		__u8 v = data[i];

		if (v != expected)
			t_error(1, 0, "payload mismatch at %u: expected %u vs got %u, diff %i, base seq %lu, seq %lu",
				(unsigned)i, expected, v, (int)expected - v,
				seq, seq + i);
	}
}

static unsigned rq_nr_queued(struct io_uring_zcrx_rq *rq)
{
	return rq->rq_tail - io_uring_smp_load_acquire(rq->khead);
}

static inline void fill_rqe(const struct io_uring_cqe *cqe,
			    struct io_uring_zcrx_rqe *rqe)
{
	const struct io_uring_zcrx_cqe *rcqe = (void *)(cqe + 1);

	rqe->off = (rcqe->off & ~IORING_ZCRX_AREA_MASK) | area_token;
	rqe->len = cqe->res;
}

static void return_buffer(struct io_uring_zcrx_rq *rq_ring,
			  const struct io_uring_cqe *cqe)
{
	struct io_uring_zcrx_rqe *rqe;
	unsigned rq_mask;

	if (rq_nr_queued(rq_ring) == rq_ring->ring_entries) {
		printf("refill queue is full, drop the buffer\n");
		return;
	}

	rq_mask = rq_ring->ring_entries - 1;
	/* processed, return back to the kernel */
	rqe = &rq_ring->rqes[rq_ring->rq_tail & rq_mask];
	fill_rqe(cqe, rqe);
	io_uring_smp_store_release(rq_ring->ktail, ++rq_ring->rq_tail);
}

static void process_recvzc_error(struct io_uring *ring,
				 struct zc_conn *conn, int ret)
{
	if (ret == -ENOSPC) {
		size_t left = 0;

		if (cfg_size) {
			left = cfg_size - conn->received;
			if (left == 0)
				t_error(1, 0, "ENOSPC for a finished request");
		}

		add_recvzc(ring, conn, left);
		return;
	}

	if (ret != 0)
		t_error(1, 0, "invalid final recvzc ret %i", ret);
	if (cfg_size && conn->received != cfg_size)
		t_error(1, 0, "total receive size mismatch %lu / %lu",
			conn->received, cfg_size);

	printf("Connection terminated: received %lu, cqes %i, nr requeues %i\n",
		conn->received,
		conn->stat_nr_cqes,
		conn->stat_nr_reqs - 1);

	close(conn->sockfd);
	free(conn);
}

static void process_recvzc(struct io_uring *ring,
			   struct io_uring_cqe *cqe)
{
	struct zc_conn *conn = get_connection(cqe->user_data);
	const struct io_uring_zcrx_cqe *rcqe;
	uint64_t mask;
	__u8 *data;

	conn->stat_nr_cqes++;

	if (!(cqe->flags & IORING_CQE_F_MORE)) {
		process_recvzc_error(ring, conn, cqe->res);
		return;
	}
	if (cqe->res < 0)
		t_error(1, 0, "recvzc(): %d", cqe->res);

	rcqe = (struct io_uring_zcrx_cqe *)(cqe + 1);
	mask = (1ULL << IORING_ZCRX_AREA_SHIFT) - 1;
	data = (__u8 *)area_ptr + (rcqe->off & mask);

	verify_data(data, cqe->res, conn->received);
	conn->received += cqe->res;
	return_buffer(&rq_ring, cqe);
}

static void server_loop(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	unsigned int head, count = 0;
	int ret;

	ret = io_uring_submit_and_wait(ring, 1);
	if (ret < 0 && ret != -ETIME)
		t_error(1, ret, "io_uring_submit_and_wait failed\n");

	io_uring_for_each_cqe(ring, head, cqe) {
		switch (cqe->user_data & REQ_TYPE_MASK) {
		case REQ_TYPE_ACCEPT:
			process_accept(ring, cqe);
			break;
		case REQ_TYPE_RX:
			process_recvzc(ring, cqe);
			break;
		default:
			t_error(1, 0, "unknown cqe");
		}
		count++;
	}
	io_uring_cq_advance(ring, count);
}

static void run_server(void)
{
	struct io_uring_params p;
	struct io_uring ring;
	int enable, ret;

	listen_fd = socket(AF_INET6, SOCK_STREAM, 0);
	if (listen_fd == -1)
		t_error(1, 0, "socket()");

	enable = 1;
	ret = setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
	if (ret < 0)
		t_error(1, 0, "setsockopt(SO_REUSEADDR)");

	ret = bind(listen_fd, (struct sockaddr *)&cfg_addr, sizeof(cfg_addr));
	if (ret < 0)
		t_error(1, 0, "bind()");

	if (listen(listen_fd, 1024) < 0)
		t_error(1, 0, "listen()");

	memset(&p, 0, sizeof(p));
	p.flags |= IORING_SETUP_COOP_TASKRUN;
	p.flags |= IORING_SETUP_SINGLE_ISSUER;
	p.flags |= IORING_SETUP_DEFER_TASKRUN;
	p.flags |= IORING_SETUP_SUBMIT_ALL;
	p.flags |= IORING_SETUP_CQE32;
	p.flags |= IORING_SETUP_CQSIZE;
	p.cq_entries = cfg_cq_entries;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret)
		t_error(1, ret, "ring init failed");

	setup_zcrx(&ring);
	add_accept(&ring, listen_fd);

	while (!stop)
		server_loop(&ring);

	close(listen_fd);
}

static void usage(const char *filepath)
{
	t_error(1, 0, "Usage: %s (-4|-6) -p<port> -i<ifname> -q<rxq_id>", filepath);
}

static void parse_opts(int argc, char **argv)
{
	struct sockaddr_in6 *addr6 = (void *) &cfg_addr;
	int c;

	if (argc <= 1)
		usage(argv[0]);

	while ((c = getopt(argc, argv, "vp:i:q:s:r:A:S:C:R:c:")) != -1) {
		switch (c) {
		case 'p':
			cfg_port = strtoul(optarg, NULL, 0);
			break;
		case 'i':
			cfg_ifname = optarg;
			break;
		case 's':
			cfg_size = strtoul(optarg, NULL, 0);
			break;
		case 'q':
			cfg_queue_id = strtoul(optarg, NULL, 0);
			break;
		case 'v':
			cfg_verify_data = true;
			break;
		case 'r':
			cfg_rq_alloc_mode = strtoul(optarg, NULL, 0);
			if (cfg_rq_alloc_mode >= __RQ_ALLOC_MAX)
				t_error(1, 0, "invalid RQ allocation mode");
			break;
		case 'S':
			cfg_area_size = strtoul(optarg, NULL, 0);
			break;
		case 'A':
			cfg_area_type = strtoul(optarg, NULL, 0);
			if (cfg_area_type >= __AREA_TYPE_MAX)
				t_error(1, 0, "Invalid area type");
			break;
		case 'C':
			cfg_cq_entries = strtoul(optarg, NULL, 0);
			break;
		case 'R':
			cfg_rq_entries = strtoul(optarg, NULL, 0);
			break;
		case 'c':
			cfg_affinity_mode = strtoul(optarg, NULL, 0);
			if (cfg_affinity_mode >= __AFFINITY_MODE_MAX)
				t_error(1, 0, "Invalid affinity mode");
		}
	}

	if (!cfg_ifname)
		t_error(1, -EINVAL, "Interface is not specified");
	if (cfg_queue_id == -1)
		t_error(1, -EINVAL, "Queue idx is not specified");

	memset(addr6, 0, sizeof(*addr6));
	addr6->sin6_family = AF_INET6;
	addr6->sin6_port = htons(cfg_port);
	addr6->sin6_addr = in6addr_any;
}

int main(int argc, char **argv)
{
	page_size = sysconf(_SC_PAGESIZE);
	if (page_size < 0) {
		perror("sysconf(_SC_PAGESIZE)");
		return 1;
	}

	parse_opts(argc, argv);
	run_server();
	return 0;
}
