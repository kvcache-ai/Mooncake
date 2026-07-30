/* SPDX-License-Identifier: MIT */
/*
 * Description: Check that io_uring_queue_init_mem() doesn't underestimate
 *		the memory required for various size rings.
 */
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <linux/mman.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <net/if.h>

#include "liburing.h"
#include "helpers.h"

#define PRE_RED	0x5aa55aa55aa55aa5ULL
#define POST_RED 0xa55aa55aa55aa55aULL

struct ctx {
	struct io_uring		ring;
	void			*ring_mem;
	void			*mem;
	unsigned long long	*pre;
	unsigned long long	*post;
};

static int page_size;

struct q_entries {
	unsigned int sqes;
	unsigned int cqes;
	unsigned int ring_flags;
};

static int setup_ctx(struct ctx *ctx, struct q_entries *q)
{
	struct io_uring_params p = { .flags = q->ring_flags, };
	int ret, rret;

	if (posix_memalign(&ctx->mem, page_size, 2*1024*1024))
		return T_EXIT_FAIL;

	memset(ctx->mem, 0, 2*1024*1024);

	ctx->pre = ctx->mem + page_size - sizeof(unsigned long long);
	*ctx->pre = PRE_RED;

	ctx->ring_mem = ctx->mem + page_size;
	p.flags |= IORING_SETUP_CQSIZE | IORING_SETUP_NO_SQARRAY;
	p.sq_entries = q->sqes;
	p.cq_entries = q->cqes;

	rret = io_uring_memory_size_params(q->sqes, &p);
	if (rret > 2 * 1024 * 1024) {
		fprintf(stderr, "Alloc size too small\n");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init_mem(q->sqes, &ctx->ring, &p,
					ctx->ring_mem, 2*1024*1024);
	if (ret < 0) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "queue init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (ret != rret) {
		fprintf(stderr, "Used mem %d differs from required %ld\n",
				ret, (long) rret);
		return T_EXIT_FAIL;
	}

	ctx->post = ctx->ring_mem + ret;
	*ctx->post = POST_RED;
	return 0;
}

static void clean_ctx(struct ctx *ctx)
{
	io_uring_queue_exit(&ctx->ring);
	free(ctx->mem);
}

static int check_red(struct ctx *ctx, unsigned long i)
{
	int fail = 0;

	if (*ctx->pre != PRE_RED) {
		printf("pre redzone=%llx at i=%lu\n", *ctx->pre, i);
		fail = 1;
	}
	if (*ctx->post != POST_RED) {
		printf("post redzone=%llx at i=%lu\n", *ctx->post, i);
		fail = 1;
	}
	return fail;
}

static int test(struct q_entries *q)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct ctx ctx = { };
	unsigned long i, ud;
	int j, ret, batch;

	ret = setup_ctx(&ctx, q);
	if (ret == T_EXIT_SKIP) {
		clean_ctx(&ctx);
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		return ret;
	}

	batch = 64;
	if (batch > q->sqes)
		batch = q->sqes;

	i = ud = 0;
	while (i < q->cqes * 2) {
		if (check_red(&ctx, i))
			return T_EXIT_FAIL;
		for (j = 0; j < batch; j++) {
			sqe = io_uring_get_sqe(&ctx.ring);
			io_uring_prep_nop(sqe);
			sqe->user_data = j + (unsigned long) i;
		}
		io_uring_submit(&ctx.ring);
		for (j = 0; j < batch; j++) {
			ret = io_uring_wait_cqe(&ctx.ring, &cqe);
			if (ret)
				goto err;
			if (cqe->user_data != ud) {
				fprintf(stderr, "ud=%lu, wanted %lu\n", (unsigned long) cqe->user_data, ud);
				goto err;
			}
			ud++;
			io_uring_cqe_seen(&ctx.ring, cqe);
		}
		i += batch;
	}

	clean_ctx(&ctx);
	return T_EXIT_PASS;
err:
	clean_ctx(&ctx);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct q_entries q_entries[] = {
		{ 256, 16384, 0 },
		{ 32, 4096, 0 },
		{ 128, 8192, 0 },
		{ 4096, 32768, 0 },
		{ 1, 8, 0 },
		{ 2, 1024, 0 },
		{ 256, 16384, IORING_SETUP_SQE128 },
		{ 32, 4096, IORING_SETUP_SQE128},
		{ 128, 8192, IORING_SETUP_SQE128},
		{ 4096, 32768, IORING_SETUP_SQE128},
		{ 1, 8, IORING_SETUP_SQE128},
		{ 2, 1024, IORING_SETUP_SQE128},
		{ 256, 16384, IORING_SETUP_CQE32 },
		{ 32, 4096, IORING_SETUP_CQE32},
		{ 128, 8192, IORING_SETUP_CQE32},
		{ 4096, 32768, IORING_SETUP_CQE32},
		{ 1, 8, IORING_SETUP_CQE32},
		{ 2, 1024, IORING_SETUP_CQE32},
		{ 256, 16384, IORING_SETUP_SQE128 | IORING_SETUP_CQE32 },
		{ 32, 4096, IORING_SETUP_SQE128 | IORING_SETUP_CQE32},
		{ 128, 8192, IORING_SETUP_SQE128 | IORING_SETUP_CQE32},
		{ 4096, 32768, IORING_SETUP_SQE128 | IORING_SETUP_CQE32},
		{ 1, 8, IORING_SETUP_SQE128 | IORING_SETUP_CQE32},
		{ 2, 1024, IORING_SETUP_SQE128 | IORING_SETUP_CQE32},
	};
	int i, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	page_size = sysconf(_SC_PAGESIZE);
	if (page_size < 0) {
		perror("sysconf(_SC_PAGESIZE)");
		return 1;
	}

	for (i = 0; i < ARRAY_SIZE(q_entries); i++) {
		ret = test(&q_entries[i]);
		if (ret == T_EXIT_SKIP) {
			return T_EXIT_SKIP;
		} else if (ret != T_EXIT_PASS) {
			fprintf(stderr, "Failed at %d/%d\n", q_entries[i].sqes,
							q_entries[i].cqes);
			return T_EXIT_FAIL;
		}
	}

	return T_EXIT_PASS;
}
