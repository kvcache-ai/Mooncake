/* SPDX-License-Identifier: MIT */
/*
 * Proof-of-concept for doing file digests using the kernel's AF_ALG API.
 * Needs a bit of error handling.
 */
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/if_alg.h>
#include "liburing.h"

#define QD		64
#define WAIT_BATCH	(QD / 8)
#define BS		(64*1024)

#define BGID		1
#define BID_MASK	(QD - 1)

enum req_state {
	IO_INIT = 0,
	IO_READ,
	IO_READ_COMPLETE,
	IO_WRITE,
	IO_WRITE_COMPLETE,
};

struct req {
	off_t offset;
	enum req_state state;
	struct iovec iov;
};

struct kdigest {
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	struct req reqs[QD];
	/* heap allocated, aligned QD*BS buffer */
	uint8_t *bufs;
};

static int infd, outfd;

static int get_file_size(int fd, size_t *size)
{
	struct stat st;

	if (fstat(fd, &st) < 0)
		return -1;
	if (S_ISREG(st.st_mode)) {
		*size = st.st_size;
	} else if (S_ISBLK(st.st_mode)) {
		unsigned long long bytes;

		if (ioctl(fd, BLKGETSIZE64, &bytes) != 0)
			return -1;

		*size = bytes;
	} else {
		return -1;
	}

	return 0;
}

static int reap_completions(struct io_uring *ring, int *inflight,
			    size_t *outsize)
{
	struct io_uring_cqe *cqe;
	unsigned head;
	int ret = 0, nr;

	nr = 0;
	io_uring_for_each_cqe(ring, head, cqe) {
		struct req *req;

		req = io_uring_cqe_get_data(cqe);
		assert(req->state == IO_READ || req->state == IO_WRITE);
		if (cqe->res < 0) {
			fprintf(stderr, "%s: cqe error %d\n",
				req->state == IO_WRITE ? "send" : "read",
				cqe->res);
			*outsize = 0;
			ret = 1;
			break;
		}

		(*inflight)--;
		req->state++;
		if (req->state == IO_WRITE_COMPLETE)
			*outsize -= cqe->res;
		nr++;
	}

	io_uring_cq_advance(ring, nr);
	return ret;
}

/*
 * Add buffers to the outgoing ring, and submit a single bundle send that
 * will finish when all of them have completed.
 */
static void submit_sends_br(struct kdigest *kdigest, int *write_idx,
			    int *inflight)
{
	struct io_uring_buf_ring *br = kdigest->br;
	struct req *req, *first_req = NULL;
	struct io_uring_sqe *sqe;
	int nr = 0;

	/*
	 * Find any completed reads, and add the buffers to the outgoing
	 * send ring. That will serialize the data sent.
	 */
	while (kdigest->reqs[*write_idx].state == IO_READ_COMPLETE) {
		req = &kdigest->reqs[*write_idx];
		io_uring_buf_ring_add(br, req->iov.iov_base, req->iov.iov_len,
					*write_idx, BID_MASK, nr++);
		/*
		 * Mark as a write/send if it's the first one, that serve
		 * as the "barrier" in the array. The rest can be marked
		 * complete upfront, if there's more in this bundle, as
		 * the first will serve a the stopping point.
		 */
		if (!first_req) {
			req->state = IO_WRITE;
			first_req = req;
		} else {
			req->state = IO_WRITE_COMPLETE;
		}
		*write_idx = (*write_idx + 1) % QD;
	}

	/*
	 * If any completed reads were found and we added buffers, advance
	 * the buffer ring and prepare a single bundle send for all of them.
	 */
	if (first_req) {
		io_uring_buf_ring_advance(br, nr);

		sqe = io_uring_get_sqe(&kdigest->ring);
		io_uring_prep_send_bundle(sqe, outfd, 0, MSG_MORE);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = BGID;
		io_uring_sqe_set_data(sqe, first_req);
		(*inflight)++;
	}
}

/*
 * Serialize multiple writes with IOSQE_IO_LINK. Not the most efficient
 * way, as it's both more expensive on the kernel side to handle link, and
 * if there's bundle support, all of the below can be done with a single
 * send rather than multiple ones.
 */
static void submit_sends_linked(struct kdigest *kdigest, int *write_idx,
				int *inflight)
{
	struct io_uring_sqe *sqe;
	struct req *req;

	/* Queue up any possible writes. Link flag ensures ordering. */
	sqe = NULL;
	while (kdigest->reqs[*write_idx].state == IO_READ_COMPLETE) {
		if (sqe)
			sqe->flags |= IOSQE_IO_LINK;

		req = &kdigest->reqs[*write_idx];
		req->state = IO_WRITE;
		sqe = io_uring_get_sqe(&kdigest->ring);
		io_uring_prep_send(sqe, outfd, req->iov.iov_base,
					req->iov.iov_len, MSG_MORE);
		io_uring_sqe_set_data(sqe, req);
		(*inflight)++;

		*write_idx = (*write_idx + 1) % QD;
	}
}

static void submit_sends(struct kdigest *kdigest, int *write_idx, int *inflight)
{
	if (kdigest->br)
		submit_sends_br(kdigest, write_idx, inflight);
	else
		submit_sends_linked(kdigest, write_idx, inflight);
}

static int digest_file(struct kdigest *kdigest, size_t insize)
{
	struct io_uring *ring = &kdigest->ring;
	off_t read_off = 0;
	size_t outsize = insize;
	int read_idx = 0, write_idx = 0, inflight = 0;

	while (outsize) {
		struct io_uring_sqe *sqe;
		struct req *req;
		int to_wait;

		submit_sends(kdigest, &write_idx, &inflight);

		/* Queue up any reads. Completions may arrive out of order. */
		while (insize && (kdigest->reqs[read_idx].state == IO_INIT
		    || kdigest->reqs[read_idx].state == IO_WRITE_COMPLETE)) {
			size_t this_size = (insize < BS ? insize : BS);

			req = &kdigest->reqs[read_idx];
			req->state = IO_READ;
			req->offset = read_off;
			req->iov.iov_base = &kdigest->bufs[read_idx * BS];
			req->iov.iov_len = this_size;

			sqe = io_uring_get_sqe(ring);
			io_uring_prep_read(sqe, infd, req->iov.iov_base,
						req->iov.iov_len, read_off);
			io_uring_sqe_set_data(sqe, req);

			read_off += this_size;
			insize -= this_size;
			inflight++;

			read_idx = (read_idx + 1) % QD;
		}

		/* wait for about half queue completion before resubmit */
		for (to_wait = (inflight >> 1) | 1; to_wait; to_wait--) {
			int ret, wait_nr;

			wait_nr = inflight;
			if (wait_nr > WAIT_BATCH)
				wait_nr = WAIT_BATCH;

			ret = io_uring_submit_and_wait(ring, wait_nr);
			if (ret < 0) {
				fprintf(stderr, "wait cqe: %s\n",
					strerror(-ret));
				return 1;
			}

			if (reap_completions(ring, &inflight, &outsize))
				return 1;
		}
	}
	assert(!inflight);

	return 0;
}

static int get_result(struct kdigest *kdigest, const char *alg, const char *file)
{
	struct io_uring *ring = &kdigest->ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;
	/* reuse I/O buf block to stash hash result */

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv(sqe, outfd, kdigest->bufs, BS, 0);

	if (io_uring_submit_and_wait(ring, 1) < 0)
		return 1;

	ret = io_uring_peek_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "peek cqe: %s\n", strerror(-ret));
		return 1;
	}

	if (cqe->res < 0) {
		fprintf(stderr, "cqe error: %s\n", strerror(-cqe->res));
		goto err;
	}

	fprintf(stdout, "uring %s%s(%s) returned(len=%u): ",
		kdigest->br ? "bundled " : "", alg, file, cqe->res);
	for (i = 0; i < cqe->res; i++)
		fprintf(stdout, "%02x", kdigest->bufs[i]);
	putc('\n', stdout);
	ret = 0;
err:
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

int main(int argc, char *argv[])
{
	const char *alg;
	const char *infile;
	size_t alg_len, insize;
	struct sockaddr_alg sa = {
		.salg_family = AF_ALG,
		.salg_type = "hash",
	};
	struct kdigest kdigest = { };
	struct io_uring_params p = { };
	int sfd, ret;

	if (argc < 3) {
		fprintf(stderr, "%s: algorithm infile\n", argv[0]);
		return 1;
	}

	alg = argv[1];
	infile = argv[2];
	alg_len = strlen(alg);
	if (alg_len >= sizeof(sa.salg_name)) {
		fprintf(stderr, "algorithm name too long\n");
		return 1;
	}
	/* +1 for null terminator */
	memcpy(sa.salg_name, alg, alg_len + 1);

	infd = open(infile, O_RDONLY);
	if (infd < 0) {
		perror("open infile");
		return 1;
	}

	sfd = socket(AF_ALG, SOCK_SEQPACKET, 0);
	if (sfd < 0) {
		if (errno == EAFNOSUPPORT)
			fprintf(stderr, "kernel AF_ALG support not available. "
				"CONFIG_CRYPTO_USER_API_HASH required.\n");
		else
			perror("AF_ALG socket");
		return 1;
	}

	if (bind(sfd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
		if (errno == ENOENT)
			fprintf(stderr, "AF_ALG bind(%s): hash not available. "
				"See /proc/crypto hash algorithm list.\n",
				alg);
		else
			fprintf(stderr, "AF_ALG bind(%s): %s\n",
				alg, strerror(errno));
		return 1;
	}

	outfd = accept(sfd, NULL, 0);
	if (outfd < 0) {
		perror("AF_ALG accept");
		return 1;
	}

	if (posix_memalign((void **)&kdigest.bufs, 4096, QD * BS)) {
		fprintf(stderr, "failed to alloc I/O bufs\n");
		return 1;
	}

	p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
	do {
		ret = io_uring_queue_init_params(QD, &kdigest.ring, &p);
		if (!ret)
			break;
		if (!p.flags) {
			fprintf(stderr, "queue_init: %s\n", strerror(-ret));
			return 1;
		}
		p.flags = 0;
	} while (1);

	/* use send bundles, if available */
	if (p.features & IORING_FEAT_RECVSEND_BUNDLE) {
		kdigest.br = io_uring_setup_buf_ring(&kdigest.ring, QD, BGID, 0, &ret);
		if (!kdigest.br) {
			fprintf(stderr, "Failed setting up bundle buffer ring: %d\n", ret);
			return 1;
		}
	}

	if (get_file_size(infd, &insize))
		return 1;

	ret = digest_file(&kdigest, insize);
	if (ret) {
		fprintf(stderr, "%s digest failed\n", alg);
		return 1;
	}

	ret = get_result(&kdigest, alg, infile);
	if (ret) {
		fprintf(stderr, "failed to retrieve %s digest result\n", alg);
		return 1;
	}

	if (kdigest.br)
		io_uring_free_buf_ring(&kdigest.ring, kdigest.br, QD, BGID);
	io_uring_queue_exit(&kdigest.ring);
	free(kdigest.bufs);
	if (close(infd) < 0)
		ret |= 1;
	if (close(sfd) < 0)
		ret |= 1;
	if (close(outfd) < 0)
		ret |= 1;
	return ret;
}
