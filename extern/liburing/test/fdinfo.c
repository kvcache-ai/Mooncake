/* SPDX-License-Identifier: MIT */
/*
 * Description: do SQPOLL IO with concurrent fdinfo reading
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>

#include "helpers.h"
#include "liburing.h"

#define FILE_SIZE	(256 * 1024)
#define BS		8192
#define BUFFERS		(FILE_SIZE / BS)

static struct iovec *vecs;
static int no_read;
static int warned;

static void fdinfo_read(struct io_uring *ring)
{
	char fd_name[128];
	char *buf;
	int fd;

	buf = malloc(4096);

	sprintf(fd_name, "/proc/self/fdinfo/%d", ring->ring_fd);
	fd = open(fd_name, O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}

	do {
		int ret = read(fd, buf, 4096);

		if (ret < 0) {
			perror("fdinfo read");
			break;
		} else if (ret == 4096) {
			continue;
		}
		break;
	} while (1);

	close(fd);
	free(buf);
}

static int __test_io(const char *file, struct io_uring *ring, int write,
		     int buffered, int sqthread, int fixed, int nonvec,
		     int buf_select, int seq, int exp_len, int mixed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int open_flags;
	int i, fd = -1, ret;
	off_t offset;

#ifdef VERBOSE
	fprintf(stdout, "%s: start %d/%d/%d/%d/%d/%d: ", __FUNCTION__, write,
							buffered, sqthread,
							fixed, nonvec, mixed);
#endif
	if (write)
		open_flags = O_WRONLY;
	else
		open_flags = O_RDONLY;
	if (!buffered)
		open_flags |= O_DIRECT;

	if (fixed) {
		ret = t_register_buffers(ring, vecs, BUFFERS);
		if (ret == T_SETUP_SKIP)
			return T_EXIT_SKIP;
		if (ret != T_SETUP_OK) {
			fprintf(stderr, "buffer reg failed: %d\n", ret);
			goto err;
		}
	}

	fd = open(file, open_flags);
	if (fd < 0) {
		if (errno == EINVAL)
			return 0;
		if (errno == EPERM || errno == EACCES)
			return T_EXIT_SKIP;
		perror("file open");
		goto err;
	}

	if (sqthread) {
		ret = io_uring_register_files(ring, &fd, 1);
		if (ret) {
			fprintf(stderr, "file reg failed: %d\n", ret);
			goto err;
		}
	}

	offset = 0;
	for (i = 0; i < BUFFERS; i++) {
		if (mixed && i & 1) {
			sqe = io_uring_get_sqe128(ring);
			if (!sqe) {
				fprintf(stderr, "sqe get failed\n");
				goto err;
			}
			io_uring_prep_nop128(sqe);
			sqe->user_data = i;
			continue;
		}

		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "sqe get failed\n");
			goto err;
		}
		if (!seq)
			offset = BS * (rand() % BUFFERS);
		if (write) {
			int do_fixed = fixed;
			int use_fd = fd;

			if (sqthread)
				use_fd = 0;
			if (fixed && (i & 1))
				do_fixed = 0;
			if (do_fixed) {
				io_uring_prep_write_fixed(sqe, use_fd, vecs[i].iov_base,
								vecs[i].iov_len,
								offset, i);
			} else if (nonvec) {
				io_uring_prep_write(sqe, use_fd, vecs[i].iov_base,
							vecs[i].iov_len, offset);
			} else {
				io_uring_prep_writev(sqe, use_fd, &vecs[i], 1,
								offset);
			}
		} else {
			int do_fixed = fixed;
			int use_fd = fd;

			if (sqthread)
				use_fd = 0;
			if (fixed && (i & 1))
				do_fixed = 0;
			if (do_fixed) {
				io_uring_prep_read_fixed(sqe, use_fd, vecs[i].iov_base,
								vecs[i].iov_len,
								offset, i);
			} else if (nonvec) {
				io_uring_prep_read(sqe, use_fd, vecs[i].iov_base,
							vecs[i].iov_len, offset);
			} else {
				io_uring_prep_readv(sqe, use_fd, &vecs[i], 1,
								offset);
			}

		}
		sqe->user_data = i;
		if (sqthread)
			sqe->flags |= IOSQE_FIXED_FILE;
		if (buf_select) {
			if (nonvec)
				sqe->addr = 0;
			sqe->flags |= IOSQE_BUFFER_SELECT;
			sqe->buf_group = buf_select;
		}
		if (seq)
			offset += BS;
	}

	__io_uring_flush_sq(ring);
	fdinfo_read(ring);

	ret = io_uring_submit(ring);
	if (!mixed && ret != BUFFERS) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret, BUFFERS);
		goto err;
	} else if (mixed && ret != BUFFERS + (BUFFERS >> 1)) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret,
			BUFFERS + (BUFFERS >> 1));
		goto err;
	}

	for (i = 0; i < 10; i++) {
		fdinfo_read(ring);
		usleep(2);
	}

	for (i = 0; i < BUFFERS; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			goto err;
		}
		if (mixed && cqe->user_data & 1) {
			if (cqe->user_data > 32) {
				fprintf(stderr, "cqe user data %lld, max expected %d\n",
					cqe->user_data, BUFFERS - 1);
				goto err;
			}
		} else if (cqe->res == -EINVAL && nonvec) {
			if (!warned) {
				fprintf(stdout, "Non-vectored IO not "
					"supported, skipping\n");
				warned = 1;
				no_read = 1;
			}
		} else if (exp_len == -1) {
			int iov_len = vecs[cqe->user_data].iov_len;

			if (cqe->res != iov_len) {
				fprintf(stderr, "cqe res %d, wanted %d\n",
					cqe->res, iov_len);
				goto err;
			}
		} else if (cqe->res != exp_len) {
			fprintf(stderr, "cqe res %d, wanted %d\n", cqe->res, exp_len);
			goto err;
		}
		if (buf_select && exp_len == BS) {
			int bid = cqe->flags >> 16;
			unsigned char *ptr = vecs[bid].iov_base;
			int j;

			for (j = 0; j < BS; j++) {
				if (ptr[j] == cqe->user_data)
					continue;

				fprintf(stderr, "Data mismatch! bid=%d, "
						"wanted=%d, got=%d\n", bid,
						(int)cqe->user_data, ptr[j]);
				return 1;
			}
		}
		io_uring_cqe_seen(ring, cqe);
	}

	if (fixed) {
		ret = io_uring_unregister_buffers(ring);
		if (ret) {
			fprintf(stderr, "buffer unreg failed: %d\n", ret);
			goto err;
		}
	}
	if (sqthread) {
		ret = io_uring_unregister_files(ring);
		if (ret) {
			fprintf(stderr, "file unreg failed: %d\n", ret);
			goto err;
		}
	}

	close(fd);
#ifdef VERBOSE
	fprintf(stdout, "PASS\n");
#endif
	return 0;
err:
#ifdef VERBOSE
	fprintf(stderr, "FAILED\n");
#endif
	if (fd != -1)
		close(fd);
	return 1;
}
static int test_io(const char *file, int write, int buffered, int sqthread,
		   int fixed, int nonvec, int exp_len, int mixed)
{
	struct io_uring ring;
	int ret, ring_flags = 0, entries = 64;

	if (sqthread)
		ring_flags = IORING_SETUP_SQPOLL;
	if (mixed) {
		ring_flags |= IORING_SETUP_SQE_MIXED;
		entries = 128;
	}

	ret = t_create_ring(entries, &ring, ring_flags);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	ret = __test_io(file, &ring, write, buffered, sqthread, fixed, nonvec,
			0, 0, exp_len, mixed);
	io_uring_queue_exit(&ring);
	return ret;
}

static int has_nonvec_read(void)
{
	struct io_uring_probe *p;
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		exit(ret);
	}

	p = t_calloc(1, sizeof(*p) + 256 * sizeof(struct io_uring_probe_op));
	ret = io_uring_register_probe(&ring, p, 256);
	/* if we don't have PROBE_REGISTER, we don't have OP_READ/WRITE */
	if (ret == -EINVAL) {
out:
		io_uring_queue_exit(&ring);
		free(p);
		return 0;
	} else if (ret) {
		fprintf(stderr, "register_probe: %d\n", ret);
		goto out;
	}

	if (p->ops_len <= IORING_OP_READ)
		goto out;
	if (!(p->ops[IORING_OP_READ].flags & IO_URING_OP_SUPPORTED))
		goto out;
	io_uring_queue_exit(&ring);
	free(p);
	return 1;
}

static int test_eventfd_read(int flags)
{
	struct io_uring ring;
	int fd, ret;
	eventfd_t event;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;

	if (no_read)
		return 0;
	ret = t_create_ring(64, &ring, flags);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		if (ret == -EINVAL)
			return 0;
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	fd = eventfd(1, 0);
	if (fd < 0) {
		perror("eventfd");
		return 1;
	}
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, &event, sizeof(eventfd_t), 0);
	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submitted %d\n", ret);
		return 1;
	}
	fdinfo_read(&ring);
	eventfd_write(fd, 1);
	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		return 1;
	}
	if (cqe->res == -EINVAL) {
		fprintf(stdout, "eventfd IO not supported, skipping\n");
	} else if (cqe->res != sizeof(eventfd_t)) {
		fprintf(stderr, "cqe res %d, wanted %d\n", cqe->res,
						(int) sizeof(eventfd_t));
		return 1;
	}
	io_uring_cqe_seen(&ring, cqe);
	return 0;
}

int main(int argc, char *argv[])
{
	int i, ret, nr;
	char buf[256];
	char *fname;

	if (argc > 1) {
		fname = argv[1];
	} else {
		srand((unsigned)time(NULL));
		snprintf(buf, sizeof(buf), ".basic-rw-%u-%u",
			(unsigned)rand(), (unsigned)getpid());
		fname = buf;
		t_create_file(fname, FILE_SIZE);
	}

	signal(SIGXFSZ, SIG_IGN);

	vecs = t_create_buffers(BUFFERS, BS);

	/* if we don't have nonvec read, skip testing that */
	nr = has_nonvec_read() ? 64 : 32;

	for (i = 0; i < nr; i++) {
		int write = (i & 1) != 0;
		int buffered = (i & 2) != 0;
		int sqthread = (i & 4) != 0;
		int fixed = (i & 8) != 0;
		int mixed = (i & 16) != 0;
		int nonvec = (i & 32) != 0;

		ret = test_io(fname, write, buffered, sqthread, fixed, nonvec,
			      BS, mixed);
		if (ret == T_EXIT_SKIP)
			continue;
		if (ret) {
			fprintf(stderr, "test_io failed %d/%d/%d/%d/%d\n",
				write, buffered, sqthread, fixed, nonvec);
			goto err;
		}
	}

	ret = test_eventfd_read(0);
	if (ret) {
		fprintf(stderr, "eventfd read 0 failed\n");
		goto err;
	}

	ret = test_eventfd_read(IORING_SETUP_DEFER_TASKRUN|IORING_SETUP_SINGLE_ISSUER);
	if (ret) {
		fprintf(stderr, "eventfd read defer failed\n");
		goto err;
	}

	ret = test_eventfd_read(IORING_SETUP_SQPOLL);
	if (ret) {
		fprintf(stderr, "eventfd read sqpoll failed\n");
		goto err;
	}

	if (fname != argv[1])
		unlink(fname);
	return 0;
err:
	if (fname != argv[1])
		unlink(fname);
	return 1;
}
