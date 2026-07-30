/* SPDX-License-Identifier: MIT */
/*
 * Description: Run recv multishot with bundle support and using
 *		incrementally consumed buffers, and verify that the
 *		ordering is correct.
 *
 *		Also see:
 *
 *		https://github.com/axboe/liburing/issues/1432
 *
 *		for the bug report, and where this test case came from.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "liburing.h"
#include "helpers.h"

static int no_buf_ring, no_recv_mshot;

/* Configuration constants */
#define TWO_KB      (1024 * 2)	/* Size of test data (2KB) */
#define BUFFER_SIZE 1024		/* Size of each buffer in bytes */

#define BUFFER_COUNT 4		/* Number of buffers in the ring */
#define QUEUE_DEPTH 16		/* io_uring queue depth */

#define min(a, b) (((a) < (b)) ? (a) : (b))

/**
 * Buffer data structure
 * Contains information about a buffer from the ring
 */
struct buf_data {
	void *addr;		/* Buffer memory address */
	uint32_t len;		/* Length of valid data in the buffer */
};

/**
 * Buffer ring data structure
 * Holds information needed to manage a buffer ring
 */
struct buf_ring_data {
	struct io_uring_buf_ring *buf_ring;	/* The io_uring buffer ring */
	void *buffer_memory;	/* Memory for all buffers */
	uint16_t ring_entries;	/* Number of entries in the ring */
	uint32_t buf_size;	/* Size of each buffer */
};

struct read_buf {
	void *buffer_memory;	/* Memory for all buffers */
	size_t cursor;
};

/**
 * Sets up and initializes the buffer ring for io_uring
 *
 * This function allocates memory for the buffer ring and all individual buffers,
 * initializes the buffer ring, registers it with io_uring, and adds all buffers
 * to the ring.
 *
 * @param ring      Pointer to the io_uring instance
 * @param entries   Number of buffer entries to create
 * @param buf_size  Size of each buffer in bytes
 * @param bgid      Buffer group ID to use
 *
 */
static int setup_buf_ring(struct buf_ring_data *data, struct io_uring *ring,
			  uint16_t entries, uint32_t buf_size, int bgid)
{
	data->ring_entries = entries;
	data->buf_size = buf_size;

	/* Allocate page-aligned memory for all buffers */
	size_t total_size = entries * buf_size;
	int page_size = sysconf(_SC_PAGESIZE);
	size_t aligned_size = (total_size + page_size - 1) & ~(page_size - 1);

	void *buffer_memory = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE,
		 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(buffer_memory != MAP_FAILED);

	/* Verify buffer memory is page-aligned as guaranteed by mmap */
	data->buffer_memory = buffer_memory;

	/* Allocate and setup buffer ring with page alignment */
	void *mapped;
	struct io_uring_buf_ring *buf_ring;
	int ring_size = entries * sizeof(struct io_uring_buf);

	/* Round up ring size to page boundary */
	ring_size = (ring_size + page_size - 1) & ~(page_size - 1);

	mapped = mmap(NULL, ring_size, PROT_READ | PROT_WRITE,
		      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(mapped != MAP_FAILED);

	buf_ring = (struct io_uring_buf_ring *)mapped;

	/* Initialize the buffer ring structure */
	io_uring_buf_ring_init(buf_ring);
	data->buf_ring = buf_ring;

	/* Prepare registration parameters */
	struct io_uring_buf_reg reg = {
		.ring_addr = (unsigned long)buf_ring,
		.ring_entries = entries,
		.bgid = 0
	};

	/* Register the buffer ring with io_uring */
	int ret = io_uring_register_buf_ring(ring, &reg, IOU_PBUF_RING_INC);

	if (ret) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "Buffer ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Add all individual buffers to the ring */
	for (int i = 0; i < entries; i++) {
		void *buf_addr = buffer_memory + i * buf_size;

		io_uring_buf_ring_add(buf_ring, buf_addr, buf_size, i,
				      io_uring_buf_ring_mask(entries), i);
	}

	/* Make all buffers available by advancing the tail pointer */
	io_uring_buf_ring_advance(buf_ring, entries);
	return T_EXIT_PASS;
}

/**
 * Verifies that received buffer data matches expected data
 *
 * This function compares each byte of received data against the expected data
 * and asserts if any mismatch is found. It also prints the comparison for debugging.
 *
 * @param buf                 Pointer to buffer containing received data
 * @param expected_data_start Pointer to start of expected data for comparison
 */
static int verify_received_buffer(struct buf_data *buf, uint8_t *expected_data_start)
{
	uint8_t *data = buf->addr;

	for (uint32_t i = 0; i < buf->len; i++) {
		if (data[i] != expected_data_start[i]) {
			fprintf(stderr, "Recv data ordering mismatch. expected: %d, get: %d\n", expected_data_start[i], data[i]);
			return 1;
		}
	}

	return 0;
}

/**
 * Processes a completed io_uring receive operation
 *
 * This function handles the completion queue entry by:
 * 1. Extracting the received data from the CQE
 * 2. Verifying the data matches the expected pattern
 *
 * @param cqe             Completion queue entry to process
 * @param rb              read_buf data struct to keep track the cursor
 * @param current_expect  Pointer to current position in expected data (updated by this function)
 */
static int process_completion(struct io_uring_cqe *cqe, struct read_buf *rb,
			      uint8_t **current_expect)
{
	usleep(1);

	if (cqe->res <= 0) {
		/* Handle error or EOF condition */
		if (cqe->res == 0) {
			fprintf(stderr, "EOF reached\n");
		} else if (cqe->res == -EINVAL) {
			/* no recv mshot support */
			no_recv_mshot = 1;
		} else if (cqe->res != -ENOBUFS) {
			fprintf(stderr, "CQE res %d\n", cqe->res);
			return 1;
		}
		return 0;
	}

	/* Extract data length from completion */
	uint32_t total_len = cqe->res;

	/* should never get a len large then bundled buffer size */
	assert(total_len <= BUFFER_SIZE);

	void *buffer_addr = (uint8_t*)rb->buffer_memory + (rb->cursor);
	/* Prepare buffer data structure for verification */
	struct buf_data buf = {
		.addr = buffer_addr,
		.len = total_len
	};

	/* Verify received data against expected pattern */
	if (verify_received_buffer(&buf, *current_expect))
		return T_EXIT_FAIL;
	rb->cursor += total_len;
	return 0;
}

/**
 * Writes all the data to the specified file descriptor
 *
 * This function ensures that all data is written, handling partial writes
 * by making repeated calls to write() until all bytes are sent.
 *
 * @param fd File descriptor to write to
 * @param data Pointer to the data buffer to write
 * @param size Number of bytes to write
 */
static void write_all(int fd, const void *data, size_t size)
{
	const uint8_t *buf = data;
	size_t bytes_sent = 0;

	/* Continue until all data is sent */
	while (bytes_sent < size) {
		ssize_t sent;

		sent = write(fd, buf + bytes_sent, size - bytes_sent);
		assert(sent > 0);	/* Ensure write succeeded */
		bytes_sent += sent;
	}
}

/**
 * Main test function for io_uring bundle receive mechanism
 *
 * This function demonstrates the complete flow of using io_uring's buffer ring
 * and multishot receive with IORING_RECVSEND_BUNDLE flag. It performs these steps:
 * 1. Set up an io_uring instance and buffer ring
 * 2. Create a TCP socket pair for testing
 * 3. Send 2KB of pattern data over the socket
 * 4. Receive and verify the data are as expected
 *
 */
static int test_recv_incr(int queue_flags)
{
	/* Initialize io_uring with parameters */
	struct io_uring ring;
	struct io_uring_params params = { .flags = queue_flags, };
	int ret, eret;

	ret = t_create_ring_params(QUEUE_DEPTH, &ring, &params);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return T_EXIT_FAIL;

	/* Set up the buffer ring for receiving data */
	struct buf_ring_data br_data;
	ret = setup_buf_ring(&br_data, &ring, BUFFER_COUNT, BUFFER_SIZE, 0);
	if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_EXIT_PASS)
		return T_EXIT_FAIL;

	/* Create socket pair for local communication testing */
	int socket_fds[2];
	ret = t_create_socket_pair(socket_fds, true);
	assert(ret == 0);

	int receiver_fd = socket_fds[0];
	int sender_fd = socket_fds[1];

	size_t data_received = 0;	/* Tracks total bytes received */

	/* Submit initial multishot receive operations with buffer selection */
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

	io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->ioprio |= IORING_RECVSEND_BUNDLE;
	sqe->buf_group = 0;
	io_uring_submit(&ring);

	/* Allocate and initialize test data with pattern */
	size_t total_bytes = TWO_KB;

	uint8_t *test_data = malloc(total_bytes);
	for (int i = 0; i < total_bytes; i++)
		test_data[i] = i % 256;	/* Create repeating pattern */

	/* Send data in chunks to test buffer boundary handling, intentionally misaligned with buffer size */
	size_t chunk_sizes[] = {512, 333, 777, 426};
	size_t sent_offset = 0;
	size_t num_chunks = sizeof(chunk_sizes) / sizeof(chunk_sizes[0]);

	struct read_buf rb = {
		.buffer_memory = br_data.buffer_memory,
		.cursor = 0
	};

	for (size_t i = 0; i < num_chunks; i++) {
		size_t chunk_size = chunk_sizes[i];
		size_t end_offset = min(sent_offset + chunk_size, total_bytes);
		size_t chunk_len = end_offset - sent_offset;
		uint8_t* chunk = test_data + sent_offset;

		/* Send test data through the socket */
		write_all(sender_fd, chunk, chunk_len);

		struct io_uring_cqe *cqe;

		size_t remaining_chunk_len = chunk_len;

		while (remaining_chunk_len) {
			/* Wait for a completion event */
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait_cqe=%d\n", ret);
				eret = T_EXIT_FAIL;
				goto exit;
			}

			/* Initialize pointer to track our position in expected data */
			uint8_t *current_expect = chunk + (chunk_len - remaining_chunk_len);

			if (process_completion(cqe, &rb, &current_expect)) {
				eret = T_EXIT_FAIL;
				goto exit;
			}
			if (no_recv_mshot) {
				eret = T_EXIT_SKIP;
				goto exit;
			}

			remaining_chunk_len -= cqe->res;
			data_received += cqe->res;

			io_uring_cq_advance(&ring, 1);
		}
		sent_offset = end_offset;
		if (sent_offset >= total_bytes)
			break;
	}

	/* Close sender side to signal EOF to receiver */
	close(sender_fd);

	/* Verify we received all expected data */
	if (data_received != total_bytes) {
		fprintf(stderr, "Received %u, wanted %u\n", (int) data_received, TWO_KB);
		return T_EXIT_FAIL;
	}

	eret = T_EXIT_PASS;
exit:
	/* Clean up all allocated resources */
	close(receiver_fd);	/* Close socket */
	io_uring_queue_exit(&ring);	/* Clean up io_uring */

	/* Free memory resources */
	munmap(br_data.buffer_memory, BUFFER_COUNT * BUFFER_SIZE);
	munmap(br_data.buf_ring, BUFFER_COUNT * sizeof(struct io_uring_buf));
	free(test_data);

	return eret;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_recv_incr(0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 failed\n");
		return ret;
	}
	if (no_buf_ring || no_recv_mshot)
		return T_EXIT_SKIP;

	ret = test_recv_incr(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test defer failed\n");
		return ret;
	}

	ret = test_recv_incr(IORING_SETUP_SQPOLL);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test sqpoll failed\n");
		return ret;
	}

	ret = test_recv_incr(IORING_SETUP_COOP_TASKRUN);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test coop failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}
