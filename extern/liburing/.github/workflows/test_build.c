#include <liburing.h>

int main(void)
{
	struct io_uring ring;
	io_uring_queue_init(8, &ring, 0);
	io_uring_queue_exit(&ring);
	return 0;
}
