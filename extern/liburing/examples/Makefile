CPPFLAGS ?=
override CPPFLAGS += -D_GNU_SOURCE -I../src/include/
CFLAGS ?= -g -O2 -Wall
LDFLAGS ?=
override LDFLAGS += -L../src/ -luring

include ../Makefile.quiet

ifneq ($(MAKECMDGOALS),clean)
include ../config-host.mak
endif

LDFLAGS ?=
override LDFLAGS += -L../src/ -luring -lpthread

ifeq ($(CONFIG_USE_SANITIZER),y)
	override CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -fno-optimize-sibling-calls
	override CPPFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -fno-optimize-sibling-calls
	override LDFLAGS += -fsanitize=address,undefined
endif

ifeq ($(CONFIG_USE_TSAN),y)
	override CFLAGS += -fsanitize=thread -fno-omit-frame-pointer -fno-optimize-sibling-calls
	override CPPFLAGS += -fsanitize=thread -fno-omit-frame-pointer -fno-optimize-sibling-calls
	override LDFLAGS += -fsanitize=thread -ltsan
endif

example_srcs := \
	io_uring-close-test.c \
	io_uring-cp.c \
	io_uring-test.c \
	io_uring-udp.c \
	link-cp.c \
	napi-busy-poll-client.c \
	napi-busy-poll-server.c \
	poll-bench.c \
	reg-wait.c \
	send-zerocopy.c \
	rsrc-update-bench.c \
	proxy.c \
	zcrx.c \
	kdigest.c

all_targets :=


ifdef CONFIG_HAVE_UCONTEXT
	example_srcs += ucontext-cp.c
endif
all_targets += ucontext-cp helpers.o

example_targets := $(patsubst %.c,%,$(patsubst %.cc,%,$(example_srcs)))
all_targets += $(example_targets)

helpers = helpers.o

all: $(example_targets)

helpers.o: helpers.c
	$(QUIET_CC)$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -c $<

%: %.c $(helpers) ../src/liburing.a
	$(QUIET_CC)$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(helpers) $(LDFLAGS)

clean:
	@rm -f $(all_targets)

.PHONY: all clean
