#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

namespace mooncake {
void print_backtrace() {
    const int max_frames = 64;
    void* buffer[max_frames];
    int nptrs = backtrace(buffer, max_frames);
    char** symbols = backtrace_symbols(buffer, nptrs);
    if (symbols == nullptr) {
        perror("backtrace_symbols");
        return;
    }

    fprintf(stderr, "Backtrace (C++):\n");
    for (int i = 0; i < nptrs; ++i) {
        fprintf(stderr, "%s\n", symbols[i]);
    }
    free(symbols);
}

void signal_handler(int signum) {
    fprintf(stderr, "Received signal %d\n", signum);
    print_backtrace();
    _exit(EXIT_FAILURE);  // avoid flushing corrupt state
}

void setup_signal_handlers() {
    signal(SIGSEGV, signal_handler);  // segmentation fault
    signal(SIGABRT, signal_handler);  // abort()
    signal(SIGFPE, signal_handler);   // divide by zero etc.
    signal(SIGILL, signal_handler);
    signal(SIGBUS, signal_handler);
}

}