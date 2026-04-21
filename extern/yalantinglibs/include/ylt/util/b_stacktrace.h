#if !defined(B_STACKTRACE_INCLUDED)
#define B_STACKTRACE_INCLUDED (1)

// This file is modified from b_stacktrace.h.
// We make it head-only
/*
b_stacktrace v0.23 -- a cross-platform stack-trace generator
SPDX-License-Identifier: MIT
URL: https://github.com/iboB/b_stacktrace

Usage
=====

#define B_STACKTRACE_IMPL before including b_stacktrace.h in *one* C or C++
file to create the implementation

#include "b_stacktrace.h" to get access to the following functions:

char* b_stacktrace_get_string();
    Returns a human-readable stack-trace string from the point of view of the
    caller.
    The string is allocated with `malloc` and needs to be freed with `free`

b_stacktrace_handle b_stacktrace_get();
    Returns a stack-trace handle from the point of view of the caller which
    can be expanded to a string via b_stacktrace_to_string.
    The handle is allocated with `malloc` and needs to be freed with `free`

b_stacktrace_to_string(b_stacktrace_handle stacktrace);
    Converts a stack-trace handle to a human-readable string.
    The string is allocated with `malloc` and needs to be freed with `free`

B_STACKTRACE_API int b_stacktrace_depth(b_stacktrace_handle stacktrace);
    Returns the number of entries (frames) in the stack-trace handle.


Config
======

#define B_STACKTRACE_API to custom export symbols to export the library
functions from a shared lib

Revision History
================

* 0.23 (2022-12-20) Add b_stacktrace_depth
* 0.21 (2022-12-20) Fixed typo
* 0.20 (2022-12-18) Beta.
                    Expanded interface
                    Minor fixes
* 0.10 (2020-12-07) Initial public release. Alpha version

MIT License
===========

Copyright (c) 2020-2025 Borislav Stanimirov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

namespace ylt::util {
typedef struct b_stacktrace_tag* b_stacktrace_handle;

/*
    Returns a stack-trace handle from the point of view of the caller which
    can be expanded to a string via b_stacktrace_to_string.
    The handle is allocated with `malloc` and needs to be freed with `free`
*/
b_stacktrace_handle b_stacktrace_get();

/*
    Returns the number of entries (frames) in the stack-trace handle.
*/
int b_stacktrace_depth(b_stacktrace_handle stacktrace);

/*
    Converts a stack-trace handle to a human-readable string.
    The string is allocated with `malloc` and needs to be freed with `free`
*/
char* b_stacktrace_to_string(b_stacktrace_handle stacktrace);

/*
    Returns a human-readable stack-trace string from the point of view of the
    caller.
    The string is allocated with `malloc` and needs to be freed with `free`
*/
char* b_stacktrace_get_string(void);
}
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

namespace ylt::util {
typedef struct print_buf {
    char* buf;
    int pos;
    int size;
} print_buf;

inline print_buf buf_init(void) {
    print_buf ret = {
        (char*) malloc(1024),
        0,
        1024
    };
    return ret;
}

inline void buf_printf(print_buf* b, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    const int len = vsnprintf(NULL, 0, fmt, args) + 1;
    va_end(args);

    const int new_end = b->pos + len;

    if (new_end > b->size) {
        while (new_end > b->size) b->size *= 2;
        b->buf = (char*)realloc(b->buf, b->size);
    }

    va_start(args, fmt);
    b->pos += vsnprintf(b->buf + b->pos, len, fmt, args);
    va_end(args);
}

inline char* b_stacktrace_get_string(void) {
    b_stacktrace_handle h = b_stacktrace_get();
    char* ret = b_stacktrace_to_string(h);
    free(h);
    return ret;
}
}
#define B_STACKTRACE_MAX_DEPTH 1024

#if defined(_WIN32)

#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <TlHelp32.h>
#include <DbgHelp.h>

#pragma comment(lib, "DbgHelp.lib")

#define B_STACKTRACE_ERROR_FLAG ((DWORD64)1 << 63)

namespace ylt::util {
typedef struct b_stacktrace_entry {
    DWORD64 AddrPC_Offset;
    DWORD64 AddrReturn_Offset;
} b_stacktrace_entry;

inline int SymInitialize_called = 0;

inline b_stacktrace_handle b_stacktrace_get(void) {
    HANDLE process = GetCurrentProcess();
    HANDLE thread = GetCurrentThread();
    CONTEXT context;
    STACKFRAME64 frame; /* in/out stackframe */
    DWORD imageType;
    b_stacktrace_entry* ret = (b_stacktrace_entry*)malloc(B_STACKTRACE_MAX_DEPTH * sizeof(b_stacktrace_entry));
    int i = 0;

    if (!SymInitialize_called) {
        SymInitialize(process, NULL, TRUE);
        SymInitialize_called = 1;
    }

    RtlCaptureContext(&context);

    memset(&frame, 0, sizeof(frame));
#ifdef _M_IX86
    imageType = IMAGE_FILE_MACHINE_I386;
    frame.AddrPC.Offset = context.Eip;
    frame.AddrPC.Mode = AddrModeFlat;
    frame.AddrFrame.Offset = context.Ebp;
    frame.AddrFrame.Mode = AddrModeFlat;
    frame.AddrStack.Offset = context.Esp;
    frame.AddrStack.Mode = AddrModeFlat;
#elif _M_X64
    imageType = IMAGE_FILE_MACHINE_AMD64;
    frame.AddrPC.Offset = context.Rip;
    frame.AddrPC.Mode = AddrModeFlat;
    frame.AddrFrame.Offset = context.Rsp;
    frame.AddrFrame.Mode = AddrModeFlat;
    frame.AddrStack.Offset = context.Rsp;
    frame.AddrStack.Mode = AddrModeFlat;
#elif _M_IA64
    imageType = IMAGE_FILE_MACHINE_IA64;
    frame.AddrPC.Offset = context.StIIP;
    frame.AddrPC.Mode = AddrModeFlat;
    frame.AddrFrame.Offset = context.IntSp;
    frame.AddrFrame.Mode = AddrModeFlat;
    frame.AddrBStore.Offset = context.RsBSP;
    frame.AddrBStore.Mode = AddrModeFlat;
    frame.AddrStack.Offset = context.IntSp;
    frame.AddrStack.Mode = AddrModeFlat;
#else
    #error "Platform not supported!"
#endif

    while (1) {
        b_stacktrace_entry* cur = ret + i++;
        if (i == B_STACKTRACE_MAX_DEPTH) {
            cur->AddrPC_Offset = 0;
            cur->AddrReturn_Offset = 0;
            break;
        }

        if (!StackWalk64(imageType, process, thread, &frame, &context, NULL, SymFunctionTableAccess64, SymGetModuleBase64, NULL)) {
            cur->AddrPC_Offset = frame.AddrPC.Offset;
            cur->AddrReturn_Offset = B_STACKTRACE_ERROR_FLAG; /* mark error */
            cur->AddrReturn_Offset |= GetLastError();
            break;
        }

        cur->AddrPC_Offset = frame.AddrPC.Offset;
        cur->AddrReturn_Offset = frame.AddrReturn.Offset;

        if (frame.AddrReturn.Offset == 0) {
            break;
        }
    }

    return (b_stacktrace_handle)(ret);
}

inline int b_stacktrace_depth(b_stacktrace_handle h) {
    const b_stacktrace_entry* entries = (b_stacktrace_entry*)h;
    int i = 0;
    while (1) {
        const b_stacktrace_entry* cur = entries + i++;
        if (cur->AddrReturn_Offset == 0) {
            break;
        }
    }
    return i;
}

inline char* b_stacktrace_to_string(b_stacktrace_handle h) {
    const b_stacktrace_entry* entries = (b_stacktrace_entry*)h;
    int i = 0;
    HANDLE process = GetCurrentProcess();
    print_buf out = buf_init();
    IMAGEHLP_SYMBOL64* symbol = (IMAGEHLP_SYMBOL64*)malloc(sizeof(IMAGEHLP_SYMBOL64) + 1024);
    symbol->SizeOfStruct = sizeof(IMAGEHLP_SYMBOL64);
    symbol->MaxNameLength = 1024;

    while (1) {
        IMAGEHLP_LINE64 lineData;
        DWORD lineOffset = 0;
        DWORD64 symOffset = 0;
        const b_stacktrace_entry* cur = entries + i++;

        if (cur->AddrReturn_Offset & B_STACKTRACE_ERROR_FLAG) {
            DWORD error = cur->AddrReturn_Offset & 0xFFFFFFFF;
            buf_printf(&out, "StackWalk64 error: %d @ %p\n", error, cur->AddrPC_Offset);
            break;
        }

        if (cur->AddrPC_Offset == cur->AddrReturn_Offset) {
            buf_printf(&out, "Stack overflow @ %p\n", cur->AddrPC_Offset);
            break;
        }

        SymGetLineFromAddr64(process, cur->AddrPC_Offset, &lineOffset, &lineData);
        buf_printf(&out, "%s(%d): ", lineData.FileName, lineData.LineNumber);

        if (SymGetSymFromAddr64(process, cur->AddrPC_Offset, &symOffset, symbol)) {
            buf_printf(&out, "%s\n", symbol->Name);
        }
        else {
            buf_printf(&out, " Unkown symbol @ %p\n", cur->AddrPC_Offset);
        }

        if (cur->AddrReturn_Offset == 0) {
            break;
        }
    }

    free(symbol);
    return out.buf;
}

#undef max 
#undef min
}
#elif defined(__APPLE__)

#include <execinfo.h>
#include <unistd.h>
#include <dlfcn.h>

namespace ylt::util {
typedef struct b_stacktrace {
    void* trace[B_STACKTRACE_MAX_DEPTH];
    int trace_size;
} b_stacktrace;

inline b_stacktrace_handle b_stacktrace_get(void) {
    b_stacktrace* ret = (b_stacktrace*)malloc(sizeof(b_stacktrace));
    ret->trace_size = backtrace(ret->trace, B_STACKTRACE_MAX_DEPTH);
    return (b_stacktrace_handle)(ret);
}

inline int b_stacktrace_depth(b_stacktrace_handle h) {
    const b_stacktrace* stacktrace = (b_stacktrace*)h;
    return stacktrace->trace_size;
}

inline char* b_stacktrace_to_string(b_stacktrace_handle h) {
    const b_stacktrace* stacktrace = (b_stacktrace*)h;
    char** messages = backtrace_symbols(stacktrace->trace, stacktrace->trace_size);
    print_buf out = buf_init();
    *out.buf = 0;

    for (int i = 0; i < stacktrace->trace_size; ++i) {
        buf_printf(&out, "%s\n", messages[i]);
    }

    free(messages);
    return out.buf;
}
}


#elif defined(__linux__)

#include <execinfo.h>
#include <ucontext.h>
#include <unistd.h>
#include <dlfcn.h>
#include <string.h>

namespace ylt::util {
typedef struct b_stacktrace {
    void* trace[B_STACKTRACE_MAX_DEPTH];
    int trace_size;
} b_stacktrace;

inline b_stacktrace_handle b_stacktrace_get(void) {
    b_stacktrace* ret = (b_stacktrace*)malloc(sizeof(b_stacktrace));
    ret->trace_size = backtrace(ret->trace, B_STACKTRACE_MAX_DEPTH);
    return (b_stacktrace_handle)(ret);
}

inline int b_stacktrace_depth(b_stacktrace_handle h) {
    const b_stacktrace* stacktrace = (b_stacktrace*)h;
    return stacktrace->trace_size;
}

inline char* b_stacktrace_to_string(b_stacktrace_handle h) {
    const b_stacktrace* stacktrace = (b_stacktrace*)h;
    char** messages = backtrace_symbols(stacktrace->trace, stacktrace->trace_size);
    print_buf out = buf_init();
    int i;

    for (i = 0; i < stacktrace->trace_size; ++i) {
        void* tracei = stacktrace->trace[i];
        char* msg = messages[i];

        /* calculate load offset */
        Dl_info info;
        // if stacktrace dont work well in linux share library, please define YLT_ENABLE_LD and link ld
#ifdef YLT_ENABLE_LD
        dladdr(tracei, &info);
#endif
        //if (info.dli_fbase == (void*)0x400000) {
            /* address from executable, so don't offset */
            info.dli_fbase = NULL;
        //}

        while (*msg && *msg != '(') ++msg;
        *msg = 0;

        {
            char cmd[1024];
            char line[2048];

            FILE* fp;
            snprintf(cmd, 1024, "addr2line -e %s -f -C -p %p 2>/dev/null", messages[i], (void*)((char*)tracei - (char*)info.dli_fbase));
            fp = popen(cmd, "r");
            if (!fp) {
                buf_printf(&out, "Failed to generate trace further...\n");
                break;
            }

            while (fgets(line, sizeof(line), fp)) {
                buf_printf(&out, "%s: ", messages[i]);
                if (strstr(line, "?? ")) {
                    /* just output address if nothing can be found */
                    buf_printf(&out, "%p\n", tracei);
                }
                else {
                    buf_printf(&out, "%s", line);
                }
            }

            pclose(fp);
        }
    }

    free(messages);
    return out.buf;
}
}
#else
/* noop implementation */

namespace ylt::util {
inline char* b_stacktrace_get_string(void) {
    print_buf out = buf_init();
    buf_printf("b_stacktrace: unsupported platform\n");
    return out.buf;
}
}
#endif /* platform */

#endif /* B_STACKTRACE_INCLUDED */
