/*
 * Copyright (c) 2015 Дмитрий Бахвалов (Dmitry Bakhvalov)
 *
 * Permission for license update:
 *   https://github.com/redis/hiredis/issues/1271#issuecomment-2258225227
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __HIREDIS_MACOSX_H__
#define __HIREDIS_MACOSX_H__

#include <CoreFoundation/CoreFoundation.h>

#include "../hiredis.h"
#include "../async.h"

typedef struct {
    redisAsyncContext *context;
    CFSocketRef socketRef;
    CFRunLoopSourceRef sourceRef;
} RedisRunLoop;

static int freeRedisRunLoop(RedisRunLoop* redisRunLoop) {
    if( redisRunLoop != NULL ) {
        if( redisRunLoop->sourceRef != NULL ) {
            CFRunLoopSourceInvalidate(redisRunLoop->sourceRef);
            CFRelease(redisRunLoop->sourceRef);
        }
        if( redisRunLoop->socketRef != NULL ) {
            CFSocketInvalidate(redisRunLoop->socketRef);
            CFRelease(redisRunLoop->socketRef);
        }
        hi_free(redisRunLoop);
    }
    return REDIS_ERR;
}

static void redisMacOSAddRead(void *privdata) {
    RedisRunLoop *redisRunLoop = (RedisRunLoop*)privdata;
    CFSocketEnableCallBacks(redisRunLoop->socketRef, kCFSocketReadCallBack);
}

static void redisMacOSDelRead(void *privdata) {
    RedisRunLoop *redisRunLoop = (RedisRunLoop*)privdata;
    CFSocketDisableCallBacks(redisRunLoop->socketRef, kCFSocketReadCallBack);
}

static void redisMacOSAddWrite(void *privdata) {
    RedisRunLoop *redisRunLoop = (RedisRunLoop*)privdata;
    CFSocketEnableCallBacks(redisRunLoop->socketRef, kCFSocketWriteCallBack);
}

static void redisMacOSDelWrite(void *privdata) {
    RedisRunLoop *redisRunLoop = (RedisRunLoop*)privdata;
    CFSocketDisableCallBacks(redisRunLoop->socketRef, kCFSocketWriteCallBack);
}

static void redisMacOSCleanup(void *privdata) {
    RedisRunLoop *redisRunLoop = (RedisRunLoop*)privdata;
    freeRedisRunLoop(redisRunLoop);
}

static void redisMacOSAsyncCallback(CFSocketRef __unused s, CFSocketCallBackType callbackType, CFDataRef __unused address, const void __unused *data, void *info) {
    redisAsyncContext* context = (redisAsyncContext*) info;

    switch (callbackType) {
        case kCFSocketReadCallBack:
            redisAsyncHandleRead(context);
            break;

        case kCFSocketWriteCallBack:
            redisAsyncHandleWrite(context);
            break;

        default:
            break;
    }
}

static int redisMacOSAttach(redisAsyncContext *redisAsyncCtx, CFRunLoopRef runLoop) {
    redisContext *redisCtx = &(redisAsyncCtx->c);

    /* Nothing should be attached when something is already attached */
    if( redisAsyncCtx->ev.data != NULL ) return REDIS_ERR;

    RedisRunLoop* redisRunLoop = (RedisRunLoop*) hi_calloc(1, sizeof(RedisRunLoop));
    if (redisRunLoop == NULL)
        return REDIS_ERR;

    /* Setup redis stuff */
    redisRunLoop->context = redisAsyncCtx;

    redisAsyncCtx->ev.addRead  = redisMacOSAddRead;
    redisAsyncCtx->ev.delRead  = redisMacOSDelRead;
    redisAsyncCtx->ev.addWrite = redisMacOSAddWrite;
    redisAsyncCtx->ev.delWrite = redisMacOSDelWrite;
    redisAsyncCtx->ev.cleanup  = redisMacOSCleanup;
    redisAsyncCtx->ev.data     = redisRunLoop;

    /* Initialize and install read/write events */
    CFSocketContext socketCtx = { 0, redisAsyncCtx, NULL, NULL, NULL };

    redisRunLoop->socketRef = CFSocketCreateWithNative(NULL, redisCtx->fd,
                                                       kCFSocketReadCallBack | kCFSocketWriteCallBack,
                                                       redisMacOSAsyncCallback,
                                                       &socketCtx);
    if( !redisRunLoop->socketRef ) return freeRedisRunLoop(redisRunLoop);

    redisRunLoop->sourceRef = CFSocketCreateRunLoopSource(NULL, redisRunLoop->socketRef, 0);
    if( !redisRunLoop->sourceRef ) return freeRedisRunLoop(redisRunLoop);

    CFRunLoopAddSource(runLoop, redisRunLoop->sourceRef, kCFRunLoopDefaultMode);

    return REDIS_OK;
}

#endif

