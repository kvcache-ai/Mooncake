package main

import(
  "fmt"
  "flag"
  "math/rand/v2"
)

/*
#cgo CXXFLAGS: -std=c++20
#cgo CFLAGS: -I../coro_rpc_lib
#cgo LDFLAGS: -L../coro_rpc_lib -lcoro_rpc -lm -lstdc++
#include "coro_rpc.h"
*/
import "C"
import "unsafe"

const (
	NONE int = 0
	TRACE int = 1
	DEBUG int = 2
  INFO int = 3
  WARNING int = 4
  ERROR int = 5
  CRITICAL int = 6
  FATAL int = 7
)

var g_resp_buf []byte

//export load_service
func load_service(ctx unsafe.Pointer, req_id C.uint64_t) {
  // fmt.Println("load req_id", req_id);
  // go business...
  if(req_id == 2) {
    var err_msg = C.CString("error from server")
    C.response_error(ctx, 1001, err_msg)
    C.free(unsafe.Pointer(err_msg))
    return;
  }

  // response rpc result
  var promise = C.response_msg(ctx, ((*C.char)(unsafe.Pointer(&g_resp_buf[0]))), C.uint64_t(len(g_resp_buf)));
  go func(p unsafe.Pointer, buf []byte) {
    result := C.wait_response_finish(p);
    if(result.ec > 0) {
      fmt.Println(result.ec, C.GoString(result.err_msg))
      C.free(unsafe.Pointer(result.err_msg))
    }

    //now release buf
  }(promise, g_resp_buf)
}

func test_client(host string, len int) {
  peer := C.CString(host)
  conf := C.client_config{connect_timeout_sec:15, req_timeout_sec:30, local_ip:C.get_first_local_ip(), enable_ib:C.bool(false)}
  pool := C.create_client_pool(peer, conf)

  outbuf := make([]byte, len)
  for i := 0; i < 3; i++ {
    req_id := uint64(i)
    result := C.load(pool, C.uint64_t(req_id), ((*C.char)(unsafe.Pointer(&outbuf[0]))), C.uint64_t(len))
    if(result.ec > 0) {
      fmt.Println("error: ", result.ec, C.GoString(result.err_msg))
      C.free(unsafe.Pointer(result.err_msg))
    }else {
      fmt.Println("result: ", string(outbuf[0:result.len]), "len", result.len)
    }
  }

  C.free_client_pool(pool)
  C.free(unsafe.Pointer(peer))
}

func init_response_buf() {
  const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  
  for i := 0; i < len(g_resp_buf); i++ {
    g_resp_buf[i] = letterBytes[rand.IntN(len(letterBytes))]
  }
}

var addr = flag.String("a", "0.0.0.0:8806", "ip:port")
var thds = flag.Int("t", 96, "thread number")
var resp_len = flag.Int("l", 256, "response data len")//134217728

func main()  {
  C.init_rpc_log(nil, C.int(INFO), 50000, 5, C.bool(false))

  flag.Parse()
  fmt.Println(*addr, "thread number,", *thds, ", response data len:", *resp_len)
  g_resp_buf = make([]byte, *resp_len)

	conf := C.server_config{
		parallel:           4,
		enable_ib:          C.bool(false),
	}
  var server = C.start_rpc_server(C.CString(*addr), conf)
  init_response_buf()

  // var quit1 string
  // fmt.Scanln(&quit1)

  test_client(*addr, *resp_len)

  C.stop_rpc_server(server)
  C.flush_rpc_log()
  fmt.Println("quit")
}