#!/bin/bash
go mod init github.com/kvcache-ai/Mooncake/mooncake-common/etcd
go mod tidy
go build -o libetcd_wrapper.so -buildmode=c-shared etcd_wrapper.go
