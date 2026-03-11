#!/bin/bash
go mod init github.com/kvcache-ai/Mooncake/mooncake-common/kubernetes
go mod tidy
go build -o libk8s_wrapper.so -buildmode=c-shared utils.go kubernetes_wrapper.go
