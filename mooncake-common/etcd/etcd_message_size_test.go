package main

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func preserveStoreMessageConfig(t *testing.T) {
	t.Helper()
	previous := atomic.LoadInt64(&storeMaxSendMessageBytes)
	previousClient := storeClient
	storeClient = nil
	t.Cleanup(func() {
		storeClient = previousClient
		atomic.StoreInt64(&storeMaxSendMessageBytes, previous)
	})
}

func TestStoreSendLimitValidation(t *testing.T) {
	preserveStoreMessageConfig(t)
	for _, invalid := range []int32{0, -1} {
		if err := configureStoreMaxSendMessageBytes(invalid); err == nil {
			t.Fatalf("accepted invalid limit %d", invalid)
		}
	}
	const raised = 16 * 1024 * 1024
	if err := configureStoreMaxSendMessageBytes(raised); err != nil {
		t.Fatal(err)
	}
	storeClient = &clientv3.Client{}
	if err := configureStoreMaxSendMessageBytes(raised); err != nil {
		t.Fatalf("repeating the current configuration failed: %v", err)
	}
	if err := configureStoreMaxSendMessageBytes(2 * 1024 * 1024); err == nil {
		t.Fatal("changed the limit after the store client was initialized")
	}
	if got := atomic.LoadInt64(&storeMaxSendMessageBytes); got != raised {
		t.Fatalf("rejected update changed the configured limit to %d", got)
	}
}

type messageSizeKVServer struct {
	pb.UnimplementedKVServer
	calls atomic.Int32
}

func (s *messageSizeKVServer) Txn(context.Context, *pb.TxnRequest) (*pb.TxnResponse, error) {
	s.calls.Add(1)
	return &pb.TxnResponse{Header: &pb.ResponseHeader{Revision: 1}, Succeeded: true}, nil
}

func TestStoreSendLimitOnRealGRPCTransactions(t *testing.T) {
	preserveStoreMessageConfig(t)
	for _, name := range []string{mooncakeEtcdCACertEnv, mooncakeEtcdClientCertEnv,
		mooncakeEtcdClientKeyEnv, mooncakeEtcdServerNameEnv} {
		t.Setenv(name, "")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4 * 1024 * 1024))
	service := &messageSizeKVServer{}
	pb.RegisterKVServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	transact := func(bytes int) error {
		config, err := newStoreClientConfig([]string{listener.Addr().String()})
		if err != nil {
			t.Fatal(err)
		}
		client, err := clientv3.New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Txn(ctx).Then(clientv3.OpPut("batch", strings.Repeat("x", bytes))).Commit()
		return err
	}

	if err := configureStoreMaxSendMessageBytes(2 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	if err := transact(3 * 1024 * 1024); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("default send limit: expected ResourceExhausted, got %v", err)
	}
	if got := service.calls.Load(); got != 0 {
		t.Fatalf("oversized request reached the server %d times", got)
	}
	if err := configureStoreMaxSendMessageBytes(16 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	// Each transaction constructs a fresh client, as reset/keepalive paths do.
	for i := 0; i < 2; i++ {
		if err := transact(3 * 1024 * 1024); err != nil {
			t.Fatalf("raised limit was not preserved across client construction: %v", err)
		}
	}
	if got := service.calls.Load(); got != 2 {
		t.Fatalf("expected two accepted transactions, got %d", got)
	}
	if err := transact(5 * 1024 * 1024); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("raising the client limit bypassed the server's limit: %v", err)
	}
}
