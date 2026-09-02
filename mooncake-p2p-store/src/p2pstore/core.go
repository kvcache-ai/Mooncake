// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package p2pstore

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
)

// When the data size larger than MAX_CHUNK_SIZE bytes, we split them into multiple buffers and registered separately.
// Warning: Memory registration is a SLOW operation.
// MAX_CHUNK_SIZE must be an integer power of 2.
// maxShardSize must be an integer power of 2, divisible by MAX_CHUNK_SIZE.
// i.e. MAX_CHUNK_SIZE % maxShardSize == 0
const MAX_CHUNK_SIZE uint64 = 4096 * 1024 * 1024
const METADATA_KEY_PREFIX string = "mooncake/checkpoint/"

type P2PStore struct {
	metadataConnString string
	localServerName    string
	catalog            *Catalog
	memory             *RegisteredMemory
	metadata           *Metadata
	transfer           *TransferEngine
}

const DEFAULT_PORT int = 12345

func parseServerName(serverName string) (host string, port int) {
	host, portStr, err := net.SplitHostPort(serverName)
	if err != nil {
		log.Println("use default port for server name:", serverName)
		return serverName, DEFAULT_PORT
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		port = DEFAULT_PORT
	}
	return host, port
}

func NewP2PStore(metadataConnString string, localServerName string, nicPriorityMatrix string) (*P2PStore, error) {
	metadata, err := NewMetadata(metadataConnString, METADATA_KEY_PREFIX)
	if err != nil {
		return nil, err
	}

	localIpAddressCStr, rpcPort := parseServerName(localServerName)
	transfer, err := NewTransferEngine(metadataConnString, localServerName, localIpAddressCStr, rpcPort)
	if err != nil {
		metadata.Close()
		return nil, err
	}

	if len(nicPriorityMatrix) == 0 {
		err = transfer.installTransport("tcp", nicPriorityMatrix)
	} else {
		err = transfer.installTransport("rdma", nicPriorityMatrix)
	}
	if err != nil {
		metadata.Close()
		return nil, err
	}

	store := &P2PStore{
		metadataConnString: metadataConnString,
		localServerName:    localServerName,
		catalog:            NewCatalog(),
		memory:             NewRegisteredMemory(transfer, MAX_CHUNK_SIZE),
		metadata:           metadata,
		transfer:           transfer,
	}
	return store, nil
}

func (store *P2PStore) Close() error {
	var retErr error = nil
	store.transfer.Close()
	err := store.metadata.Close()
	if err != nil {
		retErr = err
	}
	return retErr
}

// GetLocalServerName returns the local server name (ip:port) that this
// P2PStore instance is registered under. In P2P handshake mode the RPC
// port is allocated dynamically, so callers must use this method to
// discover the actual address after initialization.
func (store *P2PStore) GetLocalServerName() (string, error) {
	return store.transfer.GetLocalIpAndPort()
}

type Buffer struct {
	addr uintptr
	size uint64
}

func (store *P2PStore) unregisterBuffers(bufferList []Buffer, maxShardSize uint64) {
	for _, buffer := range bufferList {
		err := store.memory.Remove(buffer.addr, buffer.size, maxShardSize)
		if err != nil {
			log.Println("cascading error:", err)
		}
	}
}

func (store *P2PStore) Register(ctx context.Context,
	name string,
	addrList []uintptr,
	sizeList []uint64,
	maxShardSize uint64,
	location string,
	forceCreate bool) error {
	if len(addrList) != len(sizeList) || len(addrList) == 0 {
		return ErrInvalidArgument
	}

	if store.catalog.Contains(name) {
		return ErrPayloadOpened
	}

	var payload Payload
	var bufferList []Buffer
	payload.Name = name
	payload.MaxShardSize = maxShardSize
	payload.SizeList = sizeList
	payload.Size = 0
	for i := 0; i < len(addrList); i++ {
		addr, size := addrList[i], sizeList[i]
		payload.Size += size
		err := store.memory.Add(addr, size, maxShardSize, location)
		if err != nil {
			store.unregisterBuffers(bufferList, maxShardSize)
			return err
		}
		bufferList = append(bufferList, Buffer{addr: addr, size: size})
		var offset uint64 = 0
		for ; offset < size; offset += maxShardSize {
			shardLength := maxShardSize
			if shardLength > size-offset {
				shardLength = size - offset
			}
			goldLocation := Location{
				SegmentName: store.localServerName,
				Offset:      uint64(addr) + offset,
			}
			shard := Shard{
				Length:      shardLength,
				Gold:        []Location{goldLocation},
				ReplicaList: nil,
			}
			payload.Shards = append(payload.Shards, shard)
		}
	}

	var err error
	if forceCreate {
		err = store.metadata.Put(ctx, name, &payload)
	} else {
		err = store.metadata.Create(ctx, name, &payload)
	}

	if err != nil {
		store.unregisterBuffers(bufferList, maxShardSize)
		return err
	}

	params := CatalogParams{
		IsGold:       true,
		AddrList:     addrList,
		SizeList:     sizeList,
		MaxShardSize: maxShardSize,
	}
	store.catalog.Add(name, params)
	return nil
}

func (store *P2PStore) Unregister(ctx context.Context, name string) error {
	params, exist := store.catalog.Get(name)
	if !exist {
		return ErrPayloadNotOpened
	}

	for {
		payload, revision, err := store.metadata.Get(ctx, name)
		if err != nil {
			return err
		}

		if payload == nil {
			return ErrPayloadNotFound
		}

		for index := range payload.Shards {
			payload.Shards[index].Gold = nil
		}

		success, err := store.metadata.Update(ctx, name, payload, revision)
		if err != nil {
			return err
		}

		if success {
			store.catalog.Remove(name)
			for index := 0; index < len(params.AddrList); index++ {
				innerErr := store.memory.Remove(params.AddrList[index], params.SizeList[index], params.MaxShardSize)
				if innerErr != nil {
					log.Println("cascading error:", innerErr)
				}
			}
			return nil
		}
	}
}

type PayloadInfo struct {
	Name         string
	MaxShardSize uint64
	TotalSize    uint64
	SizeList     []uint64
}

func (store *P2PStore) List(ctx context.Context, namePrefix string) ([]PayloadInfo, error) {
	var result []PayloadInfo
	payloadList, err := store.metadata.List(ctx, namePrefix)
	if err != nil {
		return result, err
	}
	for _, payload := range payloadList {
		payloadInfo := PayloadInfo{
			Name:         payload.Name,
			TotalSize:    payload.Size,
			MaxShardSize: payload.MaxShardSize,
			SizeList:     payload.SizeList,
		}
		result = append(result, payloadInfo)
	}
	return result, nil
}

func (store *P2PStore) doGetReplica(ctx context.Context, payload *Payload, addrList []uintptr, sizeList []uint64) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	var offset uint64 = 0
	taskID := 0
	maxShardSize := payload.MaxShardSize

	for i := 0; i < len(addrList); i++ {
		addr, size := addrList[i], sizeList[i]
		err := store.memory.Add(addr, size, maxShardSize, "cpu:0")
		if err != nil {
			return err
		}
		offset = 0
		for ; offset < size; offset += maxShardSize {
			source := addr + uintptr(offset)
			shard := payload.Shards[taskID]
			taskID++
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := store.performTransfer(ctx, source, shard)
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			}()
		}
	}

	wg.Wait()
	close(errChan)
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}

func contains(slice []Location, value Location) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

func isSubsetOf(old *Payload, new *Payload) bool {
	if len(old.Shards) != len(new.Shards) {
		return false
	}
	for i := 0; i < len(old.Shards); i += 1 {
		for _, value := range old.Shards[i].Gold {
			if !contains(new.Shards[i].Gold, value) {
				return false
			}
		}
		for _, value := range old.Shards[i].ReplicaList {
			if !contains(new.Shards[i].ReplicaList, value) {
				return false
			}
		}
	}
	return true
}

func (store *P2PStore) GetReplica(ctx context.Context, name string, addrList []uintptr, sizeList []uint64) error {
	if len(addrList) != len(sizeList) || len(addrList) == 0 {
		return ErrInvalidArgument
	}

	if store.catalog.Contains(name) {
		return ErrPayloadOpened
	}

	payload, revision, err := store.metadata.Get(ctx, name)
	if err != nil {
		return err
	}
	if payload == nil {
		return ErrPayloadNotFound
	}
	for {
		err = store.doGetReplica(ctx, payload, addrList, sizeList)
		if err != nil {
			return err
		}
		newPayload, recheckRevision, err := store.metadata.Get(ctx, name)
		if err != nil {
			return err
		}
		if revision == recheckRevision {
			break
		}
		if isSubsetOf(payload, newPayload) {
			break
		}
	}
	return store.updatePayloadMetadata(ctx, name, addrList, sizeList, payload, revision)
}

func (store *P2PStore) performTransfer(ctx context.Context, source uintptr, shard Shard) error {
	retryCount := 0
	maxRetryCount := max(3, shard.Count())
	for retryCount < maxRetryCount {
		location := shard.GetLocation(retryCount)
		if location == nil {
			break
		}

		status, err := performTransferOnce(ctx, store.transfer, source, shard.Length, location, retryCount == 0)
		if err != nil {
			return err
		}

		if status == STATUS_COMPLETED {
			return nil
		}

		retryCount++
	}

	return ErrTooManyRetries
}

// batchTransport is the subset of TransferEngine a single transfer attempt
// needs. It lets the batch lifecycle be exercised without the native engine.
type batchTransport interface {
	allocateBatchID(batchSize int) (BatchID, error)
	openSegment(segmentName string, useCache bool) (int64, error)
	submitTransfer(batchID BatchID, requests []TransferRequest) error
	getTransferStatus(batchID BatchID, taskID int) (int, uint64, error)
	freeBatchID(batchID BatchID) error
}

func isTransferInFlight(status int) bool {
	return status == STATUS_WAITING || status == STATUS_PENDING
}

// performTransferOnce runs a single transfer attempt against the given
// location. The allocated batch ID is released on every return path.
//
// The engine refuses to free a batch while any of its tasks is still in
// flight (BatchBusy), so on context cancellation the task is drained to a
// terminal state before the deferred free runs. Cancellation latency is
// therefore bounded by the engine's own transfer timeout.
func performTransferOnce(ctx context.Context, transport batchTransport, source uintptr, length uint64, location *Location, useCache bool) (status int, err error) {
	batchID, err := transport.allocateBatchID(1)
	if err != nil {
		return STATUS_FAILED, err
	}
	defer func() {
		freeErr := transport.freeBatchID(batchID)
		if freeErr != nil {
			log.Println("cascading error: failed to free batch ID:", freeErr)
			if err == nil {
				err = freeErr
			}
		}
	}()

	targetID, err := transport.openSegment(location.SegmentName, useCache)
	if err != nil {
		return STATUS_FAILED, err
	}

	request := TransferRequest{
		Opcode:       OPCODE_READ,
		Source:       uint64(source),
		TargetID:     targetID,
		TargetOffset: location.Offset,
		Length:       length,
	}

	err = transport.submitTransfer(batchID, []TransferRequest{request})
	if err != nil {
		return STATUS_FAILED, err
	}

	for isTransferInFlight(status) {
		select {
		case <-ctx.Done():
			if _, drainErr := drainTransfer(transport, batchID); drainErr != nil {
				return STATUS_FAILED, drainErr
			}
			return STATUS_FAILED, ctx.Err()
		default:
			status, _, err = transport.getTransferStatus(batchID, 0)
			if err != nil {
				return STATUS_FAILED, err
			}
		}
	}

	return status, nil
}

// drainTransfer polls the batch until its task reaches a terminal state so
// the batch can be freed.
func drainTransfer(transport batchTransport, batchID BatchID) (int, error) {
	status := STATUS_WAITING
	for isTransferInFlight(status) {
		var err error
		status, _, err = transport.getTransferStatus(batchID, 0)
		if err != nil {
			return STATUS_FAILED, err
		}
	}
	return status, nil
}

func (store *P2PStore) updatePayloadMetadata(ctx context.Context, name string, addrList []uintptr, sizeList []uint64, payload *Payload, revision int64) error {
	for {
		taskID := 0
		maxShardSize := payload.MaxShardSize
		for i := 0; i < len(addrList); i++ {
			addr, size := addrList[i], sizeList[i]
			var offset uint64 = 0
			for ; offset < size; offset += maxShardSize {
				replicaLocation := Location{
					SegmentName: store.localServerName,
					Offset:      uint64(addr) + offset,
				}
				payload.Shards[taskID].ReplicaList = append(payload.Shards[taskID].ReplicaList, replicaLocation)
				taskID++
			}
		}

		success, err := store.metadata.Update(ctx, name, payload, revision)
		if err != nil {
			return err
		}
		if success {
			params := CatalogParams{
				IsGold:       false,
				AddrList:     addrList,
				SizeList:     sizeList,
				MaxShardSize: maxShardSize,
			}
			store.catalog.Add(name, params)
			return nil
		} else {
			payload, revision, err = store.metadata.Get(ctx, name)
			if err != nil {
				return err
			}

			if payload == nil {
				return ErrPayloadNotFound
			}
		}
	}
}

func (store *P2PStore) DeleteReplica(ctx context.Context, name string) error {
	params, exist := store.catalog.Get(name)
	if !exist {
		return ErrPayloadNotOpened
	}

	for {
		payload, revision, err := store.metadata.Get(ctx, name)
		if err != nil {
			return err
		}
		if payload == nil {
			return ErrPayloadNotFound
		}

		for idx, shard := range payload.Shards {
			var newReplicaList []Location
			for _, replica := range shard.ReplicaList {
				if replica.SegmentName != store.localServerName {
					newReplicaList = append(newReplicaList, replica)
				}
			}
			payload.Shards[idx].ReplicaList = newReplicaList
		}
		success, err := store.metadata.Update(ctx, name, payload, revision)
		if err != nil {
			return err
		}
		if success {
			store.catalog.Remove(name)
			for index := 0; index < len(params.AddrList); index++ {
				innerErr := store.memory.Remove(params.AddrList[index], params.SizeList[index], params.MaxShardSize)
				if innerErr != nil {
					log.Println("cascading error:", innerErr)
				}
			}
			return nil
		}
	}
}
