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
	"sync"
)

// When the data size larger than MAX_CHUNK_SIZE bytes, we split them into multiple buffers and registered seperately.
// Warning: Memory registration is a SLOW operation.
// MAX_CHUNK_SIZE must be an integer power of 2.
// maxShardSize must be an integer power of 2, divisible by MAX_CHUNK_SIZE.
// i.e. MAX_CHUNK_SIZE % maxShardSize == 0
const MAX_CHUNK_SIZE uint64 = 4096 * 1024 * 1024
const METADATA_KEY_PREFIX string = "mooncake/checkpoint/"

type P2PStore struct {
	metadataUri      string
	localSegmentName string
	catalog          *Catalog
	memory           *RegisteredMemory
	metadata         *Metadata
	transfer         *TransferEngine
}

func NewP2PStore(metadataUri string, localSegmentName string, nicPriorityMatrix string) (*P2PStore, error) {
	metadata, err := NewMetadata(metadataUri, METADATA_KEY_PREFIX)
	if err != nil {
		return nil, err
	}

	transferEngine, err := NewTransferEngine(metadataUri, localSegmentName, nicPriorityMatrix)
	if err != nil {
		innerErr := metadata.Close()
		if innerErr != nil {
			log.Println("cascading error:", innerErr)
		}
		return nil, err
	}

	store := &P2PStore{
		metadataUri:      metadataUri,
		localSegmentName: localSegmentName,
		catalog:          NewCatalog(),
		memory:           NewRegisteredMemory(transferEngine, MAX_CHUNK_SIZE),
		metadata:         metadata,
		transfer:         transferEngine,
	}
	return store, nil
}

func (store *P2PStore) Close() error {
	var retErr error = nil
	err := store.transfer.Close()
	if err != nil {
		retErr = err
	}
	err = store.metadata.Close()
	if err != nil {
		retErr = err
	}
	return retErr
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

func (store *P2PStore) Register(ctx context.Context, name string, addrList []uintptr, sizeList []uint64, maxShardSize uint64, location string) error {
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
				SegmentName: store.localSegmentName,
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

	err := store.metadata.Put(ctx, name, &payload)
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
	Name         string   // Full name of checkpoint file
	MaxShardSize uint64   // 
	TotalSize    uint64   // 
	SizeList     []uint64 // 
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

// Get replica for same name multiple times in one P2P store will return ErrPayloadOpened
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

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	var offset uint64 = 0
	taskID := 0
	maxShardSize := payload.MaxShardSize

	_ = store.transfer.syncSegmentCache()

	for i := 0; i < len(addrList); i++ {
		addr, size := addrList[i], sizeList[i]
		err := store.memory.Add(addr, size, maxShardSize, "cpu:0")
		if err != nil {
			return err
		}
		for ; offset < size; offset += maxShardSize {
			source := addr + uintptr(offset)
			shard := payload.Shards[taskID]
			taskID++
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = store.performTransfer(source, shard)
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

	return store.updatePayloadMetadata(ctx, name, addrList, sizeList, payload, revision)
}

func (store *P2PStore) performTransfer(source uintptr, shard Shard) error {
	const MAX_RETRY_COUNT int = 8
	retryCount := 0

	for retryCount < MAX_RETRY_COUNT {
		batchID, err := store.transfer.allocateBatchID(1)
		if err != nil {
			return err
		}

		location := shard.GetLocation(retryCount)
		if location == nil {
			break
		}

		targetID, err := store.transfer.openSegment(location.SegmentName)
		if err != nil {
			return err
		}

		request := TransferRequest{
			Opcode:       OPCODE_READ,
			Source:       uint64(source),
			TargetID:     targetID,
			TargetOffset: location.Offset,
			Length:       shard.Length,
		}

		err = store.transfer.submitTransfer(batchID, []TransferRequest{request})
		if err != nil {
			return err
		}

		var status int
		for status == STATUS_WAITING || status == STATUS_PENDING {
			status, _, err = store.transfer.getTransferStatus(batchID, 0)
			if err != nil {
				return err
			}
		}

		err = store.transfer.freeBatchID(batchID)
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

func (store *P2PStore) updatePayloadMetadata(ctx context.Context, name string, addrList []uintptr, sizeList []uint64, payload *Payload, revision int64) error {
	for {
		taskID := 0
		maxShardSize := payload.MaxShardSize
		for i := 0; i < len(addrList); i++ {
			addr, size := addrList[i], sizeList[i]
			var offset uint64 = 0
			for ; offset < size; offset += maxShardSize {
				replicaLocation := Location{
					SegmentName: store.localSegmentName,
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
				if replica.SegmentName != store.localSegmentName {
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
