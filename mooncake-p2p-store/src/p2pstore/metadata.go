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
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// key = payload_name
// value = {
// 	size: 1GB
// 	max_shard_size: 128MB
//  size_list: [X, X, X,...]
// 	shards: [
// 	{
// 		size: addrSize[0]
// 		gold: [{ segment_name: 'megatron_segment_name', offset: addrList[0]}]
// 		replica_list: [{segment_name: X, offset: Y}, {segment_name: X, offset: Y}]  // 高u
// 	}
// 	{
// 		size: addrSize[1]
// 		gold: [{ segment_name: 'megatron_segment_name', offset: addrList[1]}]
// 		replica_list: [{segment_name: X, offset: Y}, {segment_name: X, offset: Y}]
// 	}
// 	],
// };

type Location struct {
	SegmentName string `json:"segment_name"`
	Offset      uint64 `json:"offset"`
}

type Shard struct {
	Length      uint64     `json:"size"`
	Gold        []Location `json:"gold"`
	ReplicaList []Location `json:"replica_list"`
}

type Payload struct {
	Name         string   `json:"name"`
	Size         uint64   `json:"size"`
	SizeList     []uint64 `json:"size_list"`
	MaxShardSize uint64   `json:"max_shard_size"`
	Shards       []Shard  `json:"shards"`
}

func (s *Shard) GetLocation(retryTimes int) *Location {
	if retryTimes == 0 {
		return s.getRandomLocation()
	} else {
		return s.getRetryLocation(retryTimes - 1)
	}
}

func (s *Shard) Count() int {
	return len(s.ReplicaList) + len(s.Gold)
}

func (s *Shard) getRandomLocation() *Location {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if len(s.ReplicaList) > 0 {
		index := r.Intn(len(s.ReplicaList))
		return &s.ReplicaList[index]
	} else if len(s.Gold) > 0 {
		index := r.Intn(len(s.Gold))
		return &s.Gold[index]
	}
	return nil
}

func (s *Shard) getRetryLocation(retryTimes int) *Location {
	if len(s.ReplicaList) > retryTimes {
		return &s.ReplicaList[retryTimes]
	}
	retryTimes -= len(s.ReplicaList)
	if len(s.Gold) > retryTimes {
		return &s.Gold[retryTimes]
	}
	return nil
}

func (s *Payload) IsEmpty() bool {
	for _, shard := range s.Shards {
		if len(shard.Gold) != 0 || len(shard.ReplicaList) != 0 {
			return false
		}
	}
	return true
}

type Metadata struct {
	etcdClient *clientv3.Client
	keyPrefix  string
}

func NewMetadata(metadataConnString string, keyPrefix string) (*Metadata, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{metadataConnString},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &Metadata{etcdClient: etcdClient, keyPrefix: keyPrefix}, nil
}

func (metadata *Metadata) Close() error {
	return metadata.etcdClient.Close()
}

func (metadata *Metadata) Create(ctx context.Context, name string, payload *Payload) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	key := metadata.keyPrefix + name
	txnResp, err := metadata.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(jsonData))).
		Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("error: key '%s' already exists", key)
	}
	return nil
}

func (metadata *Metadata) Put(ctx context.Context, name string, payload *Payload) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	key := metadata.keyPrefix + name
	_, err = metadata.etcdClient.Put(ctx, key, string(jsonData))
	return err
}

func (metadata *Metadata) Update(ctx context.Context, name string, payload *Payload, revision int64) (bool, error) {
	key := metadata.keyPrefix + name
	if payload.IsEmpty() {
		txnResp, err := metadata.etcdClient.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
			Then(clientv3.OpDelete(key)).
			Commit()
		if err != nil {
			return false, err
		}
		return txnResp.Succeeded, nil
	} else {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return false, err
		}
		txnResp, err := metadata.etcdClient.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
			Then(clientv3.OpPut(key, string(jsonData))).
			Commit()
		if err != nil {
			return false, err
		}
		return txnResp.Succeeded, nil
	}
}

func (metadata *Metadata) Get(ctx context.Context, name string) (*Payload, int64, error) {
	key := metadata.keyPrefix + name
	response, err := metadata.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, -1, err
	}
	if len(response.Kvs) > 0 {
		var retrievedPayload Payload
		err = json.Unmarshal(response.Kvs[0].Value, &retrievedPayload)
		if err != nil {
			return nil, -1, err
		}
		return &retrievedPayload, response.Kvs[0].ModRevision, nil
	}
	return nil, -1, nil
}

func (metadata *Metadata) List(ctx context.Context, namePrefix string) ([]*Payload, error) {
	startRange := metadata.keyPrefix + namePrefix
	stopRange := metadata.keyPrefix + namePrefix + string([]byte{0xFF})
	response, err := metadata.etcdClient.Get(ctx, startRange, clientv3.WithRange(stopRange))
	if err != nil {
		return nil, err
	}
	var payloads []*Payload
	for _, record := range response.Kvs {
		var retrievedPayload Payload
		err = json.Unmarshal(record.Value, &retrievedPayload)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, &retrievedPayload)
	}
	return payloads, nil
}
