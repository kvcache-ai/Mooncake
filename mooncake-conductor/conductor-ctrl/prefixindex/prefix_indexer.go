package prefixindex

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"conductor/common"
	"github.com/cespare/xxhash/v2"
)

var (
	conductorBlockSize = common.LoadIntEnv("CONDUCTOR_BLOCK_SIZE", 128)
)

type ModelContext struct {
	ModelName string
	LoraID    int64 // -1 represents no LoRA adapter
}

type CacheStoreInfo struct {
	// Currently, the KV cache at different levels is not distinguished.
	// In the future, the caches of Mooncake and inference engines (vLLM, SGLang)
	// should be handled separately. TODO
	engineLastAccessTime map[string]*atomic.Int64
	TotalReplicaNums     atomic.Int64
}

type HashMapStore struct {
	// conductor prefixHash -> cachestore
	prefixMap map[uint64]*CacheStoreInfo

	createTime    time.Time
	lastAccess    atomic.Int64
	totalPrefixes int64
}

type ContextData struct {
	prefixMu  sync.RWMutex
	hashmapMu sync.RWMutex

	prefixStore      *HashMapStore
	proxyHashMapping map[uint64]uint64 // engine block hash -> conductor prefix hash
}

type PrefixCacheTable struct {
	contextMap sync.Map // ModelContext → *ContextData

	seed      uint64
	blockSize int // TODO move it to contextData

	contextCount atomic.Int32
}

func GenerateSeedFromEnv() uint64 {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	envSeed := common.LoadIntEnv("CONDUCTOR_SEED", -1)

	var seed uint64
	if envSeed != -1 {
		seed = uint64(envSeed)
	} else {
		seed = r.Uint64()
	}
	return seed
}

func NewPrefixCacheTable() *PrefixCacheTable {
	// init conductor first blockhash root
	seed := GenerateSeedFromEnv()

	slog.Info("mooncake-conductor prefix_hash_table_configurations",
		"block_size", conductorBlockSize,
		"seed", seed)

	p := &PrefixCacheTable{
		seed:      seed,
		blockSize: conductorBlockSize,
	}

	return p
}

func (p *PrefixCacheTable) getContextData(modelName string, loraID int64) *ContextData {
	ctx := ModelContext{
		ModelName: modelName,
		LoraID:    loraID,
	}
	value, exists := p.contextMap.Load(ctx)
	if exists {
		return value.(*ContextData)
	}
	newContextData := &ContextData{
		prefixStore: &HashMapStore{
			prefixMap:     make(map[uint64]*CacheStoreInfo),
			createTime:    time.Now(),
			totalPrefixes: 0,
		},
		proxyHashMapping: make(map[uint64]uint64),
	}
	newContextData.prefixStore.lastAccess.Store(time.Now().Unix())
	p.contextMap.Store(ctx, newContextData)
	slog.Debug("in func getContextData", "newContextData", newContextData)
	p.contextCount.Add(1)
	return newContextData
}

func (p *PrefixCacheTable) ComputePrefixHash(tokenIds []int32) []uint64 {
	numBlocks := len(tokenIds) / p.blockSize
	prefixHashes := make([]uint64, 0, numBlocks)
	var parentHash uint64 = p.seed

	for i := 0; i < numBlocks; i++ {
		start := i * p.blockSize
		end := start + p.blockSize
		if end > len(tokenIds) {
			break
		}
		hashValue := p.computeHash(parentHash, tokenIds[start:end])
		prefixHashes = append(prefixHashes, hashValue)
		parentHash = hashValue
	}
	return prefixHashes
}

func (p *PrefixCacheTable) CacheHitCompute(modelName string, loraID int64, tokenIds []int32, candidateEngine map[string]struct{}) map[string]int {
	ctx := ModelContext{
		ModelName: modelName,
		LoraID:    loraID,
	}
	slog.Debug("In CacheHitCompute", "ModelName", modelName, "LoraID", loraID)
	value, exists := p.contextMap.Load(ctx)
	if !exists {
		return map[string]int{}
	}

	prefixHashes := p.ComputePrefixHash(tokenIds)
	slog.Debug("In CacheHitCompute", "prefixHashes", prefixHashes)

	contextData := value.(*ContextData)
	contextData.prefixMu.RLock()
	defer contextData.prefixMu.RUnlock()
	prefixStore := contextData.prefixStore
	prefixMatchEngines := map[string]int{}

	for i, prefixHash := range prefixHashes {
		cacheStoreInfo, exists := prefixStore.prefixMap[prefixHash]
		slog.Debug("In CacheHitCompute", "cacheStoreInfo", cacheStoreInfo)
		if !exists || cacheStoreInfo.TotalReplicaNums.Load() == 0 {
			break
		}

		prefixMatchPercent := (i + 1) * 100 / len(prefixHashes)

		hasOneEngineMatch := false
		for engineIp := range cacheStoreInfo.engineLastAccessTime {
			if _, inCandidate := candidateEngine[engineIp]; inCandidate {
				prefixMatchEngines[engineIp] = prefixMatchPercent
				hasOneEngineMatch = true
			}
		}
		if !hasOneEngineMatch {
			break
		}
	}

	prefixStore.lastAccess.Store(time.Now().Unix())

	return prefixMatchEngines
}

func (p *PrefixCacheTable) ProcessStoreEvent(event common.StoredEvent) error {
	if len(event.BlockHashes) == 0 {
		return nil
	}
	slog.Debug("In ProcessStoreEvent", "event.ModelName", event.ModelName, "event.LoraID", event.LoraID)
	contextData := p.getContextData(event.ModelName, event.LoraID)

	contextData.hashmapMu.Lock()
	defer contextData.hashmapMu.Unlock()
	proxyHashMap := contextData.proxyHashMapping

	if len(event.BlockHashes)*p.blockSize != len(event.TokenIds) {
		if len(event.BlockHashes) != 1 {
			return fmt.Errorf("block hashes and tokens length mismatch")
		}
		// mooncake event, only one block hash
		prefixStore := contextData.prefixStore
		for _, blockHash := range event.BlockHashes {
			if existingHash, exists := proxyHashMap[blockHash]; exists {
				p.addNewPrefixStore(prefixStore, existingHash, event.EngineIp)
			}
		}
		return nil
	}

	newPrefixStore := make([]struct {
		hashValue uint64
		engineIp  string
	}, 0)

	var parentHash uint64 = p.seed
	if event.ParentBlockHash != 0 {
		slog.Debug("parent Block HASH is not None.")
		if pbh, exists := proxyHashMap[event.ParentBlockHash]; exists {
			parentHash = pbh
		}
	}

	// TODO currently, mooncake kv-event does not contained token_ids,
	// so you must enable vllm prefix_cache to get it.
	for i, blockHash := range event.BlockHashes {
		// cache already exists, add engine info and continue
		if existingHash, exists := proxyHashMap[blockHash]; exists {
			newPrefixStore = append(newPrefixStore, struct {
				hashValue uint64
				engineIp  string
			}{existingHash, event.EngineIp})
			continue
		}
		// if not exists, compute hash
		hashValue := p.computeHash(parentHash, event.TokenIds[i*p.blockSize:(i+1)*p.blockSize])
		parentHash = hashValue

		proxyHashMap[blockHash] = hashValue

		newPrefixStore = append(newPrefixStore, struct {
			hashValue uint64
			engineIp  string
		}{
			hashValue: hashValue,
			engineIp:  event.EngineIp,
		})

	}
	if len(newPrefixStore) > 0 {
		contextData.prefixMu.Lock()
		defer contextData.prefixMu.Unlock()

		prefixStore := contextData.prefixStore
		for _, newPrefix := range newPrefixStore {
			slog.Debug("show new prefix data", "newPrefix", newPrefix)
			p.addNewPrefixStore(prefixStore, newPrefix.hashValue, newPrefix.engineIp)
		}
	}
	p.debugPrefixCacheTable()
	p.debugStoreEvent(event)
	return nil
}

func (p *PrefixCacheTable) ProcessRemoveEvent(event common.RemovedEvent) error {
	if len(event.BlockHashes) == 0 {
		return nil
	}

	ctx := ModelContext{
		ModelName: event.ModelName,
		LoraID:    event.LoraID,
	}
	value, exists := p.contextMap.Load(ctx)
	if !exists {
		return nil
	}
	contextData := value.(*ContextData)
	contextData.hashmapMu.Lock()
	defer contextData.hashmapMu.Unlock()
	proxyHashMap := contextData.proxyHashMapping
	removeConductorHash := make([]uint64, 0, len(event.BlockHashes))

	// delete proxyHashMapping
	for _, blockHash := range event.BlockHashes {
		if conductorHash, exists := proxyHashMap[blockHash]; exists {
			delete(proxyHashMap, blockHash)
			removeConductorHash = append(removeConductorHash, conductorHash)
		}
	}

	contextData.prefixMu.Lock()
	defer contextData.prefixMu.Unlock()
	prefixStore := contextData.prefixStore
	for _, conductorHash := range removeConductorHash {
		delete(prefixStore.prefixMap, conductorHash)
		contextData.prefixStore.totalPrefixes--
	}

	p.debugPrefixCacheTable()
	return nil
}

func (p *PrefixCacheTable) computeHash(parentHash uint64, blockTokenIDs []int32) uint64 {
	digest := xxhash.NewWithSeed(p.seed)
	var parentHashBytes [8]byte
	binary.LittleEndian.PutUint64(parentHashBytes[:], parentHash)
	_, _ = digest.Write(parentHashBytes[:])

	var tokenIDsBytes [8]byte
	for _, tokenID := range blockTokenIDs {
		binary.LittleEndian.PutUint32(tokenIDsBytes[:], uint32(tokenID))
		_, _ = digest.Write(tokenIDsBytes[:])
	}
	return digest.Sum64()
}

// only kv event can flush prefixMap
func (p *PrefixCacheTable) addNewPrefixStore(prefixStore *HashMapStore, hashValue uint64, engineIp string) {
	now := time.Now().Unix()
	if prefixStore.prefixMap[hashValue] == nil {
		prefixStore.prefixMap[hashValue] = &CacheStoreInfo{
			engineLastAccessTime: make(map[string]*atomic.Int64),
		}
		prefixStore.totalPrefixes++
	}
	cacheStoreInfo := prefixStore.prefixMap[hashValue]

	if _, exists := cacheStoreInfo.engineLastAccessTime[engineIp]; !exists {
		var newTime atomic.Int64
		newTime.Store(now)
		cacheStoreInfo.engineLastAccessTime[engineIp] = &newTime
	} else {
		cacheStoreInfo.engineLastAccessTime[engineIp].Store(now)
	}
	cacheStoreInfo.TotalReplicaNums.Add(1)
	slog.Debug("new prefixstore", "conductor_hash", hashValue, "AccessTime", cacheStoreInfo.engineLastAccessTime)
}

func (p *PrefixCacheTable) debugPrefixCacheTable() {
	slog.Debug("global configuration", "seed: ", p.seed, "blockSize:", p.blockSize)
	slog.Debug("show PrefixCacheTable", "contextCount: ", p.contextCount.Load())
	p.contextMap.Range(func(key, value interface{}) bool {
		ctx := key.(ModelContext)
		ctxdata := p.getContextData(ctx.ModelName, ctx.LoraID)
		slog.Debug("modelcontext", "ModelName: ", ctx.ModelName, "LoraID:", ctx.LoraID)
		slog.Debug("show proxyHashMapping", "proxyHashMapping: ", ctxdata.proxyHashMapping)
		prefixstore := ctxdata.prefixStore
		slog.Debug("prfixstore", "totalPrefixes: ", prefixstore.totalPrefixes, "lastAccess:", prefixstore.lastAccess)
		for engip := range prefixstore.prefixMap {
			slog.Debug("prefixMap", "EngineIp", engip)
		}
		return true
	})
}

func (p *PrefixCacheTable) debugStoreEvent(storeEvent common.StoredEvent) {
	slog.Debug("StoredEvent debug information",
		"model_name", storeEvent.ModelName,
		"lora_id", storeEvent.LoraID,
		"engine_ip", storeEvent.EngineIp,
		"parent_block_hash", storeEvent.ParentBlockHash,
		"block_hashes", storeEvent.BlockHashes,
		"token_ids", storeEvent.TokenIds,
	)
}
