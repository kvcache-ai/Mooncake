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

/*
 * All memory pointed to by the "char *" parameters will not be used
 * after the C function returns.
 * This means that the caller can free the memory pointed to by "char *"
 * parameters, after the call is completed.
 * All the C functions used here follow this convention.
 */

//#include "../../../mooncake-transfer-engine/include/transfer_engine_c.h"
//#include <stdlib.h>
import "C"
import "unsafe"

type BatchID int64

type TransferEngine struct {
	engine C.transfer_engine_t
}

func NewTransferEngine(metadataConnString string,
	localServerName string,
	localIpAddress string,
	rpcPort int) (*TransferEngine, error) {
	metadataConnStringCStr := C.CString(metadataConnString)
	localServerNameCStr := C.CString(localServerName)
	localIpAddressCStr := C.CString(localIpAddress)
	defer C.free(unsafe.Pointer(metadataConnStringCStr))
	defer C.free(unsafe.Pointer(localServerNameCStr))
	defer C.free(unsafe.Pointer(localIpAddressCStr))
	native_engine := C.createTransferEngine(metadataConnStringCStr, localServerNameCStr, localIpAddressCStr, C.uint64_t(rpcPort), 0)
	if native_engine == nil {
		return nil, ErrTransferEngine
	}
	return &TransferEngine{
		engine: native_engine,
	}, nil
}

func (engine *TransferEngine) installTransport(protocol string, topologyMatrix string) error {
	protocolCStr := C.CString(protocol)
	topologyMatrixCStr := C.CString(topologyMatrix)
	defer C.free(unsafe.Pointer(protocolCStr))
	defer C.free(unsafe.Pointer(topologyMatrixCStr))
	var args [2]unsafe.Pointer
	if len(topologyMatrix) == 0 {
		args[0] = nil
	} else {
		args[0] = unsafe.Pointer(topologyMatrixCStr)
	}
	args[1] = nil
	xport := C.installTransport(engine.engine, protocolCStr, &args[0])
	if xport == nil {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) uninstallTransport(protocol string) error {
	protocolCStr := C.CString(protocol)
	defer C.free(unsafe.Pointer(protocolCStr))
	ret := C.uninstallTransport(engine.engine, protocolCStr)
	if ret < 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) Close() {
	C.destroyTransferEngine(engine.engine)
}

func (engine *TransferEngine) registerLocalMemory(addr uintptr, length uint64, location string) error {
	locationCStr := C.CString(location)
	defer C.free(unsafe.Pointer(locationCStr))
	ret := C.registerLocalMemory(engine.engine, unsafe.Pointer(addr), C.size_t(length), locationCStr, 1)
	if ret < 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) unregisterLocalMemory(addr uintptr) error {
	ret := C.unregisterLocalMemory(engine.engine, unsafe.Pointer(addr))
	if ret < 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) allocateBatchID(batchSize int) (BatchID, error) {
	ret := C.allocateBatchID(engine.engine, C.size_t(batchSize))
	if ret == C.UINT64_MAX {
		return BatchID(-1), ErrTransferEngine
	}
	return BatchID(ret), nil
}

const (
	OPCODE_READ      = 0
	OPCODE_WRITE     = 1
	STATUS_WAITING   = 0
	STATUS_PENDING   = 1
	STATUS_INVALID   = 2
	STATUS_CANCELED  = 3
	STATUS_COMPLETED = 4
	STATUS_TIMEOUT   = 5
	STATUS_FAILED    = 6
)

type TransferRequest struct {
	Opcode       int
	Source       uint64
	TargetID     int64
	TargetOffset uint64
	Length       uint64
}

func (engine *TransferEngine) submitTransfer(batchID BatchID, requests []TransferRequest) error {
	requestSlice := make([]C.transfer_request_t, len(requests))
	for i, req := range requests {
		requestSlice[i] = C.transfer_request_t{
			opcode:        C.int(req.Opcode),
			source:        unsafe.Pointer(uintptr(req.Source)),
			target_id:     C.segment_id_t(req.TargetID),
			target_offset: C.uint64_t(req.TargetOffset),
			length:        C.uint64_t(req.Length),
		}
	}

	ret := C.submitTransfer(engine.engine, C.batch_id_t(batchID), &requestSlice[0], C.size_t(len(requests)))
	if ret != 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) getTransferStatus(batchID BatchID, taskID int) (int, uint64, error) {
	var status C.transfer_status_t
	ret := C.getTransferStatus(engine.engine, C.batch_id_t(batchID), C.size_t(taskID), &status)
	if ret != 0 {
		return -1, 0, ErrTransferEngine
	}
	return int(status.status), uint64(status.transferred_bytes), nil
}

func (engine *TransferEngine) freeBatchID(batchID BatchID) error {
	ret := C.freeBatchID(engine.engine, C.batch_id_t(batchID))
	if ret != 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) openSegment(segmentName string, useCache bool) (int64, error) {
	segmentNameCStr := C.CString(segmentName)
	defer C.free(unsafe.Pointer(segmentNameCStr))

	if useCache {
		ret := C.openSegment(engine.engine, segmentNameCStr)
		if ret < 0 {
			return -1, ErrTransferEngine
		}
		return int64(ret), nil
	} else {
		ret := C.openSegmentNoCache(engine.engine, segmentNameCStr)
		if ret < 0 {
			return -1, ErrTransferEngine
		}
		return int64(ret), nil
	}
}

func (engine *TransferEngine) closeSegment(segmentID int64) error {
	ret := C.closeSegment(engine.engine, C.segment_id_t(segmentID))
	if ret < 0 {
		return ErrTransferEngine
	}
	return nil
}

func (engine *TransferEngine) syncSegmentCache() error {
	ret := C.syncSegmentCache(engine.engine)
	if ret < 0 {
		return ErrTransferEngine
	}
	return nil
}

// GetLocalIpAndPort returns the local IP address and port that the
// TransferEngine is listening on. This is particularly useful in P2P
// handshake mode (metadata_conn_string == "P2PHANDSHAKE"), where the
// RPC port is dynamically assigned at initialization time and callers
// need to discover the actual listening address to share with peers.
func (engine *TransferEngine) GetLocalIpAndPort() (string, error) {
	const bufLen = 256
	buf := make([]byte, bufLen)
	ret := C.getLocalIpAndPort(engine.engine, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(bufLen))
	if ret != 0 {
		return "", ErrTransferEngine
	}
	n := 0
	for n < len(buf) && buf[n] != 0 {
		n++
	}
	return string(buf[:n]), nil
}
