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

package mooncakestore

import "errors"

var (
	ErrStoreNil        = errors.New("mooncakestore: store handle is nil")
	ErrSetupFailed     = errors.New("mooncakestore: setup failed")
	ErrInitAllFailed   = errors.New("mooncakestore: init_all failed")
	ErrHealthCheck     = errors.New("mooncakestore: health check failed")
	ErrPut             = errors.New("mooncakestore: put failed")
	ErrGet             = errors.New("mooncakestore: get failed")
	ErrRemove          = errors.New("mooncakestore: remove failed")
	ErrExist           = errors.New("mooncakestore: existence check failed")
	ErrGetSize         = errors.New("mooncakestore: get size failed")
	ErrRegisterBuffer   = errors.New("mooncakestore: register buffer failed")
	ErrUnregisterBuffer = errors.New("mooncakestore: unregister buffer failed")
	ErrBatchOp          = errors.New("mooncakestore: batch operation failed")
	ErrHostname        = errors.New("mooncakestore: get hostname failed")
	ErrInvalidArgument = errors.New("mooncakestore: invalid argument")
)
