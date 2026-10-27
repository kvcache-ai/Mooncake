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

import "errors"

// Errors
var ErrInvalidArgument = errors.New("error: invalid argument")
var ErrAddressOverlapped = errors.New("error: address overlapped")
var ErrPayloadOpened = errors.New("error: payload has been replicated")
var ErrPayloadNotOpened = errors.New("error: payload does not replicated")
var ErrPayloadNotFound = errors.New("error: payload not found in metadata")
var ErrTooManyRetries = errors.New("error: too many retries")
var ErrTransferEngine = errors.New("error: transfer engine core")
