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

#![allow(dead_code)]

mod bindings {
    #![allow(dead_code)]
    #![allow(non_snake_case)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(clippy::const_static_lifetime)]
    #![allow(clippy::unreadable_literal)]
    #![allow(clippy::cyclomatic_complexity)]
    #![allow(clippy::useless_transmute)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use anyhow::{anyhow, bail, Result};
use std::ffi::{c_void, CString};
type BatchID = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpcodeEnum {
    Read = 0,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStatusEnum {
    Waiting = 0,
    Pending,
    Invalid,
    Canceled,
    Completed,
    Timeout,
    Failed,
}

pub struct TransferRequest {
    pub opcode: OpcodeEnum,
    pub source: *mut c_void,
    pub target_id: i32,
    pub target_offset: u64,
    pub length: u64,
}

pub struct BufferEntry {
    pub addr: *mut c_void,
    pub length: u64,
}

pub struct TransferEngine {
    engine: bindings::transfer_engine_t,
}

impl TransferEngine {
    pub fn new(
        metadata_uri: &str,
        local_server_name: &str,
        rpc_port: u64,
    ) -> Result<Self> {
        let metadata_uri_c =
            CString::new(metadata_uri).map_err(|_| anyhow!("CString::new failed"))?;
        let local_server_name_c =
            CString::new(local_server_name).map_err(|_| anyhow!("CString::new failed"))?;

        let engine = unsafe {
            bindings::createTransferEngine(
                metadata_uri_c.as_ptr(),
                local_server_name_c.as_ptr(),
                local_server_name_c.as_ptr(),
                rpc_port,
                0, // disable auto_discover
            )
        };
        if engine.is_null() {
            bail!("Failed to create TransferEngine")
        }

        Ok(Self { engine })
    }

    pub fn close(&mut self) -> Result<()> {
        unsafe {
            bindings::destroyTransferEngine(self.engine);
        }
        Ok(())
    }

    pub fn register_local_memory(
        &self,
        addr: *mut c_void,
        length: usize,
        location: &str,
    ) -> Result<()> {
        let location_c = CString::new(location).map_err(|_| anyhow!("CString::new failed"))?;
        let ret = unsafe {
            bindings::registerLocalMemory(self.engine, addr, length, location_c.as_ptr(), 1)
        };
        if ret < 0 {
            bail!("Failed to register local memory")
        } else {
            Ok(())
        }
    }

    pub fn unregister_local_memory(&self, addr: *mut c_void) -> Result<()> {
        let ret = unsafe { bindings::unregisterLocalMemory(self.engine, addr) };
        if ret < 0 {
            bail!("Failed to unregister local memory")
        } else {
            Ok(())
        }
    }

    pub fn register_local_memory_batch(
        &self,
        buffer_list: &[BufferEntry],
        location: &str,
    ) -> Result<()> {
        let location_c = CString::new(location).map_err(|_| anyhow!("CString::new failed"))?;
        let mut buffer_list_c: Vec<bindings::buffer_entry_t> = vec![];
        let buffer_len_c = buffer_list.len();
        for i in 0..buffer_list.len() {
            buffer_list_c.push(bindings::buffer_entry_t {
                addr: buffer_list[i].addr,
                length: buffer_list[i].length as usize,
            });
        }
        let ret = unsafe {
            bindings::registerLocalMemoryBatch(
                self.engine,
                buffer_list_c.as_mut_ptr(),
                buffer_len_c,
                location_c.as_ptr(),
            )
        };
        if ret < 0 {
            bail!("Failed to register local memory")
        } else {
            Ok(())
        }
    }

    pub fn unregister_local_memory_batch(&self, buffer_list: &[BufferEntry]) -> Result<()> {
        let mut addr_list: Vec<*mut c_void> = buffer_list.iter().map(|entry| entry.addr).collect();
        let addr_len = buffer_list.len();
        let ret = unsafe {
            bindings::unregisterLocalMemoryBatch(self.engine, addr_list.as_mut_ptr(), addr_len)
        };
        if ret < 0 {
            bail!("Failed to unregister local memory")
        } else {
            Ok(())
        }
    }

    pub fn allocate_batch_id(&self, batch_size: usize) -> Result<BatchID> {
        let ret = unsafe { bindings::allocateBatchID(self.engine, batch_size) };
        if ret == u64::MAX {
            bail!("Failed to allocate batch ID")
        } else {
            Ok(ret as BatchID)
        }
    }

    pub fn submit_transfer(
        &self,
        batch_id: BatchID,
        requests: &mut [TransferRequest],
    ) -> Result<()> {
        let mut requests_c: Vec<bindings::transfer_request_t> = vec![];
        for i in 0..requests.len() {
            requests_c.push(bindings::transfer_request_t {
                opcode: requests[i].opcode as i32,
                source: requests[i].source,
                target_id: requests[i].target_id,
                target_offset: requests[i].target_offset,
                length: requests[i].length,
            })
        }
        let ret = unsafe {
            bindings::submitTransfer(self.engine, batch_id, requests_c.as_mut_ptr(), requests.len())
        };
        if ret != 0 {
            bail!("Failed to submit transfer")
        } else {
            Ok(())
        }
    }

    pub fn get_transfer_status(&self, batch_id: BatchID, task_id: u64) -> Result<(i32, u64)> {
        let mut status = bindings::transfer_status_t {
            status: 0,
            transferred_bytes: 0,
        };
        let ret =
            unsafe { bindings::getTransferStatus(self.engine, batch_id, task_id as usize, &mut status) };
        if ret != 0 {
            bail!("Failed to get transfer status")
        } else {
            Ok((status.status, status.transferred_bytes))
        }
    }

    pub fn free_batch_id(&self, batch_id: BatchID) -> Result<()> {
        let ret = unsafe { bindings::freeBatchID(self.engine, batch_id) };
        if ret != 0 {
            bail!("Failed to free batch ID")
        } else {
            Ok(())
        }
    }

    pub fn open_segment(&self, name: String) -> Result<i32> {
        let name_c = CString::new(name).map_err(|_| anyhow!("CString::new failed"))?;
        let ret = unsafe { bindings::openSegment(self.engine, name_c.as_ptr()) };
        if ret < 0 {
            bail!("Failed to get segment ID")
        } else {
            Ok(ret)
        }
    }

    pub fn close_segment(&self, segment_id: i32) -> Result<()> {
        let ret = unsafe { bindings::closeSegment(self.engine, segment_id) };
        if ret < 0 {
            bail!("Failed to close segment with ID {}", segment_id)
        } else {
            Ok(())
        }
    }

    pub fn sync_segment_cache(&self) -> Result<()> {
        let ret = unsafe { bindings::syncSegmentCache(self.engine) };
        if ret < 0 {
            bail!("Failed to synchronize segment cache")
        } else {
            Ok(())
        }
    }
}

impl Drop for TransferEngine {
    fn drop(&mut self) {
        self.close().expect("Failed to close transfer engine");
    }
}

unsafe impl Send for TransferEngine {}
unsafe impl Sync for TransferEngine {}
