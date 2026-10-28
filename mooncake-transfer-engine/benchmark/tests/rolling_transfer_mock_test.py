#!/usr/bin/env python3
"""CPU-only contract test of the production rolling method with a fake engine.

Compiles the actual method body with ASan/UBSan. This checks scheduling and
accounting, not production-header compilation, verbs, or RDMA performance.
Run with Python 3 and a C++17 compiler; override the compiler using CXX.
"""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile

PREAMBLE = r"""
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#define LOG(x) std::cerr
using BatchID = uint64_t;
enum OpCode { READ, WRITE };
enum class TransferStatusEnum { WAITING, PENDING, COMPLETED, FAILED };
struct TransferStatus { TransferStatusEnum s; };
struct Status { bool valid=true; bool deferred=false; bool ok() const {return valid;} bool IsBatchCleanupDeferred() const {return deferred;} std::string ToString() const {return "test";} };
struct TransferRequest { enum { READ, WRITE }; int opcode; uint64_t length; void* source; uint64_t target_id; uint64_t target_offset; };
struct XferBenchConfig { static inline std::string seg_type="DRAM"; };
bool g_te_running=true;
uint64_t ticks=0;
struct XferBenchTimer { uint64_t start=ticks; uint64_t lap_us(bool=false) {ticks+=10; return ticks-start;} };
uint64_t checkedMul(uint64_t a,uint64_t b,const char*) {return a*b;}
struct Engine {
 struct Batch { std::vector<TransferRequest> req; int polls=0; bool rejected=false; };
 std::map<BatchID,Batch> live; uint64_t next=0, submitted=0, completed=0, bytes=0, released=0; int fail_at=0, stop_at=0; bool deferred=false;
 BatchID allocateBatchID(uint64_t) {live[++next]={};return next;}
 Status submitTransfer(BatchID id, const std::vector<TransferRequest>& req) {
  if (++submitted == uint64_t(fail_at)) {live[id].rejected=true;return {false};}
  for(auto& [other,b]:live) if(other!=id) for(auto& a:b.req) for(auto& r:req) {
   auto x=reinterpret_cast<uintptr_t>(a.source), y=reinterpret_cast<uintptr_t>(r.source);
   assert(x+a.length<=y || y+r.length<=x); // No overlap with an active slot.
  }
  live[id].req=req; return {};
 }
 Status getBatchTransferStatus(BatchID id,TransferStatus& s) {
  assert(live.count(id)); auto& b=live.at(id); assert(!b.rejected); // rejected submits must not be polled
  if (++b.polls==2) {++completed; for(auto&r:b.req) bytes+=r.length;}
  s.s=b.polls>=2?TransferStatusEnum::COMPLETED:TransferStatusEnum::PENDING;
  if(stop_at && completed>=uint64_t(stop_at)) g_te_running=false;
  return {};
 }
 Status freeBatchID(BatchID id) {assert(live.erase(id)==1);++released;return {true,deferred};}
};
Status releaseFinishedBatch(Engine& e, BatchID id) {auto s=e.freeBatchID(id);return s.IsBatchCleanupDeferred()?Status{}:s;}
struct TEBenchRunner {
 Engine* engine_; std::atomic<uint64_t> abandoned_batches_{0};
 void noteTransferFailed() {g_te_running=false;}
 Status waitForBatch(BatchID id,TransferStatus& s,uint64_t& polls) { do{++polls;engine_->getBatchTransferStatus(id,s);}while(s.s==TransferStatusEnum::PENDING); return {}; }
 int runRollingTransfers(uint64_t,uint64_t,uint64_t,uint64_t,uint64_t,OpCode,uint32_t,uint64_t,std::vector<double>&);
};
"""
TESTS = r"""
int main() {
 std::vector<unsigned char> buffer(8192);
 auto run=[&](int slots,int batch,int failure,int stop,bool deferred,bool assist){
  g_te_running=true;ticks=0; Engine e; e.fail_at=failure;e.stop_at=stop;e.deferred=deferred;
  TEBenchRunner r{&e};std::vector<double> latency;
  if(assist)setenv("MC_EXPERIMENTAL_INITIATOR_WARM_US","10",1);else unsetenv("MC_EXPERIMENTAL_INITIATOR_WARM_US");
  int rc=r.runRollingTransfers(reinterpret_cast<uint64_t>(buffer.data()),1,0,64,batch,READ,slots,300,latency);
  if(slots==0||slots>64||(slots&(slots-1))||batch<slots||batch%slots) {assert(rc==-1&&e.submitted==0);return;}
  assert(e.live.empty()); assert(e.released==e.submitted);
  assert(e.bytes==latency.size()*64*(batch/slots));
  assert(rc==((failure||stop)?-1:0)); assert(e.completed==latency.size());
 };
 for(int slots:{1,2,4,8,16,32,64}) run(slots,64,0,0,false,false);
 run(4,16,0,0,true,true); run(4,16,2,0,false,false); run(4,16,2,0,true,false); run(4,16,0,2,false,false);
 for(int slots:{0,3,65,128})run(slots,128,0,0,false,false);
 run(4,6,0,0,false,false);run(4,2,0,0,false,false);
 XferBenchConfig::seg_type="VRAM";g_te_running=true;Engine e;TEBenchRunner r{&e};std::vector<double> l;
 setenv("MC_EXPERIMENTAL_INITIATOR_WARM_US","10",1);
 assert(r.runRollingTransfers(0,1,0,64,16,READ,4,300,l)==-1);assert(e.submitted==0);
 std::cout<<"rolling mock checks passed: disjoint ranges, bytes, drain, rejected submit, deferred cleanup, stop, invalid partitions, DRAM guard\n";
}
"""


def main():
    source = (Path(__file__).resolve().parents[1] / "te_backend.cpp").read_text()
    method = source[
        source.index("int TEBenchRunner::runRollingTransfers(") : source.rindex(
            "}  // namespace tent"
        )
    ]
    with tempfile.TemporaryDirectory(prefix="te-rolling-test-") as directory:
        root = Path(directory)
        cpp = root / "rolling.cpp"
        binary = root / "rolling"
        cpp.write_text(PREAMBLE + method + TESTS)
        subprocess.run(
            shlex.split(os.environ.get("CXX", "c++"))
            + [
                "-std=c++17",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-fsanitize=address,undefined",
                "-g",
                str(cpp),
                "-o",
                str(binary),
            ],
            check=True,
        )
        subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    main()
