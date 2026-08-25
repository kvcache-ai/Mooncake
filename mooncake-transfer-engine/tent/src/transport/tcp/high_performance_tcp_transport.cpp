// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_transport.h"

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake::tent {
namespace {
std::string HostFromRpc(const std::string& address) {
    if (address.empty()) return {};
    if (address.front() == '[') { auto p = address.find(']'); return p == std::string::npos ? std::string{} : address.substr(1, p - 1); }
    auto p = address.find(':'); return p == std::string::npos ? address : address.substr(0, p);
}
bool HasTcp(const BufferDesc& buffer) { return std::find(buffer.transports.begin(), buffer.transports.end(), TransportType::TCP) != buffer.transports.end(); }
}

HighPerformanceTcpTransport::HighPerformanceTcpTransport()
    : HighPerformanceTcpTransport(HighPerformanceTcpParams{}) {}

HighPerformanceTcpTransport::HighPerformanceTcpTransport(
    HighPerformanceTcpParams params)
    : params_(std::move(params)) {
    caps.dram_to_dram = true;
}
HighPerformanceTcpTransport::~HighPerformanceTcpTransport() { uninstall(); }
std::string HighPerformanceTcpTransport::makeIncarnation() const { std::random_device d; std::mt19937_64 r(d()); std::ostringstream o; for(int i=0;i<2;++i) o << std::hex << std::setw(16) << std::setfill('0') << r(); return o.str(); }

Status HighPerformanceTcpTransport::install(std::string& name, std::shared_ptr<ControlService> metadata, std::shared_ptr<Topology>, std::shared_ptr<Config> config) {
    if (installed_) return Status::InvalidArgument("HP TCP already installed" LOC_MARK); metadata_=std::move(metadata); local_segment_name_=name;
    workers_=std::make_unique<HighPerformanceTcpWorkers>(HighPerformanceTcpWorkers::Config{params_.worker_count,params_.queue_capacity_per_worker}); CHECK_STATUS(workers_->start());
    server_=std::make_unique<HighPerformanceTcpServer>(HighPerformanceTcpServer::Config{params_.bind_address,params_.port,params_.max_transfer_bytes,params_.chunk_size},&registry_); uint16_t bound=0; auto status=server_->start(&bound); if(!status.ok()){workers_->stop();workers_.reset();server_.reset();return status;}
    auto local=metadata_->segmentManager().getLocal(); auto host=params_.advertise_address.empty()?HostFromRpc(local->rpc_server_addr):params_.advertise_address; if(host.empty()){server_->stop();workers_->stop();return Status::InvalidArgument("Unable to derive HP TCP advertise address" LOC_MARK);}
    incarnation_=makeIncarnation(); std::string attr; CHECK_STATUS(EncodeHighPerformanceTcpEndpointAttr({incarnation_,{{host,bound}},params_.max_transfer_bytes},&attr));
    status=metadata_->segmentManager().updateLocal([&](SegmentDesc& d){std::get<MemorySegmentDesc>(d.detail).transport_attrs[int(TransportType::TCP)]=attr;return Status::OK();}); if(status.ok()) status=metadata_->segmentManager().synchronizeLocal(); if(!status.ok()){server_->stop();workers_->stop();return status;}
    metadata_->setNotifyCallback([this](const Notification& n){RWSpinlock::WriteGuard g(notify_lock_);notifications_.push_back(n);return 0;}); installed_=true; return Status::OK();
}
Status HighPerformanceTcpTransport::quiesce(){if(!installed_||stopping_.exchange(true))return Status::OK();if(server_)server_->stop();return workers_?workers_->stop():Status::OK();}
Status HighPerformanceTcpTransport::uninstall(){if(!installed_)return Status::OK();quiesce();if(metadata_){metadata_->setNotifyCallback(nullptr);metadata_->segmentManager().updateLocal([](SegmentDesc& d){std::get<MemorySegmentDesc>(d.detail).transport_attrs.erase(int(TransportType::TCP));return Status::OK();});metadata_->segmentManager().synchronizeLocal();}server_.reset();workers_.reset();metadata_.reset();installed_=false;return Status::OK();}

Status HighPerformanceTcpTransport::allocateSubBatch(SubBatchRef& batch,size_t max){auto* x=Slab<HighPerformanceTcpSubBatch>::Get().allocate();if(!x)return Status::InternalError("Unable to allocate HP TCP batch" LOC_MARK);x->max_size=max;x->tasks.reserve(max);batch=x;return Status::OK();}
Status HighPerformanceTcpTransport::freeSubBatch(SubBatchRef& batch){auto* x=dynamic_cast<HighPerformanceTcpSubBatch*>(batch);if(!x)return Status::InvalidArgument("Invalid HP TCP batch" LOC_MARK);for(auto& t:x->tasks)if(t->status.load(std::memory_order_acquire)==PENDING)return Status::InvalidArgument("Cannot free pending HP TCP batch" LOC_MARK);Slab<HighPerformanceTcpSubBatch>::Get().deallocate(x);batch=nullptr;return Status::OK();}

Status HighPerformanceTcpTransport::resolve(const Request& request, HighPerformanceTcpEndpointAttr* endpoint, HighPerformanceTcpBufferAttr* buffer_attr) { return metadata_->segmentManager().withCachedSegment(request.target_id,[&](SegmentDesc* segment){auto* buffer=segment->findBuffer(request.target_offset,request.length);if(!buffer||!HasTcp(*buffer))return Status::NeedsRefreshCache("HP TCP target is not registered" LOC_MARK);auto& mem=segment->getMemory();auto it=mem.transport_attrs.find(int(TransportType::TCP));if(it==mem.transport_attrs.end())return Status::NeedsRefreshCache("HP TCP endpoint missing" LOC_MARK);CHECK_STATUS(DecodeHighPerformanceTcpEndpointAttr(it->second,endpoint));auto bt=buffer->transport_attrs.find(TransportType::TCP);if(bt==buffer->transport_attrs.end())return Status::NeedsRefreshCache("HP TCP registration missing" LOC_MARK);return DecodeHighPerformanceTcpBufferAttr(bt->second,buffer_attr);}); }
void HighPerformanceTcpTransport::finish(const std::shared_ptr<HighPerformanceTcpTask>& task, Status status){bool expected=false;if(!task->completion_claimed.compare_exchange_strong(expected,true))return;if(status.ok()){task->bytes.store(task->request.length,std::memory_order_release);task->status.store(COMPLETED,std::memory_order_release);}else task->status.store(FAILED,std::memory_order_release);outstanding_tasks_.fetch_sub(1);outstanding_bytes_.fetch_sub(task->request.length);if(task->notify)task->notify(task->progress_batch_id);}

Status HighPerformanceTcpTransport::submitTransferTasks(SubBatchRef batch,const std::vector<Request>& requests){auto* x=dynamic_cast<HighPerformanceTcpSubBatch*>(batch);if(!x||!installed_||stopping_)return Status::InvalidArgument("HP TCP transport unavailable" LOC_MARK);if(x->tasks.size()+requests.size()>x->max_size)return Status::TooManyRequests("HP TCP batch capacity exceeded" LOC_MARK);uint64_t total=0;for(auto& r:requests){if(!r.source||!r.length||r.length>params_.max_transfer_bytes||total>UINT64_MAX-r.length)return Status::InvalidArgument("Invalid HP TCP request" LOC_MARK);total+=r.length;}auto old_tasks=outstanding_tasks_.fetch_add(requests.size());auto old_bytes=outstanding_bytes_.fetch_add(total);if(old_tasks+requests.size()>params_.max_outstanding_tasks||old_bytes+total>params_.max_outstanding_bytes){outstanding_tasks_.fetch_sub(requests.size());outstanding_bytes_.fetch_sub(total);return Status::TooManyRequests("HP TCP admission limit exceeded" LOC_MARK);}for(auto& r:requests){auto task=std::make_shared<HighPerformanceTcpTask>();task->request=r;task->progress_batch_id=batch->progress_batch_id;task->notify=batch->notify_progress;auto local=registry_.acquireLocalLease(reinterpret_cast<uint64_t>(r.source),r.length,&task->lease);if(!local.ok()){finish(task,local);x->tasks.push_back(task);continue;}HighPerformanceTcpEndpointAttr endpoint;HighPerformanceTcpBufferAttr reg;auto status=resolve(r,&endpoint,&reg);if(!status.ok()){finish(task,status);x->tasks.push_back(task);continue;}auto request_id=next_request_id_.fetch_add(1);auto owner=workers_->affinityOwner({r.target_id,0,uint32_t(request_id%params_.connections_per_peer),endpoint.incarnation});auto queued=workers_->submitToWorker(owner,[this,task,endpoint,reg,request_id](size_t){HighPerformanceTcpClient client({params_.max_transfer_bytes,params_.chunk_size,params_.connect_timeout_ms});auto status=client.transfer(endpoint.endpoints[0].host,endpoint.endpoints[0].port,reg.registration_id,task->request.target_offset,task->request.source,task->request.length,task->request.opcode==Request::READ?HighPerformanceTcpOpcode::kRead:HighPerformanceTcpOpcode::kWrite,request_id);finish(task,status);});if(!queued.ok())finish(task,queued);x->tasks.push_back(std::move(task));}return Status::OK();}
Status HighPerformanceTcpTransport::getTransferStatus(SubBatchRef batch,int id,TransferStatus& status){auto* x=dynamic_cast<HighPerformanceTcpSubBatch*>(batch);if(!x||id<0||size_t(id)>=x->tasks.size())return Status::InvalidArgument("Invalid HP TCP task id" LOC_MARK);auto& task=x->tasks[id];status.s=task->status.load(std::memory_order_acquire);status.transferred_bytes=task->bytes.load(std::memory_order_acquire);return Status::OK();}
Status HighPerformanceTcpTransport::addMemoryBuffer(BufferDesc& desc,const MemoryOptions& options){if(Platform::getLoader().getMemoryType(reinterpret_cast<void*>(desc.addr))!=MTYPE_CPU)return Status::OK();uint64_t id;CHECK_STATUS(registry_.add(desc.addr,desc.length,options.perm,&id));if(options.perm==kLocalReadWrite)return Status::OK();std::string attr;CHECK_STATUS(EncodeHighPerformanceTcpBufferAttr({id,HighPerformanceTcpPermissionName(options.perm)},&attr));desc.transport_attrs[TransportType::TCP]=std::move(attr);if(!HasTcp(desc))desc.transports.push_back(TransportType::TCP);return Status::OK();}
Status HighPerformanceTcpTransport::removeMemoryBuffer(BufferDesc& desc){if(Platform::getLoader().getMemoryType(reinterpret_cast<void*>(desc.addr))!=MTYPE_CPU)return Status::OK();return registry_.remove(desc.addr,desc.length);}
Status HighPerformanceTcpTransport::sendNotification(SegmentID id,const Notification& n){return metadata_->segmentManager().withCachedSegment(id,[&](SegmentDesc* s){return ControlClient::notify(s->rpc_server_addr,n);});}
Status HighPerformanceTcpTransport::receiveNotification(std::vector<Notification>& n){RWSpinlock::WriteGuard g(notify_lock_);n.clear();n.swap(notifications_);return Status::OK();}
}  // namespace mooncake::tent
