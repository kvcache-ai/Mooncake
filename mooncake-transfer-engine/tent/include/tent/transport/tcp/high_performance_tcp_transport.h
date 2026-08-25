// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_TRANSPORT_H_
#define TENT_HIGH_PERFORMANCE_TCP_TRANSPORT_H_
#include <atomic>
#include <memory>
#include <queue>
#include <string>
#include <vector>
#include "tent/runtime/transport.h"
#include "tent/runtime/tcp_transport_config.h"
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_client.h"
#include "tent/transport/tcp/high_performance_tcp_server.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"
namespace mooncake::tent {
struct HighPerformanceTcpTask { Request request; std::atomic<TransferStatusEnum> status{PENDING}; std::atomic<size_t> bytes{0}; std::atomic<bool> completion_claimed{false}; BatchID progress_batch_id{}; std::function<void(BatchID)> notify; HighPerformanceTcpBufferRegistry::Lease lease; };
struct HighPerformanceTcpSubBatch:Transport::SubBatch{std::vector<std::shared_ptr<HighPerformanceTcpTask>> tasks;size_t max_size{};size_t size()const override{return tasks.size();}};
class HighPerformanceTcpTransport final:public Transport{
 public:
  HighPerformanceTcpTransport();
  explicit HighPerformanceTcpTransport(HighPerformanceTcpParams params);
  ~HighPerformanceTcpTransport() override;
  Status install(std::string&,std::shared_ptr<ControlService>,std::shared_ptr<Topology>,std::shared_ptr<Config>) override;
  Status uninstall() override; Status quiesce() override;
  Status allocateSubBatch(SubBatchRef&,size_t) override; Status freeSubBatch(SubBatchRef&) override;
  Status submitTransferTasks(SubBatchRef,const std::vector<Request>&) override; Status getTransferStatus(SubBatchRef,int,TransferStatus&) override;
  Status addMemoryBuffer(BufferDesc&,const MemoryOptions&) override; Status removeMemoryBuffer(BufferDesc&) override;
  bool tracksLocalBuffer(const BufferDesc& desc) const override { return registry_.tracks(desc.addr, desc.length); }
  bool supportNotification()const override{return true;} Status sendNotification(SegmentID,const Notification&) override; Status receiveNotification(std::vector<Notification>&) override;
  const char* getName()const override{return "tcp_high_performance";}
 private:
  Status resolve(const Request&,HighPerformanceTcpEndpointAttr*,HighPerformanceTcpBufferAttr*); void finish(const std::shared_ptr<HighPerformanceTcpTask>&,Status); std::string makeIncarnation() const;
  bool installed_{false}; std::atomic<bool> stopping_{false}; std::string local_segment_name_,incarnation_; HighPerformanceTcpParams params_; std::shared_ptr<ControlService> metadata_; std::unique_ptr<HighPerformanceTcpWorkers> workers_; std::unique_ptr<HighPerformanceTcpServer> server_; HighPerformanceTcpBufferRegistry registry_; std::atomic<uint64_t> next_request_id_{1},outstanding_tasks_{0},outstanding_bytes_{0}; RWSpinlock notify_lock_; std::vector<Notification> notifications_;
};
}  // namespace mooncake::tent
#endif
