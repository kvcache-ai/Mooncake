#pragma once

#ifndef MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H
#define MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H

#include "kv_event/kv_event.hpp"
#include "kv_event/kv_event_publisher_config.h"
#include "thread_safe_queue.h"
#include "thread_pool.h"

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <future>
#include <atomic>
#include <memory>
#include <optional>
#include <iostream>
#include <algorithm>

namespace mooncake {

/**
 * @brief ZeroMQ异步事件发布器
 * 
 * 单一类实现，提供完全异步的事件发布功能。
 * 内部使用事件队列和批量发送机制。
 */
class ZmqEventPublisher {
public:
    
    // 统计信息
    struct Stats {
        // 队列状态
        size_t queue_remain_events = 0;           // 队列中剩余事件数
        size_t queue_capacity = 0;                // 队列容量
        
        // 事件统计
        size_t total_events = 0;                  // 总接收事件数
        size_t total_batches = 0;                  // 总发送批次
        size_t failed_events = 0;                 // 失败事件数
        
        // 事件类型统计
        size_t store_event = 0;                    // BlockStore事件
        size_t update_event = 0;                   // BlockUpdate事件
        size_t remove_all_event = 0;               // RemoveAll事件
        
        // 性能指标
        size_t replay_requests = 0;                // 重放请求数
        double events_per_batch = 0.0;             // 平均每批次事件数
        double success_rate = 0.0;               // 发送成功率(%)

        bool has_data() const { return total_events > 0; }
        void calculate_derived_metrics();
        friend std::ostream& operator<<(std::ostream& os, const Stats& stats);
    };
    
    // 结果封装
    struct PublishResult {
        std::shared_ptr<std::promise<bool>> promise;
        
        PublishResult() : promise(std::make_shared<std::promise<bool>>()) {}
        
        // 获取future
        std::future<bool> get_future() { return promise->get_future(); }
    };

    explicit ZmqEventPublisher(const KVEventPublisherConfig& config = KVEventPublisherConfig{});
    ~ZmqEventPublisher();
    
    // 禁止拷贝和移动
    ZmqEventPublisher(const ZmqEventPublisher&) = delete;
    ZmqEventPublisher& operator=(const ZmqEventPublisher&) = delete;
    ZmqEventPublisher(ZmqEventPublisher&&) = delete;
    ZmqEventPublisher& operator=(ZmqEventPublisher&&) = delete;
    
    // 异步发布接口
    std::future<bool> publish_block_store(
        const std::string& mooncake_key,
        const std::vector<Replica::Descriptor>& replicas,
        const StoreEventInfo& store_event_info) {
        store_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<BlockStoreEvent>(
            mooncake_key, replicas, store_event_info
        );
    }
    
    std::future<bool> publish_block_update(
        const std::string& mooncake_key,
        const std::vector<Replica::Descriptor>& replicas) {
        update_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<BlockUpdateEvent>(mooncake_key, replicas);
    }
    
    std::future<bool> publish_remove_all() {
        remove_all_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<RemoveAllEvent>();
    }
    
    // 通用异步发布接口
    std::future<bool> publish_event_async(std::shared_ptr<KVCacheEvent> event);
    
    // 关闭
    void shutdown();
    
    // 获取统计信息
    Stats get_stats() const;
    
    // 检查是否运行中
    bool is_running() const noexcept { return running_; }

private:
    // 队列条目
    struct QueuedItem {
        std::shared_ptr<KVCacheEvent> event;
        std::shared_ptr<std::promise<bool>> promise;
    };
    
    // 重放缓冲区条目
    struct ReplayEntry {
        uint64_t seq;
        msgpack::sbuffer payload;
        
        ReplayEntry(uint64_t s, msgpack::sbuffer p) 
            : seq(s), payload(std::move(p)) {}
    };
    
    // 线程资源
    struct ThreadResources {
        std::unique_ptr<zmq::socket_t> pub_socket;
        std::unique_ptr<zmq::socket_t> replay_socket;
        std::deque<ReplayEntry> replay_buffer;
        uint64_t next_seq = 0;
        
        explicit ThreadResources(zmq::context_t& ctx, const KVEventPublisherConfig& config);
    };
    
    // 事件创建函数
    template<DerivedFromKVCacheEvent Event, typename... Args>
    std::future<bool> publish_event_impl(Args&&... args) {
        if (!running_) {
            auto promise = std::make_shared<std::promise<bool>>();
            promise->set_value(false);
            return promise->get_future();
        }
        
        auto event = std::make_shared<Event>(std::forward<Args>(args)...);
        return publish_event_async(std::move(event));
    }

    // 事件入队函数
    void process_enqueue_task_with_retry(QueuedItem item);

    // 发送线程函数
    void publisher_thread(std::stop_token stop_token);
    
    // 套接字设置
    void setup_sockets(ThreadResources& resources);
    
    // 重放服务
    void service_replay(ThreadResources& resources);
    
    // 处理发送错误
    void handle_error(const std::exception& e, const std::string& context);
    
    // 常量
    static constexpr double SHUTDOWN_TIMEOUT = 2.0;
    static constexpr std::array<uint8_t, 8> END_SEQ = {
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
    };
    
    KVEventPublisherConfig config_;
    zmq::context_t context_;

    using EventQueue = ThreadSafeQueue<std::optional<QueuedItem>>;
    std::unique_ptr<EventQueue> event_queue_;
    std::unique_ptr<ThreadPool> enqueue_pool_;
    std::jthread publisher_thread_;
    std::atomic<bool> running_{false};
    
    std::atomic<size_t> total_events_{0};
    std::atomic<size_t> total_batches_{0};
    std::atomic<size_t> failed_events_{0};
    std::atomic<size_t> store_event_{0};
    std::atomic<size_t> update_event_{0};
    std::atomic<size_t> remove_all_event_{0};
    std::atomic<size_t> replay_requests_{0};
}; // class ZmqEventPublisher


/**
 * @brief 简化的事件发布者工厂
 */
class EventPublisherFactory {
public:
    using PublisherConstructor = std::function<
        std::unique_ptr<ZmqEventPublisher>(const KVEventPublisherConfig&)>;
    
    /**
     * @brief 注册发布者类型
     */
    static void register_publisher(
        const std::string& name, PublisherConstructor constructor);
    
    /**
     * @brief 创建发布者
     */
    static std::unique_ptr<ZmqEventPublisher> create(
        const std::string& publisher_type = "zmq",
        const KVEventPublisherConfig& config = KVEventPublisherConfig{});
    
    static std::unique_ptr<ZmqEventPublisher> create(
        const KVEventPublisherConfig& config) {
        return create("zmq", config);
    }

private:
    static std::unordered_map<std::string, PublisherConstructor>& registry();
    
    // 禁止实例化
    EventPublisherFactory() = delete;
    ~EventPublisherFactory() = delete;
}; // class EventPublisherFactory


} // namespace mooncake

#endif // MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H