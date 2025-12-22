#pragma once

#ifndef MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H
#define MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H

#include <string>
#include <atomic>
#include <chrono>
#include <optional>

namespace mooncake {

struct KVEventPublisherConfig {
    // 网络端点配置
    std::string endpoint{"tcp://*:19997"};
    std::optional<std::string> replay_endpoint = std::nullopt;
    
    // 性能配置
    size_t buffer_steps{10000};     // 重放缓冲区大小
    int hwm{100000};                // ZeroMQ高水位标记
    size_t max_queue_size{100000};  // 内存队列大小
    std::chrono::milliseconds send_interval{0}; // 发送时间间隔
    
    // 批量配置
    size_t max_batch_size{50};                     // 最大事件批量大小
    std::chrono::milliseconds batch_timeout{200};   // 批量超时
    std::chrono::milliseconds pop_timeout{100};     // 队列弹出超时
    
    // 线程池配置
    size_t enqueue_thread_pool_size{1}; // 生产者线程池线程数
    std::chrono::milliseconds enqueue_timeout{10}; // 入队超时
    size_t enqueue_max_retries = 10;

    // 消息配置
    std::string topic{"mooncake"};
    
    // 自动端口切换配置
    bool auto_port{true};          // 是否启用自动端口切换
    size_t max_port_attempts{10};    // 最大端口尝试次数
    
    // 验证配置有效性
    bool validate() const noexcept;
};

} // namespace mooncake

#endif // MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H