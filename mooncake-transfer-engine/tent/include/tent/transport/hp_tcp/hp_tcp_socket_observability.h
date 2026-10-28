// Copyright 2026 KVCache.AI
#ifndef TENT_HP_TCP_SOCKET_OBSERVABILITY_H_
#define TENT_HP_TCP_SOCKET_OBSERVABILITY_H_

#include <asio.hpp>

#include <cstdint>

#if defined(__linux__)
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

namespace mooncake::tent {

struct TcpSocketObservation {
    uint32_t total_retransmissions{0};
    uint32_t rtt_us{0};
    uint32_t congestion_window_segments{0};
};

inline bool ReadTcpSocketObservation(asio::ip::tcp::socket& socket,
                                     TcpSocketObservation* observation) {
#if defined(__linux__)
    if (observation == nullptr || !socket.is_open()) return false;
    tcp_info info{};
    socklen_t size = sizeof(info);
    if (::getsockopt(socket.native_handle(), IPPROTO_TCP, TCP_INFO, &info,
                     &size) != 0) {
        return false;
    }
    observation->total_retransmissions = info.tcpi_total_retrans;
    observation->rtt_us = info.tcpi_rtt;
    observation->congestion_window_segments = info.tcpi_snd_cwnd;
    return true;
#else
    (void)socket;
    (void)observation;
    return false;
#endif
}

}  // namespace mooncake::tent

#endif  // TENT_HP_TCP_SOCKET_OBSERVABILITY_H_
