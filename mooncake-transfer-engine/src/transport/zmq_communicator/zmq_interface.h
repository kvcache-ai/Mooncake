#pragma once

#include "zmq_communicator.h"
#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <memory>

namespace mooncake {

class ZmqInterface {
   public:
    ZmqInterface();
    ~ZmqInterface();

    // Initialize
    bool initialize(const ZmqConfig& config);
    void shutdown();

    // Socket management
    int createSocket(ZmqSocketType type);
    bool closeSocket(int socket_id);

    // Bind and connect
    bool bind(int socket_id, const std::string& endpoint);
    bool connect(int socket_id, const std::string& endpoint);
    bool unbind(int socket_id, const std::string& endpoint);
    bool disconnect(int socket_id, const std::string& endpoint);

    // Socket options
    bool setSocketOption(int socket_id, ZmqSocketOption option, int64_t value);
    int64_t getSocketOption(int socket_id, ZmqSocketOption option);
    bool setRoutingId(int socket_id, const std::string& routing_id);
    std::string getRoutingId(int socket_id);

    // Socket state queries
    bool isBound(int socket_id);
    bool isConnected(int socket_id);
    pybind11::list getConnectedEndpoints(int socket_id);
    std::string getBoundEndpoint(int socket_id);
    ZmqSocketType getSocketType(int socket_id);

    // Start server (for bound sockets)
    bool startServer(int socket_id);

    // REQ/REP mode
    pybind11::object request(int socket_id, pybind11::handle data);
    pybind11::object requestAsync(int socket_id, pybind11::handle data,
                                  pybind11::handle loop);
    void reply(int socket_id, pybind11::handle data);

    // PUB/SUB mode
    int publish(int socket_id, const std::string& topic, pybind11::handle data);
    pybind11::object publishAsync(int socket_id, const std::string& topic,
                                  pybind11::handle data, pybind11::handle loop);
    bool subscribe(int socket_id, const std::string& topic);
    bool unsubscribe(int socket_id, const std::string& topic);
    void setSubscribeCallback(int socket_id, pybind11::function callback);

    // PUSH/PULL mode
    int push(int socket_id, pybind11::handle data);
    pybind11::object pushAsync(int socket_id, pybind11::handle data,
                               pybind11::handle loop);
    void setPullCallback(int socket_id, pybind11::function callback);

    // PAIR mode
    int send(int socket_id, pybind11::handle data);
    pybind11::object sendAsync(int socket_id, pybind11::handle data,
                               pybind11::handle loop);
    void setReceiveCallback(int socket_id, pybind11::function callback);

    // Tensor support
    int sendTensor(int socket_id, pybind11::handle tensor);
    pybind11::object sendTensorAsync(int socket_id, pybind11::handle tensor,
                                     pybind11::handle loop);
    void setTensorReceiveCallback(int socket_id, pybind11::function callback);

    // Python object serialization (pickle) - ZMQ compatible
    int sendPyobj(int socket_id, pybind11::handle obj,
                  const std::string& topic = "");
    pybind11::object sendPyobjAsync(int socket_id, pybind11::handle obj,
                                    pybind11::handle loop,
                                    const std::string& topic = "");
    void setPyobjReceiveCallback(int socket_id, pybind11::function callback);

    // Multipart messages - ZMQ compatible
    int sendMultipart(int socket_id, pybind11::list frames,
                      const std::string& topic = "");
    pybind11::object sendMultipartAsync(int socket_id, pybind11::list frames,
                                        pybind11::handle loop,
                                        const std::string& topic = "");
    void setMultipartReceiveCallback(int socket_id,
                                     pybind11::function callback);

    // JSON messages - ZMQ compatible
    int sendJson(int socket_id, pybind11::handle obj,
                 const std::string& topic = "");
    pybind11::object sendJsonAsync(int socket_id, pybind11::handle obj,
                                   pybind11::handle loop,
                                   const std::string& topic = "");
    void setJsonReceiveCallback(int socket_id, pybind11::function callback);

    // String messages - ZMQ compatible
    int sendString(int socket_id, const std::string& str,
                   const std::string& topic = "",
                   const std::string& encoding = "utf-8");
    pybind11::object sendStringAsync(int socket_id, const std::string& str,
                                     pybind11::handle loop,
                                     const std::string& topic = "",
                                     const std::string& encoding = "utf-8");
    void setStringReceiveCallback(int socket_id, pybind11::function callback,
                                  const std::string& encoding = "utf-8");

    // Blocking receive methods (ZMQ-compatible polling mode)
    pybind11::dict recv(int socket_id, int flags = 0);
    pybind11::object recvAsync(int socket_id, pybind11::handle loop,
                               int flags = 0);
    pybind11::dict recvTensor(int socket_id, int flags = 0);
    pybind11::object recvTensorAsync(int socket_id, pybind11::handle loop,
                                     int flags = 0);
    pybind11::dict recvPyobj(int socket_id, int flags = 0);
    pybind11::object recvPyobjAsync(int socket_id, pybind11::handle loop,
                                    int flags = 0);
    pybind11::dict recvMultipart(int socket_id, int flags = 0);
    pybind11::object recvMultipartAsync(int socket_id, pybind11::handle loop,
                                        int flags = 0);
    pybind11::dict recvJson(int socket_id, int flags = 0);
    pybind11::object recvJsonAsync(int socket_id, pybind11::handle loop,
                                   int flags = 0);
    pybind11::dict recvString(int socket_id, int flags = 0,
                              const std::string& encoding = "utf-8");
    pybind11::object recvStringAsync(int socket_id, pybind11::handle loop,
                                     int flags = 0,
                                     const std::string& encoding = "utf-8");

    // Enable/disable polling mode (default is callback mode)
    void setPollingMode(int socket_id, bool enable);

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;

    // Helper: extract data from Python handle
    std::string extractData(pybind11::handle data);
    TensorInfo extractTensor(pybind11::handle tensor);
};

// Python binding function
void bind_zmq_interface(pybind11::module_& m);

}  // namespace mooncake
