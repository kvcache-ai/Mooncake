#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <string>

#include "conductor/zmq/transport.h"

namespace mooncake::conductor::zmq::detail {
namespace {

TEST(ZmqTransport, MultipartPreservesEmptyAndBinaryFrames) {
    Context context;
    Socket sender(context, ZMQ_PAIR);
    Socket receiver(context, ZMQ_PAIR);
    sender.bind("inproc://binary");
    receiver.connect("inproc://binary");
    receiver.set(ZMQ_RCVTIMEO, 1000);
    const std::string binary("\0\xff\0", 3);
    const std::array<std::string_view, 3> frames = {"", binary, "tail"};
    ASSERT_TRUE(SendMultipart(sender, frames));
    // Decode with the C API directly, independently of ReceiveMultipart.
    for (size_t i = 0; i < frames.size(); ++i) {
        char bytes[16];
        const int size = zmq_recv(receiver.handle(), bytes, sizeof(bytes), 0);
        ASSERT_GE(size, 0);
        EXPECT_EQ(std::string(bytes, size), frames[i]);
        int more = 0;
        size_t option_size = sizeof(more);
        ASSERT_EQ(
            zmq_getsockopt(receiver.handle(), ZMQ_RCVMORE, &more, &option_size),
            0);
        EXPECT_EQ(more != 0, i + 1 < frames.size());
    }
    // Encode with the C API and verify both message boundaries are retained.
    ASSERT_EQ(zmq_send(sender.handle(), "", 0, ZMQ_SNDMORE), 0);
    ASSERT_EQ(zmq_send(sender.handle(), binary.data(), binary.size(), 0), 3);
    ASSERT_EQ(zmq_send(sender.handle(), "next", 4, 0), 4);
    std::vector<Message> received;
    ASSERT_TRUE(ReceiveMultipart(receiver, received));
    ASSERT_EQ(received.size(), 2);
    EXPECT_TRUE(received[0].empty());
    EXPECT_EQ(received[1].to_string(), binary);
    received.clear();
    ASSERT_TRUE(ReceiveMultipart(receiver, received));
    ASSERT_EQ(received.size(), 1);
    EXPECT_EQ(received[0].to_string(), "next");
}

TEST(ZmqTransport, TimeoutAndTerminatedContextAreDistinct) {
    Context context;
    Socket socket(context, ZMQ_PAIR);
    socket.bind("inproc://timeout");
    socket.set(ZMQ_RCVTIMEO, 10);
    Message message;
    EXPECT_FALSE(socket.recv(message).has_value());
    ASSERT_EQ(zmq_ctx_shutdown(context.handle()), 0);
    EXPECT_THROW(socket.recv(message), Error);
}

TEST(ZmqTransport, FailedConnectCanBeClosedRepeatedly) {
    Context context;
    Socket socket(context, ZMQ_SUB);
    EXPECT_THROW(socket.connect("invalid-endpoint"), Error);
    socket.close();
    socket.close();
}

TEST(ZmqTransportDeathTest, UnreachablePeerDoesNotBlockDestruction) {
    EXPECT_EXIT(
        {
            alarm(3);
            {
                Context context;
                Socket socket(context, ZMQ_DEALER);
                socket.connect("tcp://127.0.0.1:1");
                // Queue a request for a peer that cannot accept it.
                if (zmq_send(socket.handle(), "request", 7, ZMQ_DONTWAIT) != 7)
                    _exit(2);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

}  // namespace
}  // namespace mooncake::conductor::zmq::detail
