#include "benchmark_utils.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>

constexpr int MESSAGE_COUNT = 100000;

int main() {
    int subscriber_fd = connect_client();
    int publisher_fd = connect_client();

    if (subscriber_fd == -1 || publisher_fd == -1) {
        std::cerr << " Failed to return benchmark file descriptors\n";
        return 1;
    }

    std::cout << "Both clients connected\n";

    std::string subscribe_message = "SUBSCRIBE benchmark\n";

    if (!send_all(
                subscriber_fd,
                subscribe_message.data(),
                subscribe_message.size()
                )) {
        std::cerr << "Failed to send subscribe message\n";
        return 1;
    }

    char buffer[1024];

    ssize_t confirmation_bytes = recv(
            subscriber_fd,
            buffer,
            sizeof(buffer),
            0
            );

    if (confirmation_bytes <= 0) {
        std::cerr << "Failed to receive subscription confirm\n";
        return 1;
    }

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < MESSAGE_COUNT; ++i) {
        std::string publish_message = 
            "PUBLISH benchmark message-" + 
            std::to_string(i) +
            "\n";

        if (!send_all(
                    publisher_fd,
                    publish_message.data(),
                    publish_message.size()
                    )) {
            std::cerr << "failed to send benchmark message\n";
            return 1;
        }
    }

    std::string pending;
    int received = 0;

    while (received < MESSAGE_COUNT) {
        ssize_t bytes_received = recv(
                subscriber_fd,
                buffer,
                sizeof(buffer),
                0
                );

        if (bytes_received <= 0) {
            std::cerr << "Subscriber connection failed\n";
            return 1;
        }

        pending.append(buffer, bytes_received);

        std::size_t place;

        while (
                (place = pending.find('\n'))
                != std::string::npos
              ) {
            ++received;
            pending.erase(0, place + 1);
        }

    }

    std::cout << "Received "
        << received 
        << " messages\n";

    auto end = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(end - start).count();

    double throughput = 
        MESSAGE_COUNT / seconds;

    std::cout <<"Elapsed: "
        << seconds
        << " seconds\n";

    std::cout << "Throughput: "
        << throughput
        << " messages/sec\n";

    close(subscriber_fd);
    close(publisher_fd);

    return 0;
}


