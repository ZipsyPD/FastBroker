#include "benchmark_utils.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

constexpr int MESSAGES_PER_PUBLISHER = 10000;

void run_test(int publisher_count) {
    int subscriber_fd = connect_client();

    if (subscriber_fd == -1) {
        std::cerr << "Failed to create subscriber\n";
        return;
    }

    std::string topic =
        "scaling_" + std::to_string(publisher_count);

    std::string subscribe_message =
        "SUBSCRIBE " + topic + "\n";

    if (!send_all(
            subscriber_fd,
            subscribe_message.data(),
            subscribe_message.size()
        )) {
        std::cerr << "Failed to subscribe\n";
        close(subscriber_fd);
        return;
    }

    char buffer[4096];

    ssize_t confirmation_bytes = recv(
        subscriber_fd,
        buffer,
        sizeof(buffer),
        0
    );

    if (confirmation_bytes <= 0) {
        std::cerr << "Failed to receive subscription confirmation\n";
        close(subscriber_fd);
        return;
    }

    std::vector<int> publisher_fds;

    for (int i = 0; i < publisher_count; ++i) {
        int fd = connect_client();

        if (fd == -1) {
            std::cerr << "Failed to create publisher\n";
            close(subscriber_fd);
            return;
        }

        publisher_fds.push_back(fd);
    }

    int total_messages =
        publisher_count * MESSAGES_PER_PUBLISHER;

    int received = 0;

    auto start =
        std::chrono::steady_clock::now();

    std::thread receiver([&]() {
        std::string pending;

        while (received < total_messages) {
            ssize_t bytes_received = recv(
                subscriber_fd,
                buffer,
                sizeof(buffer),
                0
            );

            if (bytes_received <= 0) {
                std::cerr << "Subscriber connection failed\n";
                return;
            }

            pending.append(buffer, bytes_received);

            std::size_t newline;

            while (
                (newline = pending.find('\n'))
                != std::string::npos
            ) {
                ++received;
                pending.erase(0, newline + 1);
            }
        }
    });

    std::vector<std::thread> publishers;

    for (int p = 0; p < publisher_count; ++p) {
        publishers.emplace_back([&, p]() {
            for (
                int i = 0;
                i < MESSAGES_PER_PUBLISHER;
                ++i
            ) {
                std::string message =
                    "PUBLISH " +
                    topic +
                    " publisher-" +
                    std::to_string(p) +
                    "-message-" +
                    std::to_string(i) +
                    "\n";

                if (!send_all(
                        publisher_fds[p],
                        message.data(),
                        message.size()
                    )) {
                    std::cerr
                        << "Publisher failed to send\n";
                    return;
                }
            }
        });
    }

    for (auto& publisher : publishers) {
        publisher.join();
    }

    receiver.join();

    auto end =
        std::chrono::steady_clock::now();

    double seconds =
        std::chrono::duration<double>(
            end - start
        ).count();

    double throughput =
        total_messages / seconds;

    std::cout
        << publisher_count
        << " publishers: "
        << throughput
        << " messages/sec"
        << " ("
        << total_messages
        << " messages)\n";

    for (int fd : publisher_fds) {
        close(fd);
    }

    close(subscriber_fd);
}

int main() {
    run_test(1);
    run_test(2);
    run_test(4);
    run_test(8);

    return 0;
}
