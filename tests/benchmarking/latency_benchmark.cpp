#include "benchmark_utils.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

constexpr int MESSAGE_COUNT = 10000;

int main() {
    int subscriber_fd = connect_client();
    int publisher_fd = connect_client();

    if (subscriber_fd == -1 || publisher_fd == -1) {
        std::cerr << "Failed to create benchmark clients\n";
        return 1;
    }

    std::string subscribe_message =
        "SUBSCRIBE latency_benchmark\n";

    if (!send_all(
            subscriber_fd,
            subscribe_message.data(),
            subscribe_message.size()
        )) {
        std::cerr << "Failed to subscribe\n";
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
        std::cerr << "Failed to receive subscription confirmation\n";
        return 1;
    }

    std::vector<double> latencies;
    latencies.reserve(MESSAGE_COUNT);

    std::string pending;

    for (int i = 0; i < MESSAGE_COUNT; ++i) {
        std::string publish_message = 
            "PUBLISH latency_benchmark message-" + 
            std::to_string(i) +
            "\n";

        auto start = 
            std::chrono::steady_clock::now();

        if (!send_all(
                    publisher_fd,
                    publish_message.data(),
                    publish_message.size()
                    )) {
            std::cerr << "Failed to publish message\n";
            return 1;
        }

        bool received_message = false;

        while (!received_message) {
            std::size_t newline = pending.find('\n');

            if (newline != std::string::npos) {
                pending.erase(0, newline + 1);
                // Stop loop
                received_message = true;
                break;
            }
            
            ssize_t bytes_received = recv(
                    subscriber_fd,
                    buffer,
                    sizeof(buffer),
                    0
                    );

            if (bytes_received <= 0) {
                std::cerr << "Subcriber connection failed\n";
                return 1;
            }

            pending.append(buffer, bytes_received);

        }
        auto end = std::chrono::steady_clock::now();
        double latency_us = 
            std::chrono::duration<double, std::micro>(end - start).count();
            latencies.push_back(latency_us);
    }

    std::sort(
            latencies.begin(),
            latencies.end()
            );

    // Lambda for percentile
    auto percentile = [&](double p) {
        std::size_t index = 
            static_cast <std::size_t>(
                    p * (latencies.size() - 1)
                    );
        return latencies[index];
    };

    double p50 = percentile(0.50);
    double p95 = percentile(0.95);
    double p99 = percentile(0.99);

    double total = 0.0;

    for (double latency: latencies) {
        total += latency;
    }

    double average =
        total / latencies.size();

    std::cout << "Messages: "
        << MESSAGE_COUNT << '\n';

    std::cout << "Average latency: "
        << average << " us\n";

    std::cout << "p50 latency: "
        << p50 << " us\n";

    std::cout << "p95 latency: "
        << p95 << " us\n";

    std::cout << "p99 latency: "
        << p99 << " us\n";

    close (subscriber_fd);
    close (publisher_fd);

    return 0;

}
