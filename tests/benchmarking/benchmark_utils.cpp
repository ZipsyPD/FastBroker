#include "benchmark_utils.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

constexpr const char* HOST = "127.0.0.1";
constexpr int PORT = 9090;

// Returns our file descriptor for the socket!
int connect_client() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        std::cerr << "Failed to create socket\n";
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);

    if (inet_pton(AF_INET, HOST, &address.sin_addr) <= 0) {
        std::cerr << "Invalid address\n";
        close(fd);
        return -1;
    }

    if (connect(
            fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)
            ) == -1) {
        std::cerr << "Failed to connect to broker\n";
        close(fd);
        return -1;
    }
    return fd;
}

bool send_all(int fd, const char* data, std::size_t length) {
    std::size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t bytes_sent = send(
                fd,
                data + total_sent,
                length - total_sent,
                0
                );

        if (bytes_sent <= 0) {
            return false;
        }

        total_sent += bytes_sent;
    }

    return true;
}

