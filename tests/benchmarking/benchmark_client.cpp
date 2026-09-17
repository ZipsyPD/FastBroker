#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>

constexpr const char* HOST = "127.0.0.1";
constexpr int PORT = 9090;
constexpr int MESSAGE_COUNT = 10000;

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

    connect(
            fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)
           );

}
