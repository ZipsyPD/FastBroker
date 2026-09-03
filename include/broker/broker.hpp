#pragma once

class Broker{
public:
    Broker(int port);
    void run();

private:
    void handle_client(int client_fd);
    bool send_all(int client_fd, const char* data, std::size_t length);
    int port_;
    int server_fd_;
};
