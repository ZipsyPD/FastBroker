#pragma once

class Broker{
public:
    Broker(int port);
    void run();

private:
    void handle_client(int client_fd);
    int port_;
    int server_fd_;
};
