#pragma once

#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

class Broker{
public:
    Broker(int port);
    void run();

private:
    void handle_client(int client_fd);
    bool send_all(int client_fd, const char* data, std::size_t length);
    // was about to use a const char* here but not doing that parsing
    bool handle_command(int client_fd, const std::string& call);
    bool handle_ping(int client_fd, const std::string& args);
    bool handle_subscribe(int client_fd, const std::string& args);
    void handle_disconnect(int client_fd);
    bool handle_publish(int client_fd, const std::string& args);
    int port_;
    int server_fd_;
    std::unordered_map<std::string, std::unordered_set<int>> subscribers_;
    std::mutex subscribers_mutex_;
};
