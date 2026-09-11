#pragma once

#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <condition_variable>
#include <memory>

class Broker{
public:
    Broker(int port);
    void run();

private:
    struct ClientState {
        // Messages waiting to be sent to client
        std::queue<std::string> outbound;
        // Backpressure buffer
        std::size_t queued_bytes = 0;
        std::mutex mutex;
        /* Enables thread sleeping so that the 
         * sleeper can wake up and saves CPU time! */
        std::condition_variable cv;
        // When client disconnects we just turn this false
        bool connected = true;
    };

    // Max queue byte limit
    static constexpr std::size_t MAX_QUEUED_BYTES = 1024 * 1024;

    // Holding client->clientState pairs
    std::unordered_map<int, std::shared_ptr<ClientState>> clients_;
    std::mutex clients_mutex_;

    /* The antithesis to the handle_client in that it writes
     * BACK to the socket of a client. Mainly consumes from 
     * their queue */
    void sender_loop(int client_fd, std::shared_ptr<ClientState> state);

    // General function for inputting into buffer
    bool enqueue_message(int client_fd, const std::string& message);

    /* This is an important function as this handles
     * receives FROM a connected client. So handling command line
     * input from a connected client */
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
