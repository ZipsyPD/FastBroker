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
    // Per client state handling
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

    static constexpr std::size_t MAX_QUEUED_BYTES = 1024 * 1024;

// ----------------------------------------------------------
    // Connection lifecycle
    /* This is an important function as this handles
     * receives FROM a connected client. So handling command line
     * input from a connected client */
    void handle_client(int client_fd);

    /* The antithesis to the handle_client in that it writes
     * BACK to the socket of a client. Mainly consumes from 
     * their queue */
    void sender_loop(int client_fd, std::shared_ptr<ClientState> state);

    bool send_all(int client_fd, const char* data, std::size_t length);

    bool enqueue_message(int client_fd, const std::string& message);

    void handle_disconnect(int client_fd);

// ----------------------------------------------------------
    // Protocol handling
    bool handle_command(int client_fd, const std::string& call);

    bool handle_ping(int client_fd, const std::string& args);

    bool handle_subscribe(int client_fd, const std::string& args);

    bool handle_publish(int client_fd, const std::string& args);

// ----------------------------------------------------------
    // Socket states
    int port_;
    int server_fd_;

// ----------------------------------------------------------
    // Connected clients
    std::unordered_map<int, std::shared_ptr<ClientState>> clients_;
    std::mutex clients_mutex_;

// ----------------------------------------------------------
    // Subscriber handling
    std::unordered_map<std::string, std::unordered_set<int>> subscribers_;

    std::mutex subscribers_mutex_;

// ----------------------------------------------------------
    // Persistence
    
    bool persist_message(const std::string& topic, const std::string& payload);

    std::mutex persistence_mutex_;

    // For each log file
    std::unordered_map<std::string, std::size_t> next_offsets_;

    void initialize_offsets();

    bool replay_messages(int client_fd, const std::string& topic, std::size_t offset);
};
