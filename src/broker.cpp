#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <iostream>

#include <memory>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <string>
#include <cstring>

#include "broker/broker.hpp"

Broker::Broker(int port): 
    port_(port),
    server_fd_(-1) {
    }

void Broker::run() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1){
        std::cerr << "Socket creation failed\n";
        return;
    }
    
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    address.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(
            server_fd_,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)
        ) == -1) {
        std::cerr << "Failed to bind to socket\n";
        return;
    }

    if (listen(server_fd_, 10) == -1) {
        std::cerr << "Failed to listen on socket\n";
        return;
    }
   
    sockaddr_in client_address {};
    while (true) {
        // Accept, receive, send message
        socklen_t client_address_len = sizeof(client_address); 

        int client_fd = accept(
                server_fd_,
                reinterpret_cast<sockaddr*>(&client_address),
                &client_address_len
                );
        
        if (client_fd == -1) {
            std::cerr << "Client failed to be accepted \n";
            continue;
        }

        auto state = std::make_shared<ClientState>();
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_[client_fd] = state;
        }

        std::thread receiver(
                &Broker::handle_client,
                this,
                client_fd
                );
       
        std::thread sender(
                &Broker::sender_loop,
                this,
                client_fd,
                state
                );

        receiver.detach();
        sender.detach();

    } 
}
void Broker::handle_client(int client_fd){ 
    std::cout << "Client connected with fd: " << client_fd << '\n';
    char buffer[1024]{};
    std::string pending;
    while (true) {
        // Start of receive handling
        ssize_t bytes_received = recv(
                client_fd, 
                buffer, 
                sizeof(buffer) - 1,
                0
                );

        if (bytes_received == -1) {
            std::cerr << "Failed to receive data\n";
            break;
        }

        if (bytes_received == 0) {
            break;
        }
        pending.append(buffer, bytes_received);
        // Append one recv to the "pending" stream
        for (
            std::size_t place = pending.find('\n'); 
            place != std::string::npos; 
            place = pending.find('\n')
            ) {
            std::string message = pending.substr(0, place);
            if (!handle_command(client_fd, message)){
                handle_disconnect(client_fd);
                close(client_fd);
                return;
            }
            pending.erase(0, place + 1);
        }
        
    }
    handle_disconnect(client_fd);
    close(client_fd);
}

// Continuously sends until all bytes are sent
bool Broker::send_all(int client_fd, const char* data, std::size_t length){
    std::size_t total_len = 0;
    while (total_len < length) {
        ssize_t bytes_sent = send(
            client_fd,
            data + total_len,
            length - total_len,
            0
        );
        if (bytes_sent == -1) {
            std::cerr << "Failed to send data\n";
            return false;
        }   
        if (bytes_sent == 0) {
            std::cerr << "Connection closed while sending bytes\n";
            return false;
        }
        total_len += bytes_sent;
    }
    return true;
}

bool Broker::handle_command(int client_fd, const std::string& call){
    std::size_t space = call.find(' ');
    std::string command = 
        (space == std::string::npos) 
        ? call 
        : call.substr(0, space);
    if (command == "PING") {
        return handle_ping(client_fd, call);
    } else if (command == "SUBSCRIBE") {
        return handle_subscribe(client_fd, call);
    } else if (command == "PUBLISH") {
        return handle_publish(client_fd, call);
    } else {
        std::string unknown = 
            "Unknown command. Available commands are (PUBLISH _ _, PING, SUBSCRIBE _)\n";
        if (!send_all(
                    client_fd,
                    unknown.data(),
                    unknown.size()
                    ) ) {
            std::cerr << "Error handling client sending";
        }
        return true;
    }
}
bool Broker::handle_ping(int client_fd, const std::string&){
    const char* pong = "PONG\n";
    if (!send_all(client_fd,
                  pong,
                  std::strlen(pong)
                  )) {
        std::cerr << "Ping could not be sent\n";
        return false;
    } 
    return true;
}
bool Broker::handle_subscribe(int client_fd, const std::string& call){
    /* Can use .lock() and .unlock() here but was recommended
     * not to use it to not have to deal with forgetting about
     * locking */
    std::size_t space = call.find(' ');

    if (space == std::string::npos){
        const char* error = 
            "Incorrect usage of subscribe: should be (SUBSCRIBE topic)\n";
        return send_all(
                client_fd,
                error,
                std::strlen(error)
                );
    }

    std::string topic = call.substr(space + 1);
    if (topic.empty()) {
        const char* error = 
            "Incorrect usage of subscribe: should be (SUBSCRIBE topic)\n";

        return send_all(
                client_fd,
                error,
                std::strlen(error)
                );
    }

    // Add client to the topic's subscribe set
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        subscribers_[topic].insert(client_fd);
    }
    std::string confirmation = "Subscribed to: " + topic + "\n";

    // P.S I hate this nvim/treesitter indentation change
    if (!send_all(client_fd,
                // Just learned this gives us the underlying char ptr!
                confirmation.data(),
                confirmation.size()
                )) {
        std::cerr << "Client subscribed but confirmation not sent";
        return false;
    }
    return true;
}

void Broker::handle_disconnect(int client_fd){
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        for (auto& [topic, subscribers] : subscribers_) {
            subscribers.erase(client_fd);
        }
    }
}

bool Broker::handle_publish(int client_fd, const std::string& call){
    std::size_t first_space = call.find(' ');

    if (first_space == std::string::npos){
        const char* error = 
            "Incorrect usage of publish: should be "
            "(PUBLISH topic message)\n";

        return send_all(
                client_fd,
                error,
                std::strlen(error)
                );
    }

    std::size_t second_space = 
        call.find(' ', first_space + 1);

    if (second_space == std::string::npos) {
        const char* error = 
            "Incorrect usage of publish: should be "
            "(PUBLISH topic message)\n";

        return send_all(
                client_fd,
                error,
                std::strlen(error)
                );
    }

    std::string topic = call.substr(
            first_space + 1,
            second_space - first_space - 1
            );

    std::string payload =
        call.substr(second_space + 1);

    if (topic.empty() || payload.empty()) {
        const char* error = 
            "Incorrect usage of publish: should be "
            "(PUBLISH topic message)\n";

        return send_all(
                client_fd,
                error,
                std::strlen(error)
                );
    }
    std::string outgoing = "MESSAGE " + topic + " " + payload + "\n";
    std::vector<int> recipients;

    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto it = subscribers_.find(topic);

        if (it != subscribers_.end()) {
            for (int fd: it->second) {
                recipients.push_back(recipient_fd);
            }
        }
    }
    /* Changing this to a model of adding to a queue and handing
     * it off to a thread so that it can be modified in parallel */
    for (int recipient_fd: recipients) {
        std::shared_ptr<ClientState> state;

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            
            auto it = client_.find(recipient_fd);

            if(it == clients_.end()) {
                continue;
            }

            state = it->second;
        }
        
        /* This lock here is needed for the publisher pushing in,
         * sender popping, and disconnect setting disconnect to false */
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->outbound.push(outgoing);
        }

        state->cv.notify_one();
    }
    return true;
}

void Broker::sender_loop(int client_fd, std::shared_ptr<ClientState> state){
    while (true) {
        std::unique_lock<std::mutex> lock(state->mutex);

        state->cv.wait(lock, [&] {
                return !state->outbound.empty()
                    || !state->connected;
                    });

        if (!state->connected && state->outbound.empty()) {
            break;
        }

        std::string message = state->outbound.front();
        state->outbound.pop();

        lock.unlock;

        if (!send_all(
                    client_fd,
                    message.data(),
                    message.size()
                    )) {
            break;
        }
    }
}
