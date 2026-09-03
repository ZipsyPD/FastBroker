#include <cstring>
#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "broker/broker.hpp"
#include <iostream>

Broker::Broker(int port) 
    : port_(port),
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
        std::thread worker(
                &Broker::handle_client,
                this,
                client_fd
                );

        worker.detach();
    } 
}
void Broker::handle_client(int client_fd){ 
    std::cout << "Client connected with fd: " << client_fd << '\n';
    char buffer[1024]{};
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
        buffer[bytes_received] = '\0';
        // Start of send handling
        const char* response = "Message received\n";

        if (!send_all(
                client_fd,
                response,
                std::strlen(response)
        )) {
            break;
        }

        std::cout << "Received: " << buffer << '\n';
    }
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
