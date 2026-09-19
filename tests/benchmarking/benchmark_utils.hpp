#pragma once

#include <cstddef>

int connect_client();

bool send_all(
    int fd,
    const char* data,
    std::size_t length
);
