cmake_minimum_required(VERSION 3.20)

project(FastBroker LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(fastbroker
    src/main.cpp
    src/broker.cpp
)

target_include_directories(fastbroker
    PRIVATE
    include
)

target_compile_options(fastbroker
    PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)


