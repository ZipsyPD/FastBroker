#!/bin/bash

set -e

g++ -std=c++17 throughput_benchmark.cpp benchmark_utils.cpp \
    -o throughput_benchmark

g++ -std=c++17 latency_benchmark.cpp benchmark_utils.cpp \
    -o latency_benchmark

g++ -std=c++17 scaling_benchmark.cpp benchmark_utils.cpp \
    -pthread \
    -o scaling_benchmark

echo "All benchmarks compiled successfully."
