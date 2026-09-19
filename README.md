# FastBroker

FastBroker is a multithreaded C++ TCP publish/subscribe event broker that supports persistent per-topic logs, replay support, per-client outbound queues, and bounded backpressure.

## Features

- TCP-based subscribe/publish messaging
- Multiple concurrent clients
- Per-client sender queues
- Backpressure with bounded queued bytes
- Persisten append-only topic logs
- Message replay by offset
- Startup recovery of topic offsets
- Correctness and performance benchmarks

## Protocol

Supported commands:

PING

SUBSCIBRE <topic>

PUBLISH <topic> <message>

REPLAY <topic> <offset>

Example:

SUBSCRIBE sports
PUBLISH sports hello
MESSAGE sports hello

## Architecture

Publisher
    |
handle_client
    |
handle_publish
    |
    + -> persist to topic log
    |
    + -> enqueue for subscribers
                    |
                    sender thread
                    |
                    TCP Send

## Build
In the root folder of the project:
*Requires C++17 compatible compiler.
mkdir -p build
g++ -std=c++17 src/*.cpp -pthread -o build/fastbroker

Or you can let something like Ninja create the build for you:
cmake -S . -B build -G Ninja
cmake --build build

Then to run it:
./build/fastbroker

use nclocalhost 9090 for the normal message testing

## Correctness Tests

Run: 
pytest tests/correctness 

Covers:
publish/subscribe, replay, ping, invalid offsets, topic isolation, unknown commands, malformed publish requests

## Benchmarks

Benchmarks were run locally with one broker process on the same machine
Results depend heavily on system power mode and machine configuration
* Running it on quiet mode cut throughput in half! *

| Benchmark | Result |
|---|---:|
| Throughput | ~129.5k messages/sec |
| Median latency (p50) | ~60 µs |
| p95 latency | ~90 µs |
| p99 latency | ~160 µs typical |
| Scaling | ~70–79k msg/s with 2–8 publishers |

### Throughput
1 publisher, 1 subscriber, 100,000 messages, localhost.

### Latency
1 publisher, 1 subscriber, sequential one-message-at-a-time delivery,
10,000 samples per run.

### Scaling
1 subscriber with 1, 2, 4, and 8 concurrent publishers.

## Persistence
Each topic is stored in logs/<topic>.log.
Messages are assigned monotonically increasing offsets.
On startup, FastBroker scans existing logs to recover the next offset.

## Replay

REPLAY sports 2
will replay messages from offset 2 onward.

## Project Structure

src/
include/
tests/
├── correctness/
└── benchmarking/
    ├── benchmark_utils.cpp
    ├── benchmark_utils.hpp
    ├── throughput_benchmark.cpp
    ├── latency_benchmark.cpp
    ├── scaling_benchmark.cpp
    └── compile_benchmarks.sh

## Known Limitations

- Persistence uses append-only files rather than fsync-backed durability.
- The persistence path uses a global mutex and becomes a bottleneck with concurrent publishers.
- The server currently uses detached threads rather than an event-driven I/O model.
- Files should be truncated with expiring messages

## Comments

In hindsight I should have coded this in Rust given that language is much
more forgiving in memory safety. Oh well. It was very fun to experiment with at first
but benchmarking was rather annoying. 

It always feels bad to have to stop working on a project. I will return one day
to advance this further though. I feel as if this is my pet now. I will return! 
