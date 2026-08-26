#include <iostream>
#include "broker/broker.hpp"

int main() {
    Broker test_broker;
    std::cout << "mini broker starting!\n";
    test_broker.run();
    return 0;
}
