#include <iostream>
#include "broker/broker.hpp"

int main() {
    Broker test_broker(9090);
    test_broker.run();
    return 0;
}
