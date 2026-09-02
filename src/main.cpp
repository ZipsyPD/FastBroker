#include <iostream>
#include "broker/broker.hpp"

int main() {
    cout << "test commit";
    Broker test_broker(9090);
    test_broker.run();
    return 0;
}
