//#include <Arduino.h>
#include <iostream>
#include "micro-reactive.h"

using namespace rx;

void testBasicFunctionality() {
    std::cout << "Testing basic reactive functionality..." << std::endl;
    
    // Test Range
    auto range = Range<int>(1, 3, 1);
    auto observer = CreateObserver<int>(
        [](const int& value) { 
            std::cout << "Value: " << value << std::endl; 
        }
    );
    range->Subscribe(observer);
    
    std::cout << "Basic test completed" << std::endl;
}

int main() {
    std::cout << "Starting micro-reactive compilation test..." << std::endl;
    
    testBasicFunctionality();
    
    return 0;
}
