#include <iostream>
#include "include/micro-reactive.h"

using namespace rx;

int main() {
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
    return 0;
}
