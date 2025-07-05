#include <iostream>
#include "micro-reactive.h"

int main() {
    std::cout << "Testing basic compilation..." << std::endl;
    
    // Test basic Range
    auto source = rx::Range(1, 3, 1);
    std::cout << "Range created successfully" << std::endl;
    
    // Test basic Subject
    auto subject = rx::CreateSubject<int>();
    std::cout << "Subject created successfully" << std::endl;
    
    std::cout << "Basic compilation test passed!" << std::endl;
    return 0;
}
