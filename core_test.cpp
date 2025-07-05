#include <iostream>
#include <memory>

// Only include core headers without all the operators and sources
#include "include/core/observer.h"
#include "include/core/observable.h"
#include "include/core/rx-observer.h" 
#include "include/core/rx-observable.h"

// Test minimal functionality
int main() {
    std::cout << "Testing core headers..." << std::endl;
    
    // Test that we can create an observer
    auto observer = rx::CreateObserver<int>(
        [](const int& value) { 
            std::cout << "Value: " << value << std::endl; 
        }
    );
    
    std::cout << "Core test compiled successfully!" << std::endl;
    return 0;
}
