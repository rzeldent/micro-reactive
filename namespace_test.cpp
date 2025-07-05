#include <iostream>
#include "micro-reactive.h"

int main() {
    std::cout << "Testing namespace fixes..." << std::endl;
    
    // Test Empty
    auto empty = rx::Empty<int>();
    auto observer = rx::CreateObserver<int>(
        [](const int& value) { std::cout << "Value: " << value << std::endl; },
        []() { std::cout << "Empty completed" << std::endl; }
    );
    empty->Subscribe(observer);
    
    // Test Never
    auto never = rx::Never<int>();
    never->Subscribe(observer);
    
    // Test Iterate
    std::vector<int> values = {1, 2, 3};
    auto iterate = rx::Iterate<int>(values);
    iterate->Subscribe(observer);
    
    std::cout << "Namespace test completed successfully!" << std::endl;
    return 0;
}
