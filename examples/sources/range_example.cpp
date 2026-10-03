/**
 * Range Source Example
 * 
 * Demonstrates the Range source which emits a sequence of integers
 * within a specified range with an optional step value.
 */

#include "../arduino_mock.h"
#include <micro-reactive.h>
#include <iostream>

using namespace rx;

void traditional_range_example() {
    std::cout << "=== Traditional Range Example ===" << std::endl;
    
    // Create a range from 1 to 5
    auto range_observable = Range(1, 5);
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Range completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    // Subscribe to the observable
    auto subscription = range_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_range_example() {
    std::cout << "=== Fluent Range Example ===" << std::endl;
    
    // Create a range with step value using fluent interface
    From(Range(2, 10, 2))
        .Subscribe(
            [](int value) { 
                std::cout << "Even number: " << value << std::endl; 
            },
            []() { 
                std::cout << "Even range completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void range_with_operators_example() {
    std::cout << "=== Range with Operators Example ===" << std::endl;
    
    // Traditional approach
    auto range_obs = Range(1, 10);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; });
    auto mapped_obs = Map<int, int>(filtered_obs, [](int x) { return x * x; });
    
    mapped_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Square of even: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional chain completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional chain error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach
    From(Range(1, 10))
        .Filter([](int x) { return x % 2 == 0; })
        .Map<int>([](int x) { return x * x; })
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Square of even: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent chain completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent chain error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Range Source Examples" << std::endl;
    std::cout << "===================" << std::endl;
    
    traditional_range_example();
    fluent_range_example();
    range_with_operators_example();
}

void loop() {
    // Nothing to do in loop
}

// For native testing, provide a main that calls setup/loop
#ifndef ARDUINO
int main() {
    setup();
    while (true) {
        loop();
    }
    return 0;
}
#endif

