/**
 * FromVector Source Example
 * 
 * Demonstrates the FromVector source which emits all elements
 * from a vector in sequence.
 */

#include "../arduino_mock.h"
#include <micro-reactive.h>
#include <iostream>
#include <vector>
#include <string>

using namespace rx;

void traditional_fromvector_example() {
    std::cout << "=== Traditional FromVector Example ===" << std::endl;
    
    // Create a vector of strings
    std::vector<std::string> fruits = {"apple", "banana", "orange", "grape"};
    
    // Create observable from vector
    auto vector_observable = FromVector(fruits);
    
    // Create an observer
    auto observer = CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Fruit: " << value << std::endl; 
        },
        []() { 
            std::cout << "All fruits processed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    // Subscribe to the observable
    auto subscription = vector_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_fromvector_example() {
    std::cout << "=== Fluent FromVector Example ===" << std::endl;
    
    // Create a vector of integers
    std::vector<int> numbers = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    
    // Use fluent interface
    From(FromVector(numbers))
        .Subscribe(
            [](int value) { 
                std::cout << "Number: " << value << std::endl; 
            },
            []() { 
                std::cout << "All numbers processed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void fromvector_with_operators_example() {
    std::cout << "=== FromVector with Operators Example ===" << std::endl;
    
    std::vector<int> data = {10, 20, 30, 40, 50};
    
    // Traditional approach
    auto vector_obs = FromVector(data);
    auto filtered_obs = Filter(vector_obs, [](int x) { return x > 25; });
    auto mapped_obs = Map<int, double>(filtered_obs, [](int x) { return x / 10.0; });
    
    mapped_obs->Subscribe(CreateObserver<double>(
        [](double value) { 
            std::cout << "Traditional - Scaled value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional processing completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional processing error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach
    From(FromVector(data))
        .Filter([](int x) { return x > 25; })
        .Map<double>([](int x) { return x / 10.0; })
        .Subscribe(
            [](double value) { 
                std::cout << "Fluent - Scaled value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent processing completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent processing error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void fromvector_aggregation_example() {
    std::cout << "=== FromVector Aggregation Example ===" << std::endl;
    
    std::vector<int> values = {1, 2, 3, 4, 5};
    
    // Traditional sum
    auto sum_obs = Sum(FromVector(values));
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Sum: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional sum completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional sum error: " << e.what() << std::endl; 
        }
    ));
    
    // Fluent sum
    From(FromVector(values))
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Sum: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent sum error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "FromVector Source Examples" << std::endl;
    std::cout << "=========================" << std::endl;
    
    traditional_fromvector_example();
    fluent_fromvector_example();
    fromvector_with_operators_example();
    fromvector_aggregation_example();
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

