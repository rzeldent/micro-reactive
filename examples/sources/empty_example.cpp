/**
 * Empty Source Example
 * 
 * Demonstrates the Empty source which immediately completes
 * without emitting any values.
 */

#include "../../include/micro-reactive.h"
#include <iostream>

using namespace rx;

void traditional_empty_example() {
    std::cout << "=== Traditional Empty Example ===" << std::endl;
    
    // Create an empty observable
    auto empty_observable = Empty<int>();
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "This should never be called! Received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Empty observable completed (as expected)" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    // Subscribe to the observable
    auto subscription = empty_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_empty_example() {
    std::cout << "=== Fluent Empty Example ===" << std::endl;
    
    // Use fluent interface with empty observable
    Observable(Empty<std::string>())
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "This should never be called! Received: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent empty observable completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void empty_with_default_example() {
    std::cout << "=== Empty with DefaultIfEmpty Example ===" << std::endl;
    
    // Traditional approach - empty with default value
    auto empty_obs = Empty<int>();
    auto default_obs = DefaultIfEmpty(empty_obs, 42);
    
    default_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Default value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional - DefaultIfEmpty completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional - DefaultIfEmpty error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach
    Observable(Empty<int>())
        .DefaultIfEmpty(99)
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Default value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent - DefaultIfEmpty completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent - DefaultIfEmpty error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void empty_vs_never_example() {
    std::cout << "=== Empty vs Never Example ===" << std::endl;
    
    std::cout << "Empty observable:" << std::endl;
    Observable(Empty<int>())
        .Subscribe(
            [](int value) { 
                std::cout << "Empty - Value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Empty - Completed immediately" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Empty - Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << "Never observable (will not complete in this example):" << std::endl;
    Observable(Never<int>())
        .Subscribe(
            [](int value) { 
                std::cout << "Never - Value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Never - This will never be called" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Never - Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << "Never observable does not complete or emit values" << std::endl;
    std::cout << std::endl;
}

void empty_error_handling_example() {
    std::cout << "=== Empty Error Handling Example ===" << std::endl;
    
    // Empty observables don't emit errors, they just complete
    Observable(Empty<int>())
        .Catch([](const std::exception& e) -> std::shared_ptr<IObservable<int>> {
            std::cout << "This catch block should not be called" << std::endl;
            return Range(1, 3);
        })
        .Subscribe(
            [](int value) { 
                std::cout << "Value after catch: " << value << std::endl; 
            },
            []() { 
                std::cout << "Empty with error handling completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Empty with error handling error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Empty Source Examples" << std::endl;
    std::cout << "====================" << std::endl;
    
    traditional_empty_example();
    fluent_empty_example();
    empty_with_default_example();
    empty_vs_never_example();
    empty_error_handling_example();
}

void loop() {
    // Nothing to do in loop
}
