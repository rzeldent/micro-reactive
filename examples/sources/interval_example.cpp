/**
 * Interval Source Example
 * 
 * Demonstrates the Interval source which emits values at
 * regular intervals for a specified count.
 */

#include <Arduino.h>
#include "../../include/micro-reactive.h"
#include <iostream>
#include <chrono>

using namespace rx;

void traditional_interval_example() {
    std::cout << "=== Traditional Interval Example ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Create an interval that emits every 200ms for 5 values
    auto interval_observable = Interval<int>(std::chrono::milliseconds(200), 5);
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [start_time](int value) { 
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            std::cout << "Interval value " << value << " at " << elapsed << "ms" << std::endl; 
        },
        []() { 
            std::cout << "Interval completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Interval error: " << e.what() << std::endl; 
        }
    );
    
    // Subscribe to the observable
    auto subscription = interval_observable->Subscribe(observer);
    
    // Wait for interval to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    
    std::cout << std::endl;
}

void fluent_interval_example() {
    std::cout << "=== Fluent Interval Example ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Use fluent interface with interval
    From(Interval<int>(std::chrono::milliseconds(150), 3))
        .Subscribe(
            [start_time](int value) { 
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_time).count();
                std::cout << "Fluent interval " << value << " at " << elapsed << "ms" << std::endl; 
            },
            []() { 
                std::cout << "Fluent interval completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    // Wait for interval
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    
    std::cout << std::endl;
}

void interval_with_operators_example() {
    std::cout << "=== Interval with Operators Example ===" << std::endl;
    
    // Traditional approach - interval with filtering and transformation
    auto interval_obs = Interval<int>(std::chrono::milliseconds(100), 10);
    auto filtered_obs = Filter(interval_obs, [](int x) { return x % 2 == 0; });
    auto mapped_obs = Map<int, std::string>(filtered_obs, [](int x) { 
        return "Even tick: " + std::to_string(x); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional interval chain completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional interval chain error: " << e.what() << std::endl; 
        }
    ));
    
    // Wait for first interval
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    
    // Fluent approach
    From(Interval<int>(std::chrono::milliseconds(100), 6))
        .Filter([](int x) { return x % 2 == 1; })
        .Map<std::string>([](int x) { 
            return "Odd tick: " + std::to_string(x); 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent interval chain completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent interval chain error: " << e.what() << std::endl; 
            }
        );
    
    // Wait for second interval
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    
    std::cout << std::endl;
}

void interval_throttle_example() {
    std::cout << "=== Interval with Throttle Example ===" << std::endl;
    
    // Fast interval with throttling
    From(Interval<int>(std::chrono::milliseconds(50), 20))
        .Throttle(3)  // Only emit every 3rd value
        .Subscribe(
            [](int value) { 
                std::cout << "Throttled interval value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Throttled interval completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Throttled interval error: " << e.what() << std::endl; 
            }
        );
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    
    std::cout << std::endl;
}

void interval_take_example() {
    std::cout << "=== Interval with Take Example ===" << std::endl;
    
    // Long interval but take only first 3 values
    From(Interval<int>(std::chrono::milliseconds(200), 10))
        .Take(3)
        .Subscribe(
            [](int value) { 
                std::cout << "Taking only first 3: " << value << std::endl; 
            },
            []() { 
                std::cout << "Take completed early" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Take interval error: " << e.what() << std::endl; 
            }
        );
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    
    std::cout << std::endl;
}

void interval_cancellation_example() {
    std::cout << "=== Interval Cancellation Example ===" << std::endl;
    
    auto interval_obs = Interval<int>(std::chrono::milliseconds(100), 20);
    
    auto subscription = interval_obs->Subscribe(CreateObserver<int>(
        [](int value) {
            std::cout << "Interval value before cancellation: " << value << std::endl;
        },
        []() {
            std::cout << "Interval completed (unexpected)" << std::endl;
        },
        [](const std::exception& e) {
            std::cout << "Interval cancellation error: " << e.what() << std::endl;
        }
    ));
    
    // Let it run for a few emissions
    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    
    // Cancel subscription
    subscription->Dispose();
    std::cout << "Interval subscription cancelled" << std::endl;
    
    // Wait to see if interval still fires (it shouldn't)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "Interval cancellation test completed" << std::endl;
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Interval Source Examples" << std::endl;
    std::cout << "=======================" << std::endl;
    
    traditional_interval_example();
    fluent_interval_example();
    interval_with_operators_example();
    interval_throttle_example();
    interval_take_example();
    interval_cancellation_example();
}

void loop() {
    // Nothing to do in loop
}
