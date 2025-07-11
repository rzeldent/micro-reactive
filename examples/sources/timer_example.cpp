/**
 * Timer Source Example
 * 
 * Demonstrates the Timer source which emits a single value
 * after a specified delay.
 */

#include "../../include/micro-reactive.h"
#include <iostream>
#include <chrono>

using namespace rx;

void traditional_timer_example() {
    std::cout << "=== Traditional Timer Example ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Create a timer that fires after 1 second
    auto timer_observable = Timer<int>(std::chrono::milliseconds(1000));
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [start_time](int value) { 
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            std::cout << "Timer fired after " << elapsed << "ms with value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Timer completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Timer error: " << e.what() << std::endl; 
        }
    );
    
    // Subscribe to the observable
    auto subscription = timer_observable->Subscribe(observer);
    
    // Wait for timer to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    
    std::cout << std::endl;
}

void fluent_timer_example() {
    std::cout << "=== Fluent Timer Example ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Use fluent interface with timer
    Observable(Timer<int>(std::chrono::milliseconds(500)))
        .Subscribe(
            [start_time](int value) { 
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_time).count();
                std::cout << "Fluent timer fired after " << elapsed << "ms" << std::endl; 
            },
            []() { 
                std::cout << "Fluent timer completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    // Wait for timer
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    
    std::cout << std::endl;
}

void timer_with_operators_example() {
    std::cout << "=== Timer with Operators Example ===" << std::endl;
    
    // Traditional approach - timer with transformation
    auto timer_obs = Timer<int>(std::chrono::milliseconds(300));
    auto mapped_obs = Map<int, std::string>(timer_obs, [](int x) { 
        return "Timer result: " + std::to_string(x * 100); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional timer chain completed" << std::endl; 
        }
    ));
    
    // Wait for first timer
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    
    // Fluent approach
    Observable(Timer<int>(std::chrono::milliseconds(300)))
        .Map<std::string>([](int x) { 
            return "Fluent timer result: " + std::to_string(x * 200); 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent timer chain completed" << std::endl; 
            }
        );
    
    // Wait for second timer
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    
    std::cout << std::endl;
}

void multiple_timers_example() {
    std::cout << "=== Multiple Timers Example ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Create multiple timers with different delays
    auto timer1 = Timer<int>(std::chrono::milliseconds(200));
    auto timer2 = Timer<int>(std::chrono::milliseconds(400));
    auto timer3 = Timer<int>(std::chrono::milliseconds(600));
    
    timer1->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            std::cout << "Timer 1 fired at " << elapsed << "ms" << std::endl;
        }
    ));
    
    timer2->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            std::cout << "Timer 2 fired at " << elapsed << "ms" << std::endl;
        }
    ));
    
    timer3->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            std::cout << "Timer 3 fired at " << elapsed << "ms" << std::endl;
        }
    ));
    
    // Wait for all timers
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    
    std::cout << std::endl;
}

void timer_cancellation_example() {
    std::cout << "=== Timer Cancellation Example ===" << std::endl;
    
    auto timer_obs = Timer<int>(std::chrono::milliseconds(1000));
    
    auto subscription = timer_obs->Subscribe(CreateObserver<int>(
        [](int value) {
            std::cout << "This should not print - timer was cancelled" << std::endl;
        },
        []() {
            std::cout << "Timer completed (unexpected)" << std::endl;
        }
    ));
    
    // Cancel timer after 200ms
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    subscription->Dispose();
    std::cout << "Timer subscription cancelled" << std::endl;
    
    // Wait to see if timer still fires (it shouldn't)
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::cout << "Timer cancellation test completed" << std::endl;
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Timer Source Examples" << std::endl;
    std::cout << "====================" << std::endl;
    
    traditional_timer_example();
    fluent_timer_example();
    timer_with_operators_example();
    multiple_timers_example();
    timer_cancellation_example();
}

void loop() {
    // Nothing to do in loop
}
