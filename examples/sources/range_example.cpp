#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Range Source Example
 * 
 * Demonstrates the Range source which emits a sequence of integers
 * within a specified range with an optional step value.
 */

void traditional_range_example() {
    Serial.println("=== Traditional Range Example ===");
    
    // Create a range from 1 to 5
    auto range_observable = Range(1, 5);
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Range completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to the observable
    auto subscription = range_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_range_example() {
    Serial.println("=== Fluent Range Example ===");
    
    // Create a range with step value using fluent interface
    From(Range(2, 10, 2))
        .Subscribe(
            [](int value) { 
                Serial.print("Even number: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Even range completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void range_with_operators_example() {
    Serial.println("=== Range with Operators Example ===");
    
    // Traditional approach
    auto range_obs = Range(1, 10);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; });
    auto mapped_obs = Map<int, int>(filtered_obs, [](int x) { return x * x; });
    
    mapped_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Square of even: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional chain completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional chain error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(Range(1, 10))
        .Filter([](int x) { return x % 2 == 0; })
        .Map<int>([](int x) { return x * x; })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Square of even: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent chain completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent chain error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Range Source Examples");
    Serial.println("===================");
    
    traditional_range_example();
    fluent_range_example();
    range_with_operators_example();
}

void loop() {
    // Nothing to do in loop
}
