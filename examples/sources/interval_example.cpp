#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Interval Source Example
 * 
 * Demonstrates the Interval source which emits values at
 * regular intervals for a specified count.
 */

void traditional_interval_example() {
    Serial.println("=== Traditional Interval Example ===");
    
    unsigned long start_time = millis();
    
    // Create an interval that emits every 200ms for 5 values
    auto interval_observable = Interval<int>(std::chrono::milliseconds(200), 5);
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [start_time](int value) { 
            unsigned long elapsed = millis() - start_time;
            Serial.print("Interval value "); 
            Serial.print(value); 
            Serial.print(" at "); 
            Serial.print(elapsed); 
            Serial.println("ms"); 
        },
        []() { 
            Serial.println("Interval completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Interval error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to the observable
    auto subscription = interval_observable->Subscribe(observer);
    
    // Wait for interval to complete
    delay(1200);
    
    Serial.println();
}

void fluent_interval_example() {
    Serial.println("=== Fluent Interval Example ===");
    
    unsigned long start_time = millis();
    
    // Use fluent interface with interval
    From(Interval<int>(std::chrono::milliseconds(150), 3))
        .Subscribe(
            [start_time](int value) { 
                unsigned long elapsed = millis() - start_time;
                Serial.print("Fluent interval "); 
                Serial.print(value); 
                Serial.print(" at "); 
                Serial.print(elapsed); 
                Serial.println("ms"); 
            },
            []() { 
                Serial.println("Fluent interval completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for interval
    delay(600);
    
    Serial.println();
}

void interval_with_operators_example() {
    Serial.println("=== Interval with Operators Example ===");
    
    // Traditional approach - interval with filtering and transformation
    auto interval_obs = Interval<int>(std::chrono::milliseconds(100), 10);
    auto filtered_obs = Filter(interval_obs, [](int x) { return x % 2 == 0; });
    auto mapped_obs = Map<int, std::string>(filtered_obs, [](int x) { 
        return "Even tick: " + std::to_string(x); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional interval chain completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional interval chain error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Wait for first interval
    delay(1200);
    
    // Fluent approach
    From(Interval<int>(std::chrono::milliseconds(100), 6))
        .Filter([](int x) { return x % 2 == 1; })
        .Map<std::string>([](int x) { 
            return "Odd tick: " + std::to_string(x); 
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent - "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent interval chain completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent interval chain error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for second interval
    delay(800);
    
    Serial.println();
}

void interval_throttle_example() {
    Serial.println("=== Interval with Throttle Example ===");
    
    // Fast interval with throttling
    From(Interval<int>(std::chrono::milliseconds(50), 20))
        .Throttle(3)  // Only emit every 3rd value
        .Subscribe(
            [](int value) { 
                Serial.print("Throttled interval value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Throttled interval completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Throttled interval error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for completion
    delay(1200);
    
    Serial.println();
}

void interval_take_example() {
    Serial.println("=== Interval with Take Example ===");
    
    // Long interval but take only first 3 values
    From(Interval<int>(std::chrono::milliseconds(200), 10))
        .Take(3)
        .Subscribe(
            [](int value) { 
                Serial.print("Taking only first 3: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Take completed early"); 
            },
            [](const std::exception& e) { 
                Serial.print("Take interval error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for completion
    delay(800);
    
    Serial.println();
}

void interval_cancellation_example() {
    Serial.println("=== Interval Cancellation Example ===");
    
    auto interval_obs = Interval<int>(std::chrono::milliseconds(100), 20);
    
    auto subscription = interval_obs->Subscribe(CreateObserver<int>(
        [](int value) {
            Serial.print("Interval value before cancellation: "); 
            Serial.println(value);
        },
        []() {
            Serial.println("Interval completed (unexpected)"); 
        },
        [](const std::exception& e) {
            Serial.print("Interval cancellation error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Let it run for a few emissions
    delay(350);
    
    // Cancel subscription
    subscription->Dispose();
    Serial.println("Interval subscription cancelled"); 
    
    // Wait to see if interval still fires (it shouldn't)
    delay(500);
    Serial.println("Interval cancellation test completed"); 
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Interval Source Examples");
    Serial.println("=======================");
    
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
