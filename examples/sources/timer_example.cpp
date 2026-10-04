#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Timer Source Example
 * 
 * Demonstrates the Timer source which emits a single value
 * after a specified delay.
 */

void traditional_timer_example() {
    Serial.println("=== Traditional Timer Example ===");
    
    unsigned long start_time = millis();
    
    // Create a timer that fires after 1 second
    auto timer_observable = Timer<int>(std::chrono::milliseconds(1000));
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [start_time](int value) { 
            unsigned long elapsed = millis() - start_time;
            Serial.print("Timer fired after "); 
            Serial.print(elapsed); 
            Serial.print("ms with value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Timer completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Timer error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to the observable
    auto subscription = timer_observable->Subscribe(observer);
    
    // Wait for timer to complete
    delay(1200);
    
    Serial.println();
}

void fluent_timer_example() {
    Serial.println("=== Fluent Timer Example ===");
    
    unsigned long start_time = millis();
    
    // Use fluent interface with timer
    From(Timer<int>(std::chrono::milliseconds(500)))
        .Subscribe(
            [start_time](int value) { 
                unsigned long elapsed = millis() - start_time;
                Serial.print("Fluent timer fired after "); 
                Serial.print(elapsed); 
                Serial.println("ms"); 
            },
            []() { 
                Serial.println("Fluent timer completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for timer
    delay(700);
    
    Serial.println();
}

void timer_with_operators_example() {
    Serial.println("=== Timer with Operators Example ===");
    
    // Traditional approach - timer with transformation
    auto timer_obs = Timer<int>(std::chrono::milliseconds(300));
    auto mapped_obs = Map<int, std::string>(timer_obs, [](int x) { 
        return "Timer result: " + std::to_string(x * 100); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional timer chain completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional timer chain error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Wait for first timer
    delay(400);
    
    // Fluent approach
    From(Timer<int>(std::chrono::milliseconds(300)))
        .Map<std::string>([](int x) { 
            return "Fluent timer result: " + std::to_string(x * 200); 
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent - "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent timer chain completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent timer chain error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Wait for second timer
    delay(400);
    
    Serial.println();
}

void multiple_timers_example() {
    Serial.println("=== Multiple Timers Example ===");
    
    unsigned long start_time = millis();
    
    // Create multiple timers with different delays
    auto timer1 = Timer<int>(std::chrono::milliseconds(200));
    auto timer2 = Timer<int>(std::chrono::milliseconds(400));
    auto timer3 = Timer<int>(std::chrono::milliseconds(600));
    
    timer1->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            unsigned long elapsed = millis() - start_time;
            Serial.print("Timer 1 fired at "); 
            Serial.print(elapsed); 
            Serial.println("ms");
        },
        []() {
            Serial.println("Timer 1 completed");
        },
        [](const std::exception& e) {
            Serial.print("Timer 1 error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    timer2->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            unsigned long elapsed = millis() - start_time;
            Serial.print("Timer 2 fired at "); 
            Serial.print(elapsed); 
            Serial.println("ms");
        },
        []() {
            Serial.println("Timer 2 completed");
        },
        [](const std::exception& e) {
            Serial.print("Timer 2 error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    timer3->Subscribe(CreateObserver<int>(
        [start_time](int value) {
            unsigned long elapsed = millis() - start_time;
            Serial.print("Timer 3 fired at "); 
            Serial.print(elapsed); 
            Serial.println("ms");
        },
        []() {
            Serial.println("Timer 3 completed");
        },
        [](const std::exception& e) {
            Serial.print("Timer 3 error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Wait for all timers
    delay(800);
    
    Serial.println();
}

void timer_cancellation_example() {
    Serial.println("=== Timer Cancellation Example ===");
    
    auto timer_obs = Timer<int>(std::chrono::milliseconds(1000));
    
    auto subscription = timer_obs->Subscribe(CreateObserver<int>(
        [](int value) {
            Serial.println("This should not print - timer was cancelled");
        },
        []() {
            Serial.println("Timer completed (unexpected)");
        },
        [](const std::exception& e) {
            Serial.print("Timer cancellation error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Cancel timer after 200ms
    delay(200);
    subscription->Dispose();
    Serial.println("Timer subscription cancelled"); 
    
    // Wait to see if timer still fires (it shouldn't)
    delay(1000);
    Serial.println("Timer cancellation test completed"); 
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Timer Source Examples");
    Serial.println("====================");
    
    traditional_timer_example();
    fluent_timer_example();
    timer_with_operators_example();
    multiple_timers_example();
    timer_cancellation_example();
}

void loop() {
    // Nothing to do in loop
}
