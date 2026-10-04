#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Empty Source Example
 * 
 * Demonstrates the Empty source which immediately completes
 * without emitting any values.
 */

void traditional_empty_example() {
    Serial.println("=== Traditional Empty Example ===");
    
    // Create an empty observable
    auto empty_observable = Empty<int>();
    
    // Create an observer
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("This should never be called! Received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Empty observable completed (as expected)"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to the observable
    auto subscription = empty_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_empty_example() {
    Serial.println("=== Fluent Empty Example ===");
    
    // Use fluent interface with empty observable
    From(Empty<std::string>())
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("This should never be called! Received: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent empty observable completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void empty_with_default_example() {
    Serial.println("=== Empty with DefaultIfEmpty Example ===");
    
    // Traditional approach - empty with default value
    auto empty_obs = Empty<int>();
    auto default_obs = DefaultIfEmpty(empty_obs, 42);
    
    default_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Default value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional - DefaultIfEmpty completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional - DefaultIfEmpty error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(Empty<int>())
        .DefaultIfEmpty(99)
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Default value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent - DefaultIfEmpty completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent - DefaultIfEmpty error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void empty_vs_never_example() {
    Serial.println("=== Empty vs Never Example ===");
    
    Serial.println("Empty observable:");
    From(Empty<int>())
        .Subscribe(
            [](int value) { 
                Serial.print("Empty - Value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Empty - Completed immediately"); 
            },
            [](const std::exception& e) { 
                Serial.print("Empty - Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println("Never observable (will not complete in this example):");
    From(Never<int>())
        .Subscribe(
            [](int value) { 
                Serial.print("Never - Value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Never - This will never be called"); 
            },
            [](const std::exception& e) { 
                Serial.print("Never - Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println("Never observable does not complete or emit values");
    Serial.println();
}

void empty_error_handling_example() {
    Serial.println("=== Empty Error Handling Example ===");
    
    // Empty observables don't emit errors, they just complete
    From(Empty<int>())
        .Catch([](const std::exception& e) -> std::shared_ptr<IObservable<int>> {
            Serial.println("This catch block should not be called");
            return Range(1, 3);
        })
        .Subscribe(
            [](int value) { 
                Serial.print("Value after catch: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Empty with error handling completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Empty with error handling error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Empty Source Examples");
    Serial.println("====================");
    
    traditional_empty_example();
    fluent_empty_example();
    empty_with_default_example();
    empty_vs_never_example();
    empty_error_handling_example();
}

void loop() {
    // Nothing to do in loop
}
