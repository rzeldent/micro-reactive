#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * BehaviorSubject Example
 * 
 * Demonstrates the BehaviorSubject which stores the latest value
 * and immediately emits it to new subscribers.
 */

void traditional_behaviorsubject_example() {
    Serial.println("=== Traditional BehaviorSubject Example ===");
    
    // Create a behavior subject with initial value
    auto behavior_subject = CreateBehaviorSubject<int>(42);
    
    // Create observers
    auto observer1 = CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer 1 received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Observer 1 completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Observer 1 error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe observer1 - should immediately receive 42
    auto sub1 = behavior_subject->Subscribe(observer1);
    
    // Emit new values
    behavior_subject->OnNext(100);
    behavior_subject->OnNext(200);
    
    // Subscribe observer2 - should immediately receive 200 (latest value)
    auto observer2 = CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer 2 received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Observer 2 completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Observer 2 error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto sub2 = behavior_subject->Subscribe(observer2);
    
    // Emit more values
    behavior_subject->OnNext(300);
    
    // Complete
    behavior_subject->OnCompleted();
    
    Serial.println();
}

void fluent_behaviorsubject_example() {
    Serial.println("=== Fluent BehaviorSubject Example ===");
    
    // Create a behavior subject with initial value
    auto behavior_subject = CreateBehaviorSubject<std::string>("initial");
    
    // Subscribe using fluent interface
    From(behavior_subject)
        .Map<std::string>([](const std::string& s) { 
            return "Fluent: " + s; 
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent observer received: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent observer completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent observer error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Emit new values
    behavior_subject->OnNext("first");
    behavior_subject->OnNext("second");
    
    // Complete
    behavior_subject->OnCompleted();
    
    Serial.println();
}

void behaviorsubject_latest_value_example() {
    Serial.println("=== BehaviorSubject Latest Value Example ===");
    
    auto behavior_subject = CreateBehaviorSubject<int>(0);
    
    // Subscribe multiple observers at different times
    Serial.println("Subscribing Observer A...");
    auto subA = behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer A: "); 
            Serial.println(value); 
        }
    ));
    
    behavior_subject->OnNext(10);
    behavior_subject->OnNext(20);
    
    Serial.println("Subscribing Observer B (should get 20)...");
    auto subB = behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer B: "); 
            Serial.println(value); 
        }
    ));
    
    behavior_subject->OnNext(30);
    
    Serial.println("Subscribing Observer C (should get 30)...");
    auto subC = behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer C: "); 
            Serial.println(value); 
        }
    ));
    
    behavior_subject->OnCompleted();
    
    Serial.println();
}

void behaviorsubject_with_operators_example() {
    Serial.println("=== BehaviorSubject with Operators Example ===");
    
    auto behavior_subject = CreateBehaviorSubject<int>(5);
    
    // Traditional approach
    auto filtered = Filter(behavior_subject->AsObservable(), [](int x) { return x > 10; });
    auto mapped = Map<int, std::string>(filtered, [](int x) { 
        return "Large: " + std::to_string(x); 
    });
    
    mapped->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - "); 
            Serial.println(value.c_str()); 
        }
    ));
    
    // Fluent approach
    From(behavior_subject)
        .Filter([](int x) { return x <= 10; })
        .Map<std::string>([](int x) { 
            return "Small: " + std::to_string(x); 
        })
        .Subscribe([](const std::string& value) { 
            Serial.print("Fluent - "); 
            Serial.println(value.c_str()); 
        });
    
    // Emit test values
    behavior_subject->OnNext(3);
    behavior_subject->OnNext(8);
    behavior_subject->OnNext(15);
    behavior_subject->OnNext(20);
    behavior_subject->OnNext(5);
    
    behavior_subject->OnCompleted();
    
    Serial.println();
}

void behaviorsubject_error_handling_example() {
    Serial.println("=== BehaviorSubject Error Handling Example ===");
    
    auto behavior_subject = CreateBehaviorSubject<int>(100);
    
    // Subscribe with error handling
    behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Value before error: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("This should not be called after error"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error handled: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Emit some values
    behavior_subject->OnNext(200);
    behavior_subject->OnNext(300);
    
    // Emit an error
    behavior_subject->OnError(std::runtime_error("BehaviorSubject error"));
    
    // These should not be received after error
    behavior_subject->OnNext(400);
    behavior_subject->OnCompleted();
    
    Serial.println();
}

void behaviorsubject_esp32_config_example() {
    Serial.println("=== ESP32 Config BehaviorSubject Example ===");
    
    // Simulate ESP32 configuration that can be updated at runtime
    auto config_subject = CreateBehaviorSubject<int>(115200); // default baud rate
    
    // Multiple components subscribe to config
    config_subject->Subscribe(CreateObserver<int>(
        [](int baud) { 
            Serial.print("UART config updated to: "); 
            Serial.println(baud); 
        }
    ));
    
    config_subject->Subscribe(CreateObserver<int>(
        [](int baud) { 
            Serial.print("Logger config updated to: "); 
            Serial.println(baud); 
        }
    ));
    
    // Update config at runtime
    config_subject->OnNext(9600);
    config_subject->OnNext(57600);
    config_subject->OnNext(115200);
    
    config_subject->OnCompleted();
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("BehaviorSubject Examples");
    Serial.println("========================");
    
    traditional_behaviorsubject_example();
    fluent_behaviorsubject_example();
    behaviorsubject_latest_value_example();
    behaviorsubject_with_operators_example();
    behaviorsubject_error_handling_example();
    behaviorsubject_esp32_config_example();
}

void loop() {
    // Nothing to do in loop
}
