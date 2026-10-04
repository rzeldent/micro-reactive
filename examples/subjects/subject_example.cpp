#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Subject Example
 * 
 * Demonstrates the Subject which is both an Observable and Observer.
 * It can multicast values to multiple subscribers.
 */

void traditional_subject_example() {
    Serial.println("=== Traditional Subject Example ===");
    
    // Create a subject
    auto subject = CreateSubject<int>();
    
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
    
    // Subscribe observers
    auto sub1 = subject->Subscribe(observer1);
    auto sub2 = subject->Subscribe(observer2);
    
    // Push values through the subject
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnNext(30);
    
    // Complete the subject
    subject->OnCompleted();
    
    Serial.println();
}

void fluent_subject_example() {
    Serial.println("=== Fluent Subject Example ===");
    
    // Create a subject
    auto subject = CreateSubject<std::string>();
    
    // Subscribe using fluent interface
    From(subject)
        .Map<std::string>([](const std::string& s) { 
            return "Processed: " + s; 
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
    
    // Push values
    subject->OnNext("Hello");
    subject->OnNext("World");
    subject->OnNext("RxCpp");
    
    // Complete
    subject->OnCompleted();
    
    Serial.println();
}

void subject_multicast_example() {
    Serial.println("=== Subject Multicast Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Subscribe multiple observers at different times
    Serial.println("Subscribing Observer A...");
    auto subA = subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer A: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Observer A completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Observer A error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Emit some values
    subject->OnNext(1);
    subject->OnNext(2);
    
    // Subscribe another observer
    Serial.println("Subscribing Observer B...");
    auto subB = subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer B: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Observer B completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Observer B error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Emit more values (both observers should receive these)
    subject->OnNext(3);
    subject->OnNext(4);
    
    // Unsubscribe Observer A
    Serial.println("Unsubscribing Observer A...");
    subA->Dispose();
    
    // Emit final values (only Observer B should receive these)
    subject->OnNext(5);
    subject->OnNext(6);
    
    // Complete
    subject->OnCompleted();
    
    Serial.println();
}

void subject_as_bridge_example() {
    Serial.println("=== Subject as Bridge Example ===");
    
    // Create a subject to bridge between different observables
    auto bridge_subject = CreateSubject<int>();
    
    // Subscribe to the bridge
    bridge_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Bridge received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Bridge completed"); 
        }
    ));
    
    // Connect a range observable to the bridge
    auto range_obs = Range(1, 5);
    range_obs->Subscribe(CreateObserver<int>(
        [bridge_subject](int value) { 
            bridge_subject->OnNext(value * 10); // Transform and forward
        },
        [bridge_subject]() { 
            bridge_subject->OnCompleted(); 
        },
        [bridge_subject](const std::exception& e) { 
            bridge_subject->OnError(e); 
        }
    ));
    
    Serial.println();
}

void subject_error_handling_example() {
    Serial.println("=== Subject Error Handling Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Subscribe with error handling
    subject->Subscribe(CreateObserver<int>(
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
    subject->OnNext(100);
    subject->OnNext(200);
    
    // Emit an error
    subject->OnError(std::runtime_error("Something went wrong"));
    
    // These should not be received after error
    subject->OnNext(300);
    subject->OnCompleted();
    
    Serial.println();
}

void subject_with_operators_example() {
    Serial.println("=== Subject with Operators Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto filtered = Filter(subject->AsObservable(), [](int x) { return x > 15; });
    auto mapped = Map<int, std::string>(filtered, [](int x) { 
        return "Value: " + std::to_string(x); 
    });
    
    mapped->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - "); 
            Serial.println(value.c_str()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Filter([](int x) { return x <= 15; })
        .Map<std::string>([](int x) { 
            return "Small value: " + std::to_string(x); 
        })
        .Subscribe([](const std::string& value) { 
            Serial.print("Fluent - "); 
            Serial.println(value.c_str()); 
        });
    
    // Emit test values
    subject->OnNext(5);
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnNext(25);
    subject->OnNext(8);
    
    subject->OnCompleted();
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Subject Examples");
    Serial.println("===============");
    
    traditional_subject_example();
    fluent_subject_example();
    subject_multicast_example();
    subject_as_bridge_example();
    subject_error_handling_example();
    subject_with_operators_example();
}

void loop() {
    // Nothing to do in loop
}
