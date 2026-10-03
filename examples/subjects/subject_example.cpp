/**
 * Subject Example
 * 
 * Demonstrates the Subject which is both an Observable and Observer.
 * It can multicast values to multiple subscribers.
 */

#include <Arduino.h>
#include "../../include/micro-reactive.h"
#include <iostream>
#include <string>

using namespace rx;

void traditional_subject_example() {
    std::cout << "=== Traditional Subject Example ===" << std::endl;
    
    // Create a subject
    auto subject = CreateSubject<int>();
    
    // Create observers
    auto observer1 = CreateObserver<int>(
        [](int value) { 
            std::cout << "Observer 1 received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Observer 1 completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Observer 1 error: " << e.what() << std::endl; 
        }
    );
    
    auto observer2 = CreateObserver<int>(
        [](int value) { 
            std::cout << "Observer 2 received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Observer 2 completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Observer 2 error: " << e.what() << std::endl; 
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
    
    std::cout << std::endl;
}

void fluent_subject_example() {
    std::cout << "=== Fluent Subject Example ===" << std::endl;
    
    // Create a subject
    auto subject = CreateSubject<std::string>();
    
    // Subscribe using fluent interface
    Observable(subject)
        .Map<std::string>([](const std::string& s) { 
            return "Processed: " + s; 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent observer received: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent observer completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent observer error: " << e.what() << std::endl; 
            }
        );
    
    // Push values
    subject->OnNext("Hello");
    subject->OnNext("World");
    subject->OnNext("RxCpp");
    
    // Complete
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void subject_multicast_example() {
    std::cout << "=== Subject Multicast Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Subscribe multiple observers at different times
    std::cout << "Subscribing Observer A..." << std::endl;
    auto subA = subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Observer A: " << value << std::endl; 
        },
        []() { 
            std::cout << "Observer A completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Observer A error: " << e.what() << std::endl; 
        }
    ));
    
    // Emit some values
    subject->OnNext(1);
    subject->OnNext(2);
    
    // Subscribe another observer
    std::cout << "Subscribing Observer B..." << std::endl;
    auto subB = subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Observer B: " << value << std::endl; 
        },
        []() { 
            std::cout << "Observer B completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Observer B error: " << e.what() << std::endl; 
        }
    ));
    
    // Emit more values (both observers should receive these)
    subject->OnNext(3);
    subject->OnNext(4);
    
    // Unsubscribe Observer A
    std::cout << "Unsubscribing Observer A..." << std::endl;
    subA->Dispose();
    
    // Emit final values (only Observer B should receive these)
    subject->OnNext(5);
    subject->OnNext(6);
    
    // Complete
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void subject_as_bridge_example() {
    std::cout << "=== Subject as Bridge Example ===" << std::endl;
    
    // Create a subject to bridge between different observables
    auto bridge_subject = CreateSubject<int>();
    
    // Subscribe to the bridge
    bridge_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Bridge received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Bridge completed" << std::endl; 
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
    
    std::cout << std::endl;
}

void subject_error_handling_example() {
    std::cout << "=== Subject Error Handling Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Subscribe with error handling
    subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Value before error: " << value << std::endl; 
        },
        []() { 
            std::cout << "This should not be called after error" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error handled: " << e.what() << std::endl; 
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
    
    std::cout << std::endl;
}

void subject_with_operators_example() {
    std::cout << "=== Subject with Operators Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto filtered = Filter(subject->AsObservable(), [](int x) { return x > 15; });
    auto mapped = Map<int, std::string>(filtered, [](int x) { 
        return "Value: " + std::to_string(x); 
    });
    
    mapped->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        }
    ));
    
    // Fluent approach
    Observable(subject)
        .Filter([](int x) { return x <= 15; })
        .Map<std::string>([](int x) { 
            return "Small value: " + std::to_string(x); 
        })
        .Subscribe([](const std::string& value) { 
            std::cout << "Fluent - " << value << std::endl; 
        });
    
    // Emit test values
    subject->OnNext(5);
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnNext(25);
    subject->OnNext(8);
    
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Subject Examples" << std::endl;
    std::cout << "===============" << std::endl;
    
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
