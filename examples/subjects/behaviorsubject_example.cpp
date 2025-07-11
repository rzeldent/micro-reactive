/**
 * BehaviorSubject Example
 * 
 * Demonstrates the BehaviorSubject which stores the latest value
 * and immediately emits it to new subscribers.
 */

#include "../../include/micro-reactive.h"
#include <iostream>
#include <string>

using namespace rx;

void traditional_behaviorsubject_example() {
    std::cout << "=== Traditional BehaviorSubject Example ===" << std::endl;
    
    // Create a behavior subject with initial value
    auto behavior_subject = CreateBehaviorSubject<int>(42);
    
    // First observer - should immediately receive the initial value
    std::cout << "Subscribing Observer 1..." << std::endl;
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
    
    auto sub1 = behavior_subject->Subscribe(observer1);
    
    // Push new values
    std::cout << "Emitting new values..." << std::endl;
    behavior_subject->OnNext(100);
    behavior_subject->OnNext(200);
    
    // Second observer - should immediately receive the latest value (200)
    std::cout << "Subscribing Observer 2..." << std::endl;
    auto observer2 = CreateObserver<int>(
        [](int value) { 
            std::cout << "Observer 2 received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Observer 2 completed" << std::endl; 
        }
    );
    
    auto sub2 = behavior_subject->Subscribe(observer2);
    
    // Push more values (both observers should receive these)
    behavior_subject->OnNext(300);
    behavior_subject->OnNext(400);
    
    // Complete the subject
    behavior_subject->OnCompleted();
    
    std::cout << std::endl;
}

void fluent_behaviorsubject_example() {
    std::cout << "=== Fluent BehaviorSubject Example ===" << std::endl;
    
    // Create behavior subject with string initial value
    auto behavior_subject = CreateBehaviorSubject<std::string>("Initial");
    
    // Subscribe using fluent interface
    Observable(behavior_subject)
        .Map<std::string>([](const std::string& s) { 
            return "Processed: " + s; 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent observer received: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent observer completed" << std::endl; 
            }
        );
    
    // Push new values
    behavior_subject->OnNext("Hello");
    behavior_subject->OnNext("World");
    
    // Complete
    behavior_subject->OnCompleted();
    
    std::cout << std::endl;
}

void behaviorsubject_late_subscription_example() {
    std::cout << "=== BehaviorSubject Late Subscription Example ===" << std::endl;
    
    auto behavior_subject = CreateBehaviorSubject<int>(0);
    
    // Emit values before any subscription
    std::cout << "Emitting values before subscription..." << std::endl;
    behavior_subject->OnNext(10);
    behavior_subject->OnNext(20);
    behavior_subject->OnNext(30);
    
    // Late subscriber should immediately get the latest value (30)
    std::cout << "Late subscriber joining..." << std::endl;
    behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Late subscriber received: " << value << std::endl; 
        },
        []() { 
            std::cout << "Late subscriber completed" << std::endl; 
        }
    ));
    
    // Emit more values
    behavior_subject->OnNext(40);
    behavior_subject->OnNext(50);
    
    // Another late subscriber
    std::cout << "Another late subscriber joining..." << std::endl;
    behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Second late subscriber received: " << value << std::endl; 
        }
    ));
    
    behavior_subject->OnNext(60);
    behavior_subject->OnCompleted();
    
    std::cout << std::endl;
}

void behaviorsubject_vs_subject_example() {
    std::cout << "=== BehaviorSubject vs Subject Example ===" << std::endl;
    
    // Regular subject
    auto regular_subject = CreateSubject<int>();
    
    // Behavior subject with initial value
    auto behavior_subject = CreateBehaviorSubject<int>(999);
    
    // Emit values before subscription
    std::cout << "Emitting values before subscription..." << std::endl;
    regular_subject->OnNext(10);
    regular_subject->OnNext(20);
    behavior_subject->OnNext(100);
    behavior_subject->OnNext(200);
    
    // Subscribe to both
    std::cout << "Subscribing to regular subject:" << std::endl;
    regular_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Regular subject: " << value << std::endl; 
        }
    ));
    
    std::cout << "Subscribing to behavior subject:" << std::endl;
    behavior_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Behavior subject: " << value << std::endl; 
        }
    ));
    
    // Emit new values to both
    std::cout << "Emitting new values..." << std::endl;
    regular_subject->OnNext(30);
    behavior_subject->OnNext(300);
    
    regular_subject->OnCompleted();
    behavior_subject->OnCompleted();
    
    std::cout << std::endl;
}

void behaviorsubject_state_tracking_example() {
    std::cout << "=== BehaviorSubject State Tracking Example ===" << std::endl;
    
    // Use behavior subject to track application state
    auto state_subject = CreateBehaviorSubject<std::string>("Idle");
    
    // Subscribe to state changes
    state_subject->Subscribe(CreateObserver<std::string>(
        [](const std::string& state) { 
            std::cout << "Application state: " << state << std::endl; 
        }
    ));
    
    // Simulate state changes
    state_subject->OnNext("Loading");
    
    // New component subscribes and immediately gets current state
    std::cout << "New component subscribing..." << std::endl;
    state_subject->Subscribe(CreateObserver<std::string>(
        [](const std::string& state) { 
            std::cout << "New component sees state: " << state << std::endl; 
        }
    ));
    
    state_subject->OnNext("Processing");
    state_subject->OnNext("Complete");
    
    state_subject->OnCompleted();
    
    std::cout << std::endl;
}

void behaviorsubject_with_operators_example() {
    std::cout << "=== BehaviorSubject with Operators Example ===" << std::endl;
    
    auto behavior_subject = CreateBehaviorSubject<int>(5);
    
    // Traditional approach
    auto filtered = Filter(behavior_subject->AsObservable(), [](int x) { return x > 10; });
    auto mapped = Map<int, std::string>(filtered, [](int x) { 
        return "Large value: " + std::to_string(x); 
    });
    
    mapped->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        }
    ));
    
    // Fluent approach
    Observable(behavior_subject)
        .Filter([](int x) { return x <= 10; })
        .Map<std::string>([](int x) { 
            return "Small value: " + std::to_string(x); 
        })
        .Subscribe([](const std::string& value) { 
            std::cout << "Fluent - " << value << std::endl; 
        });
    
    // Emit test values
    behavior_subject->OnNext(3);   // Should trigger small value output
    behavior_subject->OnNext(15);  // Should trigger large value output
    behavior_subject->OnNext(8);   // Should trigger small value output
    behavior_subject->OnNext(25);  // Should trigger large value output
    
    behavior_subject->OnCompleted();
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "BehaviorSubject Examples" << std::endl;
    std::cout << "=======================" << std::endl;
    
    traditional_behaviorsubject_example();
    fluent_behaviorsubject_example();
    behaviorsubject_late_subscription_example();
    behaviorsubject_vs_subject_example();
    behaviorsubject_state_tracking_example();
    behaviorsubject_with_operators_example();
}

void loop() {
    // Nothing to do in loop
}
