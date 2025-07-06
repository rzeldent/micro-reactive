/**
 * Arduino Example for Micro-Reactive Library (Core Components Only)
 * 
 * This example demonstrates basic usage of the micro-reactive library
 * core components with thread safety and subscription improvements.
 */

#include <Arduino.h>
#include "core.h"
#include "sources.h"
#include "subjects.h"

class ArduinoObserver : public rx::IObserver<int> {
public:
    ArduinoObserver(const String& name) : name_(name) {}
    
    void OnNext(const int& value) override {
        Serial.print(name_);
        Serial.print(": ");
        Serial.println(value);
    }
    
    void OnCompleted() override {
        Serial.print(name_);
        Serial.println(": Completed");
    }
    
    void OnError(const std::exception& e) override {
        Serial.print(name_);
        Serial.print(" error: ");
        Serial.println(e.what());
    }
    
private:
    String name_;
};

void setup() {
    Serial.begin(115200);
    delay(1000);
    
    Serial.println("Micro-Reactive Arduino Example - Core Components with Thread Safety");
    Serial.println("===================================================================");
    
    // Example 1: Simple Range Observable
    Serial.println("\n1. Range Observable (1 to 5):");
    auto range_obs = rx::Range(1, 5);
    auto range_observer = std::make_shared<ArduinoObserver>("Range");
    auto subscription1 = range_obs->Subscribe(range_observer);
    Serial.print("Subscription1 disposed: ");
    Serial.println(subscription1->IsDisposed() ? "true" : "false");
    
    // Example 2: FromVector Observable
    Serial.println("\n2. FromVector Observable:");
    std::vector<int> values = {10, 20, 30, 40, 50};
    auto vector_obs = rx::FromVector(values);
    auto vector_observer = std::make_shared<ArduinoObserver>("Vector");
    auto subscription2 = vector_obs->Subscribe(vector_observer);
    
    // Example 3: Subject with multiple observers
    Serial.println("\n3. Subject with multiple observers:");
    auto subject = std::make_shared<rx::Subject<int>>();
    auto subject_observer1 = std::make_shared<ArduinoObserver>("Subject1");
    auto subject_observer2 = std::make_shared<ArduinoObserver>("Subject2");
    
    auto sub_subscription1 = subject->Subscribe(subject_observer1);
    auto sub_subscription2 = subject->Subscribe(subject_observer2);
    
    subject->OnNext(100);
    subject->OnNext(200);
    subject->OnCompleted();
    
    // Example 4: BehaviorSubject
    Serial.println("\n4. BehaviorSubject:");
    auto behavior_subject = std::make_shared<rx::BehaviorSubject<int>>(999);
    auto behavior_observer = std::make_shared<ArduinoObserver>("Behavior");
    auto behavior_subscription = behavior_subject->Subscribe(behavior_observer);
    
    behavior_subject->OnNext(1000);
    behavior_subject->OnNext(2000);
    behavior_subject->OnCompleted();
    
    // Example 5: Timer Observable (threaded)
    Serial.println("\n5. Timer Observable (500ms delay):");
    auto timer_obs = rx::Timer<int>(std::chrono::milliseconds(500));
    auto timer_observer = std::make_shared<ArduinoObserver>("Timer");
    auto timer_subscription = timer_obs->Subscribe(timer_observer);
    
    // Example 6: Interval Observable (threaded)
    Serial.println("\n6. Interval Observable (200ms, 3 emissions):");
    auto interval_obs = rx::Interval<int>(std::chrono::milliseconds(200), 3);
    auto interval_observer = std::make_shared<ArduinoObserver>("Interval");
    auto interval_subscription = interval_obs->Subscribe(interval_observer);
    
    // Test subscription disposal
    Serial.println("\n7. Testing subscription disposal:");
    subscription1->Dispose();
    Serial.print("Subscription1 disposed after manual disposal: ");
    Serial.println(subscription1->IsDisposed() ? "true" : "false");
    
    Serial.println("\nExample completed. Timer and Interval will continue in background.");
}

void loop() {
    // Nothing in loop - everything runs in background threads or completed synchronously
    delay(1000);
}
