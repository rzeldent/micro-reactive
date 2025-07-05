/**
 * Main Arduino/PlatformIO entry point for Micro-Reactive Library
 * 
 * This file demonstrates the simplified micro-reactive library usage
 * on ESP32/Arduino platforms.
 */

#include <Arduino.h>
#include "micro-reactive.h"

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
        Serial.println(" completed");
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
    
    Serial.println("Micro-Reactive Arduino Example");
    Serial.println("==============================");
    
    // Example 1: Simple Range Observable
    Serial.println("\n1. Range Observable (1 to 5):");
    auto range_obs = rx::Range(1, 5, 1);
    auto range_observer = std::make_shared<ArduinoObserver>("Range");
    range_obs->Subscribe(range_observer);
    
    // Example 2: Map Operator
    Serial.println("\n2. Range with Map (multiply by 2):");
    auto range_obs2 = rx::Range(1, 3, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs2;
    std::function<int(const int&)> mapFunc = [](const int& x) { return x * 2; };
    auto mapped_obs = rx::Map<int, int>(obs, mapFunc);
    auto map_observer = std::make_shared<ArduinoObserver>("Map");
    mapped_obs->Subscribe(map_observer);
    
    // Example 3: Filter Operator
    Serial.println("\n3. Range with Filter (even numbers only):");
    auto range_obs3 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs2 = range_obs3;
    std::function<bool(const int&)> filterFunc = [](const int& x) { return x % 2 == 0; };
    auto filtered_obs = rx::Filter(obs2, filterFunc);
    auto filter_observer = std::make_shared<ArduinoObserver>("Filter");
    filtered_obs->Subscribe(filter_observer);
    
    // Example 4: Subject
    Serial.println("\n4. Subject Example:");
    auto subject = rx::Subject<int>();
    auto subject_observer = std::make_shared<ArduinoObserver>("Subject");
    subject.Subscribe(subject_observer);
    
    // Emit values
    subject.OnNext(100);
    subject.OnNext(200);
    subject.OnNext(300);
    
    // Example 5: BehaviorSubject with initial value
    Serial.println("\n5. BehaviorSubject Example:");
    auto behavior = rx::BehaviorSubject<int>(42);
    auto behavior_observer = std::make_shared<ArduinoObserver>("Behavior");
    behavior.Subscribe(behavior_observer);
    
    behavior.OnNext(43);
    behavior.OnNext(44);
    
    Serial.println("\nSetup completed!");
}

void loop() {
    // Example of using subjects in loop
    static unsigned long lastTime = 0;
    static int counter = 0;
    static auto loopSubject = rx::Subject<int>();
    static bool initialized = false;
    
    if (!initialized) {
        auto loop_observer = std::make_shared<ArduinoObserver>("Loop");
        loopSubject.Subscribe(loop_observer);
        initialized = true;
    }
    
    // Emit a value every 3 seconds
    if (millis() - lastTime > 3000) {
        loopSubject.OnNext(++counter);
        lastTime = millis();
    }
    
    delay(100);
}