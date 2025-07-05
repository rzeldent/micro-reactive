#include <Arduino.h>
#include "micro-reactive.h"

using namespace rx;

void setup() {
    // Wait for the serial port to initialize
    delay(2000); 
    // Initialize serial communication at 115200 baud rate
    Serial.begin(115200); 
    Serial.println("Starting micro-reactive examples...");

    // Example 1: Range Observable (this should work)
    Serial.println("\n=== Range Observable Example ===");
    auto rangeObs = Range<int>(1, 5, 1);
    auto observer = CreateObserver<int>(
        [](const int& value) { 
            Serial.print("Range value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Range completed"); 
        }
    );
    rangeObs->Subscribe(observer);

    // Example 2: Simple Create Observable using function factory
    Serial.println("\n=== Simple Create Observable Example ===");
    std::function<void(std::shared_ptr<IObserver<int>>)> factory = [](std::shared_ptr<IObserver<int>> obs) {
        obs->OnNext(42);
        obs->OnNext(100);
        obs->OnCompleted();
    };
    auto simpleCreateObs = rx::Create<int>(factory);
    
    auto intObserver = CreateObserver<int>(
        [](const int& value) { 
            Serial.print("Created value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Create completed"); 
        }
    );
    simpleCreateObs->Subscribe(intObserver);

    Serial.println("\nSetup completed. Starting loop...");
}

void loop() {
    delay(1000);
    Serial.println("Loop running...");
}