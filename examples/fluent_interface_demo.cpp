#include <Arduino.h>
#include "../include/micro-reactive.h"

using namespace rx;

// Example demonstrating fluent interface usage
void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Fluent Interface Demo");
    Serial.println("====================");
    
    // Create a simple observer to capture results
    class PrintObserver : public IObserver<int> {
    public:
        void OnNext(const int& value) override {
            Serial.print("Value: ");
            Serial.println(value);
        }
        
        void OnCompleted() override {
            Serial.println("Completed");
        }
        
        void OnError(const std::exception& e) override {
            Serial.print("Error: ");
            Serial.println(e.what());
        }
    };
    
    auto observer = std::make_shared<PrintObserver>();
    
    // Traditional (functional) style
    Serial.println("\nTraditional Style:");
    auto range1 = Range(1, 5);
    auto mapped1 = Map<int, int>(range1, [](const int& x) { return x * 2; });
    auto filtered1 = Filter<int>(mapped1, [](const int& x) { return x > 4; });
    auto taken1 = Take<int>(filtered1, 3);
    
    auto subscription1 = taken1->Subscribe(observer);
    delay(100);
    subscription1->Dispose();
    
    // Fluent style demonstration (when implemented)
    Serial.println("\nFluent Style (Future Implementation):");
    Serial.println("auto result = From(Range(1, 5))");
    Serial.println("    .Map([](int x) { return x * 2; })");
    Serial.println("    .Filter([](int x) { return x > 4; })");
    Serial.println("    .Take(3);");
    Serial.println();
    Serial.println("This would enable method chaining for more readable code!");
    
    // For now, we can show how it would work with explicit conversions
    Serial.println("\nCurrent Fluent Wrapper (Basic):");
    auto range2 = Range(1, 3);
    auto fluent = From(range2);
    auto subscription2 = fluent.Subscribe(observer);
    delay(100);
    subscription2->Dispose();
}

void loop() {
    // Nothing to do
}
