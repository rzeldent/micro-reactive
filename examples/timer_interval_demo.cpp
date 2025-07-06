#include <micro-reactive.h>
#include <Arduino.h>

using namespace rx;

class PrintObserver : public IObserver<int> {
private:
    String name_;

public:
    PrintObserver(const String& name) : name_(name) {}

    void OnNext(const int& value) override {
        Serial.println("[" + name_ + "] OnNext: " + String(value) + " at " + String(millis()) + "ms");
    }

    void OnCompleted() override {
        Serial.println("[" + name_ + "] OnCompleted at " + String(millis()) + "ms");
    }

    void OnError(const std::exception& e) override {
        Serial.println("[" + name_ + "] OnError: " + String(e.what()));
    }
};

void setup() {
    Serial.begin(115200);
    delay(2000);
    
    Serial.println("=== Threaded Timer and Interval Observable Demo ===");
    Serial.println("Start time: " + String(millis()) + "ms");
    
    // Timer Observable Demo
    Serial.println("\n--- Timer Observable (2000ms delay) ---");
    auto timer = Timer<int>(std::chrono::milliseconds(2000));
    auto timerObserver = std::make_shared<PrintObserver>("Timer");
    timer->Subscribe(timerObserver);
    Serial.println("Timer subscribed - will fire in 2 seconds...");
    
    // Interval Observable Demo  
    Serial.println("\n--- Interval Observable (800ms interval, 4 emissions) ---");
    auto interval = Interval<int>(std::chrono::milliseconds(800), 4);
    auto intervalObserver = std::make_shared<PrintObserver>("Interval");
    interval->Subscribe(intervalObserver);
    Serial.println("Interval subscribed - will emit every 800ms...");
    
    // Multiple observers on same timer
    Serial.println("\n--- Multiple observers on same timer ---");
    auto sharedTimer = Timer<int>(std::chrono::milliseconds(1500));
    auto observer1 = std::make_shared<PrintObserver>("SharedTimer-A");
    auto observer2 = std::make_shared<PrintObserver>("SharedTimer-B");
    sharedTimer->Subscribe(observer1);
    sharedTimer->Subscribe(observer2);
    Serial.println("Shared timer with 2 observers subscribed...");
    
    Serial.println("\nMain thread continues immediately - observables run in background!");
    Serial.println("Watch for async emissions...\n");
}

void loop() {
    // Main loop continues running while background threads emit values
    static unsigned long lastPrint = 0;
    if (millis() - lastPrint > 3000) {  // Print every 3 seconds
        Serial.println("Main loop running... " + String(millis()) + "ms");
        lastPrint = millis();
    }
    delay(100);
}
