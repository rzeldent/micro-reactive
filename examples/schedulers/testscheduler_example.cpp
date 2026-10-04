#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * TestScheduler Example
 * 
 * Demonstrates the TestScheduler which provides deterministic scheduling
 * for testing reactive code with time-based operations.
 */

void basic_test_scheduler_example() {
    Serial.println("=== Basic TestScheduler Example ===");
    
    TestScheduler scheduler;
    std::vector<std::string> execution_log;
    
    // Schedule some actions at different virtual times
    scheduler.Schedule([&execution_log]() { execution_log.push_back("Action at 0ms"); }, 0);
    scheduler.Schedule([&execution_log]() { execution_log.push_back("Action at 100ms"); }, 100);
    scheduler.Schedule([&execution_log]() { execution_log.push_back("Action at 200ms"); }, 200);
    
    // Advance time and execute
    scheduler.AdvanceTo(0);
    scheduler.AdvanceTo(100);
    scheduler.AdvanceTo(200);
    
    // Print execution log
    for (const auto& entry : execution_log) {
        Serial.println(entry.c_str());
    }
    
    Serial.println();
}

void test_scheduler_with_observable_example() {
    Serial.println("=== TestScheduler with Observable Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<int> received_values;
    
    // Create a range and delay it using test scheduler
    auto delayed = Delay<int>(Range(1, 3), std::chrono::milliseconds(100), scheduler);
    
    // Subscribe and collect values
    delayed->Subscribe(CreateObserver<int>(
        [&received_values](int value) { 
            received_values.push_back(value); 
            Serial.print("Received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Delayed completed"); 
        }
    ));
    
    // Advance time to trigger emissions
    scheduler->AdvanceBy(100);  // First emission
    scheduler->AdvanceBy(100);  // Second emission
    scheduler->AdvanceBy(100);  // Third emission
    
    Serial.print("Total values received: ");
    Serial.println(received_values.size());
    
    Serial.println();
}

void test_scheduler_delay_example() {
    Serial.println("=== TestScheduler Delay Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<int> received_values;
    
    // Create a range and delay it
    auto delayed = Delay<int>(Range(1, 3), std::chrono::milliseconds(50), scheduler);
    
    delayed->Subscribe(CreateObserver<int>(
        [&received_values](int value) { 
            received_values.push_back(value); 
            Serial.print("Delayed received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Delayed completed"); 
        }
    ));
    
    // Advance time
    scheduler->AdvanceBy(50);  // First value
    scheduler->AdvanceBy(50);  // Second value
    scheduler->AdvanceBy(50);  // Third value
    
    Serial.print("Total delayed values: ");
    Serial.println(received_values.size());
    
    Serial.println();
}

void test_scheduler_debounce_example() {
    Serial.println("=== TestScheduler Debounce Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<int> received_values;
    
    auto subject = std::make_shared<Subject<int>>();
    auto debounced = Debounce<int>(subject, std::chrono::milliseconds(100), scheduler);
    
    debounced->Subscribe(CreateObserver<int>(
        [&received_values](int value) { 
            received_values.push_back(value); 
            Serial.print("Debounced received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Debounced completed"); 
        }
    ));
    
    // Emit values rapidly
    subject->OnNext(1);
    scheduler->AdvanceBy(50);
    subject->OnNext(2);
    scheduler->AdvanceBy(50);
    subject->OnNext(3);
    
    // Wait for debounce window
    scheduler->AdvanceBy(100);
    
    Serial.print("Total debounced values: ");
    Serial.println(received_values.size());
    
    Serial.println();
}

void test_scheduler_multiple_actions_example() {
    Serial.println("=== TestScheduler Multiple Actions Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<std::string> execution_order;
    
    // Schedule multiple actions at same time
    scheduler->Schedule([&execution_order]() { execution_order.push_back("A1"); }, 100);
    scheduler->Schedule([&execution_order]() { execution_order.push_back("A2"); }, 100);
    scheduler->Schedule([&execution_order]() { execution_order.push_back("A3"); }, 100);
    
    // Schedule at different times
    scheduler->Schedule([&execution_order]() { execution_order.push_back("B1"); }, 200);
    scheduler->Schedule([&execution_order]() { execution_order.push_back("B2"); }, 200);
    
    // Execute all
    scheduler->AdvanceTo(300);
    
    for (const auto& entry : execution_order) {
        Serial.println(entry.c_str());
    }
    
    Serial.println();
}

void test_scheduler_advance_to_example() {
    Serial.println("=== TestScheduler AdvanceTo Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<int> received_values;
    
    // Use Delay with Range instead of Interval
    auto delayed = Delay<int>(Range(1, 5), std::chrono::milliseconds(100), scheduler);
    
    delayed->Subscribe(CreateObserver<int>(
        [&received_values](int value) { 
            received_values.push_back(value); 
            Serial.print("AdvanceTo received: "); 
            Serial.println(value); 
        }
    ));
    
    // Jump directly to 500ms
    scheduler->AdvanceTo(500);
    
    Serial.print("Values after AdvanceTo(500): ");
    Serial.println(received_values.size());
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("TestScheduler Examples");
    Serial.println("======================");
    
    basic_test_scheduler_example();
    test_scheduler_with_observable_example();
    test_scheduler_delay_example();
    test_scheduler_debounce_example();
    test_scheduler_multiple_actions_example();
    test_scheduler_advance_to_example();
}

void loop() {
    // Nothing to do in loop
}
