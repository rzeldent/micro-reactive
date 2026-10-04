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
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<std::string> execution_log;
    
    // Schedule some actions at different virtual times
    scheduler->ScheduleDelayed([&execution_log]() { execution_log.push_back("Action at 0ms"); }, std::chrono::milliseconds(0));
    scheduler->ScheduleDelayed([&execution_log]() { execution_log.push_back("Action at 100ms"); }, std::chrono::milliseconds(100));
    scheduler->ScheduleDelayed([&execution_log]() { execution_log.push_back("Action at 200ms"); }, std::chrono::milliseconds(200));
    
    // Advance time and execute
    scheduler->AdvanceTo(std::chrono::milliseconds(0));
    scheduler->AdvanceTo(std::chrono::milliseconds(100));
    scheduler->AdvanceTo(std::chrono::milliseconds(200));
    
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
    scheduler->AdvanceBy(std::chrono::milliseconds(100));  // First emission
    scheduler->AdvanceBy(std::chrono::milliseconds(100));  // Second emission
    scheduler->AdvanceBy(std::chrono::milliseconds(100));  // Third emission
    
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
    scheduler->AdvanceBy(std::chrono::milliseconds(50));  // First value
    scheduler->AdvanceBy(std::chrono::milliseconds(50));  // Second value
    scheduler->AdvanceBy(std::chrono::milliseconds(50));  // Third value
    
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
    scheduler->AdvanceBy(std::chrono::milliseconds(50));
    subject->OnNext(2);
    scheduler->AdvanceBy(std::chrono::milliseconds(50));
    subject->OnNext(3);
    
    // Wait for debounce window
    scheduler->AdvanceBy(std::chrono::milliseconds(100));
    
    Serial.print("Total debounced values: ");
    Serial.println(received_values.size());
    
    Serial.println();
}

void test_scheduler_multiple_actions_example() {
    Serial.println("=== TestScheduler Multiple Actions Example ===");
    
    auto scheduler = std::make_shared<TestScheduler>();
    std::vector<std::string> execution_order;
    
    // Schedule multiple actions at same time
    scheduler->ScheduleDelayed([&execution_order]() { execution_order.push_back("A1"); }, std::chrono::milliseconds(100));
    scheduler->ScheduleDelayed([&execution_order]() { execution_order.push_back("A2"); }, std::chrono::milliseconds(100));
    scheduler->ScheduleDelayed([&execution_order]() { execution_order.push_back("A3"); }, std::chrono::milliseconds(100));
    
    // Schedule at different times
    scheduler->ScheduleDelayed([&execution_order]() { execution_order.push_back("B1"); }, std::chrono::milliseconds(200));
    scheduler->ScheduleDelayed([&execution_order]() { execution_order.push_back("B2"); }, std::chrono::milliseconds(200));
    
    // Execute all
    scheduler->AdvanceTo(std::chrono::milliseconds(300));
    
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
    scheduler->AdvanceTo(std::chrono::milliseconds(500));
    
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
