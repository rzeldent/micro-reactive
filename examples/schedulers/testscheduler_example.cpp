/**
 * TestScheduler Example
 * 
 * Demonstrates the TestScheduler which provides deterministic scheduling
 * for testing reactive code with time-based operations.
 */

#include "../../include/micro-reactive.h"
#include <iostream>
#include <vector>

using namespace rx;

void basic_test_scheduler_example() {
    std::cout << "=== Basic TestScheduler Example ===" << std::endl;
    
    TestScheduler scheduler;
    std::vector<std::string> execution_log;
    
    // Schedule some actions at different virtual times
    scheduler.ScheduleDelayed([&execution_log]() {
        execution_log.push_back("Action at 100ms");
    }, std::chrono::milliseconds(100));
    
    scheduler.ScheduleDelayed([&execution_log]() {
        execution_log.push_back("Action at 200ms");
    }, std::chrono::milliseconds(200));
    
    scheduler.ScheduleDelayed([&execution_log]() {
        execution_log.push_back("Action at 50ms");
    }, std::chrono::milliseconds(50));
    
    std::cout << "Virtual time before execution: " << scheduler.GetVirtualTime().count() << "ms" << std::endl;
    std::cout << "Scheduled actions count: 3" << std::endl;
    
    // Execute all scheduled actions
    scheduler.Start();
    
    std::cout << "Virtual time after execution: " << scheduler.GetVirtualTime().count() << "ms" << std::endl;
    std::cout << "Execution log:" << std::endl;
    for (const auto& entry : execution_log) {
        std::cout << "  " << entry << std::endl;
    }
    
    std::cout << std::endl;
}

void advance_by_example() {
    std::cout << "=== TestScheduler AdvanceBy Example ===" << std::endl;
    
    TestScheduler scheduler;
    std::vector<int> results;
    
    // Schedule actions at different times
    scheduler.ScheduleDelayed([&results]() {
        results.push_back(1);
    }, std::chrono::milliseconds(100));
    
    scheduler.ScheduleDelayed([&results]() {
        results.push_back(2);
    }, std::chrono::milliseconds(200));
    
    scheduler.ScheduleDelayed([&results]() {
        results.push_back(3);
    }, std::chrono::milliseconds(300));
    
    // Advance time step by step
    std::cout << "Advancing by 50ms..." << std::endl;
    scheduler.AdvanceBy(std::chrono::milliseconds(50));
    std::cout << "Results: ";
    for (int r : results) std::cout << r << " ";
    std::cout << "(should be empty)" << std::endl;
    
    std::cout << "Advancing by 75ms more (total 125ms)..." << std::endl;
    scheduler.AdvanceBy(std::chrono::milliseconds(75));
    std::cout << "Results: ";
    for (int r : results) std::cout << r << " ";
    std::cout << "(should have 1)" << std::endl;
    
    std::cout << "Advancing by 100ms more (total 225ms)..." << std::endl;
    scheduler.AdvanceBy(std::chrono::milliseconds(100));
    std::cout << "Results: ";
    for (int r : results) std::cout << r << " ";
    std::cout << "(should have 1, 2)" << std::endl;
    
    std::cout << "Advancing by 100ms more (total 325ms)..." << std::endl;
    scheduler.AdvanceBy(std::chrono::milliseconds(100));
    std::cout << "Results: ";
    for (int r : results) std::cout << r << " ";
    std::cout << "(should have 1, 2, 3)" << std::endl;
    
    std::cout << std::endl;
}

void advance_to_example() {
    std::cout << "=== TestScheduler AdvanceTo Example ===" << std::endl;
    
    TestScheduler scheduler;
    std::vector<std::string> timeline;
    
    // Schedule actions
    scheduler.ScheduleDelayed([&timeline]() {
        timeline.push_back("Event A at 150ms");
    }, std::chrono::milliseconds(150));
    
    scheduler.ScheduleDelayed([&timeline]() {
        timeline.push_back("Event B at 300ms");
    }, std::chrono::milliseconds(300));
    
    scheduler.ScheduleDelayed([&timeline]() {
        timeline.push_back("Event C at 100ms");
    }, std::chrono::milliseconds(100));
    
    // Advance to specific time points
    std::cout << "Advancing to 120ms..." << std::endl;
    scheduler.AdvanceTo(std::chrono::milliseconds(120));
    std::cout << "Timeline at 120ms:" << std::endl;
    for (const auto& event : timeline) {
        std::cout << "  " << event << std::endl;
    }
    
    std::cout << "Advancing to 200ms..." << std::endl;
    scheduler.AdvanceTo(std::chrono::milliseconds(200));
    std::cout << "Timeline at 200ms:" << std::endl;
    for (const auto& event : timeline) {
        std::cout << "  " << event << std::endl;
    }
    
    std::cout << "Advancing to 400ms..." << std::endl;
    scheduler.AdvanceTo(std::chrono::milliseconds(400));
    std::cout << "Final timeline:" << std::endl;
    for (const auto& event : timeline) {
        std::cout << "  " << event << std::endl;
    }
    
    std::cout << std::endl;
}

void periodic_scheduling_example() {
    std::cout << "=== TestScheduler Periodic Scheduling Example ===" << std::endl;
    
    TestScheduler scheduler;
    int counter = 0;
    std::vector<int> snapshots;
    
    // Schedule a periodic action every 100ms
    auto periodic_work = scheduler.SchedulePeriodic([&counter, &snapshots]() {
        counter++;
        snapshots.push_back(counter);
    }, std::chrono::milliseconds(100));
    
    // Run for 350ms to see periodic execution
    std::cout << "Running periodic action for 350ms..." << std::endl;
    scheduler.AdvanceTo(std::chrono::milliseconds(350));
    
    std::cout << "Counter snapshots: ";
    for (int snapshot : snapshots) {
        std::cout << snapshot << " ";
    }
    std::cout << std::endl;
    std::cout << "Final counter value: " << counter << std::endl;
    
    // Cancel the periodic work
    periodic_work->Cancel();
    
    // Advance more time - should not execute more
    scheduler.AdvanceTo(std::chrono::milliseconds(500));
    std::cout << "Counter after cancellation: " << counter << std::endl;
    
    std::cout << std::endl;
}

void observable_with_scheduler_example() {
    std::cout << "=== Observable with TestScheduler Example ===" << std::endl;
    
    TestScheduler scheduler;
    std::vector<int> received_values;
    std::vector<int> timestamps;
    
    // Create a custom observable that uses the scheduler
    auto custom_obs = Create<int>([&scheduler, &timestamps](std::shared_ptr<IObserver<int>> observer) {
        // Schedule emissions at specific times
        scheduler.ScheduleDelayed([observer, &timestamps]() {
            timestamps.push_back(100);
            observer->OnNext(1);
        }, std::chrono::milliseconds(100));
        
        scheduler.ScheduleDelayed([observer, &timestamps]() {
            timestamps.push_back(200);
            observer->OnNext(2);
        }, std::chrono::milliseconds(200));
        
        scheduler.ScheduleDelayed([observer, &timestamps]() {
            timestamps.push_back(300);
            observer->OnNext(3);
            observer->OnCompleted();
        }, std::chrono::milliseconds(300));
    });
    
    // Subscribe to the observable
    custom_obs->Subscribe(CreateObserver<int>(
        [&received_values](int value) {
            received_values.push_back(value);
        },
        []() {
            std::cout << "Custom observable completed" << std::endl;
        },
        [](const std::exception& e) {
            std::cout << "Custom observable error: " << e.what() << std::endl;
        }
    ));
    
    // Test different time points
    std::cout << "At 0ms - Values received: " << received_values.size() << std::endl;
    
    scheduler.AdvanceTo(std::chrono::milliseconds(150));
    std::cout << "At 150ms - Values received: " << received_values.size() << " (";
    for (int v : received_values) std::cout << v << " ";
    std::cout << ")" << std::endl;
    
    scheduler.AdvanceTo(std::chrono::milliseconds(250));
    std::cout << "At 250ms - Values received: " << received_values.size() << " (";
    for (int v : received_values) std::cout << v << " ";
    std::cout << ")" << std::endl;
    
    scheduler.AdvanceTo(std::chrono::milliseconds(400));
    std::cout << "At 400ms - Values received: " << received_values.size() << " (";
    for (int v : received_values) std::cout << v << " ";
    std::cout << ")" << std::endl;
    
    std::cout << std::endl;
}

void scheduler_comparison_example() {
    std::cout << "=== Scheduler Comparison Example ===" << std::endl;
    
    std::cout << "Using ImmediateScheduler:" << std::endl;
    {
        auto& immediate = Schedulers::Immediate();
        std::vector<int> execution_order;
        
        immediate.Schedule([&execution_order]() {
            execution_order.push_back(1);
            std::cout << "  Immediate action 1 executed" << std::endl;
        });
        
        immediate.Schedule([&execution_order]() {
            execution_order.push_back(2);
            std::cout << "  Immediate action 2 executed" << std::endl;
        });
        
        std::cout << "  All immediate actions completed synchronously" << std::endl;
    }
    
    std::cout << "\nUsing TestScheduler:" << std::endl;
    {
        TestScheduler test_scheduler;
        std::vector<int> execution_order;
        
        test_scheduler.Schedule([&execution_order]() {
            execution_order.push_back(1);
            std::cout << "  Test action 1 scheduled for execution" << std::endl;
        });
        
        test_scheduler.Schedule([&execution_order]() {
            execution_order.push_back(2);
            std::cout << "  Test action 2 scheduled for execution" << std::endl;
        });
        
        std::cout << "  Actions scheduled but not executed yet" << std::endl;
        std::cout << "  Starting test scheduler..." << std::endl;
        test_scheduler.Start();
        std::cout << "  All test actions completed" << std::endl;
    }
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "TestScheduler Examples" << std::endl;
    std::cout << "=====================" << std::endl;
    
    basic_test_scheduler_example();
    advance_by_example();
    advance_to_example();
    periodic_scheduling_example();
    observable_with_scheduler_example();
    scheduler_comparison_example();
}

void loop() {
    // Nothing to do in loop
}
