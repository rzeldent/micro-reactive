#include "test_utils.h"
#include "../include/core.h"
#include "../include/scheduler.h"

// Test scheduler functionality
void test_scheduler_functionality() {
    auto scheduler = std::make_shared<ThreadPoolScheduler>();
    bool work_executed = false;
    
    // Test immediate scheduling
    scheduler->Schedule([&work_executed]() {
        work_executed = true;
    });
    
    // Wait for work to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    TEST_ASSERT_TRUE(work_executed);
    
    // Test delayed scheduling
    work_executed = false;
    auto start_time = std::chrono::steady_clock::now();
    
    auto work = scheduler->ScheduleDelayed([&work_executed]() {
        work_executed = true;
    }, std::chrono::milliseconds(100));
    
    // Should not execute immediately
    TEST_ASSERT_FALSE(work_executed);
    
    // Wait for delayed execution
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    auto end_time = std::chrono::steady_clock::now();
    
    TEST_ASSERT_TRUE(work_executed);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    TEST_ASSERT_TRUE(duration.count() >= 90); // Should have waited at least ~100ms
}

void test_test_scheduler_basic() {
    TestScheduler scheduler;
    int execution_count = 0;
    
    // Schedule an action
    scheduler.Schedule([&execution_count]() {
        execution_count++;
    });
    
    // Should not execute immediately
    TEST_ASSERT_EQUAL(0, execution_count);
    
    // Advance time should execute the action
    scheduler.AdvanceBy(std::chrono::milliseconds(1));
    TEST_ASSERT_EQUAL(1, execution_count);
}

void test_test_scheduler_delayed() {
    TestScheduler scheduler;
    int execution_count = 0;
    
    // Schedule a delayed action
    scheduler.ScheduleDelayed([&execution_count]() {
        execution_count++;
    }, std::chrono::milliseconds(100));
    
    // Should not execute immediately
    TEST_ASSERT_EQUAL(0, execution_count);
    
    // Advance by less than delay - should not execute
    scheduler.AdvanceBy(std::chrono::milliseconds(50));
    TEST_ASSERT_EQUAL(0, execution_count);
    
    // Advance past delay - should execute
    scheduler.AdvanceBy(std::chrono::milliseconds(60));
    TEST_ASSERT_EQUAL(1, execution_count);
}

void test_test_scheduler_multiple_actions() {
    TestScheduler scheduler;
    std::vector<int> execution_order;
    
    // Schedule actions at different times
    scheduler.ScheduleDelayed([&execution_order]() {
        execution_order.push_back(2);
    }, std::chrono::milliseconds(200));
    
    scheduler.ScheduleDelayed([&execution_order]() {
        execution_order.push_back(1);
    }, std::chrono::milliseconds(100));
    
    scheduler.ScheduleDelayed([&execution_order]() {
        execution_order.push_back(3);
    }, std::chrono::milliseconds(300));
    
    // Execute all actions
    scheduler.Start();
    
    // Should execute in time order
    TEST_ASSERT_EQUAL(3, execution_order.size());
    TEST_ASSERT_EQUAL(1, execution_order[0]);
    TEST_ASSERT_EQUAL(2, execution_order[1]);
    TEST_ASSERT_EQUAL(3, execution_order[2]);
}

void test_test_scheduler_advance_to() {
    TestScheduler scheduler;
    int execution_count = 0;
    
    scheduler.ScheduleDelayed([&execution_count]() {
        execution_count++;
    }, std::chrono::milliseconds(150));
    
    scheduler.ScheduleDelayed([&execution_count]() {
        execution_count++;
    }, std::chrono::milliseconds(250));
    
    // Advance to time 200ms - should execute first action only
    scheduler.AdvanceTo(std::chrono::milliseconds(200));
    TEST_ASSERT_EQUAL(1, execution_count);
    
    // Advance to time 300ms - should execute second action
    scheduler.AdvanceTo(std::chrono::milliseconds(300));
    TEST_ASSERT_EQUAL(2, execution_count);
}
