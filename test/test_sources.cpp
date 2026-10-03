#include "test_utils.h"
#include "../include/core.h"
#include "../include/sources.h"
#include <stdexcept>

void test_create_observer()
{
    int value_received = 0;
    bool completed = false;
    bool errored = false;

    auto observer = CreateObserver<int>(
        [&value_received](const int &value) { value_received = value; },
        [&completed]() { completed = true; },
        [&errored](const std::exception &) { errored = true; });

    observer->OnNext(42);
    observer->OnCompleted();
    observer->OnError(std::runtime_error("expected test error"));

    TEST_ASSERT_EQUAL(42, value_received);
    TEST_ASSERT_TRUE(completed);
    TEST_ASSERT_TRUE(errored);
}

// Test Range source
void test_range_basic() {
    auto range = Range(10, 3); // Start at 10, count of 3
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(12, observer->GetLastValue()); // Should end at 12
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
    
    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

// Test FromVector source
void test_fromvector_basic() {
    std::vector<int> values = {100, 200, 300};
    auto source = FromVector(values);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(300, observer->GetLastValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test Empty source
void test_empty_basic() {
    auto empty = Empty<int>();
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = empty->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test TimerObservable basic functionality
void test_timer_basic() {
    auto timer = Timer<int>(std::chrono::milliseconds(100)); // 100ms timer
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to timer
    auto subscription = timer->Subscribe(observer);
    
    // Timer should not have fired immediately
    TEST_ASSERT_FALSE(observer->HasValue());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    // Wait for timer to fire (100ms + some buffer)
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Timer should have fired
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_EQUAL(0, observer->GetLastValue()); // Default int value
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test IntervalObservable basic functionality
void test_interval_basic() {
    // Create interval that emits 3 values every 50ms
    auto interval = Interval<int>(std::chrono::milliseconds(50), 3);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to interval
    auto subscription = interval->Subscribe(observer);
    
    // Initially no values
    TEST_ASSERT_FALSE(observer->HasValue());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    // Wait for first emission (50ms + buffer)
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(0, observer->GetLastValue()); // First value should be 0
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_FALSE(observer->IsCompleted()); // Should not be completed yet
    
    // Wait for all emissions to complete (3 * 50ms + buffer)
    std::this_thread::sleep_for(std::chrono::milliseconds((120)));
    
    // Should have all 3 values and be completed
    TEST_ASSERT_EQUAL(2, observer->GetLastValue()); // Last value should be 2
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}
