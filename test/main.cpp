#include <unity.h>
#include <Arduino.h>
#include "../include/core.h"
#include "../include/sources.h"
#include "../include/subjects.h"
#include "../include/operators.h" // Add operators header

// Simple test observer for basic functionality testing
template<typename T>
class SimpleTestObserver : public rx::IObserver<T> {
private:
    T _lastValue;
    bool _hasValue = false;
    bool _completed = false;
    int _count = 0;

public:
    void OnNext(const T& value) override {
        _lastValue = value;
        _hasValue = true;
        _count++;
    }

    void OnCompleted() override {
        _completed = true;
    }

    void OnError(const std::exception& e) override {
        _completed = true;
    }

    T GetLastValue() const { return _lastValue; }
    bool HasValue() const { return _hasValue; }
    bool IsCompleted() const { return _completed; }
    int GetCount() const { return _count; }
    void Reset() { _hasValue = false; _completed = false; _count = 0; }
};

// Test Range source
void test_range_basic() {
    auto range = rx::Range(10, 3); // Start at 10, count of 3
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
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(300, observer->GetLastValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test Subject basic functionality
void test_subject_basic() {
    auto subject = std::make_shared<rx::Subject<int>>();
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = subject->Subscribe(observer);
    
    subject->OnNext(42);
    subject->OnNext(84);
    subject->OnCompleted();
    
    TEST_ASSERT_EQUAL(84, observer->GetLastValue());
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test BehaviorSubject basic functionality
void test_behaviorsubject_basic() {
    auto behaviorSubject = std::make_shared<rx::BehaviorSubject<int>>(99);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe after creation - should immediately receive initial value
    auto subscription = behaviorSubject->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(99, observer->GetLastValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    
    behaviorSubject->OnNext(150);
    
    TEST_ASSERT_EQUAL(150, observer->GetLastValue());
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test Empty source
void test_empty_basic() {
    auto empty = rx::Empty<int>();
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = empty->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test Subject with multiple observers (thread safety test)
void test_subject_multiple_observers() {
    auto subject = std::make_shared<rx::Subject<int>>();
    auto observer1 = std::make_shared<SimpleTestObserver<int>>();
    auto observer2 = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription1 = subject->Subscribe(observer1);
    auto subscription2 = subject->Subscribe(observer2);
    
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnCompleted();
    
    // Both observers should receive the same values
    TEST_ASSERT_EQUAL(2, observer1->GetCount());
    TEST_ASSERT_EQUAL(2, observer2->GetCount());
    TEST_ASSERT_EQUAL(20, observer1->GetLastValue());
    TEST_ASSERT_EQUAL(20, observer2->GetLastValue());
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
    
    // Test subscription cleanup
    subscription1->Dispose();
    TEST_ASSERT_TRUE(subscription1->IsDisposed());
    TEST_ASSERT_FALSE(subscription2->IsDisposed());
}

// Test TimerObservable basic functionality
void test_timer_basic() {
    auto timer = rx::Timer<int>(std::chrono::milliseconds(100)); // 100ms timer
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to timer
    auto subscription = timer->Subscribe(observer);
    
    // Timer should not have fired immediately
    TEST_ASSERT_FALSE(observer->HasValue());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    // Wait for timer to fire (100ms + some buffer)
    delay(200);
    
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
    auto interval = rx::Interval<int>(std::chrono::milliseconds(50), 3);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to interval
    auto subscription = interval->Subscribe(observer);
    
    // Initially no values
    TEST_ASSERT_FALSE(observer->HasValue());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    // Wait for first emission (50ms + buffer)
    delay(80);
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(0, observer->GetLastValue()); // First value should be 0
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_FALSE(observer->IsCompleted()); // Should not be completed yet
    
    // Wait for all emissions to complete (3 * 50ms + buffer)
    delay(120);
    
    // Should have all 3 values and be completed
    TEST_ASSERT_EQUAL(2, observer->GetLastValue()); // Last value should be 2
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test MapOperator with new subscription pattern
void test_map_operator() {
    auto range = rx::Range(1, 3); // 1, 2, 3
    auto mapOp = rx::Map<int, int>(range, [](const int& x) { return x * 2; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = mapOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // 3 * 2 = 6
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
    
    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

// Test FilterOperator with new subscription pattern
void test_filter_operator() {
    auto range = rx::Range(1, 5); // 1, 2, 3, 4, 5
    auto filterOp = rx::Filter<int>(range, [](const int& x) { return x % 2 == 0; }); // Only even numbers
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = filterOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->GetLastValue()); // Last even number is 4
    TEST_ASSERT_EQUAL(2, observer->GetCount()); // Should have 2 and 4
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test TakeOperator with new subscription pattern
void test_take_operator() {
    auto range = rx::Range(10, 5); // 10, 11, 12, 13, 14
    auto takeOp = rx::Take<int>(range, 2); // Take first 2
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = takeOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(11, observer->GetLastValue()); // Second value is 11
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test ScanOperator with new subscription pattern
void test_scan_operator() {
    auto range = rx::Range(1, 4); // 1, 2, 3, 4
    auto scanOp = rx::Scan<int, int>(range, 0, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = scanOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(10, observer->GetLastValue()); // 0+1+2+3+4 = 10
    TEST_ASSERT_EQUAL(4, observer->GetCount()); // Should emit accumulated value for each input
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
    
    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

// Test ReduceOperator with new subscription pattern  
void test_reduce_operator() {
    auto range = rx::Range(1, 3); // 1, 2, 3
    auto reduceOp = rx::Reduce<int, int>(range, 10, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = reduceOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(16, observer->GetLastValue()); // 10+1+2+3 = 16
    TEST_ASSERT_EQUAL(1, observer->GetCount()); // Should emit only final result
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test ThrottleOperator with new subscription pattern
void test_throttle_operator() {
    auto range = rx::Range(1, 6); // 1, 2, 3, 4, 5, 6
    auto throttleOp = rx::Throttle<int>(range, 2); // Every 2nd item
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = throttleOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // Should get items 2, 4, 6
    TEST_ASSERT_EQUAL(3, observer->GetCount()); // Every 2nd item: 2, 4, 6
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

void setUp(void) {
    // Set up code here - runs before each test
}

void tearDown(void) {
    // Clean up code here - runs after each test
}

// Test runner for PlatformIO
void setup() {
    delay(2000); // Give time for serial to initialize
    
    UNITY_BEGIN();
    
    // Core functionality tests
    RUN_TEST(test_range_basic);
    RUN_TEST(test_fromvector_basic);
    RUN_TEST(test_empty_basic);
    RUN_TEST(test_subject_basic);
    RUN_TEST(test_behaviorsubject_basic);
    RUN_TEST(test_subject_multiple_observers);
    RUN_TEST(test_timer_basic);
    RUN_TEST(test_interval_basic);
    
    // Operator tests
    RUN_TEST(test_map_operator);
    RUN_TEST(test_filter_operator);
    RUN_TEST(test_take_operator);
    RUN_TEST(test_scan_operator);
    RUN_TEST(test_reduce_operator);
    RUN_TEST(test_throttle_operator);
    RUN_TEST(test_scan_operator);
    RUN_TEST(test_reduce_operator);
    RUN_TEST(test_throttle_operator);
    
    UNITY_END();
}

void loop() {
    // Nothing to do here
}
