#include <unity.h>
#include <Arduino.h>
#include "../include/micro-reactive.h"

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

// Test basic Map operator
void test_map_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 3));
    std::function<int(const int&)> transform = [](const int& x) { return x * 2; };
    auto mapped = rx::Map<int, int>(source, transform);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    mapped->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // Last value should be 3*2=6
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test basic Filter operator
void test_filter_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    std::function<bool(const int&)> predicate = [](const int& x) { return x % 2 == 0; };
    auto filtered = rx::Filter(source, predicate);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    filtered->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->GetLastValue()); // Last even number should be 4
    TEST_ASSERT_EQUAL(2, observer->GetCount()); // Should have 2 even numbers (2, 4)
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Range source
void test_range_basic() {
    auto range = rx::Range(10, 3); // Start at 10, count of 3
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(12, observer->GetLastValue()); // Should end at 12
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test FromVector source
void test_fromvector_basic() {
    std::vector<int> values = {100, 200, 300};
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(300, observer->GetLastValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Subject basic functionality
void test_subject_basic() {
    auto subject = rx::Subject<int>();
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    subject.Subscribe(observer);
    
    subject.OnNext(42);
    subject.OnNext(84);
    subject.OnCompleted();
    
    TEST_ASSERT_EQUAL(84, observer->GetLastValue());
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test BehaviorSubject basic functionality
void test_behaviorsubject_basic() {
    auto behaviorSubject = rx::BehaviorSubject<int>(99);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe after creation - should immediately receive initial value
    behaviorSubject.Subscribe(observer);
    
    TEST_ASSERT_EQUAL(99, observer->GetLastValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    
    behaviorSubject.OnNext(150);
    
    TEST_ASSERT_EQUAL(150, observer->GetLastValue());
    TEST_ASSERT_EQUAL(2, observer->GetCount());
}

// Test Take operator
void test_take_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    auto taken = rx::Take(source, static_cast<size_t>(3));
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    taken->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Scan operator  
void test_scan_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 3));
    std::function<int(const int&, const int&)> accumulator = [](const int& acc, const int& x) { return acc + x; };
    auto scan = rx::Scan<int, int>(source, 0, accumulator);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    scan->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // 0+1+2+3 = 6
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Sum operator
void test_sum_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto sum = rx::Sum<int>(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    sum->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(15, observer->GetLastValue()); // 1+2+3+4+5 = 15
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Distinct operator
void test_distinct_basic() {
    std::vector<int> values = {1, 2, 2, 3, 3, 4, 1, 5};
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::FromVector(values));
    auto distinct = rx::Distinct(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    distinct->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->GetLastValue()); // Last unique value should be 5
    TEST_ASSERT_EQUAL(5, observer->GetCount()); // Should have 5 unique values
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Skip operator
void test_skip_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 7));
    auto skip = rx::Skip(source, static_cast<size_t>(3));
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    skip->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(7, observer->GetLastValue()); // Last value after skipping 3
    TEST_ASSERT_EQUAL(4, observer->GetCount()); // Should have 4 values (4,5,6,7)
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test First operator
void test_first_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 5));
    auto first = rx::First(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    first->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(10, observer->GetLastValue()); // First value
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Last operator
void test_last_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 5));
    auto last = rx::Last(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    last->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(14, observer->GetLastValue()); // Last value (10+4)
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test operator chaining
void test_operator_chaining() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 6));
    std::function<bool(const int&)> predicate = [](const int& x) { return x > 3; };
    auto filtered = rx::Filter(source, predicate);
    std::function<int(const int&)> transform = [](const int& x) { return x * 10; };
    auto mapped = rx::Map<int, int>(filtered, transform);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    mapped->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(60, observer->GetLastValue()); // 6 * 10 = 60
    TEST_ASSERT_EQUAL(3, observer->GetCount()); // Should have 4, 5, 6 -> 40, 50, 60
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Empty source
void test_empty_basic() {
    auto empty = rx::Empty<int>();
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    empty->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test complex operator chain (from old integration tests)
void test_complex_chain() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    
    // Chain: Range -> Filter (odd) -> Map (*2) -> Take(3) -> Sum
    std::function<bool(const int&)> oddFilter = [](const int& x) { return x % 2 == 1; };
    auto filtered = rx::Filter(source, oddFilter);
    
    std::function<int(const int&)> doubleMap = [](const int& x) { return x * 2; };
    auto mapped = rx::Map<int, int>(filtered, doubleMap);
    
    auto mappedPtr = std::static_pointer_cast<rx::IObservable<int>>(mapped);
    auto taken = rx::Take(mappedPtr, static_cast<size_t>(3));
    auto sum = rx::Sum<int>(taken);
    
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    sum->Subscribe(observer);
    
    // Odd numbers: 1,3,5 -> doubled: 2,6,10 -> sum: 18
    TEST_ASSERT_EQUAL(18, observer->GetLastValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Reduce operator (from old additional_operators_test)
void test_reduce_basic() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    std::function<int(const int&, const int&)> accumulator = [](const int& acc, const int& x) { return acc + x; };
    auto reduce = rx::Reduce<int, int>(source, 0, accumulator);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    reduce->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(15, observer->GetLastValue()); // 1+2+3+4+5 = 15
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Subject with multiple observers (from old comprehensive tests)
void test_subject_multiple_observers() {
    auto subject = rx::Subject<int>();
    auto observer1 = std::make_shared<SimpleTestObserver<int>>();
    auto observer2 = std::make_shared<SimpleTestObserver<int>>();
    
    subject.Subscribe(observer1);
    subject.Subscribe(observer2);
    
    subject.OnNext(10);
    subject.OnNext(20);
    subject.OnCompleted();
    
    // Both observers should receive the same values
    TEST_ASSERT_EQUAL(2, observer1->GetCount());
    TEST_ASSERT_EQUAL(2, observer2->GetCount());
    TEST_ASSERT_EQUAL(20, observer1->GetLastValue());
    TEST_ASSERT_EQUAL(20, observer2->GetLastValue());
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test TimerObservable basic functionality
void test_timer_basic() {
    auto timer = rx::Timer<int>(std::chrono::milliseconds(100)); // 100ms timer
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to timer
    timer->Subscribe(observer);
    
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
}

// Test TimerObservable with multiple observers
void test_timer_multiple_observers() {
    auto timer = rx::Timer<int>(std::chrono::milliseconds(100));
    auto observer1 = std::make_shared<SimpleTestObserver<int>>();
    auto observer2 = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe both observers
    timer->Subscribe(observer1);
    timer->Subscribe(observer2);
    
    // Initially no values
    TEST_ASSERT_FALSE(observer1->HasValue());
    TEST_ASSERT_FALSE(observer2->HasValue());
    
    // Wait for timer to fire
    delay(200);
    
    // Both observers should receive the timer event
    TEST_ASSERT_TRUE(observer1->HasValue());
    TEST_ASSERT_TRUE(observer2->HasValue());
    TEST_ASSERT_EQUAL(1, observer1->GetCount());
    TEST_ASSERT_EQUAL(1, observer2->GetCount());
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test IntervalObservable basic functionality
void test_interval_basic() {
    // Create interval that emits 3 values every 50ms
    auto interval = rx::Interval<int>(std::chrono::milliseconds(50), 3);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe to interval
    interval->Subscribe(observer);
    
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
}

// Test IntervalObservable with multiple observers
void test_interval_multiple_observers() {
    auto interval = rx::Interval<int>(std::chrono::milliseconds(50), 2);
    auto observer1 = std::make_shared<SimpleTestObserver<int>>();
    auto observer2 = std::make_shared<SimpleTestObserver<int>>();
    
    // Subscribe both observers
    interval->Subscribe(observer1);
    interval->Subscribe(observer2);
    
    // Wait for all emissions to complete
    delay(150);
    
    // Both observers should receive all interval emissions
    TEST_ASSERT_EQUAL(1, observer1->GetLastValue()); // Last value should be 1
    TEST_ASSERT_EQUAL(1, observer2->GetLastValue());
    TEST_ASSERT_EQUAL(2, observer1->GetCount());
    TEST_ASSERT_EQUAL(2, observer2->GetCount());
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test Timer with custom type
void test_timer_custom_type() {
    auto timer = rx::Timer<float>(std::chrono::milliseconds(100));
    auto observer = std::make_shared<SimpleTestObserver<float>>();
    
    timer->Subscribe(observer);
    delay(200);
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FLOAT_WITHIN(0.01f, 0.0f, observer->GetLastValue()); // Default float value
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
    
    // Basic functionality tests (merged from original main.cpp)
    RUN_TEST(test_range_basic);
    RUN_TEST(test_fromvector_basic);
    RUN_TEST(test_empty_basic);
    RUN_TEST(test_map_basic);
    RUN_TEST(test_filter_basic);
    RUN_TEST(test_take_basic);
    RUN_TEST(test_skip_basic);
    RUN_TEST(test_scan_basic);
    RUN_TEST(test_sum_basic);
    RUN_TEST(test_distinct_basic);
    RUN_TEST(test_first_basic);
    RUN_TEST(test_last_basic);
    RUN_TEST(test_reduce_basic);
    RUN_TEST(test_subject_basic);
    RUN_TEST(test_behaviorsubject_basic);
    RUN_TEST(test_subject_multiple_observers);
    RUN_TEST(test_operator_chaining);
    RUN_TEST(test_complex_chain);
    RUN_TEST(test_timer_basic);
    RUN_TEST(test_timer_multiple_observers);
    RUN_TEST(test_interval_basic);
    RUN_TEST(test_interval_multiple_observers);
    RUN_TEST(test_timer_custom_type);
    
    UNITY_END();
}

void loop() {
    // Nothing to do here
}
