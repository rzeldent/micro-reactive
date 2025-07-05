#include <unity.h>
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
        // For simplicity, just mark as completed
        _completed = true;
    }

    T GetLastValue() const { return _lastValue; }
    bool HasValue() const { return _hasValue; }
    bool IsCompleted() const { return _completed; }
    int GetCount() const { return _count; }
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
    auto taken = rx::Take(source, 3);
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
    
    // Run basic functionality tests
    RUN_TEST(test_range_basic);
    RUN_TEST(test_fromvector_basic);
    RUN_TEST(test_map_basic);
    RUN_TEST(test_filter_basic);
    RUN_TEST(test_take_basic);
    RUN_TEST(test_scan_basic);
    RUN_TEST(test_sum_basic);
    RUN_TEST(test_subject_basic);
    RUN_TEST(test_behaviorsubject_basic);
    RUN_TEST(test_operator_chaining);
    
    UNITY_END();
}

void loop() {
    // Nothing to do here
}
