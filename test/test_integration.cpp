#ifndef TEST_INTEGRATION_CPP
#define TEST_INTEGRATION_CPP

#include "../include/micro-reactive.h"
#include <vector>
#include <memory>

// Helper observer for integration tests
template<typename T>
class IntegrationTestObserver : public rx::IObserver<T> {
private:
    std::vector<T> _values;
    bool _completed = false;
    bool _errored = false;

public:
    void OnNext(const T& value) override {
        _values.push_back(value);
    }

    void OnCompleted() override {
        _completed = true;
    }

    void OnError(const std::exception& e) override {
        _errored = true;
    }

    const std::vector<T>& GetValues() const { return _values; }
    bool IsCompleted() const { return _completed; }
    bool IsErrored() const { return _errored; }
    size_t Count() const { return _values.size(); }
    void Reset() { _values.clear(); _completed = false; _errored = false; }
};

// Test complex operator chaining
void test_complex_operator_chain() {
    // Chain: Range -> Filter (odd) -> Map (*3) -> Take(4) -> Scan(sum)
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    auto filtered = rx::Filter(source, [](const int& x) { return x % 2 == 1; }); // odd numbers
    auto mapped = rx::Map<int, int>(filtered, [](const int& x) { return x * 3; }); // multiply by 3
    auto taken = rx::Take(mapped, 4); // take first 4
    auto scanned = rx::Scan<int, int>(taken, 0, [](const int& acc, const int& x) { return acc + x; }); // running sum
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    scanned->Subscribe(observer);
    
    // Source: 1,2,3,4,5,6,7,8,9,10
    // Filter (odd): 1,3,5,7,9
    // Map (*3): 3,9,15,21,27
    // Take(4): 3,9,15,21
    // Scan(sum): 3,12,27,48
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0]);   // 0 + 3
    TEST_ASSERT_EQUAL(12, observer->GetValues()[1]);  // 3 + 9
    TEST_ASSERT_EQUAL(27, observer->GetValues()[2]);  // 12 + 15
    TEST_ASSERT_EQUAL(48, observer->GetValues()[3]);  // 27 + 21
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Subject with operators
void test_subject_with_complex_operators() {
    auto subject = rx::Subject<int>();
    
    // Chain: Subject -> Where(>5) -> Select(*2) -> TakeWhile(<50)
    auto where = rx::Where(subject.AsObservable(), [](const int& x) { return x > 5; });
    auto select = rx::Select<int, int>(where, [](const int& x) { return x * 2; });
    auto takeWhile = rx::TakeWhile<int>(select, [](const int& x) { return x < 50; });
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    takeWhile->Subscribe(observer);
    
    // Send values through subject
    subject.OnNext(3);   // Filtered out (≤5)
    subject.OnNext(8);   // 8 > 5 -> 8*2 = 16 < 50 ✓
    subject.OnNext(12);  // 12 > 5 -> 12*2 = 24 < 50 ✓
    subject.OnNext(30);  // 30 > 5 -> 30*2 = 60 ≥ 50 ✗ (stops here)
    subject.OnNext(2);   // Not processed (TakeWhile already completed)
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(16, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(24, observer->GetValues()[1]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test BehaviorSubject with operators
void test_behaviorsubject_with_operators() {
    auto behaviorSubject = rx::BehaviorSubject<int>(10);
    
    // Chain: BehaviorSubject -> Filter(even) -> Map(sqrt simulation)
    auto filtered = rx::Filter(behaviorSubject.AsObservable(), [](const int& x) { return x % 2 == 0; });
    auto mapped = rx::Map<int, int>(filtered, [](const int& x) { return x / 2; }); // Simple "sqrt" simulation
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    mapped->Subscribe(observer);
    
    // Should immediately get initial value (10 is even -> 10/2 = 5)
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    
    behaviorSubject.OnNext(15); // Odd, filtered out
    behaviorSubject.OnNext(20); // Even, 20/2 = 10
    behaviorSubject.OnNext(7);  // Odd, filtered out
    behaviorSubject.OnNext(8);  // Even, 8/2 = 4
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(10, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[2]);
}

// Test aggregation operators
void test_aggregation_operators() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(
        rx::FromVector<int>({3, 1, 4, 1, 5, 9, 2, 6})
    );
    
    // Test Count
    auto count = rx::Count<int>(source);
    auto countObserver = std::make_shared<IntegrationTestObserver<size_t>>();
    count->Subscribe(countObserver);
    
    TEST_ASSERT_EQUAL(1, countObserver->Count());
    TEST_ASSERT_EQUAL(8, countObserver->GetValues()[0]);
    
    // Test Sum
    auto sum = rx::Sum<int>(source);
    auto sumObserver = std::make_shared<IntegrationTestObserver<int>>();
    sum->Subscribe(sumObserver);
    
    TEST_ASSERT_EQUAL(1, sumObserver->Count());
    TEST_ASSERT_EQUAL(31, sumObserver->GetValues()[0]); // 3+1+4+1+5+9+2+6
    
    // Test Min
    auto min = rx::Min<int>(source);
    auto minObserver = std::make_shared<IntegrationTestObserver<int>>();
    min->Subscribe(minObserver);
    
    TEST_ASSERT_EQUAL(1, minObserver->Count());
    TEST_ASSERT_EQUAL(1, minObserver->GetValues()[0]);
    
    // Test Max
    auto max = rx::Max<int>(source);
    auto maxObserver = std::make_shared<IntegrationTestObserver<int>>();
    max->Subscribe(maxObserver);
    
    TEST_ASSERT_EQUAL(1, maxObserver->Count());
    TEST_ASSERT_EQUAL(9, maxObserver->GetValues()[0]);
}

// Test conditional operators combination
void test_conditional_operators() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 20));
    
    // Chain: Range -> SkipWhile(<5) -> TakeWhile(<15) -> DefaultIfEmpty
    auto skipWhile = rx::SkipWhile<int>(source, [](const int& x) { return x < 5; });
    auto takeWhile = rx::TakeWhile<int>(skipWhile, [](const int& x) { return x < 15; });
    auto defaultIfEmpty = rx::DefaultIfEmpty<int>(takeWhile, 999);
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    defaultIfEmpty->Subscribe(observer);
    
    // Source: 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    // SkipWhile(<5): 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    // TakeWhile(<15): 5,6,7,8,9,10,11,12,13,14
    // DefaultIfEmpty: 5,6,7,8,9,10,11,12,13,14 (not empty, so no default)
    
    TEST_ASSERT_EQUAL(10, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(14, observer->GetValues()[9]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Buffer with complex processing
void test_buffer_processing() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    auto buffered = rx::Buffer(source, 3);
    
    auto observer = std::make_shared<IntegrationTestObserver<std::vector<int>>>();
    buffered->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    
    // First buffer: [1,2,3]
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0].size());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0][0]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0][2]);
    
    // Second buffer: [4,5,6]
    TEST_ASSERT_EQUAL(3, observer->GetValues()[1].size());
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1][0]);
    
    // Third buffer: [7,8,9]
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2].size());
    TEST_ASSERT_EQUAL(7, observer->GetValues()[2][0]);
    
    // Fourth buffer: [10]
    TEST_ASSERT_EQUAL(1, observer->GetValues()[3].size());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[3][0]);
    
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test StartWith with complex chain
void test_startwith_complex() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 3));
    auto startWith = rx::StartWith<int>(source, {1, 2, 3});
    auto filtered = rx::Filter(startWith, [](const int& x) { return x != 2; }); // Remove 2s
    auto mapped = rx::Map<int, int>(filtered, [](const int& x) { return x * 10; });
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    mapped->Subscribe(observer);
    
    // StartWith: 1,2,3,10,11,12
    // Filter(!= 2): 1,3,10,11,12
    // Map(*10): 10,30,100,110,120
    
    TEST_ASSERT_EQUAL(5, observer->Count());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(30, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(100, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(110, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(120, observer->GetValues()[4]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test error propagation through chain
void test_error_propagation() {
    auto subject = rx::Subject<int>();
    auto mapped = rx::Map<int, int>(subject.AsObservable(), [](const int& x) { 
        if (x == 5) throw std::runtime_error("Test error");
        return x * 2; 
    });
    auto filtered = rx::Filter(mapped, [](const int& x) { return x > 0; });
    
    auto observer = std::make_shared<IntegrationTestObserver<int>>();
    filtered->Subscribe(observer);
    
    subject.OnNext(1); // 1*2 = 2
    subject.OnNext(3); // 3*2 = 6
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(2, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(6, observer->GetValues()[1]);
    TEST_ASSERT_FALSE(observer->IsErrored());
    
    // This should cause an error
    subject.OnNext(5);
    
    TEST_ASSERT_EQUAL(2, observer->Count()); // No additional values
    TEST_ASSERT_TRUE(observer->IsErrored());
    TEST_ASSERT_FALSE(observer->IsCompleted());
}

// Test multiple observers on same chain
void test_multiple_observers_on_chain() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto mapped = rx::Map<int, int>(source, [](const int& x) { return x * 2; });
    
    auto observer1 = std::make_shared<IntegrationTestObserver<int>>();
    auto observer2 = std::make_shared<IntegrationTestObserver<int>>();
    
    mapped->Subscribe(observer1);
    mapped->Subscribe(observer2);
    
    // Both observers should receive the same transformed values
    TEST_ASSERT_EQUAL(5, observer1->Count());
    TEST_ASSERT_EQUAL(5, observer2->Count());
    
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_EQUAL(observer1->GetValues()[i], observer2->GetValues()[i]);
        TEST_ASSERT_EQUAL((i + 1) * 2, observer1->GetValues()[i]);
    }
    
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test suite runner
void test_integration_suite() {
    RUN_TEST(test_complex_operator_chain);
    RUN_TEST(test_subject_with_complex_operators);
    RUN_TEST(test_behaviorsubject_with_operators);
    RUN_TEST(test_aggregation_operators);
    RUN_TEST(test_conditional_operators);
    RUN_TEST(test_buffer_processing);
    RUN_TEST(test_startwith_complex);
    RUN_TEST(test_error_propagation);
    RUN_TEST(test_multiple_observers_on_chain);
}

#endif // TEST_INTEGRATION_CPP
