#ifndef TEST_OPERATORS_CPP
#define TEST_OPERATORS_CPP

#include "../include/micro-reactive.h"
#include <vector>
#include <memory>

// Helper observer for testing
template<typename T>
class TestObserver : public rx::IObserver<T> {
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

// Specialization for vector output (Buffer operator)
template<>
class TestObserver<std::vector<int>> : public rx::IObserver<std::vector<int>> {
private:
    std::vector<std::vector<int>> _values;
    bool _completed = false;
    bool _errored = false;

public:
    void OnNext(const std::vector<int>& value) override {
        _values.push_back(value);
    }

    void OnCompleted() override {
        _completed = true;
    }

    void OnError(const std::exception& e) override {
        _errored = true;
    }

    const std::vector<std::vector<int>>& GetValues() const { return _values; }
    bool IsCompleted() const { return _completed; }
    bool IsErrored() const { return _errored; }
    size_t Count() const { return _values.size(); }
    void Reset() { _values.clear(); _completed = false; _errored = false; }
};

// Test Map operator
void test_map_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 3));
    auto mapped = rx::Map<int, int>(source, [](const int& x) { return x * 2; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    mapped->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(2, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(6, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Filter operator
void test_filter_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    std::function<bool(const int&)> predicate = [](const int& x) { return x % 2 == 0; };
    auto filtered = rx::Filter(source, predicate);
    auto observer = std::make_shared<TestObserver<int>>();
    
    filtered->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(2, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Take operator
void test_take_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    auto taken = rx::Take(source, 3);
    auto observer = std::make_shared<TestObserver<int>>();
    
    taken->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Skip operator
void test_skip_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto skipped = rx::Skip(source, 2);
    auto observer = std::make_shared<TestObserver<int>>();
    
    skipped->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(5, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Distinct operator
void test_distinct_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(
        rx::FromVector<int>({1, 2, 2, 3, 1, 4, 3, 5})
    );
    auto distinct = rx::Distinct(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    distinct->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(5, observer->GetValues()[4]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Scan operator
void test_scan_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 4));
    auto scan = rx::Scan<int, int>(source, 0, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    scan->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);  // 0 + 1
    TEST_ASSERT_EQUAL(3, observer->GetValues()[1]);  // 1 + 2
    TEST_ASSERT_EQUAL(6, observer->GetValues()[2]);  // 3 + 3
    TEST_ASSERT_EQUAL(10, observer->GetValues()[3]); // 6 + 4
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Reduce operator
void test_reduce_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto reduce = rx::Reduce<int, int>(source, 0, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    reduce->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(15, observer->GetValues()[0]); // 1+2+3+4+5
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test First operator
void test_first_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 5));
    auto first = rx::First(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    first->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Last operator
void test_last_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 3));
    auto last = rx::Last(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    last->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(12, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Buffer operator
void test_buffer_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 7));
    auto buffered = rx::Buffer(source, 3);
    auto observer = std::make_shared<TestObserver<std::vector<int>>>();
    
    buffered->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    
    // First buffer: [1, 2, 3]
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0].size());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0][0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[0][1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[0][2]);
    
    // Second buffer: [4, 5, 6]
    TEST_ASSERT_EQUAL(3, observer->GetValues()[1].size());
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1][0]);
    
    // Third buffer: [7]
    TEST_ASSERT_EQUAL(1, observer->GetValues()[2].size());
    TEST_ASSERT_EQUAL(7, observer->GetValues()[2][0]);
    
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test TakeWhile operator
void test_takewhile_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    auto takeWhile = rx::TakeWhile<int>(source, [](const int& x) { return x < 5; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    takeWhile->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[3]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test SkipWhile operator
void test_skipwhile_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 8));
    auto skipWhile = rx::SkipWhile<int>(source, [](const int& x) { return x < 5; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    skipWhile->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(6, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(7, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(8, observer->GetValues()[3]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test StartWith operator
void test_startwith_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(5, 3));
    auto startWith = rx::StartWith<int>(source, {1, 2, 3});
    auto observer = std::make_shared<TestObserver<int>>();
    
    startWith->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(5, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(6, observer->GetValues()[4]);
    TEST_ASSERT_EQUAL(7, observer->GetValues()[5]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test DefaultIfEmpty operator with empty source
void test_defaultifempty_empty() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::FromVector<int>({}));
    auto defaultIfEmpty = rx::DefaultIfEmpty<int>(source, 42);
    auto observer = std::make_shared<TestObserver<int>>();
    
    defaultIfEmpty->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(42, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test DefaultIfEmpty operator with non-empty source
void test_defaultifempty_nonempty() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 2));
    auto defaultIfEmpty = rx::DefaultIfEmpty<int>(source, 42);
    auto observer = std::make_shared<TestObserver<int>>();
    
    defaultIfEmpty->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(11, observer->GetValues()[1]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Count operator
void test_count_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto count = rx::Count<int>(source);
    auto observer = std::make_shared<TestObserver<size_t>>();
    
    count->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Sum operator
void test_sum_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
    auto sum = rx::Sum<int>(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    sum->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(15, observer->GetValues()[0]); // 1+2+3+4+5
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Min operator
void test_min_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(
        rx::FromVector<int>({5, 2, 8, 1, 9})
    );
    auto min = rx::Min<int>(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    min->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Max operator
void test_max_operator() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(
        rx::FromVector<int>({5, 2, 8, 1, 9})
    );
    auto max = rx::Max<int>(source);
    auto observer = std::make_shared<TestObserver<int>>();
    
    max->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(9, observer->GetValues()[0]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test LINQ-style aliases
void test_linq_aliases() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 6));
    auto where = rx::Where(source, [](const int& x) { return x > 3; });
    auto select = rx::Select<int, int>(where, [](const int& x) { return x * 10; });
    auto observer = std::make_shared<TestObserver<int>>();
    
    select->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(40, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(50, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(60, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test operator chaining
void test_operator_chaining() {
    auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
    
    // Chain: Range -> Filter (even) -> Map (*2) -> Take(3)
    auto filtered = rx::Filter(source, [](const int& x) { return x % 2 == 0; });
    auto mapped = rx::Map<int, int>(filtered, [](const int& x) { return x * 2; });
    auto taken = rx::Take(mapped, 3);
    auto observer = std::make_shared<TestObserver<int>>();
    
    taken->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(4, observer->GetValues()[0]);  // 2 * 2
    TEST_ASSERT_EQUAL(8, observer->GetValues()[1]);  // 4 * 2
    TEST_ASSERT_EQUAL(12, observer->GetValues()[2]); // 6 * 2
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test suite runner
void test_operators_suite() {
    RUN_TEST(test_map_operator);
    RUN_TEST(test_filter_operator);
    RUN_TEST(test_take_operator);
    RUN_TEST(test_skip_operator);
    RUN_TEST(test_distinct_operator);
    RUN_TEST(test_scan_operator);
    RUN_TEST(test_reduce_operator);
    RUN_TEST(test_first_operator);
    RUN_TEST(test_last_operator);
    RUN_TEST(test_buffer_operator);
    RUN_TEST(test_takewhile_operator);
    RUN_TEST(test_skipwhile_operator);
    RUN_TEST(test_startwith_operator);
    RUN_TEST(test_defaultifempty_empty);
    RUN_TEST(test_defaultifempty_nonempty);
    RUN_TEST(test_count_operator);
    RUN_TEST(test_sum_operator);
    RUN_TEST(test_min_operator);
    RUN_TEST(test_max_operator);
    RUN_TEST(test_linq_aliases);
    RUN_TEST(test_operator_chaining);
}

#endif // TEST_OPERATORS_CPP
