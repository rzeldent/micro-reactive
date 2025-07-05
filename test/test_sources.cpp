#ifndef TEST_SOURCES_CPP
#define TEST_SOURCES_CPP

#include "../include/micro-reactive.h"
#include <vector>
#include <memory>

// Helper observer for testing sources
template<typename T>
class SourceTestObserver : public rx::IObserver<T> {
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

// Test Range with 3 parameters
void test_range_three_params() {
    auto range = rx::Range(1, 5, 1);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(5, observer->GetValues()[4]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Range with 2 parameters (count mode)
void test_range_two_params() {
    auto range = rx::Range(10, 3); // Start at 10, count of 3
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(11, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(12, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Range with step
void test_range_with_step() {
    auto range = rx::Range(0, 10, 3);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->Count());
    TEST_ASSERT_EQUAL(0, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(6, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(9, observer->GetValues()[3]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Range descending
void test_range_descending() {
    auto range = rx::Range(5, 1, -1);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->Count());
    TEST_ASSERT_EQUAL(5, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(4, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(1, observer->GetValues()[4]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Range empty (no values)
void test_range_empty() {
    auto range = rx::Range(1, 0); // Count of 0
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(0, observer->Count());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test FromVector with integers
void test_fromvector_integers() {
    std::vector<int> values = {10, 20, 30, 40, 50};
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->Count());
    TEST_ASSERT_EQUAL(10, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(20, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(30, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(40, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(50, observer->GetValues()[4]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test FromVector with strings
void test_fromvector_strings() {
    std::vector<std::string> values = {"hello", "world", "reactive"};
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SourceTestObserver<std::string>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL_STRING("hello", observer->GetValues()[0].c_str());
    TEST_ASSERT_EQUAL_STRING("world", observer->GetValues()[1].c_str());
    TEST_ASSERT_EQUAL_STRING("reactive", observer->GetValues()[2].c_str());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test FromVector empty vector
void test_fromvector_empty() {
    std::vector<int> values;
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(0, observer->Count());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test FromVector with duplicates
void test_fromvector_duplicates() {
    std::vector<int> values = {1, 2, 2, 3, 3, 3};
    auto source = rx::FromVector(values);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[2]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[3]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[4]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[5]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Iterate function (if available)
void test_iterate() {
    std::vector<int> values = {100, 200, 300};
    auto source = rx::Iterate(values);
    auto observer = std::make_shared<SourceTestObserver<int>>();
    
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(100, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(200, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(300, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Create function
void test_create() {
    auto source = rx::Create<int>([](std::shared_ptr<rx::IObserver<int>> observer) {
        observer->OnNext(1);
        observer->OnNext(2);
        observer->OnNext(3);
        observer->OnCompleted();
    });
    
    auto observer = std::make_shared<SourceTestObserver<int>>();
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Create with error
void test_create_with_error() {
    auto source = rx::Create<int>([](std::shared_ptr<rx::IObserver<int>> observer) {
        observer->OnNext(1);
        observer->OnNext(2);
        std::runtime_error error("Test error");
        observer->OnError(error);
    });
    
    auto observer = std::make_shared<SourceTestObserver<int>>();
    source->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_FALSE(observer->IsCompleted());
    TEST_ASSERT_TRUE(observer->IsErrored());
}

// Test multiple subscriptions to same source
void test_multiple_subscriptions() {
    auto range = rx::Range(1, 3);
    auto observer1 = std::make_shared<SourceTestObserver<int>>();
    auto observer2 = std::make_shared<SourceTestObserver<int>>();
    
    range->Subscribe(observer1);
    range->Subscribe(observer2);
    
    // Both observers should receive the same sequence
    TEST_ASSERT_EQUAL(3, observer1->Count());
    TEST_ASSERT_EQUAL(3, observer2->Count());
    
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL(observer1->GetValues()[i], observer2->GetValues()[i]);
    }
    
    TEST_ASSERT_TRUE(observer1->IsCompleted());
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test suite runner
void test_sources_suite() {
    RUN_TEST(test_range_three_params);
    RUN_TEST(test_range_two_params);
    RUN_TEST(test_range_with_step);
    RUN_TEST(test_range_descending);
    RUN_TEST(test_range_empty);
    RUN_TEST(test_fromvector_integers);
    RUN_TEST(test_fromvector_strings);
    RUN_TEST(test_fromvector_empty);
    RUN_TEST(test_fromvector_duplicates);
    RUN_TEST(test_iterate);
    RUN_TEST(test_create);
    RUN_TEST(test_create_with_error);
    RUN_TEST(test_multiple_subscriptions);
}

#endif // TEST_SOURCES_CPP
