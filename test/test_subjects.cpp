#ifndef TEST_SUBJECTS_CPP
#define TEST_SUBJECTS_CPP

#include "../include/micro-reactive.h"
#include <vector>
#include <memory>

// Helper observer for testing subjects
template<typename T>
class SubjectTestObserver : public rx::IObserver<T> {
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

// Test basic Subject functionality
void test_subject_basic() {
    auto subject = rx::Subject<int>();
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    
    subject.Subscribe(observer);
    
    subject.OnNext(1);
    subject.OnNext(2);
    subject.OnNext(3);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(1, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(3, observer->GetValues()[2]);
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    subject.OnCompleted();
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Subject with multiple observers
void test_subject_multiple_observers() {
    auto subject = rx::Subject<int>();
    auto observer1 = std::make_shared<SubjectTestObserver<int>>();
    auto observer2 = std::make_shared<SubjectTestObserver<int>>();
    
    subject.Subscribe(observer1);
    subject.Subscribe(observer2);
    
    subject.OnNext(10);
    subject.OnNext(20);
    
    // Both observers should receive the same values
    TEST_ASSERT_EQUAL(2, observer1->Count());
    TEST_ASSERT_EQUAL(2, observer2->Count());
    TEST_ASSERT_EQUAL(10, observer1->GetValues()[0]);
    TEST_ASSERT_EQUAL(10, observer2->GetValues()[0]);
    TEST_ASSERT_EQUAL(20, observer1->GetValues()[1]);
    TEST_ASSERT_EQUAL(20, observer2->GetValues()[1]);
}

// Test Subject unsubscription
void test_subject_unsubscribe() {
    auto subject = rx::Subject<int>();
    auto observer1 = std::make_shared<SubjectTestObserver<int>>();
    auto observer2 = std::make_shared<SubjectTestObserver<int>>();
    
    subject.Subscribe(observer1);
    subject.Subscribe(observer2);
    
    subject.OnNext(1);
    
    // Unsubscribe observer1
    subject.UnSubscribe(observer1);
    
    subject.OnNext(2);
    
    // Observer1 should only have received the first value
    TEST_ASSERT_EQUAL(1, observer1->Count());
    TEST_ASSERT_EQUAL(1, observer1->GetValues()[0]);
    
    // Observer2 should have received both values
    TEST_ASSERT_EQUAL(2, observer2->Count());
    TEST_ASSERT_EQUAL(1, observer2->GetValues()[0]);
    TEST_ASSERT_EQUAL(2, observer2->GetValues()[1]);
}

// Test BehaviorSubject with initial value
void test_behaviorsubject_initial_value() {
    auto behaviorSubject = rx::BehaviorSubject<int>(42);
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    
    // Subscribe after creation - should immediately receive initial value
    behaviorSubject.Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(42, observer->GetValues()[0]);
    
    // Send more values
    behaviorSubject.OnNext(100);
    behaviorSubject.OnNext(200);
    
    TEST_ASSERT_EQUAL(3, observer->Count());
    TEST_ASSERT_EQUAL(42, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(100, observer->GetValues()[1]);
    TEST_ASSERT_EQUAL(200, observer->GetValues()[2]);
}

// Test BehaviorSubject with late subscriber
void test_behaviorsubject_late_subscriber() {
    auto behaviorSubject = rx::BehaviorSubject<int>(10);
    
    // Update value before any subscription
    behaviorSubject.OnNext(20);
    behaviorSubject.OnNext(30);
    
    // Now subscribe - should get the latest value (30)
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    behaviorSubject.Subscribe(observer);
    
    TEST_ASSERT_EQUAL(1, observer->Count());
    TEST_ASSERT_EQUAL(30, observer->GetValues()[0]);
    
    // Send another value
    behaviorSubject.OnNext(40);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(30, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(40, observer->GetValues()[1]);
}

// Test BehaviorSubject with multiple observers
void test_behaviorsubject_multiple_observers() {
    auto behaviorSubject = rx::BehaviorSubject<int>(5);
    auto observer1 = std::make_shared<SubjectTestObserver<int>>();
    auto observer2 = std::make_shared<SubjectTestObserver<int>>();
    
    behaviorSubject.Subscribe(observer1);
    behaviorSubject.OnNext(10);
    
    // Second observer subscribes later
    behaviorSubject.Subscribe(observer2);
    
    // Observer1 should have initial value + update
    TEST_ASSERT_EQUAL(2, observer1->Count());
    TEST_ASSERT_EQUAL(5, observer1->GetValues()[0]);
    TEST_ASSERT_EQUAL(10, observer1->GetValues()[1]);
    
    // Observer2 should only have the latest value at subscription time
    TEST_ASSERT_EQUAL(1, observer2->Count());
    TEST_ASSERT_EQUAL(10, observer2->GetValues()[0]);
    
    // Send another value - both should receive it
    behaviorSubject.OnNext(15);
    
    TEST_ASSERT_EQUAL(3, observer1->Count());
    TEST_ASSERT_EQUAL(2, observer2->Count());
    TEST_ASSERT_EQUAL(15, observer1->GetValues()[2]);
    TEST_ASSERT_EQUAL(15, observer2->GetValues()[1]);
}

// Test Subject error propagation
void test_subject_error() {
    auto subject = rx::Subject<int>();
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    
    subject.Subscribe(observer);
    
    subject.OnNext(1);
    subject.OnNext(2);
    
    // Send error
    std::runtime_error error("Test error");
    subject.OnError(error);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    TEST_ASSERT_TRUE(observer->IsErrored());
}

// Test BehaviorSubject error propagation
void test_behaviorsubject_error() {
    auto behaviorSubject = rx::BehaviorSubject<int>(0);
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    
    behaviorSubject.Subscribe(observer);
    behaviorSubject.OnNext(1);
    
    // Send error
    std::runtime_error error("Test error");
    behaviorSubject.OnError(error);
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    TEST_ASSERT_TRUE(observer->IsErrored());
}

// Test Subject as Observable (can be used with operators)
void test_subject_with_operators() {
    auto subject = rx::Subject<int>();
    
    // Use subject as source for operators
    auto filtered = rx::Filter(subject.AsObservable(), [](const int& x) { return x > 5; });
    auto mapped = rx::Map<int, int>(filtered, [](const int& x) { return x * 2; });
    
    auto observer = std::make_shared<SubjectTestObserver<int>>();
    mapped->Subscribe(observer);
    
    // Send values through subject
    subject.OnNext(3);  // Filtered out
    subject.OnNext(7);  // 7 -> 14
    subject.OnNext(10); // 10 -> 20
    subject.OnNext(2);  // Filtered out
    
    TEST_ASSERT_EQUAL(2, observer->Count());
    TEST_ASSERT_EQUAL(14, observer->GetValues()[0]);
    TEST_ASSERT_EQUAL(20, observer->GetValues()[1]);
}

// Test suite runner
void test_subjects_suite() {
    RUN_TEST(test_subject_basic);
    RUN_TEST(test_subject_multiple_observers);
    RUN_TEST(test_subject_unsubscribe);
    RUN_TEST(test_behaviorsubject_initial_value);
    RUN_TEST(test_behaviorsubject_late_subscriber);
    RUN_TEST(test_behaviorsubject_multiple_observers);
    RUN_TEST(test_subject_error);
    RUN_TEST(test_behaviorsubject_error);
    RUN_TEST(test_subject_with_operators);
}

#endif // TEST_SUBJECTS_CPP
