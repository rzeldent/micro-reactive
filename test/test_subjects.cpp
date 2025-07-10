#include "test_utils.h"
#include "../include/core.h"
#include "../include/subjects.h"

// Test Subject basic functionality
void test_subject_basic() {
    auto subject = std::make_shared<Subject<int>>();
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
    auto behaviorSubject = std::make_shared<BehaviorSubject<int>>(99);
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

// Test Subject with multiple observers (thread safety test)
void test_subject_multiple_observers() {
    auto subject = std::make_shared<Subject<int>>();
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
