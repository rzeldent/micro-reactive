#include "test_utils.h"
#include <core.h>
#include <sources.h>
#include <subjects.h>
#include <operators.h>

// Test FilterOperator with new subscription pattern
void test_filter_operator() {
    auto range = Range(1, 5); // 1, 2, 3, 4, 5
    auto filterOp = Filter<int>(range, [](const int& x) { return x % 2 == 0; }); // Only even numbers
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = filterOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(4, observer->GetLastValue()); // Last even number is 4
    TEST_ASSERT_EQUAL(2, observer->GetCount()); // Should have 2 and 4
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test TakeOperator with new subscription pattern
void test_take_operator() {
    auto range = Range(10, 5); // 10, 11, 12, 13, 14
    auto takeOp = Take<int>(range, 2); // Take first 2
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = takeOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(11, observer->GetLastValue()); // Second value is 11
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test SkipOperator with new subscription pattern
void test_skip_operator() {
    auto range = Range(1, 5); // 1, 2, 3, 4, 5
    auto skip_op = Skip(range, 2);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = skip_op->Subscribe(observer);
    
    // Should skip first 2 elements (1, 2), emit 3, 4, 5
    TEST_ASSERT_EQUAL(5, observer->GetLastValue()); // Last value is 5
    TEST_ASSERT_EQUAL(3, observer->GetCount()); // Should have 3 values (3, 4, 5)
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// Test TakeWhile operator - RE-ENABLED (TakeWhile operator is implemented)
void test_take_while_operator() {
    auto range = Range(1, 10);
    std::function<bool(const int&)> predicate = [](const int& value) { return value < 5; };
    auto take_while_op = TakeWhile(range, predicate);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = take_while_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(4, observer->GetCount());
    TEST_ASSERT_EQUAL(4, observer->GetLastValue()); // Should take while < 5, so 1,2,3,4
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

// Test SkipWhile operator - RE-ENABLED (SkipWhile operator is implemented)
void test_skip_while_operator() {
    auto range = Range(1, 6);
    std::function<bool(const int&)> predicate = [](const int& value) { return value < 4; };
    auto skip_while_op = SkipWhile(range, predicate);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = skip_while_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // Should skip 1,2,3 and emit 4,5,6
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

void test_first_operator() {
    auto range = Range(1, 5);
    auto first_op = First(range);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = first_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(1, observer->GetLastValue()); // Should only emit the first value
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

void test_last_operator() {
    auto range = Range(1, 5);
    auto last_op = Last(range);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = last_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(5, observer->GetLastValue()); // Should only emit the last value
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

// Test distinct operator
void test_distinct_operator() {
    auto subject = std::make_shared<Subject<int>>();
    auto distinct_op = Distinct(subject);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = distinct_op->Subscribe(observer);
    
    // Emit sequence with duplicates: 1, 2, 2, 3, 1, 4
    subject->OnNext(1);
    subject->OnNext(2);
    subject->OnNext(2); // Duplicate - should be filtered
    subject->OnNext(3);
    subject->OnNext(1); // Duplicate - should be filtered  
    subject->OnNext(4);
    subject->OnCompleted();
    
    // Should only emit distinct values: 1, 2, 3, 4
    TEST_ASSERT_EQUAL(4, observer->GetLastValue()); // Last distinct value is 4
    TEST_ASSERT_EQUAL(4, observer->GetCount()); // Should have 4 distinct values
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}
