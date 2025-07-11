#include "test_utils.h"
#include "../include/core.h"
#include "../include/sources.h"
#include "../include/subjects.h"
#include "../include/operators.h"

// Test ReduceOperator with new subscription pattern  
void test_reduce_operator() {
    auto range = Range(1, 3); // 1, 2, 3
    auto reduceOp = Reduce<int, int>(range, 10, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = reduceOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(16, observer->GetLastValue()); // 10+1+2+3 = 16
    TEST_ASSERT_EQUAL(1, observer->GetCount()); // Should emit only final result
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

// These operators are now implemented
void test_count_operator() {
    auto range = Range(1, 5);
    auto count_op = Count(range);
    auto observer = std::make_shared<SimpleTestObserver<size_t>>();
    
    auto subscription = count_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(5, observer->GetLastValue()); // Should count all 5 values
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

void test_sum_operator() {
    auto range = Range(1, 5);
    auto sum_op = Sum(range);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = sum_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(15, observer->GetLastValue()); // 1+2+3+4+5 = 15
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

// Test average operator - NOW IMPLEMENTED
void test_average_operator() {
    auto range = Range(1, 5); // 1, 2, 3, 4, 5
    auto average_op = Average(range);
    auto observer = std::make_shared<SimpleTestObserver<double>>();
    
    auto subscription = average_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    // Should emit average: (1+2+3+4+5)/5 = 15/5 = 3.0
    TEST_ASSERT_EQUAL(3.0, observer->GetLastValue()); // Average is 3.0
    TEST_ASSERT_EQUAL(1, observer->GetCount()); // Should emit only final average
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

void test_min_operator() {
    std::vector<int> data = {5, 2, 8, 1, 9, 3};
    auto source = FromVector(data);
    auto min_op = Min(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = min_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(1, observer->GetLastValue()); // Minimum value
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

void test_max_operator() {
    std::vector<int> data = {5, 2, 8, 1, 9, 3};
    auto source = FromVector(data);
    auto max_op = Max(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = max_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(9, observer->GetLastValue()); // Maximum value
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

// Test All operator
void test_all_operator() {
    auto range = Range(2, 4); // 2, 3, 4, 5
    
    std::function<bool(const int&)> predicate = [](const int& x) { return x > 1; };
    auto all_op = All(range, predicate);
    auto observer = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription = all_op->Subscribe(observer);
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_TRUE(observer->GetLastValue()); // All values > 1
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Test with predicate that fails
    std::function<bool(const int&)> predicate2 = [](const int& x) { return x > 3; };
    auto all_op2 = All(range, predicate2);
    auto observer2 = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription2 = all_op2->Subscribe(observer2);
    
    TEST_ASSERT_TRUE(observer2->HasValue());
    TEST_ASSERT_FALSE(observer2->GetLastValue()); // Not all values > 3 (2 and 3 fail)
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test Any operator
void test_any_operator() {
    auto range = Range(1, 3); // 1, 2, 3
    
    std::function<bool(const int&)> predicate = [](const int& x) { return x > 2; };
    auto any_op = Any(range, predicate);
    auto observer = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription = any_op->Subscribe(observer);
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_TRUE(observer->GetLastValue()); // 3 > 2
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Test with predicate that never matches
    std::function<bool(const int&)> predicate2 = [](const int& x) { return x > 5; };
    auto any_op2 = Any(range, predicate2);
    auto observer2 = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription2 = any_op2->Subscribe(observer2);
    
    TEST_ASSERT_TRUE(observer2->HasValue());
    TEST_ASSERT_FALSE(observer2->GetLastValue()); // No values > 5
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}
