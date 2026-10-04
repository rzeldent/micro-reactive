#include "test_utils.h"
#include <core.h>
#include <sources.h>
#include <subjects.h>
#include <operators.h>

// Test ThrottleOperator with new subscription pattern
void test_throttle_operator() {
    auto range = Range(1, 6); // 1, 2, 3, 4, 5, 6
    auto throttleOp = Throttle<int>(range, 2); // Every 2nd item
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = throttleOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // Should get items 2, 4, 6
    TEST_ASSERT_EQUAL(3, observer->GetCount()); // Every 2nd item: 2, 4, 6
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
}

void test_default_if_empty_operator() {
    auto empty_source = Empty<int>();
    auto default_op = DefaultIfEmpty(empty_source, 42);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = default_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(42, observer->GetLastValue()); // Should emit default value
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

void test_start_with_operator() {
    auto range = Range(3, 2); // Emits 3, 4
    auto start_with_op = StartWith(range, std::vector<int>{1, 2});
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = start_with_op->Subscribe(observer);
    
    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(4, observer->GetCount());
    // Values should be: 1, 2, 3, 4 (start values first, then original)
    TEST_ASSERT_EQUAL(4, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    subscription->Dispose();
}

// Test Do/Tap operator
void test_do_operator() {
    auto range = Range(1, 3); // 1, 2, 3
    int side_effect_count = 0;
    int last_side_effect_value = 0;
    
    std::function<void(const int&)> action = [&side_effect_count, &last_side_effect_value](const int& x) {
        side_effect_count++;
        last_side_effect_value = x;
    };
    
    auto do_op = Do(range, action);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = do_op->Subscribe(observer);
    
    // Original stream should be unmodified
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Side effects should have occurred
    TEST_ASSERT_EQUAL(3, side_effect_count);
    TEST_ASSERT_EQUAL(3, last_side_effect_value);
}

// Test TakeUntil operator
void test_take_until_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto trigger = std::make_shared<Subject<bool>>();
    
    auto take_until_op = TakeUntil(source, trigger);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = take_until_op->Subscribe(observer);
    
    // Send some values
    source->OnNext(1);
    source->OnNext(2);
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(2, observer->GetLastValue());
    TEST_ASSERT_FALSE(observer->IsCompleted());
    
    // Trigger should complete the stream
    trigger->OnNext(true);
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Additional values should not be received
    source->OnNext(3);
    TEST_ASSERT_EQUAL(2, observer->GetCount());
}

// Test SkipUntil operator
void test_skip_until_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto trigger = std::make_shared<Subject<bool>>();
    
    auto skip_until_op = SkipUntil(source, trigger);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = skip_until_op->Subscribe(observer);
    
    // Send some values before trigger - should be skipped
    source->OnNext(1);
    source->OnNext(2);
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_FALSE(observer->HasValue());
    
    // Trigger should start passing values
    trigger->OnNext(true);
    source->OnNext(3);
    source->OnNext(4);
    
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(4, observer->GetLastValue());
    
    source->OnCompleted();
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Contains operator
void test_contains_operator() {
    auto range = Range(1, 5); // 1, 2, 3, 4, 5
    
    auto contains_op = Contains(range, 3);
    auto observer = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription = contains_op->Subscribe(observer);
    
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_TRUE(observer->GetLastValue()); // Should find 3
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Test with value not in range
    auto range2 = Range(1, 3); // 1, 2, 3
    auto contains_op2 = Contains(range2, 5);
    auto observer2 = std::make_shared<SimpleTestObserver<bool>>();
    
    auto subscription2 = contains_op2->Subscribe(observer2);
    
    TEST_ASSERT_TRUE(observer2->HasValue());
    TEST_ASSERT_FALSE(observer2->GetLastValue()); // Should not find 5
    TEST_ASSERT_TRUE(observer2->IsCompleted());
}

// Test DistinctUntilChanged operator
void test_distinct_until_changed_operator() {
    auto source = std::make_shared<Subject<int>>();
    
    auto distinct_op = DistinctUntilChanged(source);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = distinct_op->Subscribe(observer);
    
    // Send duplicate values
    source->OnNext(1);
    source->OnNext(1); // Should be filtered out
    source->OnNext(2);
    source->OnNext(2); // Should be filtered out
    source->OnNext(3);
    source->OnNext(1); // Different from previous, so should pass
    
    TEST_ASSERT_EQUAL(4, observer->GetCount()); // 1, 2, 3, 1
    TEST_ASSERT_EQUAL(1, observer->GetLastValue());
    
    source->OnCompleted();
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Test Pairwise operator
void test_pairwise_operator() {
    auto source = std::make_shared<Subject<int>>();
    
    auto pairwise_op = Pairwise(source);
    auto observer = std::make_shared<SimpleTestObserver<std::pair<int, int>>>();
    
    auto subscription = pairwise_op->Subscribe(observer);
    
    // Send values
    source->OnNext(1); // No pair yet
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    
    source->OnNext(2); // First pair: (1, 2)
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(1, observer->GetLastValue().first);
    TEST_ASSERT_EQUAL(2, observer->GetLastValue().second);
    
    source->OnNext(3); // Second pair: (2, 3)
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(2, observer->GetLastValue().first);
    TEST_ASSERT_EQUAL(3, observer->GetLastValue().second);
    
    source->OnCompleted();
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

// Debug operator tests
void test_debug_operator() {
    auto range = Range(1, 3);
    auto debug_op = Debug(range, "TestRange");
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = debug_op->Subscribe(observer);
    
    // Wait for completion
    testSleepForMilliseconds(10);
    
    // Verify the observable works normally
    TEST_ASSERT_TRUE(observer->HasValue());
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    
    // Verify metrics were collected
    auto metrics = debug_op->GetMetrics();
    TEST_ASSERT_TRUE(metrics != nullptr);
    TEST_ASSERT_EQUAL(3, metrics->GetEmissionsCount());
    TEST_ASSERT_EQUAL(1, metrics->GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(1, metrics->GetCompletionsCount());
    TEST_ASSERT_EQUAL(0, metrics->GetErrorsCount());
    
    // Verify name
    TEST_ASSERT_EQUAL_STRING("TestRange", debug_op->GetName().c_str());
    
    subscription->Dispose();
}

void test_debug_operator_with_error() {
    auto subject = std::make_shared<Subject<int>>();
    auto debug_op = Debug(subject, "ErrorTest");
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = debug_op->Subscribe(observer);
    
    // Emit some values
    subject->OnNext(1);
    subject->OnNext(2);
    
    // Emit an error
    subject->OnError(std::runtime_error("Test error"));
    
    // Verify metrics
    auto metrics = debug_op->GetMetrics();
    TEST_ASSERT_EQUAL(2, metrics->GetEmissionsCount());
    TEST_ASSERT_EQUAL(1, metrics->GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(0, metrics->GetCompletionsCount());
    TEST_ASSERT_EQUAL(1, metrics->GetErrorsCount());
    
    subscription->Dispose();
}
