#include "test_utils.h"
#include "../include/core.h"
#include "../include/sources.h"
#include "../include/subjects.h"
#include "../include/operators.h"

// Test MapOperator with new subscription pattern
void test_map_operator() {
    auto range = Range(1, 3); // 1, 2, 3
    auto mapOp = Map<int, int>(range, [](const int& x) { return x * 2; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = mapOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // 3 * 2 = 6
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
    
    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

// Test ScanOperator with new subscription pattern
void test_scan_operator() {
    auto range = Range(1, 4); // 1, 2, 3, 4
    auto scanOp = Scan<int, int>(range, 0, [](const int& acc, const int& x) { return acc + x; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = scanOp->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(10, observer->GetLastValue()); // 0+1+2+3+4 = 10
    TEST_ASSERT_EQUAL(4, observer->GetCount()); // Should emit accumulated value for each input
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());
    
    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}
