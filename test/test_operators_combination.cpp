#include "test_utils.h"
#include "../include/core.h"
#include "../include/subjects.h"
#include "../include/operators.h"

// Test Race operator
void test_race_operator() {
    auto source1 = std::make_shared<Subject<int>>();
    auto source2 = std::make_shared<Subject<int>>();
    
    std::vector<std::shared_ptr<IObservable<int>>> sources = {source1, source2};
    
    auto race_op = Race(sources);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    
    auto subscription = race_op->Subscribe(observer);
    
    // source2 emits first, so it should win
    source2->OnNext(42);
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(42, observer->GetLastValue());
    
    // source1 emits later, but should be ignored
    source1->OnNext(100);
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(42, observer->GetLastValue());
    
    // More emissions from winning source should pass through
    source2->OnNext(43);
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(43, observer->GetLastValue());
    
    source2->OnCompleted();
    TEST_ASSERT_TRUE(observer->IsCompleted());
}
