#include "test_utils.h"
#include <sources.h>
#include <subjects.h>
#include <operators.h>

void test_debounce_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto scheduler = std::make_shared<TestScheduler>();
    auto debounced = Debounce<int>(
        source, std::chrono::milliseconds(10), scheduler);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    auto subscription = debounced->Subscribe(observer);

    source->OnNext(1);
    scheduler->AdvanceBy(std::chrono::milliseconds(5));
    source->OnNext(2);
    source->OnCompleted();
    scheduler->AdvanceBy(std::chrono::milliseconds(9));
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    scheduler->AdvanceBy(std::chrono::milliseconds(1));

    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(2, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}

void test_delay_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto scheduler = std::make_shared<TestScheduler>();
    auto delayed = Delay<int>(
        source, std::chrono::milliseconds(10), scheduler);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    auto subscription = delayed->Subscribe(observer);

    source->OnNext(42);
    source->OnCompleted();
    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_FALSE(observer->IsCompleted());

    scheduler->AdvanceBy(std::chrono::milliseconds(10));
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(42, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}

void test_sample_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto scheduler = std::make_shared<TestScheduler>();
    auto sampled = Sample<int>(
        source, std::chrono::milliseconds(10), scheduler);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    auto subscription = sampled->Subscribe(observer);

    source->OnNext(1);
    source->OnNext(2);
    scheduler->AdvanceBy(std::chrono::milliseconds(10));
    TEST_ASSERT_EQUAL(1, observer->GetCount());
    TEST_ASSERT_EQUAL(2, observer->GetLastValue());

    source->OnNext(3);
    source->OnCompleted();
    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}
