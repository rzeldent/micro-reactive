#include "test_utils.h"
#include <core.h>
#include <subjects.h>
#include <operators.h>

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

void test_merge_operator() {
    auto first = std::make_shared<Subject<int>>();
    auto second = std::make_shared<Subject<int>>();
    auto merged = Merge<int>(first, second);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    auto subscription = merged->Subscribe(observer);

    first->OnNext(1);
    second->OnNext(2);
    first->OnCompleted();
    TEST_ASSERT_FALSE(observer->IsCompleted());
    second->OnNext(3);
    second->OnCompleted();

    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}

void test_merge_empty_sources() {
    auto merged = Merge<int>({});
    auto observer = std::make_shared<SimpleTestObserver<int>>();

    merged->Subscribe(observer);

    TEST_ASSERT_EQUAL(0, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

void test_zip_operator() {
    auto zipped = Zip<int, const char *>(
        FromVector(std::vector<int>{1, 2, 3}),
        FromVector(std::vector<const char *>{"a", "b"}));
    auto observer =
        std::make_shared<SimpleTestObserver<std::pair<int, const char *>>>();

    zipped->Subscribe(observer);

    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(2, observer->GetLastValue().first);
    TEST_ASSERT_EQUAL_STRING("b", observer->GetLastValue().second);
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

void test_flat_map_operator() {
    auto flattened = FlatMap<int, int>(
        Range(1, 3),
        std::function<std::shared_ptr<IObservable<int>>(const int &)>(
            [](const int &value) {
                return Range(value * 10, 2);
            }));
    auto observer = std::make_shared<SimpleTestObserver<int>>();

    flattened->Subscribe(observer);

    TEST_ASSERT_EQUAL(6, observer->GetCount());
    TEST_ASSERT_EQUAL(31, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

void test_concat_operator() {
    auto concatenated = Concat<int>({
        FromVector(std::vector<int>{1, 2}),
        FromVector(std::vector<int>{3, 4})});
    auto observer = std::make_shared<SimpleTestObserver<int>>();

    concatenated->Subscribe(observer);

    TEST_ASSERT_EQUAL(4, observer->GetCount());
    TEST_ASSERT_EQUAL(4, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
}

void test_switch_operator() {
    auto outer =
        std::make_shared<Subject<std::shared_ptr<IObservable<int>>>>();
    auto first = std::make_shared<Subject<int>>();
    auto second = std::make_shared<Subject<int>>();
    auto switched = Switch<int>(outer);
    auto observer = std::make_shared<SimpleTestObserver<int>>();
    auto subscription = switched->Subscribe(observer);

    outer->OnNext(first);
    first->OnNext(1);
    outer->OnNext(second);
    first->OnNext(2);
    second->OnNext(3);
    outer->OnCompleted();
    TEST_ASSERT_FALSE(observer->IsCompleted());
    second->OnCompleted();

    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}

void test_with_latest_from_operator() {
    auto source = std::make_shared<Subject<int>>();
    auto other = std::make_shared<Subject<const char *>>();
    auto combined = WithLatestFrom<int, const char *>(source, other);
    auto observer =
        std::make_shared<SimpleTestObserver<std::pair<int, const char *>>>();
    auto subscription = combined->Subscribe(observer);

    source->OnNext(1);
    other->OnNext("ready");
    source->OnNext(2);
    other->OnNext("updated");
    source->OnNext(3);
    source->OnCompleted();

    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_EQUAL(3, observer->GetLastValue().first);
    TEST_ASSERT_EQUAL_STRING("updated", observer->GetLastValue().second);
    TEST_ASSERT_TRUE(observer->IsCompleted());
    subscription->Dispose();
}
