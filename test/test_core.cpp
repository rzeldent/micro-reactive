#include <unity.h>
#include <vector>
#include <memory>
#include "micro-reactive.h"

using namespace rx;

void test_observer_interface() {
    class TestObserver : public IObserver<int> {
    public:
        std::vector<int> values;
        bool completed = false;
        bool errorOccurred = false;
        std::string errorMessage;
        
        void OnNext(const int &value) override {
            values.push_back(value);
        }

        void OnCompleted() override {
            completed = true;
        }

        void OnError(const std::exception &e) override {
            errorOccurred = true;
            errorMessage = e.what();
        }
    };
    
    auto observer = std::make_shared<TestObserver>();
    
    // Test OnNext
    observer->OnNext(1);
    observer->OnNext(2);
    observer->OnNext(3);
    
    TEST_ASSERT_EQUAL(3, observer->values.size());
    TEST_ASSERT_EQUAL(1, observer->values[0]);
    TEST_ASSERT_EQUAL(2, observer->values[1]);
    TEST_ASSERT_EQUAL(3, observer->values[2]);
    
    // Test OnCompleted
    observer->OnCompleted();
    TEST_ASSERT_TRUE(observer->completed);
    
    // Test OnError
    observer->OnError(std::runtime_error("Test error"));
    TEST_ASSERT_TRUE(observer->errorOccurred);
    TEST_ASSERT_EQUAL_STRING("Test error", observer->errorMessage.c_str());
}

void test_function_observer_creation() {
    bool onNextCalled = false;
    bool onCompletedCalled = false;
    bool onErrorCalled = false;
    int lastValue = 0;
    
    auto observer = CreateObserver<int>(
        [&](const int& value) { 
            onNextCalled = true;
            lastValue = value;
        },
        [&]() {
            onCompletedCalled = true;
        },
        [&](const std::exception& e) {
            onErrorCalled = true;
        }
    );
    
    TEST_ASSERT_NOT_NULL(observer);
    
    observer->OnNext(42);
    TEST_ASSERT_TRUE(onNextCalled);
    TEST_ASSERT_EQUAL(42, lastValue);
    
    observer->OnCompleted();
    TEST_ASSERT_TRUE(onCompletedCalled);
    
    observer->OnError(std::runtime_error("Test"));
    TEST_ASSERT_TRUE(onErrorCalled);
}

void test_function_observer_partial_functions() {
    // Test with only onNext function
    bool onNextCalled = false;
    int receivedValue = 0;
    
    auto observer = CreateObserver<int>(
        [&](const int& value) { 
            onNextCalled = true;
            receivedValue = value;
        }
    );
    
    TEST_ASSERT_NOT_NULL(observer);
    
    observer->OnNext(123);
    TEST_ASSERT_TRUE(onNextCalled);
    TEST_ASSERT_EQUAL(123, receivedValue);
    
    // These should not crash even though handlers weren't provided
    observer->OnCompleted();
    observer->OnError(std::runtime_error("Test"));
}

void test_observable_subscribe_unsubscribe() {
    class TestObservable : public IObservable<int> {
    public:
        std::vector<std::shared_ptr<IObserver<int>>> observers;
        
        void Subscribe(std::shared_ptr<IObserver<int>> observer) override {
            observers.push_back(observer);
        }

        void UnSubscribe(std::shared_ptr<IObserver<int>> observer) override {
            auto it = std::find(observers.begin(), observers.end(), observer);
            if (it != observers.end()) {
                observers.erase(it);
            }
        }
        
        void EmitValue(int value) {
            for (auto& obs : observers) {
                obs->OnNext(value);
            }
        }
        
        void EmitCompleted() {
            for (auto& obs : observers) {
                obs->OnCompleted();
            }
        }
    };
    
    auto observable = std::make_shared<TestObservable>();
    auto observer1 = CreateObserver<int>([](const int&){});
    auto observer2 = CreateObserver<int>([](const int&){});
    
    // Test subscribe
    observable->Subscribe(observer1);
    observable->Subscribe(observer2);
    TEST_ASSERT_EQUAL(2, observable->observers.size());
    
    // Test unsubscribe
    observable->UnSubscribe(observer1);
    TEST_ASSERT_EQUAL(1, observable->observers.size());
    TEST_ASSERT_EQUAL(observer2, observable->observers[0]);
}

void test_range_basic_functionality() {
    class CountingObserver : public IObserver<int> {
    public:
        std::vector<int> values;
        bool completed = false;
        
        void OnNext(const int &value) override {
            values.push_back(value);
        }

        void OnCompleted() override {
            completed = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("Unexpected error in range test");
        }
    };
    
    auto observer = std::make_shared<CountingObserver>();
    auto range = Range<int>(1, 5, 1);
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->values.size());
    TEST_ASSERT_TRUE(observer->completed);
    
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_EQUAL(i + 1, observer->values[i]);
    }
}

void test_range_with_step() {
    class CountingObserver : public IObserver<int> {
    public:
        std::vector<int> values;
        bool completed = false;
        
        void OnNext(const int &value) override {
            values.push_back(value);
        }

        void OnCompleted() override {
            completed = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("Unexpected error in range step test");
        }
    };
    
    auto observer = std::make_shared<CountingObserver>();
    auto range = Range<int>(2, 10, 2); // 2, 4, 6, 8, 10
    
    range->Subscribe(observer);
    
    TEST_ASSERT_EQUAL(5, observer->values.size());
    TEST_ASSERT_TRUE(observer->completed);
    
    std::vector<int> expected = {2, 4, 6, 8, 10};
    for (size_t i = 0; i < expected.size(); i++) {
        TEST_ASSERT_EQUAL(expected[i], observer->values[i]);
    }
}

void test_range_edge_cases() {
    class CountingObserver : public IObserver<int> {
    public:
        std::vector<int> values;
        bool completed = false;
        
        void OnNext(const int &value) override {
            values.push_back(value);
        }

        void OnCompleted() override {
            completed = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("Unexpected error in range edge case test");
        }
    };
    
    // Test single value range
    auto observer1 = std::make_shared<CountingObserver>();
    auto range1 = Range<int>(5, 5, 1);
    range1->Subscribe(observer1);
    
    TEST_ASSERT_EQUAL(1, observer1->values.size());
    TEST_ASSERT_EQUAL(5, observer1->values[0]);
    TEST_ASSERT_TRUE(observer1->completed);
    
    // Test empty range (start > end with positive step)
    auto observer2 = std::make_shared<CountingObserver>();
    auto range2 = Range<int>(10, 5, 1);
    range2->Subscribe(observer2);
    
    TEST_ASSERT_EQUAL(0, observer2->values.size());
    TEST_ASSERT_TRUE(observer2->completed);
}

void test_shared_ptr_memory_safety() {
    std::weak_ptr<IObserver<int>> weakObserver;
    
    {
        auto observer = CreateObserver<int>([](const int&){});
        weakObserver = observer;
        
        TEST_ASSERT_FALSE(weakObserver.expired());
        
        auto range = Range<int>(1, 3, 1);
        range->Subscribe(observer);
        
        // Observer should still be alive
        TEST_ASSERT_FALSE(weakObserver.expired());
    }
    
    // Note: Depending on implementation, observer might still be referenced by range
    // This test mainly ensures no crashes occur during cleanup
}

void run_core_tests() {
    UNITY_BEGIN();
    
    RUN_TEST(test_observer_interface);
    RUN_TEST(test_function_observer_creation);
    RUN_TEST(test_function_observer_partial_functions);
    RUN_TEST(test_observable_subscribe_unsubscribe);
    RUN_TEST(test_range_basic_functionality);
    RUN_TEST(test_range_with_step);
    RUN_TEST(test_range_edge_cases);
    RUN_TEST(test_shared_ptr_memory_safety);
    
    UNITY_END();
}
