#include <vector>
#include <iostream>
#include "micro-reactive.h"

// Simple test framework replacement for Unity
#define TEST_ASSERT_TRUE_MESSAGE(condition, message) \
    if (!(condition)) { \
        std::cerr << "FAIL: " << message << std::endl; \
        return; \
    }

#define TEST_ASSERT_EQUAL_MESSAGE(expected, actual, message) \
    if ((expected) != (actual)) { \
        std::cerr << "FAIL: " << message << " (expected: " << (expected) << ", actual: " << (actual) << ")" << std::endl; \
        return; \
    }

#define TEST_FAIL_MESSAGE(message) \
    std::cerr << "FAIL: " << message << std::endl; \
    return;

void Test_Scope()
{
    class ScopeTestObserver : public rx::IObserver<int>
    {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;

        void OnNext(const int &value) override
        {
            receivedValues.push_back(value);
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called - Scope should not error");
        }

        ~ScopeTestObserver() = default;
    };

    int resourceCallCount = 0;
    int observableCallCount = 0;
    
    auto scopeObservable = rx::Scope<int>(
        [&resourceCallCount]() -> std::vector<int> {
            resourceCallCount++;
            return {10, 20, 30};
        },
        [&observableCallCount](std::vector<int> resource) -> std::shared_ptr<rx::IObservable<int>> {
            observableCallCount++;
            return rx::Iterate<int>(resource);
        }
    );

    auto observer = std::make_shared<ScopeTestObserver>();
    scopeObservable->Subscribe(observer);
    
    TEST_ASSERT_EQUAL_MESSAGE(1, resourceCallCount, "Resource factory should be called once");
    TEST_ASSERT_EQUAL_MESSAGE(1, observableCallCount, "Observable factory should be called once");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(3, observer->receivedValues.size(), "Expected 3 values");
    TEST_ASSERT_EQUAL_MESSAGE(10, observer->receivedValues[0], "First value should be 10");
    TEST_ASSERT_EQUAL_MESSAGE(20, observer->receivedValues[1], "Second value should be 20");
    TEST_ASSERT_EQUAL_MESSAGE(30, observer->receivedValues[2], "Third value should be 30");
    
    std::cout << "Test_Scope PASSED" << std::endl;
}

int main() {
    Test_Scope();
    return 0;
}
