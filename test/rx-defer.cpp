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

void Test_Defer()
{
    class DeferTestObserver : public rx::IObserver<int>
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
            TEST_FAIL_MESSAGE("OnError was called - Defer should not error");        
        }

        ~DeferTestObserver() = default;
    };

    int factoryCallCount = 0;
    auto deferObservable = rx::Defer<int>([&factoryCallCount]() -> std::shared_ptr<rx::IObservable<int>> {
        factoryCallCount++;
        std::vector<int> values = {100, 200};
        return rx::Iterate<int>(values);
    });

    auto observer = std::make_shared<DeferTestObserver>();
    deferObservable->Subscribe(observer);
    
    TEST_ASSERT_EQUAL_MESSAGE(1, factoryCallCount, "Factory should be called once");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(2, observer->receivedValues.size(), "Expected 2 values");
    TEST_ASSERT_EQUAL_MESSAGE(100, observer->receivedValues[0], "First value should be 100");
    TEST_ASSERT_EQUAL_MESSAGE(200, observer->receivedValues[1], "Second value should be 200");
    
    std::cout << "Test_Defer PASSED" << std::endl;
}

int main() {
    Test_Defer();
    return 0;
}
