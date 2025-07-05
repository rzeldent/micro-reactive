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

void Test_Iterate()
{
    // Create an observable that emits a single value and completes
    class IterateTestObserver : public rx::IObserver<int>
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
            TEST_FAIL_MESSAGE("OnError was called - Iterate should not error");
        }

        ~IterateTestObserver() = default;
    };

    std::vector<int> sourceValues = {10, 20, 30, 40};
    auto observer = std::make_shared<IterateTestObserver>();
    auto observable = rx::Iterate<int>(sourceValues);
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(4, observer->receivedValues.size(), "Expected 4 values");
    TEST_ASSERT_EQUAL_MESSAGE(10, observer->receivedValues[0], "First value should be 10");
    TEST_ASSERT_EQUAL_MESSAGE(20, observer->receivedValues[1], "Second value should be 20");
    TEST_ASSERT_EQUAL_MESSAGE(30, observer->receivedValues[2], "Third value should be 30");
    TEST_ASSERT_EQUAL_MESSAGE(40, observer->receivedValues[3], "Fourth value should be 40");
    
    std::cout << "Test_Iterate PASSED" << std::endl;
}

int main() {
    Test_Iterate();
    return 0;
}