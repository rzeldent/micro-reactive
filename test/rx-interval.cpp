#include <vector>
#include <iostream>
#include "micro-reactive.h"

// Simple test framework replacement for Unity
#define TEST_ASSERT_TRUE_MESSAGE(condition, message) \
    if (!(condition)) { \
        std::cerr << "FAIL: " << message << std::endl; \
        return; \
    }

#define TEST_FAIL_MESSAGE(message) \
    std::cerr << "FAIL: " << message << std::endl; \
    return;

void Test_Interval()
{
    class IntervalTestObserver : public rx::IObserver<unsigned long>
    {
    public:
        std::vector<unsigned long> receivedValues;
        bool _onCompleted = false;

        void OnNext(const unsigned long &value) override
        {
            receivedValues.push_back(value);
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called - Interval should not error");
        }

        ~IntervalTestObserver() = default;
    };

    auto observer = std::make_shared<IntervalTestObserver>();
    auto observable = rx::Interval<unsigned long>(100); // 100ms interval
    observable->Subscribe(observer);
    
    // In desktop mode, interval emits 3 values (0, 1, 2) then completes
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->receivedValues.size() == 3, "Expected 3 values");
    TEST_ASSERT_TRUE_MESSAGE(observer->receivedValues[0] == 0, "First value should be 0");
    TEST_ASSERT_TRUE_MESSAGE(observer->receivedValues[1] == 1, "Second value should be 1");
    TEST_ASSERT_TRUE_MESSAGE(observer->receivedValues[2] == 2, "Third value should be 2");
    
    std::cout << "Test_Interval PASSED" << std::endl;
}

int main() {
    Test_Interval();
    return 0;
}
