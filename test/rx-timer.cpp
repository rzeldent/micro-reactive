#include <vector>
#include <iostream>
#include "micro-reactive.h"

// Simple test framework replacement for Unity
#define TEST_ASSERT_EQUAL_MESSAGE(expected, actual, message) \
    if ((expected) != (actual)) { \
        std::cerr << "FAIL: " << message << " (expected: " << (expected) << ", actual: " << (actual) << ")" << std::endl; \
        return; \
    }

#define TEST_ASSERT_TRUE_MESSAGE(condition, message) \
    if (!(condition)) { \
        std::cerr << "FAIL: " << message << std::endl; \
        return; \
    }

#define TEST_FAIL_MESSAGE(message) \
    std::cerr << "FAIL: " << message << std::endl; \
    return;

void Test_Timer()
{
    class TimerTestObserver : public rx::IObserver<unsigned long>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;
        unsigned long _receivedValue = 999;

        void OnNext(const unsigned long &value) override
        {
            _onNext = true;
            _receivedValue = value;
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called - Timer should not error");
        }

        ~TimerTestObserver() = default;
    };

    auto observer = std::make_shared<TimerTestObserver>();
    auto observable = rx::Timer<unsigned long>(100); // 100ms delay
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(0UL, observer->_receivedValue, "Timer should emit 0 as first value");
    
    std::cout << "Test_Timer PASSED" << std::endl;
}

int main() {
    Test_Timer();
    return 0;
}