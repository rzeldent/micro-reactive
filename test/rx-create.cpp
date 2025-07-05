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

void Test_Create()
{
    class CreateTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;
        int _receivedValue = 0;

        void OnNext(const int &value) override
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
            TEST_FAIL_MESSAGE("OnError was called - Create should not error");
        }

        ~CreateTestObserver() = default;
    };

    bool invoked = false;
    // Create an observable that emits a single value and completes
    auto observable = rx::Create<int>(
        [&](std::shared_ptr<rx::IObserver<int>> observer)
        {
            // Emit a value and complete the observable
            invoked = true;
            observer->OnNext(42);
            observer->OnCompleted();
        });

    auto observer = std::make_shared<CreateTestObserver>();
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(invoked, "Create observable was not invoked");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(42, observer->_receivedValue, "Received value is not 42");
    
    std::cout << "Test_Create PASSED" << std::endl;
}

int main() {
    Test_Create();
    return 0;
}