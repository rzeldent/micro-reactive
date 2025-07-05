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

void Test_Empty()
{
    class EmptyTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onCompleted = false;
        
        void OnNext(const int &value) override
        {
            TEST_FAIL_MESSAGE("OnNext was called - Empty should not emit values");
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called - Empty should not error");
        }

        ~EmptyTestObserver() = default;
    };

    auto observer = std::make_shared<EmptyTestObserver>();
    auto observable = rx::Empty<int>();
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    std::cout << "Test_Empty PASSED" << std::endl;
}

int main() {
    Test_Empty();
    return 0;
}
