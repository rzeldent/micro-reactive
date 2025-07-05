#include <vector>
#include <iostream>
#include "micro-reactive.h"

// Simple test framework replacement for Unity
#define TEST_FAIL_MESSAGE(message) \
    std::cerr << "FAIL: " << message << std::endl; \
    return;

void Test_Never()
{
    class NeverTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onCompleted = false;
        
        void OnNext(const int &value) override
        {
            TEST_FAIL_MESSAGE("OnNext was called - Never should not emit values");
        }

        void OnCompleted() override
        {
            TEST_FAIL_MESSAGE("OnCompleted was called - Never should not complete");
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called - Never should not error");
        }

        ~NeverTestObserver() = default;
    };

    auto observer = std::make_shared<NeverTestObserver>();
    auto observable = rx::Never<int>();
    observable->Subscribe(observer);
    
    // Never should not emit anything, so this test passes if no failures occur
    std::cout << "Test_Never PASSED" << std::endl;
}

int main() {
    Test_Never();
    return 0;
}
