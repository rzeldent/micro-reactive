#include <unity.h>
#include <vector>
#include "micro-reactive.h"

void Test_Range()
{
    class RangeTestObserver : public rx::IObserver<int>
    {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;
        bool _onError = false;
        
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
            _onError = true;
            TEST_FAIL_MESSAGE("OnError was called");        
        }

        ~RangeTestObserver() = default;
    };

    auto observer = std::make_shared<RangeTestObserver>();
    auto observable = rx::Range<int>(0, 10, 1);
    observable->Subscribe(observer);
    
    TEST_ASSERT_EQUAL_MESSAGE(11, observer->receivedValues.size(), "Not all values were emitted");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_FALSE_MESSAGE(observer->_onError, "OnError should not have been called");
    
    // Verify the sequence is correct: 0, 1, 2, ..., 10
    for (int i = 0; i <= 10; i++) {
        TEST_ASSERT_EQUAL_MESSAGE(i, observer->receivedValues[i], "Value emitted is not in the expected range");
    }
}
