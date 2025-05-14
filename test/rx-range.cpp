#include <unity.h>
#include <micro-reactive.h>

void Test_Range()
{
    class RangeTestObserver : public rx::IObserver<int>
    {
    public:
        int _next = 0;
        bool _onNext = false;
        bool _onCompleted = false;
        
        void OnNext(const int &value) override
        {
            _onNext = true;
            TEST_ASSERT_EQUAL_MESSAGE(_next++, value, "Value emitted is not in the expected range");
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called");        
        }

        ~RangeTestObserver() = default;
    };

    auto observer = std::make_shared<RangeTestObserver>();
    auto observable = rx::Range<int>(0, 10, 1);
    observable->Subscribe(observer);
    TEST_ASSERT_TRUE_MESSAGE(observer->_next == 11, "Not all values were emitted");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}
