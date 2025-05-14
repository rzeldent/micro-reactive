#include <unity.h>
#include <micro-reactive.h>

void Test_Timer()
{
    class TimerTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;

        void OnNext(const int &value) override
        {
            _onNext = true;
            TEST_ASSERT_EQUAL_MESSAGE(0, value, "Value emitted is not 0");
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~TimerTestObserver() = default;
    };

    auto observer = std::make_shared<TimerTestObserver>();
    auto observable = rx::Timer<int>((size_t)100);
    observable->Subscribe(observer);
    delay(150); // Wait for the interval to emit a value
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}