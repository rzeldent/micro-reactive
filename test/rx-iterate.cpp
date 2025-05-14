#include <unity.h>
#include <micro-reactive.h>

void Test_Iterate()
{
    // Create an observable that emits a single value and completes
    class IterateTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;

        void OnNext(const int &value)
        {
            _onNext = true;
            TEST_ASSERT_EQUAL_MESSAGE(1, value, "Value emitted is not 1");
        }

        void OnCompleted()
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e)
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~IterateTestObserver() = default;
    };

    auto observer = std::make_shared<IterateTestObserver>();
    auto observable = rx::Iterate<int>(std::vector<int>{1});
    observable->Subscribe(observer);
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}