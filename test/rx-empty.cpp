#include <unity.h>
#include <micro-reactive.h>

void Test_Empty()
{
    class EmptyTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onCompleted = false;
        
        void OnNext(const int &value) override
        {
            TEST_FAIL_MESSAGE("OnNext was called");
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~EmptyTestObserver() = default;
    };

    auto observer = std::make_shared<EmptyTestObserver>();
    auto observable = rx::Empty<int>();
    observable->Subscribe(observer);
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}
