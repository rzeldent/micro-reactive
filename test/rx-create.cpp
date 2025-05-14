#include <unity.h>
#include <micro-reactive.h>

void Test_Create()
{
    class CreateTestObserver : public rx::IObserver<int>
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

        ~CreateTestObserver() = default;
    };

    bool invoked = false;
    // Create an observable that emits a single value and completes
    auto observable = rx::Create<int>(
        [&](std::shared_ptr<rx::IObserver<int>> observer)
        {
            // Emit a value and complete the observable
            invoked = true;
            observer->OnNext(1);
            observer->OnCompleted();
        });

    auto observer = std::make_shared<CreateTestObserver>();
    observable->Subscribe(observer);
    TEST_ASSERT_TRUE_MESSAGE(invoked, "Create observable was not invoked");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}