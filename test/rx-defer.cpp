#include <unity.h>
#include <micro-reactive.h>

void Test_Defer()
{
    class DeferTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;

        void OnNext(const int &value) override
        {
            _onNext = true;
            TEST_ASSERT_EQUAL_MESSAGE(1, value, "Value emitted is not 1");
        }

        void OnCompleted() override
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called");        
        }

        ~DeferTestObserver() = default;
    };

    bool invoked = false;
    auto observer = std::make_shared<DeferTestObserver>();
    auto observable = rx::Defer<int>(
        [&]()
        {
            // Create an observable that emits a single value and completes
            return rx::Create<int>(
                [&](std::shared_ptr<rx::IObserver<int> >observer)
                {
                    // Emit a value and complete the observable
                    invoked = true;
                    observer->OnNext(1);
                    observer->OnCompleted();
                });
        });
    observable->Subscribe(observer);
    TEST_ASSERT_TRUE_MESSAGE(invoked, "Defer observable was not invoked");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
}
