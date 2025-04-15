#include <unity.h>
#include <micro-reactive.h>

void Defer()
{
    bool invoked = false;
    auto observable = rx::DeferObservable<int>(
        [&]()
        {
            // Create an observable that emits a single value and completes
            return rx::CreateObservable<int>(
                [&](rx::IObserver<int> *observer)
                {
                    // Emit a value and complete the observable
                    invoked = true;
                    observer->OnNext(1);
                    observer->OnCompleted();
                });
        });

    class TestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;
        bool _onError = false;

        void OnNext(const int &value) override
        {
            _onNext = true;
            TEST_ASSERT_EQUAL(1, value);
        }

        void OnCompleted() override
        {
            _onCompleted = true;
            TEST_ASSERT_TRUE(true);
        }

        void OnError(const std::exception &e) override
        {
            _onError = true;
            TEST_ASSERT_TRUE(false);
        }

        ~TestObserver() = default;
    };

    auto observer = std::make_shared<TestObserver>();
    observable->Subscribe(observer.get());
    TEST_ASSERT_TRUE(invoked);
    TEST_ASSERT_TRUE(observer->_onNext);
    TEST_ASSERT_TRUE(observer->_onCompleted);
    TEST_ASSERT_FALSE(observer->_onError);
}
