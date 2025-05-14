#include <unity.h>
#include <micro-reactive.h>

void Test_Defer()
{
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

    bool invoked = false;
    TestObserver observer;
    auto observable = rx::Defer<int>(
        [&]()
        {
            // Create an observable that emits a single value and completes
            return rx::Create<int>(
                [&](rx::IObserver<int> *observer)
                {
                    // Emit a value and complete the observable
                    invoked = true;
                    observer->OnNext(1);
                    observer->OnCompleted();
                });
        });
    observable->Subscribe(&observer);
    TEST_ASSERT_TRUE(invoked);
    TEST_ASSERT_TRUE(observer._onNext);
    TEST_ASSERT_TRUE(observer._onCompleted);
    TEST_ASSERT_FALSE(observer._onError);
    delete observable;
}
