#include <unity.h>
#include <micro-reactive.h>

void Timer()
{
    auto observable = rx::TimerObservable<int>((size_t)100);

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
    delay(1000); // Wait for the interval to emit a value
    TEST_ASSERT_TRUE(observer->_onNext);
    TEST_ASSERT_TRUE(observer->_onCompleted);
    TEST_ASSERT_FALSE(observer->_onError);
}
