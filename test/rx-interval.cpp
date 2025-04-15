#include <unity.h>
#include <micro-reactive.h>

void Test_Interval()
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
            TEST_ASSERT_EQUAL(0, value);
        }

        void OnCompleted() override
        {
            _onCompleted = true;
            TEST_ASSERT_TRUE(false);
        }

        void OnError(const std::exception &e) override
        {
            _onError = true;
            TEST_ASSERT_TRUE(false);
        }

        ~TestObserver() = default;
    };

    TestObserver observer;
    auto observable = rx::IntervalObservable<int>((size_t)100);
    observable->Subscribe(&observer);
    delay(150); // Wait for the interval to emit a value
    TEST_ASSERT_TRUE(observer._onNext);
    TEST_ASSERT_FALSE(observer._onCompleted);
    TEST_ASSERT_FALSE(observer._onError);
    delete observable;
    TEST_ASSERT_TRUE(observer._onCompleted);
}
