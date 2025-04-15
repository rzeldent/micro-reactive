#include <unity.h>
#include <micro-reactive.h>

void Test_Range()
{
    class TestObserver : public rx::IObserver<int>
    {
    public:
        int _next = 0;
        bool _onNext = false;
        bool _onCompleted = false;
        bool _onError = false;
        void OnNext(const int &value) override
        {
            _onNext = true;
            TEST_ASSERT_EQUAL(_next++, value);
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

    TestObserver observer;
    auto observable = rx::RangeObservable<int>(0, 10, 1);
    observable->Subscribe(&observer);
    TEST_ASSERT_TRUE(observer._next == 11);
    TEST_ASSERT_TRUE(observer._onNext);
    TEST_ASSERT_TRUE(observer._onCompleted);
    TEST_ASSERT_FALSE(observer._onError);
    delete observable;
}
