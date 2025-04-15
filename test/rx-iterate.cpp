#include <unity.h>
#include <micro-reactive.h>

void Test_Iterate()
{
    // Create an observable that emits a single value and completes
    class TestObserver : public rx::IObserver<int>
    {
    public:
        bool _onNext = false;
        bool _onCompleted = false;
        bool _onError = false;

        void OnNext(const int &value)
        {
            _onNext = true;
            TEST_ASSERT_EQUAL(1, value);
        }

        void OnCompleted()
        {
            _onCompleted = true;
            TEST_ASSERT_TRUE(true);
        }

        void OnError(const std::exception &e)
        {
            _onError = true;
            TEST_ASSERT_TRUE(false);
        }

        ~TestObserver() = default;
    };

    TestObserver observer;
    auto observable = rx::IterateObservable<int>(std::vector<int>{1});
    observable->Subscribe(&observer);
    TEST_ASSERT_TRUE(observer._onNext);
    TEST_ASSERT_TRUE(observer._onCompleted);
    TEST_ASSERT_FALSE(observer._onError);
    delete observable;
}