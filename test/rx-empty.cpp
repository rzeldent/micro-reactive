#include <unity.h>
#include <micro-reactive.h>

void Test_Empty()
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
            TEST_ASSERT_TRUE(false);
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
    auto observable = rx::Empty<int>();
    observable->Subscribe(&observer);
    TEST_ASSERT_FALSE(observer._onNext);
    TEST_ASSERT_TRUE(observer._onCompleted);
    TEST_ASSERT_FALSE(observer._onError);
    delete observable;
}
