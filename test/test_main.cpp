#include <unity.h>

#include <micro-reactive.h>

// pio settings set force_verbose true

void setup()
{
    sleep(10);
}

void rxCreate()
{
    // Create an observable that emits a single value and completes
    auto observable = rx::Create<int>(
        [](rx::IObserver<int> &observer) {
            // Emit a value and complete the observable
            observer.OnNext(1);
            observer.OnCompleted();
        });

    class TestObserver : public rx::IObserver<int>
    {
    public:
        void OnNext(const int &value)
        {
            TEST_ASSERT_EQUAL(1, value);
        }

        void OnCompleted()
        {
            TEST_ASSERT_TRUE(true);
        }

        void OnError(const std::exception &e)
        {
            TEST_ASSERT_TRUE(false);
        }
    };

    TestObserver observer;
    observable.Subscribe(observer);
}

void loop()
{
    UNITY_BEGIN();
    RUN_TEST(rxCreate);
    UNITY_END();
}