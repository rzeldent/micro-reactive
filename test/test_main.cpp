#include <unity.h>

#include <micro-reactive.h>

// pio settings set force_verbose true

void setup()
{
    sleep(10);
}

void Create()
{
    bool invoked = false;
    // Create an observable that emits a single value and completes
    auto observable = rx::CreateObservable<int>(
        [&](rx::IObserver<int> *observer)
        {
            // Emit a value and complete the observable
            invoked = true;
            observer->OnNext(1);
            observer->OnCompleted();
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

        ~TestObserver() = default;
    };

    auto observer = std::make_shared<TestObserver>();
    observable->Subscribe(observer.get());
    TEST_ASSERT_TRUE(invoked);
}

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
        void OnNext(const int &value) override
        {
            TEST_ASSERT_EQUAL(1, value);
        }

        void OnCompleted() override
        {
            TEST_ASSERT_TRUE(true);
        }

        void OnError(const std::exception &e) override
        {
            TEST_ASSERT_TRUE(false);
        }

        ~TestObserver() = default;
    };

    auto observer = std::make_shared<TestObserver>();
    observable->Subscribe(observer.get());
    TEST_ASSERT_TRUE(invoked);
}

void loop()
{
    UNITY_BEGIN();
    RUN_TEST(Create);
    RUN_TEST(Defer);

    UNITY_END();
}