#include <unity.h>
#include <micro-reactive.h>

void Test_Never()
{
    class NeverTestObserver : public rx::IObserver<int>
    {
    public:
        bool _onCompleted = false;
        
        void OnNext(const int &value) override
        {
            TEST_FAIL_MESSAGE("OnNext was called");
        }

        void OnCompleted() override
        {
            TEST_FAIL_MESSAGE("OnCompleted was called");
        }

        void OnError(const std::exception &e) override
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~NeverTestObserver() = default;
    };

    auto observer = std::make_shared<NeverTestObserver>();
    auto observable = rx::Never<int>();
    observable->Subscribe(observer);
}
