#include <unity.h>
#include <micro-reactive.h>

void Test_All()
{
    class AllTestObserver : public rx::IObserver<bool>
    {
    public:
        std::vector<int> _sequence;
        bool _onCompleted = false;

        void OnNext(const bool &value)
        {
            _sequence.push_back(value ? 1 : 0);
        }

        void OnCompleted()
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e)
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~AllTestObserver() = default;
    };

    auto observer = std::make_shared<AllTestObserver>();

    auto generator = rx::Range<int>(1, 10, 1);
    rx::AllOperator<int>(generator, [](const int &value)
                         { return value % 2; })
        .Subscribe(observer);

    auto expected = (const int[]){1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    auto actual = (const int *)observer->_sequence.data();
    TEST_ASSERT_EQUAL_INT8_ARRAY_MESSAGE(expected, actual, observer->_sequence.size(), "OnNext was not called with the expected value");
}