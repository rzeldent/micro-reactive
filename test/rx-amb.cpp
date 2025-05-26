#include <unity.h>
#include <micro-reactive.h>

void Test_Amb()
{
    class AmbTestObserver : public rx::IObserver<int>
    {
    public:
        std::vector<int> _sequence;
        bool _onCompleted = false;

        void OnNext(const int &value)
        {
            _sequence.push_back(value);
        }

        void OnCompleted()
        {
            _onCompleted = true;
        }

        void OnError(const std::exception &e)
        {
            TEST_FAIL_MESSAGE("OnError was called");
        }

        ~AmbTestObserver() = default;
    };

    auto observer = std::make_shared<AmbTestObserver>();

    auto generator1 = rx::Range<int>(1, 5, 1); // Emits 1, 2, 3, 4, 5
    auto generator2 = rx::Range<int>(6, 10, 1); // Emits 6, 7, 8, 9, 10

    rx::AmbOperator<int>({generator1, generator2}).Subscribe(observer);

    // Since AmbOperator emits values from the first observable that emits,
    // we expect the sequence from generator1.
    auto expected = (const int[]){1, 2, 3, 4, 5};
    auto actual = (const int *)observer->_sequence.data();
    TEST_ASSERT_EQUAL_INT_ARRAY_MESSAGE(expected, actual, observer->_sequence.size(), "OnNext was not called with the expected value");
}