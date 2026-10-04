#include "test_utils.h"
#include <core.h>
#include <sources.h>
#include <subjects.h>
#include <operators.h>
#include <cmath>

// Test MapOperator with new subscription pattern
void test_map_operator()
{
    auto range = Range(1, 3); // 1, 2, 3
    auto mapOp = Map<int, int>(range, [](const int &x)
                               { return x * 2; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();

    auto subscription = mapOp->Subscribe(observer);

    TEST_ASSERT_EQUAL(6, observer->GetLastValue()); // 3 * 2 = 6
    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());

    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

// Test ScanOperator with new subscription pattern
void test_scan_operator()
{
    auto range = Range(1, 4); // 1, 2, 3, 4
    auto scanOp = Scan<int, int>(range, 0, [](const int &acc, const int &x)
                                 { return acc + x; });
    auto observer = std::make_shared<SimpleTestObserver<int>>();

    auto subscription = scanOp->Subscribe(observer);

    TEST_ASSERT_EQUAL(10, observer->GetLastValue()); // 0+1+2+3+4 = 10
    TEST_ASSERT_EQUAL(4, observer->GetCount());      // Should emit accumulated value for each input
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_FALSE(subscription->IsDisposed());

    subscription->Dispose();
    TEST_ASSERT_TRUE(subscription->IsDisposed());
}

void test_pid_operator()
{
    auto source = FromVector<double>({8.0, 9.0, 10.0});
    auto pid = PID(source, 10.0, 2.0, 1.0, 0.5, 1.0, -100.0, 100.0);
    auto observer = std::make_shared<SimpleTestObserver<double>>();

    pid->Subscribe(observer);

    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_TRUE(std::fabs(observer->GetLastValue() - 2.5) < 0.0001);
}

void test_pid_output_limits()
{
    auto source = FromVector<int>({0});
    auto pid = PID(source, 10.0, 2.0, 0.0, 0.0, 1.0, -5.0, 5.0);
    auto observer = std::make_shared<SimpleTestObserver<double>>();

    pid->Subscribe(observer);

    TEST_ASSERT_TRUE(std::fabs(observer->GetLastValue() - 5.0) < 0.0001);
}

void test_pid_fluent()
{
    auto observer = std::make_shared<SimpleTestObserver<double>>();

    From(Range(8, 3))
        .PID(10.0, 1.0, 0.0, 0.0, 1.0, -10.0, 10.0)
        .Subscribe(observer);

    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(std::fabs(observer->GetLastValue() - 0.0) < 0.0001);
}

void test_pid_rejects_invalid_sample_interval()
{
    auto source = Range(1, 1);
    bool threw = false;

    try
    {
        PID(source, 0.0, 1.0, 0.0, 0.0, 0.0, -1.0, 1.0);
    }
    catch (const std::invalid_argument &)
    {
        threw = true;
    }

    TEST_ASSERT_TRUE(threw);
}

void test_kalman_operator()
{
    auto source = FromVector<double>({10.0, 10.0});
    auto kalman = Kalman(source, 0.0, 1.0, 0.0, 1.0);
    auto observer = std::make_shared<SimpleTestObserver<double>>();

    kalman->Subscribe(observer);

    TEST_ASSERT_EQUAL(2, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_TRUE(
        std::fabs(observer->GetLastValue() - (20.0 / 3.0)) < 0.0001);
}

void test_kalman_fluent()
{
    auto observer = std::make_shared<SimpleTestObserver<double>>();

    From(Range(1, 3))
        .Kalman(0.0, 1.0, 0.0, 1.0)
        .Subscribe(observer);

    TEST_ASSERT_EQUAL(3, observer->GetCount());
    TEST_ASSERT_TRUE(observer->IsCompleted());
    TEST_ASSERT_TRUE(observer->GetLastValue() > 0.0);
}

void test_kalman_rejects_invalid_measurement_noise()
{
    auto source = Range(1, 1);
    bool threw = false;

    try
    {
        Kalman(source, 0.0, 0.0);
    }
    catch (const std::invalid_argument &)
    {
        threw = true;
    }

    TEST_ASSERT_TRUE(threw);
}
