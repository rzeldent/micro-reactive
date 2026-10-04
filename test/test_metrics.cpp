#include "test_utils.h"
#include <subjects.h>
#include <operators.h>

void test_observable_metrics_basic()
{
    ObservableMetrics metrics;

    TEST_ASSERT_EQUAL(0, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetCompletionsCount());

    metrics.RecordEmission();
    metrics.RecordEmission();
    metrics.RecordSubscription();
    metrics.RecordError();
    metrics.RecordCompletion();

    TEST_ASSERT_EQUAL(2, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetCompletionsCount());

    metrics.Reset();
    TEST_ASSERT_EQUAL(0, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetCompletionsCount());
}

void test_observable_metrics_timing()
{
    ObservableMetrics metrics;

    metrics.RecordEmission();
    testSleepForMilliseconds(30);
    metrics.RecordEmission();
    testSleepForMilliseconds(30);
    metrics.RecordEmission();

    TEST_ASSERT_TRUE(metrics.GetElapsedTime().count() >= 50);
    TEST_ASSERT_TRUE(metrics.GetTimeSinceLastEmission().count() >= 0);
    TEST_ASSERT_TRUE(metrics.GetEmissionRate() > 0);
}

void test_observable_metrics_summary()
{
    ObservableMetrics metrics;
    metrics.RecordEmission();
    metrics.RecordEmission();
    metrics.RecordSubscription();
    metrics.RecordCompletion();

    const std::string summary = metrics.GetSummary();
    TEST_ASSERT_TRUE(summary.length() > 0);
    TEST_ASSERT_TRUE(summary.find("Emissions: 2") != std::string::npos);
    TEST_ASSERT_TRUE(summary.find("Subscriptions: 1") != std::string::npos);
    TEST_ASSERT_TRUE(summary.find("Completions: 1") != std::string::npos);
}
