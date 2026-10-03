#include "test_utils.h"
#include <core.h>
#include <sources.h>
#include <subjects.h>
#include <operators.h>
#include <error_handling.h>
#include <scheduler.h>

#if defined(ARDUINO)
#include <Arduino.h>
#endif

// Forward declarations for all test functions
// Sources tests
void test_create_observer();
void test_range_basic();
void test_fromvector_basic();
void test_empty_basic();
void test_timer_basic();
void test_interval_basic();

// Subjects tests
void test_subject_basic();
void test_behaviorsubject_basic();
void test_subject_multiple_observers();

// Transformation operator tests
void test_map_operator();
void test_scan_operator();
void test_pid_operator();
void test_pid_output_limits();
void test_pid_fluent();
void test_pid_rejects_invalid_sample_interval();
void test_kalman_operator();
void test_kalman_fluent();
void test_kalman_rejects_invalid_measurement_noise();

// Filtering operator tests
void test_filter_operator();
void test_take_operator();
void test_skip_operator();
void test_take_while_operator();
void test_skip_while_operator();
void test_first_operator();
void test_last_operator();
void test_distinct_operator();

// Aggregation operator tests
void test_reduce_operator();
void test_count_operator();
void test_sum_operator();
void test_average_operator();
void test_min_operator();
void test_max_operator();
void test_all_operator();
void test_any_operator();

// Utility operator tests
void test_throttle_operator();
void test_default_if_empty_operator();
void test_start_with_operator();
void test_do_operator();
void test_take_until_operator();
void test_skip_until_operator();
void test_contains_operator();
void test_distinct_until_changed_operator();
void test_pairwise_operator();
void test_debug_operator();
void test_debug_operator_with_error();

// Combination operator tests
void test_race_operator();
void test_merge_operator();
void test_merge_empty_sources();
void test_zip_operator();
void test_flat_map_operator();
void test_concat_operator();
void test_switch_operator();
void test_with_latest_from_operator();
void test_debounce_operator();
void test_delay_operator();
void test_sample_operator();

// Error handling tests
void test_retry_operator();
void test_catch_operator();
void test_catch_and_return_operator();
void test_finally_operator();
void test_finally_operator_on_error();
void test_on_error_resume_next_operator();
void test_timeout_error_operator();
void test_timeout_error_operator_with_emission();
void test_safe_observer();

// Scheduler tests
void test_scheduler_functionality();
void test_test_scheduler_basic();
void test_test_scheduler_delayed();
void test_test_scheduler_multiple_actions();
void test_test_scheduler_advance_to();

// Metrics tests
void test_observable_metrics_basic();
void test_observable_metrics_timing();
void test_observable_metrics_summary();

int run_unit_tests()
{
    UNITY_BEGIN();

    // Core functionality tests
    RUN_TEST(test_create_observer);
    RUN_TEST(test_range_basic);
    RUN_TEST(test_fromvector_basic);
    RUN_TEST(test_empty_basic);
    RUN_TEST(test_subject_basic);
    RUN_TEST(test_behaviorsubject_basic);
    RUN_TEST(test_subject_multiple_observers);
    RUN_TEST(test_timer_basic);
    RUN_TEST(test_interval_basic);

    // Transformation operator tests
    RUN_TEST(test_map_operator);
    RUN_TEST(test_scan_operator);
    RUN_TEST(test_pid_operator);
    RUN_TEST(test_pid_output_limits);
    RUN_TEST(test_pid_fluent);
    RUN_TEST(test_pid_rejects_invalid_sample_interval);
    RUN_TEST(test_kalman_operator);
    RUN_TEST(test_kalman_fluent);
    RUN_TEST(test_kalman_rejects_invalid_measurement_noise);

    // Filtering operator tests
    RUN_TEST(test_filter_operator);
    RUN_TEST(test_take_operator);
    RUN_TEST(test_skip_operator);
    RUN_TEST(test_take_while_operator);
    RUN_TEST(test_skip_while_operator);
    RUN_TEST(test_first_operator);
    RUN_TEST(test_last_operator);
    RUN_TEST(test_distinct_operator);

    // Aggregation operator tests
    RUN_TEST(test_reduce_operator);
    RUN_TEST(test_count_operator);
    RUN_TEST(test_sum_operator);
    RUN_TEST(test_average_operator);
    RUN_TEST(test_min_operator);
    RUN_TEST(test_max_operator);
    RUN_TEST(test_all_operator);
    RUN_TEST(test_any_operator);

    // Utility operator tests
    RUN_TEST(test_throttle_operator);
    RUN_TEST(test_default_if_empty_operator);
    RUN_TEST(test_start_with_operator);
    RUN_TEST(test_do_operator);
    RUN_TEST(test_take_until_operator);
    RUN_TEST(test_skip_until_operator);
    RUN_TEST(test_contains_operator);
    RUN_TEST(test_distinct_until_changed_operator);
    RUN_TEST(test_pairwise_operator);
    RUN_TEST(test_debug_operator);
    RUN_TEST(test_debug_operator_with_error);

    // Combination operator tests
    RUN_TEST(test_race_operator);
    RUN_TEST(test_merge_operator);
    RUN_TEST(test_merge_empty_sources);
    RUN_TEST(test_zip_operator);
    RUN_TEST(test_flat_map_operator);
    RUN_TEST(test_concat_operator);
    RUN_TEST(test_switch_operator);
    RUN_TEST(test_with_latest_from_operator);
    RUN_TEST(test_debounce_operator);
    RUN_TEST(test_delay_operator);
    RUN_TEST(test_sample_operator);

    // Error handling tests
    RUN_TEST(test_retry_operator);
    RUN_TEST(test_catch_operator);
    RUN_TEST(test_catch_and_return_operator);
    RUN_TEST(test_finally_operator);
    RUN_TEST(test_finally_operator_on_error);
    RUN_TEST(test_on_error_resume_next_operator);
    RUN_TEST(test_timeout_error_operator);
    RUN_TEST(test_timeout_error_operator_with_emission);
    RUN_TEST(test_safe_observer);

    // Scheduler tests
    RUN_TEST(test_scheduler_functionality);
    RUN_TEST(test_test_scheduler_basic);
    RUN_TEST(test_test_scheduler_delayed);
    RUN_TEST(test_test_scheduler_multiple_actions);
    RUN_TEST(test_test_scheduler_advance_to);

    // Metrics tests
    RUN_TEST(test_observable_metrics_basic);
    RUN_TEST(test_observable_metrics_timing);
    RUN_TEST(test_observable_metrics_summary);

    return UNITY_END();
}

#if defined(ARDUINO)
void setup()
{
    Serial.begin(115200);
    while (!Serial)
        delay(10);
    run_unit_tests();
}

void loop()
{
    // Nothing to do here
}
#else
int main()
{
    return run_unit_tests();
}
#endif
