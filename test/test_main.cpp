#include <unity.h>

#include <micro-reactive.h>

// pio settings set force_verbose true

void setup()
{
    sleep(10);
}

// Observables
extern void Create();
extern void Defer();
extern void Empty();
extern void Interval();
extern void Iterate();
extern void Never();
extern void Range();
extern void Scope();
extern void Timer();

void loop()
{
    UNITY_BEGIN();
    RUN_TEST(Create);
    RUN_TEST(Defer);
    RUN_TEST(Empty);
    RUN_TEST(Interval);
    RUN_TEST(Iterate);
    RUN_TEST(Never);
    RUN_TEST(Range);
    RUN_TEST(Scope);
    RUN_TEST(Timer);
    UNITY_END();
}