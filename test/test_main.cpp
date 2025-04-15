#include <unity.h>

#include <micro-reactive.h>

void setup()
{
    // Wait for the serial port to initialize
    delay(2000);
    // Initialize serial communication at 115200 baud rate
//    Serial.begin(460800);
}

// Observables
extern void Test_Create();
extern void Test_Defer();
extern void Test_Empty();
extern void Test_Interval();
extern void Test_Iterate();
extern void Test_Never();
extern void Test_Range();
extern void Test_Scope();
extern void Test_Timer();

void loop()
{
    UNITY_BEGIN();
    RUN_TEST(Test_Create);
    RUN_TEST(Test_Defer);
    RUN_TEST(Test_Empty);
    RUN_TEST(Test_Interval);
    RUN_TEST(Test_Iterate);
    RUN_TEST(Test_Never);
    RUN_TEST(Test_Range);
    RUN_TEST(Test_Scope);
    RUN_TEST(Test_Timer);
    UNITY_END();
}