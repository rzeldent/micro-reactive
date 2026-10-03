#include <Arduino.h>
#include <micro-reactive.h>
#include <chrono>

using namespace rx;

void traditional_delay_example()
{
    auto delayed = Delay<int>(
        Range(1, 3), std::chrono::milliseconds(100));
    auto subscription = delayed->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); },
        []() { Serial.println("Delayed sequence completed"); }));
}

void fluent_delay_example()
{
    auto subscription = From(Range(4, 3))
        .Delay(std::chrono::milliseconds(100))
        .Subscribe([](const int &value) { Serial.println(value); });
}
