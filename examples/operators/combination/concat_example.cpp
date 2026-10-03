#include <Arduino.h>
#include <micro-reactive.h>
#include <vector>

using namespace rx;

void traditional_concat_example()
{
    auto concatenated = Concat<int>({
        Range(1, 2),
        Range(10, 2),
        Range(20, 1)});
    auto subscription = concatenated->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); }));
}

void fluent_concat_example()
{
    auto subscription = From(Range(1, 2))
        .Concat(Range(10, 2))
        .Subscribe([](const int &value) { Serial.println(value); });
}
