#include <Arduino.h>
#include <micro-reactive.h>

using namespace rx;

void traditional_merge_example()
{
    auto merged = Merge<int>({Range(1, 2), Range(10, 2)});
    auto subscription = merged->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); },
        []() { Serial.println("Merge completed"); }));
}

void fluent_merge_example()
{
    auto other = Range(10, 2);
    auto subscription = From(Range(1, 2))
        .Merge(other)
        .Subscribe([](const int &value) { Serial.println(value); });
}
