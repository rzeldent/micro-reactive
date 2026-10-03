#include <Arduino.h>
#include <micro-reactive.h>
#include <chrono>

using namespace rx;

void traditional_sample_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto sampled = Sample<int>(source, std::chrono::milliseconds(100));
    auto subscription = sampled->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); }));

    source->OnNext(1);
    source->OnNext(2);
}

void fluent_sample_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto subscription = From(source)
        .Sample(std::chrono::milliseconds(100))
        .Subscribe([](const int &value) { Serial.println(value); });

    source->OnNext(3);
}
