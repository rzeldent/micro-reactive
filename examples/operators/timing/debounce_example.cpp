#include <Arduino.h>
#include <micro-reactive.h>
#include <chrono>

using namespace rx;

void traditional_debounce_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto debounced = Debounce<int>(source, std::chrono::milliseconds(100));
    auto subscription = debounced->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); }));

    source->OnNext(1);
    source->OnNext(2);
}

void fluent_debounce_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto subscription = From(source)
        .Debounce(std::chrono::milliseconds(100))
        .Subscribe([](const int &value) { Serial.println(value); });

    source->OnNext(3);
}
