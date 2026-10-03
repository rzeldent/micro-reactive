#include <Arduino.h>
#include <micro-reactive.h>

using namespace rx;

void traditional_flat_map_example()
{
    auto expanded = FlatMap<int, int>(
        Range(1, 3),
        std::function<std::shared_ptr<IObservable<int>>(const int &)>(
            [](const int &value) { return Range(value * 10, 2); }));
    auto subscription = expanded->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); }));
}

void fluent_flat_map_example()
{
    auto subscription = From(Range(1, 3))
        .FlatMap<int>([](const int &value) {
            return Range(value * 10, 2);
        })
        .Subscribe([](const int &value) { Serial.println(value); });
}
