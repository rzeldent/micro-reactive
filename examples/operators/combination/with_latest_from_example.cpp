#include <Arduino.h>
#include <micro-reactive.h>

using namespace rx;

void traditional_with_latest_from_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto status = std::make_shared<BehaviorSubject<int>>(0);
    auto combined = WithLatestFrom<int, int>(source, status);
    auto subscription = combined->Subscribe(
        CreateObserver<std::pair<int, int>>(
            [](const std::pair<int, int> &item) {
                Serial.print(item.first);
                Serial.print(" / ");
                Serial.println(item.second);
            }));

    source->OnNext(1);
}

void fluent_with_latest_from_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto status = std::make_shared<BehaviorSubject<int>>(0);
    auto subscription = From(source)
        .WithLatestFrom(status)
        .Subscribe([](const std::pair<int, int> &item) {
            Serial.println(item.second);
        });

    source->OnNext(2);
}
