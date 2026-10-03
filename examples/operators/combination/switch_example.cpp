#include <Arduino.h>
#include <micro-reactive.h>

using namespace rx;

void traditional_switch_example()
{
    auto outer = std::make_shared<Subject<std::shared_ptr<IObservable<int>>>>();
    auto switched = Switch<int>(outer);
    auto subscription = switched->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); }));

    outer->OnNext(Range(1, 2));
    outer->OnNext(Range(10, 2));
    outer->OnCompleted();
}

void fluent_switch_example()
{
    auto outer = std::make_shared<Subject<std::shared_ptr<IObservable<int>>>>();
    auto subscription = From(outer)
        .Switch<int>()
        .Subscribe([](const int &value) { Serial.println(value); });

    outer->OnNext(Range(20, 2));
    outer->OnCompleted();
}
