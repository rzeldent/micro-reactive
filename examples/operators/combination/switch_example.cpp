#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_switch_example()
{
    auto outer = std::make_shared<Subject<std::shared_ptr<IObservable<int>>>>();
    auto switched = Switch<int>(outer);
    example_subscriptions.push_back(switched->Subscribe(
        CreateObserver<int>(
            [](const int &value) { Serial.println(value); })));

    outer->OnNext(Range(1, 2));
    outer->OnNext(Range(10, 2));
    outer->OnCompleted();
}

void fluent_switch_example()
{
    auto outer = std::make_shared<Subject<std::shared_ptr<IObservable<int>>>>();
    example_subscriptions.push_back(From(outer)
        .Switch<int>()
        .Subscribe([](const int &value) { Serial.println(value); }));

    outer->OnNext(Range(20, 2));
    outer->OnCompleted();
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_switch_example();
    fluent_switch_example();
}

void loop()
{
}
