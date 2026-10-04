#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_flat_map_example()
{
    auto expanded = FlatMap<int, int>(
        Range(1, 3),
        std::function<std::shared_ptr<IObservable<int>>(const int &)>(
            [](const int &value) { return Range(value * 10, 2); }));
    example_subscriptions.push_back(expanded->Subscribe(
        CreateObserver<int>(
            [](const int &value) { Serial.println(value); })));
}

void fluent_flat_map_example()
{
    example_subscriptions.push_back(From(Range(1, 3))
        .FlatMap<int>([](const int &value) {
            return Range(value * 10, 2);
        })
        .Subscribe([](const int &value) { Serial.println(value); }));
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_flat_map_example();
    fluent_flat_map_example();
}

void loop()
{
}
