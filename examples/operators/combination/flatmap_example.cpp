#include "../../arduino_mock.h"
#include <micro-reactive.h>
#include <vector>

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
    traditional_flat_map_example();
    fluent_flat_map_example();
}

void loop()
{
    delay(1000);
}

// For native testing, provide a main that calls setup/loop
#ifndef ARDUINO
int main() {
    setup();
    while (true) {
        loop();
    }
    return 0;
}
#endif

