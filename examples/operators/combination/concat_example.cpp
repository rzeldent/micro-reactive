#include <Arduino.h>
#include <micro-reactive.h>
#include <vector>

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_concat_example()
{
    auto concatenated = Concat<int>({
        Range(1, 2),
        Range(10, 2),
        Range(20, 1)});
    example_subscriptions.push_back(concatenated->Subscribe(
        CreateObserver<int>(
            [](const int &value) { Serial.println(value); })));
}

void fluent_concat_example()
{
    example_subscriptions.push_back(From(Range(1, 2))
        .Concat(Range(10, 2))
        .Subscribe([](const int &value) { Serial.println(value); }));
}

void setup()
{
    Serial.begin(115200);
    traditional_concat_example();
    fluent_concat_example();
}

void loop()
{
    delay(1000);
}
