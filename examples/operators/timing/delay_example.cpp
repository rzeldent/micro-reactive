#include <Arduino.h>
#include <micro-reactive.h>
#include <chrono>
#include <vector>

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_delay_example()
{
    auto delayed = Delay<int>(
        Range(1, 3), std::chrono::milliseconds(100));
    example_subscriptions.push_back(delayed->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); },
        []() { Serial.println("Delayed sequence completed"); })));
}

void fluent_delay_example()
{
    example_subscriptions.push_back(From(Range(4, 3))
        .Delay(std::chrono::milliseconds(100))
        .Subscribe([](const int &value) { Serial.println(value); }));
}

void setup()
{
    Serial.begin(115200);
    traditional_delay_example();
    fluent_delay_example();
}

void loop()
{
    delay(1000);
}
