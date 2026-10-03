#include <Arduino.h>
#include <micro-reactive.h>
#include <vector>

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_merge_example()
{
    auto merged = Merge<int>({Range(1, 2), Range(10, 2)});
    example_subscriptions.push_back(merged->Subscribe(CreateObserver<int>(
        [](const int &value) { Serial.println(value); },
        []() { Serial.println("Merge completed"); })));
}

void fluent_merge_example()
{
    auto other = Range(10, 2);
    example_subscriptions.push_back(From(Range(1, 2))
        .Merge(other)
        .Subscribe([](const int &value) { Serial.println(value); }));
}

void setup()
{
    Serial.begin(115200);
    traditional_merge_example();
    fluent_merge_example();
}

void loop()
{
    delay(1000);
}
