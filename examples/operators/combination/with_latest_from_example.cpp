#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_with_latest_from_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto status = std::make_shared<BehaviorSubject<int>>(0);
    auto combined = WithLatestFrom<int, int>(source, status);
    example_subscriptions.push_back(combined->Subscribe(
        CreateObserver<std::pair<int, int>>(
            [](const std::pair<int, int> &item) {
                Serial.print(item.first);
                Serial.print(" / ");
                Serial.println(item.second);
            })));

    source->OnNext(1);
}

void fluent_with_latest_from_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto status = std::make_shared<BehaviorSubject<int>>(0);
    example_subscriptions.push_back(From(source)
        .WithLatestFrom(status)
        .Subscribe([](const std::pair<int, int> &item) {
            Serial.println(item.second);
        }));

    source->OnNext(2);
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_with_latest_from_example();
    fluent_with_latest_from_example();
}

void loop()
{
}
