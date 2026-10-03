#include "../../arduino_mock.h"
#include <micro-reactive.h>
#include <vector>

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
    traditional_with_latest_from_example();
    fluent_with_latest_from_example();
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

