#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_sample_example()
{
    auto source = std::make_shared<Subject<int>>();
    auto sampled = Sample<int>(source, std::chrono::milliseconds(100));
    example_subscriptions.push_back(sampled->Subscribe(
        CreateObserver<int>(
            [](const int &value) { Serial.println(value); })));

    source->OnNext(1);
    source->OnNext(2);
    source->OnCompleted();
}

void fluent_sample_example()
{
    auto source = std::make_shared<Subject<int>>();
    example_subscriptions.push_back(From(source)
        .Sample(std::chrono::milliseconds(100))
        .Subscribe([](const int &value) { Serial.println(value); }));

    source->OnNext(3);
    source->OnCompleted();
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_sample_example();
    fluent_sample_example();
}

void loop()
{
}
