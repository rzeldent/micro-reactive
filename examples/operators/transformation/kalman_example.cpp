#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> kalman_subscriptions;

void traditional_kalman_example()
{
    auto readings = Range(8, 3);
    auto estimate = Kalman(readings, 1.0, 0.1, 0.01);
    kalman_subscriptions.push_back(estimate->Subscribe(CreateObserver<double>(
        [](const double &value)
        {
            Serial.print("Kalman estimate: ");
            Serial.println(value);
        },
        []()
        { Serial.println("Traditional Kalman complete"); })));
}

void fluent_kalman_example()
{
    kalman_subscriptions.push_back(
        From(Range(8, 3))
            .Kalman(1.0, 0.1, 0.01)
            .Subscribe(
                [](const double &value)
                {
                    Serial.print("Fluent Kalman estimate: ");
                    Serial.println(value);
                },
                []()
                { Serial.println("Fluent Kalman complete"); }));
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_kalman_example();
    fluent_kalman_example();
}

void loop()
{
}
