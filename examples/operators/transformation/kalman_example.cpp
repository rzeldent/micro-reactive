#include <Arduino.h>
#include <micro-reactive.h>
#include <vector>

using namespace rx;

std::vector<std::shared_ptr<Subscription>> kalman_subscriptions;

void traditional_kalman_example()
{
    const std::vector<double> measurements = {10.2, 9.8, 10.1, 10.0};
    auto source = FromVector(measurements);
    auto filtered = Kalman(source, 0.05, 1.0, 10.0, 1.0);

    kalman_subscriptions.push_back(
        filtered->Subscribe(CreateObserver<double>(
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
    const std::vector<double> measurements = {10.2, 9.8, 10.1, 10.0};

    kalman_subscriptions.push_back(
        From(FromVector(measurements))
            .Kalman(0.05, 1.0, 10.0, 1.0)
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
    traditional_kalman_example();
    fluent_kalman_example();
}

void loop()
{
}
