#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

std::vector<std::shared_ptr<Subscription>> pid_subscriptions;

void traditional_pid_example()
{
    auto readings = Range(8, 3);
    auto control = PID(readings, 10.0, 2.0, 0.5, 0.1, 1.0, -100.0, 100.0);
    pid_subscriptions.push_back(control->Subscribe(CreateObserver<double>(
        [](const double &value)
        {
            Serial.print("PID output: ");
            Serial.println(value);
        },
        []()
        { Serial.println("Traditional PID complete"); })));
}

void fluent_pid_example()
{
    pid_subscriptions.push_back(
        From(Range(8, 3))
            .PID(10.0, 2.0, 0.5, 0.1, 1.0, -100.0, 100.0)
            .Subscribe(
                [](const double &value)
                {
                    Serial.print("Fluent PID output: ");
                    Serial.println(value);
                },
                []()
                { Serial.println("Fluent PID complete"); }));
}

void setup()
{
    Serial.begin(115200);
    while (!Serial) delay(10);
    traditional_pid_example();
    fluent_pid_example();
}

void loop()
{
}
