#include "../../arduino_mock.h"
#include <micro-reactive.h>
#include <string>
#include <vector>

using namespace rx;

std::vector<std::shared_ptr<Subscription>> example_subscriptions;

void traditional_zip_example()
{
    auto numbers = Range(1, 3);
    auto names = FromVector(std::vector<std::string>{"one", "two", "three"});
    auto zipped = Zip<int, std::string>(numbers, names);
    example_subscriptions.push_back(zipped->Subscribe(
        CreateObserver<std::pair<int, std::string>>(
            [](const std::pair<int, std::string> &item) {
                Serial.print(item.first);
                Serial.print(": ");
                Serial.println(item.second.c_str());
            })));
}

void fluent_zip_example()
{
    auto names = FromVector(std::vector<std::string>{"one", "two", "three"});
    example_subscriptions.push_back(From(Range(1, 3))
        .Zip<std::string>(names)
        .Subscribe([](const std::pair<int, std::string> &item) {
            Serial.println(item.second.c_str());
        }));
}

void setup()
{
    Serial.begin(115200);
    traditional_zip_example();
    fluent_zip_example();
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

