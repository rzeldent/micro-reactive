#include <Arduino.h>
#include <micro-reactive.h>
#include <string>
#include <vector>

using namespace rx;

void traditional_zip_example()
{
    auto numbers = Range(1, 3);
    auto names = FromVector(std::vector<std::string>{"one", "two", "three"});
    auto zipped = Zip<int, std::string>(numbers, names);
    auto subscription = zipped->Subscribe(
        CreateObserver<std::pair<int, std::string>>(
            [](const std::pair<int, std::string> &item) {
                Serial.print(item.first);
                Serial.print(": ");
                Serial.println(item.second.c_str());
            }));
}

void fluent_zip_example()
{
    auto names = FromVector(std::vector<std::string>{"one", "two", "three"});
    auto subscription = From(Range(1, 3))
        .Zip<std::string>(names)
        .Subscribe([](const std::pair<int, std::string> &item) {
            Serial.println(item.second.c_str());
        });
}
