#include <Arduino.h>
#include "micro-reactive.h"

using namespace rx;

void setup() {
    Serial.begin(115200);
    delay(1000);
    Serial.println("Compilation test successful!");
}

void loop() {
    delay(1000);
}
