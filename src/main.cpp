#include <Arduino.h>

void setup() {
    // Wait for the serial port to initialize
    delay(2000); 
    // Initialize serial communication at 115200 baud rate
    Serial.begin(460800); 
}

void loop() {
    // Main code to run repeatedly
    Serial.println("Hello, Arduino!");
    delay(1000); // Wait for 1 second
}