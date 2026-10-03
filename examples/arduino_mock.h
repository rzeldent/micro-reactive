/**
 * Arduino Mock for Native Testing
 * Provides minimal Arduino API for running examples on native platform
 */

#ifndef ARDUINO_MOCK_H
#define ARDUINO_MOCK_H

#if !defined(ARDUINO)

#include <iostream>
#include <chrono>
#include <thread>

// Mock Serial
class MockSerial {
public:
    void begin(unsigned long baud) {
        std::cout << "[Serial] Begin at " << baud << " baud" << std::endl;
    }
    
    void println(const char* str) {
        std::cout << str << std::endl;
    }
    
    void println(int value) {
        std::cout << value << std::endl;
    }
    
    void println(double value) {
        std::cout << value << std::endl;
    }
    
    void print(const char* str) {
        std::cout << str;
    }
    
    void print(int value) {
        std::cout << value;
    }
    
    void print(double value) {
        std::cout << value;
    }
    
    bool operator!() const { return false; }
};

#if !defined(ARDUINO)
MockSerial Serial;
#endif

// Mock delay
inline void delay(unsigned long ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// Mock millis
inline unsigned long millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

#endif // !defined(ARDUINO)

#endif // ARDUINO_MOCK_H
