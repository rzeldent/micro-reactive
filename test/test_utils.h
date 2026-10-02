#pragma once

#include <unity.h>
#include <chrono>
#include <exception>
#include <thread>
#include "../include/core.h"

using namespace rx;

inline void testSleepForMilliseconds(unsigned int milliseconds)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}

// Simple test observer for basic functionality testing
template<typename T>
class SimpleTestObserver : public IObserver<T> {
private:
    T _lastValue;
    bool _hasValue = false;
    bool _completed = false;
    int _count = 0;

public:
    void OnNext(const T& value) override {
        _lastValue = value;
        _hasValue = true;
        _count++;
    }

    void OnCompleted() override {
        _completed = true;
    }

    void OnError(const std::exception& e) override {
        _completed = true;
    }

    T GetLastValue() const { return _lastValue; }
    bool HasValue() const { return _hasValue; }
    bool IsCompleted() const { return _completed; }
    int GetCount() const { return _count; }
    void Reset() { _hasValue = false; _completed = false; _count = 0; }
};
