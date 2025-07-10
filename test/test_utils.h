#pragma once

#include <Arduino.h>
#include <unity.h>

// Forward declarations to avoid multiple definition issues
namespace rx {
    template<typename T>
    class IObserver;
}

using namespace rx;

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
