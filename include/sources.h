#ifndef MICRO_REACTIVE_SOURCES_H
#define MICRO_REACTIVE_SOURCES_H

#include "core.h"
#include <functional>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>

namespace rx {

// =============================================================================
// EMPTY OBSERVABLE - Returns an observable that sends no items and completes
// =============================================================================
template <typename T>
class EmptyObservable : public IObservable<T> {
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~EmptyObservable() = default;
};

template <typename T>
std::shared_ptr<EmptyObservable<T>> Empty() {
    return std::make_shared<EmptyObservable<T>>();
}

// =============================================================================
// NEVER OBSERVABLE - Returns an observable that never emits anything
// =============================================================================
template <typename T>
class NeverObservable : public IObservable<T> {
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        // Never emits anything, never completes
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~NeverObservable() = default;
};

template <typename T>
std::shared_ptr<NeverObservable<T>> Never() {
    return std::make_shared<NeverObservable<T>>();
}

// =============================================================================
// RANGE OBSERVABLE - Returns an observable that sends values in a range
// =============================================================================
template <typename T>
class RangeObservable : public IObservable<T> {
private:
    T _first, _last, _step;

public:
    RangeObservable(T first, T last, T step)
        : _first(first), _last(last), _step(step) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~RangeObservable() = default;
};

template <typename T>
std::shared_ptr<RangeObservable<T>> Range(T first, T last, T step) {
    return std::make_shared<RangeObservable<T>>(first, last, step);
}

// Overload for Range with default step of 1
template <typename T>
std::shared_ptr<RangeObservable<T>> Range(T first, T count) {
    return std::make_shared<RangeObservable<T>>(first, first + count - 1, T(1));
}

// =============================================================================
// FROMVECTOR OBSERVABLE - Creates an observable from a vector
// =============================================================================
template <typename T>
class FromVectorObservable : public IObservable<T> {
private:
    std::vector<T> _values;

public:
    FromVectorObservable(const std::vector<T>& values) : _values(values) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (const auto& value : _values) {
            observer->OnNext(value);
        }
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~FromVectorObservable() = default;
};

template <typename T>
std::shared_ptr<FromVectorObservable<T>> FromVector(const std::vector<T>& values) {
    return std::make_shared<FromVectorObservable<T>>(values);
}

// =============================================================================
// CREATE OBSERVABLE - Creates an observable from a function
// =============================================================================
template <typename T>
class CreateObservable : public IObservable<T> {
private:
    std::function<void(std::shared_ptr<IObserver<T>>)> _create;

public:
    CreateObservable(std::function<void(std::shared_ptr<IObserver<T>>)> create)
        : _create(create) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        try {
            _create(observer);
        } catch (const std::exception& e) {
            observer->OnError(e);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~CreateObservable() = default;
};

template <typename T>
std::shared_ptr<CreateObservable<T>> Create(std::function<void(std::shared_ptr<IObserver<T>>)> create) {
    return std::make_shared<CreateObservable<T>>(create);
}

// =============================================================================
// ITERATE OBSERVABLE - Creates an observable from a container
// =============================================================================
template <typename T>
class IterateObservable : public IObservable<T> {
private:
    std::vector<T> _values;

public:
    template <typename Container>
    IterateObservable(const Container& container) 
        : _values(container.begin(), container.end()) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (const auto& value : _values) {
            observer->OnNext(value);
        }
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~IterateObservable() = default;
};

template <typename T, typename Container>
std::shared_ptr<IterateObservable<T>> Iterate(const Container& container) {
    return std::make_shared<IterateObservable<T>>(container);
}

// =============================================================================
// DEFER OBSERVABLE - Defers observable creation until subscription
// =============================================================================
template <typename T>
class DeferObservable : public IObservable<T> {
private:
    std::function<std::shared_ptr<IObservable<T>>()> _factory;

public:
    DeferObservable(std::function<std::shared_ptr<IObservable<T>>()> factory)
        : _factory(factory) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        try {
            auto observable = _factory();
            observable->Subscribe(observer);
        } catch (const std::exception& e) {
            observer->OnError(e);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~DeferObservable() = default;
};

template <typename T>
std::shared_ptr<DeferObservable<T>> Defer(std::function<std::shared_ptr<IObservable<T>>()> factory) {
    return std::make_shared<DeferObservable<T>>(factory);
}

// =============================================================================
// TIMER OBSERVABLE - Desktop/testing compatible version
// =============================================================================
template <typename T = int>
class TimerObservable : public IObservable<T> {
private:
    std::chrono::milliseconds _delay;

public:
    TimerObservable(std::chrono::milliseconds delay) : _delay(delay) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        std::this_thread::sleep_for(_delay);
        observer->OnNext(T{});
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~TimerObservable() = default;
};

template <typename T = int>
std::shared_ptr<TimerObservable<T>> Timer(std::chrono::milliseconds delay) {
    return std::make_shared<TimerObservable<T>>(delay);
}

// =============================================================================
// INTERVAL OBSERVABLE - Desktop/testing compatible version
// =============================================================================
template <typename T = int>
class IntervalObservable : public IObservable<T> {
private:
    std::chrono::milliseconds _interval;
    int _count;

public:
    IntervalObservable(std::chrono::milliseconds interval, int count = 5) 
        : _interval(interval), _count(count) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (int i = 0; i < _count; ++i) {
            std::this_thread::sleep_for(_interval);
            observer->OnNext(T(i));
        }
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
    }

    ~IntervalObservable() = default;
};

template <typename T = int>
std::shared_ptr<IntervalObservable<T>> Interval(std::chrono::milliseconds interval, int count = 5) {
    return std::make_shared<IntervalObservable<T>>(interval, count);
}

} // namespace rx

#endif // MICRO_REACTIVE_SOURCES_H
