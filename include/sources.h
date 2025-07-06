#ifndef MICRO_REACTIVE_SOURCES_H
#define MICRO_REACTIVE_SOURCES_H

#include "core.h"
#include <functional>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>
#include <algorithm>

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
    T first_, last_, step_;

public:
    RangeObservable(T first, T last, T step)
        : first_(first), last_(last), step_(step) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (auto value = first_; value <= last_; value += step_)
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
    std::vector<T> values_;

public:
    FromVectorObservable(const std::vector<T>& values) : values_(values) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (const auto& value : values_) {
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
    std::function<void(std::shared_ptr<IObserver<T>>)> create_;

public:
    CreateObservable(std::function<void(std::shared_ptr<IObserver<T>>)> create)
        : create_(create) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        try {
            create_(observer);
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
    std::vector<T> values_;

public:
    template <typename Container>
    IterateObservable(const Container& container) 
        : values_(container.begin(), container.end()) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        for (const auto& value : values_) {
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
    std::function<std::shared_ptr<IObservable<T>>()> factory_;

public:
    DeferObservable(std::function<std::shared_ptr<IObservable<T>>()> factory)
        : factory_(factory) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        try {
            auto observable = factory_();
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
// TIMER OBSERVABLE - Threaded version for non-blocking operation
// =============================================================================
template <typename T = int>
class TimerObservable : public IObservable<T> {
private:
    std::chrono::milliseconds delay_;
    std::vector<std::shared_ptr<IObserver<T>>> observers_;
    std::thread timer_thread_;
    bool is_running_;

public:
    TimerObservable(std::chrono::milliseconds delay) 
        : delay_(delay), is_running_(false) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        observers_.push_back(observer);
        
        if (!is_running_) {
            is_running_ = true;
            timer_thread_ = std::thread([this]() {
                std::this_thread::sleep_for(delay_);
                
                // Notify all observers
                for (auto& obs : observers_) {
                    if (obs) {
                        obs->OnNext(T{});
                        obs->OnCompleted();
                    }
                }
                is_running_ = false;
            });
            timer_thread_.detach(); // Detach to avoid blocking destructor
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto it = std::find(observers_.begin(), observers_.end(), observer);
        if (it != observers_.end()) {
            observers_.erase(it);
        }
    }

    ~TimerObservable() = default;
};

template <typename T = int>
std::shared_ptr<TimerObservable<T>> Timer(std::chrono::milliseconds delay) {
    return std::make_shared<TimerObservable<T>>(delay);
}

// =============================================================================
// INTERVAL OBSERVABLE - Threaded version for non-blocking operation
// =============================================================================
template <typename T = int>
class IntervalObservable : public IObservable<T> {
private:
    std::chrono::milliseconds interval_;
    int count_;
    std::vector<std::shared_ptr<IObserver<T>>> observers_;
    std::thread interval_thread_;
    bool is_running_;

public:
    IntervalObservable(std::chrono::milliseconds interval, int count = 5) 
        : interval_(interval), count_(count), is_running_(false) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        observers_.push_back(observer);
        
        if (!is_running_) {
            is_running_ = true;
            interval_thread_ = std::thread([this]() {
                for (int i = 0; i < count_; ++i) {
                    std::this_thread::sleep_for(interval_);
                    
                    // Notify all observers
                    for (auto& obs : observers_) {
                        if (obs) {
                            obs->OnNext(T(i));
                        }
                    }
                }
                
                // Complete all observers
                for (auto& obs : observers_) {
                    if (obs) {
                        obs->OnCompleted();
                    }
                }
                is_running_ = false;
            });
            interval_thread_.detach(); // Detach to avoid blocking destructor
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto it = std::find(observers_.begin(), observers_.end(), observer);
        if (it != observers_.end()) {
            observers_.erase(it);
        }
    }

    ~IntervalObservable() = default;
};

template <typename T = int>
std::shared_ptr<IntervalObservable<T>> Interval(std::chrono::milliseconds interval, int count = 5) {
    return std::make_shared<IntervalObservable<T>>(interval, count);
}

} // namespace rx

#endif // MICRO_REACTIVE_SOURCES_H
