#ifndef MICRO_REACTIVE_SUBJECTS_H
#define MICRO_REACTIVE_SUBJECTS_H

#include "core.h"
#include <memory>
#include <list>
#include <mutex>
#include <cstddef>

namespace rx {

// =============================================================================
// SUBJECT - Basic subject for multicasting
// =============================================================================
template <typename T>
class Subject : public ISubject<T> {
private:
    std::list<std::shared_ptr<IObserver<T>>> child_observers_;

public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.push_back(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.remove(observer);
    }

    void OnNext(const T &value) override {
        for (auto observer : child_observers_)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : child_observers_)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : child_observers_)
            observer->OnError(e);
    }

    // Convert to IObservable for use with operators
    std::shared_ptr<IObservable<T>> AsObservable() {
        return std::static_pointer_cast<IObservable<T>>(std::shared_ptr<Subject<T>>(this, [](Subject<T>*){}));
    }

    ~Subject() = default;
};

template <typename T>
std::shared_ptr<Subject<T>> CreateSubject() {
    return std::make_shared<Subject<T>>();
}

// =============================================================================
// BEHAVIOR SUBJECT - Maintains current value and emits to new subscribers
// =============================================================================
template <typename T>
class BehaviorSubject : public ISubject<T> {
private:
    std::list<std::shared_ptr<IObserver<T>>> child_observers_;
    T value_;
    bool has_value_;

public:
    BehaviorSubject(const T &value) : value_(value), has_value_(true) {
    }

    BehaviorSubject() : has_value_(false) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.push_back(observer);
        if (has_value_)
            observer->OnNext(value_);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.remove(observer);
    }

    void OnNext(const T &value) override {
        value_ = value;
        has_value_ = true;
        for (auto observer : child_observers_)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : child_observers_)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : child_observers_)
            observer->OnError(e);
    }

    T GetValue() const {
        return value_;
    }

    bool HasValue() const {
        return has_value_;
    }

    // Convert to IObservable for use with operators
    std::shared_ptr<IObservable<T>> AsObservable() {
        return std::static_pointer_cast<IObservable<T>>(std::shared_ptr<BehaviorSubject<T>>(this, [](BehaviorSubject<T>*){}));
    }

    ~BehaviorSubject() = default;
};

template <typename T>
std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject(const T &value) {
    return std::make_shared<BehaviorSubject<T>>(value);
}

template <typename T>
std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject() {
    return std::make_shared<BehaviorSubject<T>>();
}

// =============================================================================
// REPLAY SUBJECT - Replays a subset of previously emitted items to new subscribers
// =============================================================================
template <typename T>
class ReplaySubject : public ISubject<T> {
private:
    std::list<std::shared_ptr<IObserver<T>>> child_observers_;
    size_t size_;
    std::list<T> values_;

public:
    ReplaySubject(size_t size) : size_(size) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.push_back(observer);
        for (const auto &value : values_)
            observer->OnNext(value);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.remove(observer);
    }

    void OnNext(const T &value) override {
        values_.push_back(value);
        if (values_.size() > size_)
            values_.pop_front();

        for (auto observer : child_observers_)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : child_observers_)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : child_observers_)
            observer->OnError(e);
    }

    ~ReplaySubject() = default;
};

template <typename T>
std::shared_ptr<ReplaySubject<T>> CreateReplaySubject(size_t size = 10) {
    return std::make_shared<ReplaySubject<T>>(size);
}

// =============================================================================
// SYNCHRONIZED SUBJECT - Thread-safe subject
// =============================================================================
template <typename T>
class SynchronizedSubject : public ISubject<T> {
private:
    std::shared_ptr<Subject<T>> subject_;
    std::mutex mutex_;

public:
    SynchronizedSubject() : subject_(CreateSubject<T>()) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        std::lock_guard<std::mutex> lock(mutex_);
        subject_->Subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        std::lock_guard<std::mutex> lock(mutex_);
        subject_->UnSubscribe(observer);
    }

    void OnNext(const T &value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        subject_->OnNext(value);
    }

    void OnCompleted() override {
        std::lock_guard<std::mutex> lock(mutex_);
        subject_->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        std::lock_guard<std::mutex> lock(mutex_);
        subject_->OnError(e);
    }

    ~SynchronizedSubject() = default;
};

template <typename T>
std::shared_ptr<SynchronizedSubject<T>> CreateSynchronizedSubject() {
    return std::make_shared<SynchronizedSubject<T>>();
}

} // namespace rx

#endif // MICRO_REACTIVE_SUBJECTS_H
