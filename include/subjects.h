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
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;

public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.push_back(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override {
        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : _childObservers)
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
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    T _value;
    bool _hasValue;

public:
    BehaviorSubject(const T &value) : _value(value), _hasValue(true) {
    }

    BehaviorSubject() : _hasValue(false) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.push_back(observer);
        if (_hasValue)
            observer->OnNext(_value);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override {
        _value = value;
        _hasValue = true;
        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : _childObservers)
            observer->OnError(e);
    }

    T GetValue() const {
        return _value;
    }

    bool HasValue() const {
        return _hasValue;
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
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    size_t _size;
    std::list<T> _values;

public:
    ReplaySubject(size_t size) : _size(size) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.push_back(observer);
        for (const auto &value : _values)
            observer->OnNext(value);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override {
        _values.push_back(value);
        if (_values.size() > _size)
            _values.pop_front();

        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void OnCompleted() override {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        for (auto observer : _childObservers)
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
    std::shared_ptr<Subject<T>> _subject;
    std::mutex _mutex;

public:
    SynchronizedSubject() : _subject(CreateSubject<T>()) {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->Subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->UnSubscribe(observer);
    }

    void OnNext(const T &value) override {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnNext(value);
    }

    void OnCompleted() override {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnCompleted();
    }

    void OnError(const std::exception &e) override {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnError(e);
    }

    ~SynchronizedSubject() = default;
};

template <typename T>
std::shared_ptr<SynchronizedSubject<T>> CreateSynchronizedSubject() {
    return std::make_shared<SynchronizedSubject<T>>();
}

} // namespace rx

#endif // MICRO_REACTIVE_SUBJECTS_H
