#ifndef MICRO_REACTIVE_CORE_H
#define MICRO_REACTIVE_CORE_H

#include <memory>
#include <list>
#include <exception>

namespace rx {

// Core Observer Interface - Consumes values from an observable
template <typename T>
class IObserver {
public:
    virtual void OnNext(const T &value) = 0;
    virtual void OnCompleted() = 0;
    virtual void OnError(const std::exception &e) = 0;

protected:
    virtual ~IObserver() = default;
};

// Core Observable Interface - Source of values
template <typename T>
class IObservable {
public:
    virtual void Subscribe(std::shared_ptr<IObserver<T>> observer) = 0;
    virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) = 0;

protected:
    virtual ~IObservable() = default;
};

// Subject Interface - Both Observer and Observable
template <typename T>
class ISubject : public IObservable<T>, public IObserver<T> {
protected:
    virtual ~ISubject() = default;
};

// Base Operator Class - Provides common functionality for operators
template <typename T>
class Operator : public IObservable<T> {
public:
    std::list<std::shared_ptr<IObserver<T>>> child_observers_;

    void NotifyOnNext(const T &value) {
        for (auto observer : child_observers_)
            observer->OnNext(value);
    }

    void NotifyOnCompleted() {
        for (auto observer : child_observers_)
            observer->OnCompleted();
    }

    void NotifyOnError(const std::exception &e) {
        for (auto observer : child_observers_)
            observer->OnError(e);
    }

    virtual void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.push_back(observer);
    }

    virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        child_observers_.remove(observer);
    }
};

} // namespace rx

#endif // MICRO_REACTIVE_CORE_H
