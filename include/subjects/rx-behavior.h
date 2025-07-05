#ifndef RX_BEHAVIOR_H
#define RX_BEHAVIOR_H

#include <memory>
#include <list>
#include <exception>
#include "../core/core.h"

namespace rx {

//  Maintains the current value and emits it to any new subscribers, ensuring they receive the most recent data upon subscription

template <typename T>
class BehaviorSubject : public ISubject<T>
{
private:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    T _value;
    bool _hasValue;

public:
    BehaviorSubject(const T &value)
        : _value(value), _hasValue(true)
    {
    }

    BehaviorSubject()
        : _hasValue(false)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.push_back(observer);
        if (_hasValue)
            observer->OnNext(_value);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override
    {
        _value = value;
        _hasValue = true;
        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void OnCompleted() override
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override
    {
        for (auto observer : _childObservers)
            observer->OnError(e);
    }

    T GetValue() const
    {
        return _value;
    }

    bool HasValue() const
    {
        return _hasValue;
    }

    ~BehaviorSubject() = default;
};

template <typename T>
std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject(const T &value)
{
    return std::make_shared<BehaviorSubject<T>>(value);
}

template <typename T>
std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject()
{
    return std::make_shared<BehaviorSubject<T>>();
}

} // namespace rx

#endif // RX_BEHAVIOR_H