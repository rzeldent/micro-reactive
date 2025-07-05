#ifndef RX_FILTER_H
#define RX_FILTER_H

#include <functional>
#include <memory>
#include <exception>
#include "../../core/core.h"

namespace rx {

// Filter operator - only emits items that pass a predicate test

template <typename T>
class FilterOperator : public Operator<T>
{
    class FilterObserver : public IObserver<T>
    {
    private:
        Operator<T> *_operator;
        std::function<bool(const T &)> _predicate;

    public:
        FilterObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : _operator(op), _predicate(predicate)
        {
        }

        void OnNext(const T &value) override
        {
            if (_predicate(value))
                _operator->NotifyOnNext(value);
        }

        void OnCompleted() override
        {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<FilterObserver> _observer;

public:
    FilterOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : _observable(observable)
    {
        _observer = std::make_shared<FilterObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<FilterOperator<T>> Filter(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
{
    return std::make_shared<FilterOperator<T>>(observable, predicate);
}

} // namespace rx

#endif // RX_FILTER_H
