#ifndef RX_TAKE_H
#define RX_TAKE_H

#include <functional>
#include <memory>
#include <exception>
#include <cstddef>
#include "../../core/core.h"

namespace rx {

// Take operator - emits only the first n items

template <typename T>
class TakeOperator : public Operator<T>
{
    class TakeObserver : public IObserver<T>
    {
    private:
        Operator<T> *_operator;
        size_t _count;
        size_t _taken = 0;

    public:
        TakeObserver(Operator<T> *op, size_t count)
            : _operator(op), _count(count)
        {
        }

        void OnNext(const T &value) override
        {
            if (_taken < _count)
            {
                _operator->NotifyOnNext(value);
                _taken++;
                if (_taken == _count)
                    _operator->NotifyOnCompleted();
            }
        }

        void OnCompleted() override
        {
            if (_taken < _count)
                _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<TakeObserver> _observer;

public:
    TakeOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : _observable(observable)
    {
        _observer = std::make_shared<TakeObserver>(this, count);
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
std::shared_ptr<TakeOperator<T>> Take(std::shared_ptr<IObservable<T>> observable, size_t count)
{
    return std::make_shared<TakeOperator<T>>(observable, count);
}

} // namespace rx

#endif // RX_TAKE_H
