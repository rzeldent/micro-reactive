#ifndef RX_SKIP_H
#define RX_SKIP_H

#include <functional>
#include <memory>
#include <exception>
#include <cstddef>
#include "../../core/core.h"

namespace rx {

// Skip operator - skips the first n items

template <typename T>
class SkipOperator : public Operator<T>
{
    class SkipObserver : public IObserver<T>
    {
    private:
        Operator<T> *_operator;
        size_t _count;
        size_t _skipped = 0;

    public:
        SkipObserver(Operator<T> *op, size_t count)
            : _operator(op), _count(count)
        {
        }

        void OnNext(const T &value) override
        {
            if (_skipped < _count)
            {
                _skipped++;
            }
            else
            {
                _operator->NotifyOnNext(value);
            }
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
    std::shared_ptr<SkipObserver> _observer;

public:
    SkipOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : _observable(observable)
    {
        _observer = std::make_shared<SkipObserver>(this, count);
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
std::shared_ptr<SkipOperator<T>> Skip(std::shared_ptr<IObservable<T>> observable, size_t count)
{
    return std::make_shared<SkipOperator<T>>(observable, count);
}

} // namespace rx

#endif // RX_SKIP_H
