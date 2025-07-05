#ifndef RX_CONCAT_H
#define RX_CONCAT_H

#include <functional>
#include <memory>
#include <exception>
#include <vector>
#include <cstddef>
#include "../../core/core.h"

namespace rx {

// Concatenates multiple observables into a single observable sequence

template <typename T>
class ConcatOperator : public Operator<T>
{
    class ConcatObserver : public IObserver<T>
    {
    private:
        ConcatOperator<T> *_parent;

    public:
        ConcatObserver(ConcatOperator<T> *parent)
            : _parent(parent)
        {
        }

        void OnNext(const T &value) override
        {
            _parent->NotifyOnNext(value);
        }

        void OnCompleted() override
        {
            _parent->OnObservableCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    std::vector<std::shared_ptr<IObservable<T>>> _observables;
    std::shared_ptr<ConcatObserver> _observer;
    size_t _currentIndex = 0;
    bool _isSubscribed = false;

public:
    ConcatOperator(const std::vector<std::shared_ptr<IObservable<T>>> &observables)
        : _observables(observables)
    {
        _observer = std::make_shared<ConcatObserver>(this);
    }

    void OnObservableCompleted()
    {
        if (_currentIndex < _observables.size())
        {
            _observables[_currentIndex]->UnSubscribe(_observer);
        }

        _currentIndex++;
        
        if (_currentIndex < _observables.size())
        {
            _observables[_currentIndex]->Subscribe(_observer);
        }
        else
        {
            this->NotifyOnCompleted();
        }
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1 && !_isSubscribed && !_observables.empty())
        {
            _isSubscribed = true;
            _currentIndex = 0;
            _observables[_currentIndex]->Subscribe(_observer);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty() && _isSubscribed)
        {
            _isSubscribed = false;
            if (_currentIndex < _observables.size())
            {
                _observables[_currentIndex]->UnSubscribe(_observer);
            }
        }
    }
};

template <typename T>
std::shared_ptr<ConcatOperator<T>> Concat(const std::vector<std::shared_ptr<IObservable<T>>> &observables)
{
    return std::make_shared<ConcatOperator<T>>(observables);
}

} // namespace rx

#endif // RX_CONCAT_H
