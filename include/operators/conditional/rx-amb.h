#ifndef RX_AMB_H
#define RX_AMB_H

#include <functional>
#include <memory>
#include <exception>
#include <vector>
#include "../../core/core.h"

namespace rx {

// For each item from only the first of the given observables deliver from the new observable that is returned

template <typename T>
class AmbOperator : public Operator<T>
{
    class AmbObserver : public IObserver<T>
    {
    private:
        AmbOperator<T> *_operator;
        int _index;

    public:
        AmbObserver(AmbOperator<T> *op, int index)
            : _operator(op), _index(index)
        {
        }

        void OnNext(const T &value) override
        {
            if (_operator->SetActiveObserver(_index))
            {
                _operator->NotifyOnNext(value);
            }
        }

        void OnCompleted() override
        {
            if (_operator->IsActiveObserver(_index))
                _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            if (_operator->IsActiveObserver(_index))
                _operator->NotifyOnError(e);
        }
    };

private:
    std::vector<std::shared_ptr<AmbObserver>> _observers;
    std::vector<std::shared_ptr<IObservable<T>>> _observables;
    int _activeObserverIndex = -1;

public:
    AmbOperator(const std::vector<std::shared_ptr<IObservable<T>>> &observables)
        : _observables(observables)
    {
        for (size_t i = 0; i < _observables.size(); ++i)
        {
            _observers.push_back(std::make_shared<AmbObserver>(this, static_cast<int>(i)));
        }
    }

    bool SetActiveObserver(int index)
    {
        if (_activeObserverIndex == -1)
        {
            _activeObserverIndex = index;
            return true;
        }
        return _activeObserverIndex == index;
    }

    bool IsActiveObserver(int index)
    {
        return _activeObserverIndex == index;
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->Subscribe(_observers[i]);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->UnSubscribe(_observers[i]);
        }
    }
};

template <typename T>
std::shared_ptr<AmbOperator<T>> Amb(const std::vector<std::shared_ptr<IObservable<T>>> &observables)
{
    return std::make_shared<AmbOperator<T>>(observables);
}

} // namespace rx

#endif // RX_AMB_H
