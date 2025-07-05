#ifndef RX_ANY_H
#define RX_ANY_H

#include <functional>
#include <memory>
#include <exception>
#include "../../core/core.h"

namespace rx {

// Returns an Observable that emits true if any item emitted by the source Observable satisfies a specified condition, otherwise false. Emits false if the source Observable terminates without emitting any item

template <typename Tsrc, typename Tdest = bool>
class AnyOperator : public Operator<Tdest>
{
    class AnyObserver : public IObserver<Tsrc>
    {
    private:
        Operator<Tdest> *_operator;
        std::function<bool(const Tsrc &)> _predicate;
        bool _emitted;

    public:
        AnyObserver(Operator<Tdest> *op, std::function<bool(const Tsrc &)> predicate)
            : _operator(op), _predicate(predicate), _emitted(false)
        {
        }

        void OnNext(const Tsrc &value) override
        {
            if (!_emitted && _predicate(value))
            {
                _emitted = true;
                _operator->NotifyOnNext(true);
                _operator->NotifyOnCompleted();
            }
        }

        void OnCompleted() override
        {
            if (!_emitted)
            {
                _operator->NotifyOnNext(false);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<Tsrc>> _observable;
    std::shared_ptr<AnyObserver> _observer;

public:
    AnyOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<bool(const Tsrc &)> predicate)
        : _observable(observable)
    {
        _observer = std::make_shared<AnyObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<Tdest>> observer) override
    {
        Operator<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer) override
    {
        Operator<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename Tsrc, typename Tdest = bool>
std::shared_ptr<AnyOperator<Tsrc, Tdest>> Any(std::shared_ptr<IObservable<Tsrc>> observable, std::function<bool(const Tsrc &)> predicate)
{
    return std::make_shared<AnyOperator<Tsrc, Tdest>>(observable, predicate);
}

} // namespace rx

#endif // RX_ANY_H
