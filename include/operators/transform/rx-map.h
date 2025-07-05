#ifndef RX_MAP_H
#define RX_MAP_H

#include <functional>
#include <memory>
#include <exception>
#include "../../core/core.h"

namespace rx {

// Map operator - transforms each emitted item by applying a function to it

template <typename Tsrc, typename Tdest>
class MapOperator : public Operator<Tdest>
{
    class MapObserver : public IObserver<Tsrc>
    {
    private:
        Operator<Tdest> *_operator;
        std::function<Tdest(const Tsrc &)> _transform;

    public:
        MapObserver(Operator<Tdest> *op, std::function<Tdest(const Tsrc &)> transform)
            : _operator(op), _transform(transform)
        {
        }

        void OnNext(const Tsrc &value) override
        {
            _operator->NotifyOnNext(_transform(value));
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

    std::shared_ptr<IObservable<Tsrc>> _observable;
    std::shared_ptr<MapObserver> _observer;

public:
    MapOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
        : _observable(observable)
    {
        _observer = std::make_shared<MapObserver>(this, transform);
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

template <typename Tsrc, typename Tdest>
std::shared_ptr<MapOperator<Tsrc, Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
{
    return std::make_shared<MapOperator<Tsrc, Tdest>>(observable, transform);
}

} // namespace rx

#endif // RX_MAP_H
