#ifndef RX_COMBINELATEST_H
#define RX_COMBINELATEST_H

#include <functional>
#include <memory>
#include <exception>
#include <tuple>
#include "../../core/core.h"

namespace rx {

// when an item is emitted by either of two Observables, combine the latest item emitted by each Observable via a specified function and emit items based on the results of this function

template <typename Tsrc1, typename Tsrc2, typename Tdest = std::tuple<Tsrc1, Tsrc2>>
class CombineLatestOperator : public Operator<Tdest>
{
    class CombineLatestObserver1 : public IObserver<Tsrc1>
    {
    private:
        CombineLatestOperator<Tsrc1, Tsrc2, Tdest> *_parent;

    public:
        CombineLatestObserver1(CombineLatestOperator<Tsrc1, Tsrc2, Tdest> *parent)
            : _parent(parent)
        {
        }

        void OnNext(const Tsrc1 &value) override
        {
            _parent->OnNext1(value);
        }

        void OnCompleted() override
        {
            _parent->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _parent->NotifyOnError(e);
        }
    };

    class CombineLatestObserver2 : public IObserver<Tsrc2>
    {
    private:
        CombineLatestOperator<Tsrc1, Tsrc2, Tdest> *_parent;

    public:
        CombineLatestObserver2(CombineLatestOperator<Tsrc1, Tsrc2, Tdest> *parent)
            : _parent(parent)
        {
        }

        void OnNext(const Tsrc2 &value) override
        {
            _parent->OnNext2(value);
        }

        void OnCompleted() override
        {
            _parent->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    std::shared_ptr<CombineLatestObserver1> _observer1;
    std::shared_ptr<CombineLatestObserver2> _observer2;

    Tsrc1 _value1;
    Tsrc2 _value2;

    bool _value1Seen = false;
    bool _value2Seen = false;

    std::shared_ptr<IObservable<Tsrc1>> _observable1;
    std::shared_ptr<IObservable<Tsrc2>> _observable2;

public:
    CombineLatestOperator(std::shared_ptr<IObservable<Tsrc1>> observable1, std::shared_ptr<IObservable<Tsrc2>> observable2)
        : _observable1(observable1), _observable2(observable2)
    {
        _observer1 = std::make_shared<CombineLatestObserver1>(this);
        _observer2 = std::make_shared<CombineLatestObserver2>(this);
    }

    void OnNext1(const Tsrc1 &value)
    {
        _value1 = value;
        _value1Seen = true;
        
        if (_value1Seen && _value2Seen)
        {
            this->NotifyOnNext(std::make_tuple(_value1, _value2));
        }
    }

    void OnNext2(const Tsrc2 &value)
    {
        _value2 = value;
        _value2Seen = true;
        
        if (_value1Seen && _value2Seen)
        {
            this->NotifyOnNext(std::make_tuple(_value1, _value2));
        }
    }

    void Subscribe(std::shared_ptr<IObserver<Tdest>> observer) override
    {
        Operator<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
        {
            _observable1->Subscribe(_observer1);
            _observable2->Subscribe(_observer2);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer) override
    {
        Operator<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
        {
            _observable1->UnSubscribe(_observer1);
            _observable2->UnSubscribe(_observer2);
        }
    }
};

template <typename Tsrc1, typename Tsrc2, typename Tdest = std::tuple<Tsrc1, Tsrc2>>
std::shared_ptr<CombineLatestOperator<Tsrc1, Tsrc2, Tdest>> CombineLatest(
    std::shared_ptr<IObservable<Tsrc1>> observable1, 
    std::shared_ptr<IObservable<Tsrc2>> observable2)
{
    return std::make_shared<CombineLatestOperator<Tsrc1, Tsrc2, Tdest>>(observable1, observable2);
}

} // namespace rx

#endif // RX_COMBINELATEST_H