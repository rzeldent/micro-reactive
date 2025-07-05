#ifndef RX_BUFFERCOUNT_H
#define RX_BUFFERCOUNT_H

#include <functional>
#include <memory>
#include <exception>
#include <vector>
#include <cstddef>
#include "../../core/core.h"

namespace rx {

// Return an observable that emits connected, non-overlapping buffer, each containing at most count items from the source observable.
// If the skip parameter is set, return an observable that emits buffers every skip items containing at most count items from the source observable.

template <typename Tsrc, typename Tdest = std::vector<Tsrc>>
class BufferCountOperator : public Operator<Tdest>
{
    class BufferCountObserver : public IObserver<Tsrc>
    {
    private:
        Operator<Tdest> *_operator;
        Tdest _buffer;
        size_t _count;

    public:
        BufferCountObserver(Operator<Tdest> *op, size_t count)
            : _operator(op), _count(count)
        {
        }

        void OnNext(const Tsrc &value) override
        {
            _buffer.push_back(value);
            if (_buffer.size() == _count)
            {
                _operator->NotifyOnNext(_buffer);
                _buffer.clear();
            }
        }

        void OnCompleted() override
        {
            if (_buffer.size() > 0)
            {
                _operator->NotifyOnNext(_buffer);
                _buffer.clear();
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<Tsrc>> _observable;
    std::shared_ptr<BufferCountObserver> _observer;

public:
    BufferCountOperator(std::shared_ptr<IObservable<Tsrc>> observable, size_t count)
        : _observable(observable)
    {
        _observer = std::make_shared<BufferCountObserver>(this, count);
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

template <typename Tsrc, typename Tdest = std::vector<Tsrc>>
std::shared_ptr<BufferCountOperator<Tsrc, Tdest>> BufferCount(std::shared_ptr<IObservable<Tsrc>> observable, size_t count)
{
    return std::make_shared<BufferCountOperator<Tsrc, Tdest>>(observable, count);
}

} // namespace rx

#endif // RX_BUFFERCOUNT_H
