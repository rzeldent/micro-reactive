// Source of values whose methods block until all values have been emitted. subscribe or use one of the operator methods that reduce the values emitted to a single value
#pragma once

namespace rx 
{
    template <typename T>
    class IObservable
    {
    public:
        virtual void Subscribe(std::shared_ptr<IObserver<T>> observer) = 0;
        virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) = 0;

     protected:
        virtual ~IObservable() = default;
    };
}