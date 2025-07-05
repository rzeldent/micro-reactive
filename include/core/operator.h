#pragma once

namespace rx 
{
    template <typename T>
    class Operator : public IObservable<T>
{
public:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;

    void NotifyOnNext(const T &value)
    {
        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void NotifyOnCompleted()
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void NotifyOnError(const std::exception &e)
    {
        for (auto observer : _childObservers)
            observer->OnError(e);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        _childObservers.push_back(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
        _childObservers.remove(observer);
    }
    };
}