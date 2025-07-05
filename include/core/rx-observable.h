#pragma once

#include <functional>
#include <memory>

namespace rx 
{
    template <typename T>
    class Observable : public IObservable<T>
{
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        // Default implementation does nothing
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        // Default implementation does nothing
    }

    virtual ~Observable() = default;
};

// Helper function to create observable from a function
template <typename T>
class FunctionObservable : public IObservable<T>
{
private:
    std::function<void(std::shared_ptr<IObserver<T>>)> _subscribe;

public:
    FunctionObservable(std::function<void(std::shared_ptr<IObserver<T>>)> subscribe)
        : _subscribe(subscribe)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        if (_subscribe)
            _subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        // Default implementation does nothing
    }

    virtual ~FunctionObservable() = default;
};

template <typename T>
std::shared_ptr<FunctionObservable<T>> CreateFunctionObservable(std::function<void(std::shared_ptr<IObserver<T>>)> subscribe)
{
    return std::make_shared<FunctionObservable<T>>(subscribe);
}
}