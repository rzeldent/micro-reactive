//  Returns an observable that sends no items to observer and immediately completes
#pragma once

namespace rx {

template <typename T>
class EmptyObservable : public IObservable<T>
{
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
    }

    ~EmptyObservable() = default;
};

template <typename T>
std::shared_ptr<EmptyObservable<T>> Empty()
{
    return std::make_shared<EmptyObservable<T>>();
}

}
