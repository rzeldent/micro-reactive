// Returns an observable that never sends any items or notifications to observer
#pragma once

namespace rx {

template <typename T>
class NeverObservable : public IObservable<T>
{
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
    }

    ~NeverObservable() = default;
};

template <typename T>
std::shared_ptr<NeverObservable<T>> Never()
{
    return std::make_shared<NeverObservable<T>>();
}

}