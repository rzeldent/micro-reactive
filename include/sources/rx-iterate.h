//  Returns an observable that sends each value in the collection
#pragma once
#include <vector>
#include <memory>

namespace rx {

template <typename T>
class IterateObservable : public IObservable<T>
{
public:
    explicit IterateObservable(std::vector<T> values)
        : _values(std::move(values))
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        for (const auto& value : _values)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
    }

    ~IterateObservable() = default;

private:
    std::vector<T> _values;
};

template <typename T>
std::shared_ptr<IterateObservable<T>> Iterate(std::vector<T> values)
{
    return std::make_shared<IterateObservable<T>>(std::move(values));
}

}
