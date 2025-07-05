#ifndef RX_SUBJECT_H
#define RX_SUBJECT_H

#include <memory>
#include <list>
#include <exception>
#include "../core/core.h"

namespace rx {

// Emits new items to its subscribers

template <typename T>
class Subject : public ISubject<T>
{
private:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;

public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.push_back(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override
    {
        for (auto observer : _childObservers)
            observer->OnNext(value);
    }

    void OnCompleted() override
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();
    }

    void OnError(const std::exception &e) override
    {
        for (auto observer : _childObservers)
            observer->OnError(e);
    }

    ~Subject() = default;
};

template <typename T>
std::shared_ptr<Subject<T>> CreateSubject()
{
    return std::make_shared<Subject<T>>();
}

} // namespace rx

#endif // RX_SUBJECT_H