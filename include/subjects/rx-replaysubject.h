#ifndef RX_REPLAYSUBJECT_H
#define RX_REPLAYSUBJECT_H

#include <memory>
#include <list>
#include <exception>
#include <cstddef>
#include "../core/core.h"

namespace rx {

// Emits new items to its subscribers but also replays a specified subset of its previously emitted items to any new subscribers

template <typename T>
class ReplaySubject : public ISubject<T>
{
private:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    size_t _size;
    std::list<T> _values;

public:
    ReplaySubject(size_t size)
        : _size(size)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.push_back(observer);
        for (const auto &value : _values)
            observer->OnNext(value);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.remove(observer);
    }

    void OnNext(const T &value) override
    {
        _values.push_back(value);
        if (_values.size() > _size)
            _values.pop_front();

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

    ~ReplaySubject() = default;
};

template <typename T>
std::shared_ptr<ReplaySubject<T>> CreateReplaySubject(size_t size = 10)
{
    return std::make_shared<ReplaySubject<T>>(size);
}

} // namespace rx

#endif // RX_REPLAYSUBJECT_H